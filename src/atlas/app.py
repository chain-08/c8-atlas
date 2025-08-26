from __future__ import annotations

import asyncio
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import settings
from .digest import build_slack_blocks
from .integrations.airflow import AirflowClient
from .integrations.slack import SlackClient
from .metrics import (
    ATLAS_INCIDENTS,
    ATLAS_FAILED_DAGS,
    ATLAS_LATE_DAGS,
    ATLAS_POLL_LATENCY,
)
from .models import DagSummary, DigestReport


app = FastAPI(title="Atlas Observer (Milestone A)")

# ---- singletons (reuse across loops) ----------------------------------------
_air = AirflowClient()   # uses env: AIRFLOW_BASE_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD
_slack = SlackClient()   # uses env: SLACK_BOT_TOKEN
IST = ZoneInfo("Asia/Kolkata")


@app.get("/healthz")
def healthz():
    return {"ok": True, "env": settings.env}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def poll_airflow_loop() -> None:
    """
    Poll Airflow for DAG failures and update Prometheus gauges.
    Includes paused DAGs (only_active=False) so test DAGs are counted.
    """
    env = settings.env
    poll_interval = settings.poll_interval_sec

    while True:
        t0 = time.time()
        try:
            # include paused DAGs too
            dags = _air.list_dags(only_active=False)

            failed_count = 0
            late_count = 0  # placeholder for future SLA/latency checks

            for d in dags:
                dag_id = d.get("dag_id")
                if not dag_id:
                    continue

                failed_runs = _air.list_dag_runs(dag_id, state="failed", limit=5)
                if failed_runs:
                    failed_count += 1

            ATLAS_FAILED_DAGS.labels(env=env).set(failed_count)
            ATLAS_LATE_DAGS.labels(env=env).set(late_count)

        except Exception as e:
            ATLAS_INCIDENTS.labels(env, "poll_error").inc()
            print("[poll] error:", repr(e))
        finally:
            ATLAS_POLL_LATENCY.observe((time.time() - t0) * 1000.0)

        await asyncio.sleep(poll_interval)


async def daily_digest_loop() -> None:
    """
    Build a daily digest at the configured IST cron and post to Slack.
    Uses Airflow logs to pull a short error snippet for top offenders.
    """
    env = settings.env
    cron = settings.digest_cron_ist

    # compute the next fire time in IST
    now_ist = datetime.now(IST).replace(second=0, microsecond=0)
    itr = croniter(cron, now_ist)
    next_fire = itr.get_next(datetime)

    while True:
        try:
            now = datetime.now(IST)
            if now >= next_fire:
                # include paused DAGs so tests/dev DAGs show up
                dags = _air.list_dags(only_active=False)

                summaries: list[DagSummary] = []
                total_failed = 0
                total_late = 0  # reserved for future late-run logic

                for d in dags:
                    dag_id = d.get("dag_id")
                    if not dag_id:
                        continue

                    failed_runs = _air.list_dag_runs(dag_id, state="failed", limit=5)
                    fr_count = len(failed_runs)
                    if fr_count > 0:
                        total_failed += 1

                    last_error: str | None = None
                    if failed_runs:
                        run_id = failed_runs[0].get("dag_run_id")
                        # naive guesses of task ids to fetch a small error snippet
                        for guess in ("main", "extract", "transform", "load"):
                            try:
                                log_text = _air.get_task_log(dag_id, run_id, guess, 1)
                                if log_text:
                                    last_error = log_text[-500:]  # last 500 chars
                                    break
                            except Exception:
                                # best-effort only; continue trying other guesses
                                continue

                    summaries.append(
                        DagSummary(
                            dag_id=dag_id,
                            failed_runs=fr_count,
                            late_runs=0,
                            last_error_snippet=last_error,
                        )
                    )

                report = DigestReport(
                    total_failed=total_failed,
                    total_late=total_late,
                    top_offenders=sorted(
                        summaries, key=lambda s: s.failed_runs, reverse=True
                    )[:10],
                )
                blocks = build_slack_blocks(report)
                await _slack.post_message(
                    settings.slack_channel_digest, "Atlas daily digest", blocks
                )

                # schedule the next fire time
                itr = croniter(cron, now.replace(second=0, microsecond=0))
                next_fire = itr.get_next(datetime)

        except Exception as e:
            ATLAS_INCIDENTS.labels(env, "digest_error").inc()
            print("[digest] error:", repr(e))
            # even on error, advance the iterator so we don't spin the loop endlessly
            now = datetime.now(IST)
            itr = croniter(cron, now.replace(second=0, microsecond=0))
            next_fire = itr.get_next(datetime)

        await asyncio.sleep(5)


@app.on_event("startup")
async def on_startup() -> None:
    # kick off background tasks
    asyncio.create_task(poll_airflow_loop())
    asyncio.create_task(daily_digest_loop())
