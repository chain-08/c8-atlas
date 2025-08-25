from __future__ import annotations
import asyncio, time
from datetime import datetime, timezone
from croniter import croniter
from fastapi import FastAPI, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from .config import settings
# These integrations are simple clients you'll add next:
# from .integrations.airflow import AirflowClient
# from .integrations.slack import SlackClient
from .metrics import ATLAS_INCIDENTS, ATLAS_FAILED_DAGS, ATLAS_LATE_DAGS, ATLAS_POLL_LATENCY
from .models import DagSummary, DigestReport
from .digest import build_slack_blocks

app = FastAPI(title="Atlas Observer (Milestone A)")

@app.get("/healthz")
def healthz():
    return {"ok": True, "env": settings.env}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# --- Temporary local stubs so the service runs even before integrations are added ---
class _AirflowStub:
    def list_dags(self, only_active=True):
        return []  # return empty for now

    def list_dag_runs(self, dag_id, state=None, limit=5):
        return []

    def get_task_log(self, dag_id, run_id, guess, try_number=1):
        return ""

class _SlackStub:
    async def post_message(self, channel: str, text: str, blocks=None):
        print(f"[SLACK STUB] {channel}: {text}")
        if blocks:
            print(blocks)
# ----------------------------------------------------------------------------

async def poll_airflow_loop():
    # replace with AirflowClient() when you add integrations
    air = _AirflowStub()
    env = settings.env
    while True:
        t0 = time.time()
        try:
            dags = air.list_dags(only_active=True)
            failed_count = 0
            late_count = 0
            for d in dags:
                dag_id = d.get("dag_id")
                failed = air.list_dag_runs(dag_id, state="failed", limit=5)
                if failed:
                    failed_count += 1
            ATLAS_FAILED_DAGS.labels(env=env).set(failed_count)
            ATLAS_LATE_DAGS.labels(env=env).set(late_count)
        except Exception as e:
            ATLAS_INCIDENTS.labels(env, "poll_error").inc()
            print("[poll] error:", e)
        finally:
            ATLAS_POLL_LATENCY.observe((time.time()-t0)*1000.0)
        await asyncio.sleep(settings.poll_interval_sec)

async def daily_digest_loop():
    # replace with SlackClient() & AirflowClient() when you add integrations
    slack = _SlackStub()
    air = _AirflowStub()
    env = settings.env
    from zoneinfo import ZoneInfo
    ist = ZoneInfo("Asia/Kolkata")
    cron = settings.digest_cron_ist
    now_ist = datetime.now(ist).replace(second=0, microsecond=0)
    itr = croniter(cron, now_ist)
    next_fire = itr.get_next(datetime)

    while True:
        now = datetime.now(ist)
        if now >= next_fire:
            try:
                dags = air.list_dags(only_active=True)
                summaries = []
                total_failed = 0
                total_late = 0
                for d in dags:
                    dag_id = d.get("dag_id")
                    failed_runs = air.list_dag_runs(dag_id, state="failed", limit=5)
                    fr_count = len(failed_runs)
                    total_failed += 1 if fr_count>0 else 0
                    last_error = None
                    if failed_runs:
                        run_id = failed_runs[0].get("dag_run_id")
                        for guess in ["main","extract","transform","load"]:
                            try:
                                last_error = air.get_task_log(dag_id, run_id, guess, 1)
                                if last_error:
                                    break
                            except Exception:
                                continue
                    summaries.append(DagSummary(
                        dag_id=dag_id, failed_runs=fr_count, late_runs=0,
                        last_error_snippet=(last_error or "")[-500:] or None
                    ))
                report = DigestReport(
                    total_failed=total_failed, total_late=total_late,
                    top_offenders=sorted(summaries, key=lambda s: s.failed_runs, reverse=True)[:10]
                )
                blocks = build_slack_blocks(report)
                await slack.post_message(settings.slack_channel_digest, text="Atlas daily digest", blocks=blocks)
            except Exception as e:
                ATLAS_INCIDENTS.labels(env, "digest_error").inc()
                print("[digest] error:", e)
            finally:
                itr = croniter(cron, now.replace(second=0, microsecond=0))
                next_fire = itr.get_next(datetime)
        await asyncio.sleep(5)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(poll_airflow_loop())
    asyncio.create_task(daily_digest_loop())
