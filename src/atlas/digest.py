from __future__ import annotations
from typing import List
from .models import DagSummary, DigestReport

def build_slack_blocks(report: DigestReport) -> list[dict]:
    header = {"type":"header","text":{"type":"plain_text","text":"Atlas Daily Digest"}}
    summary = {"type":"section","text":{"type":"mrkdwn","text":f"*Failed DAGs:* {report.total_failed}   |   *Late DAGs:* {report.total_late}"}}
    divider = {"type":"divider"}
    blocks = [header, summary, divider]
    for ds in report.top_offenders[:10]:
        txt = f"*{ds.dag_id}* — failed_runs: {ds.failed_runs}, late_runs: {ds.late_runs}"
        if ds.last_error_snippet:
            snippet = ds.last_error_snippet.strip().splitlines()[-1][:180]
            txt += f"\n```{snippet}```"
        blocks.append({"type":"section","text":{"type":"mrkdwn","text":txt}})
    return blocks
