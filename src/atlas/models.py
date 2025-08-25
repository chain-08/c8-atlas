from pydantic import BaseModel
from typing import Optional, List

class DagSummary(BaseModel):
    dag_id: str
    failed_runs: int
    late_runs: int
    longest_task: Optional[str] = None
    last_error_snippet: Optional[str] = None

class DigestReport(BaseModel):
    total_failed: int
    total_late: int
    top_offenders: List[DagSummary]
