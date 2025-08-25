from __future__ import annotations
from typing import Any, Dict, List, Optional
import requests
from ..config import settings

class AirflowClient:
    def __init__(self, base_url: Optional[str]=None, username: Optional[str]=None, password: Optional[str]=None):
        self.base = base_url or settings.airflow_base_url
        self.session = requests.Session()
        self.session.auth = (username or settings.airflow_username, password or settings.airflow_password)

    def list_dags(self, only_active: bool=True, limit:int=200) -> List[Dict[str,Any]]:
        r = self.session.get(
            f"{self.base}/api/v1/dags",
            params={"only_active": only_active, "limit": limit},
            timeout=30
        )
        r.raise_for_status()
        return r.json().get("dags", [])

    def list_dag_runs(self, dag_id: str, state: Optional[str]=None, order_by:str="-start_date", limit:int=50) -> List[Dict[str,Any]]:
        params = {"limit": limit, "order_by": order_by}
        if state:
            params["state"] = state
        r = self.session.get(f"{self.base}/api/v1/dags/{dag_id}/dagRuns", params=params, timeout=30)
        r.raise_for_status()
        return r.json().get("dag_runs", [])

    def get_task_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number:int=1) -> str:
        url = f"{self.base}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        r = self.session.get(url, timeout=60)
        r.raise_for_status()
        if r.headers.get("Content-Type", "").startswith("application/json"):
            return r.json().get("content", "")
        return r.text
