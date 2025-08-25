# Atlas — Milestone A (Observer)

This service polls Airflow, exposes Prometheus metrics at `/metrics`, and posts a daily Slack digest at **09:15 IST**.

## Quickstart (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env
# edit AIRFLOW_* and Slack vars
uvicorn atlas.app:app --host 0.0.0.0 --port 8087
