from pydantic import BaseModel
import os

class Settings(BaseModel):
    airflow_base_url: str = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
    airflow_username: str = os.getenv("AIRFLOW_USERNAME", "airflow")
    airflow_password: str = os.getenv("AIRFLOW_PASSWORD", "airflow")

    slack_bot_token: str | None = os.getenv("SLACK_BOT_TOKEN")
    slack_channel_digest: str = os.getenv("SLACK_CHANNEL_DIGEST", "#atlas-digest")

    poll_interval_sec: int = int(os.getenv("POLL_INTERVAL_SEC", "120"))
    digest_cron_ist: str = os.getenv("DIGEST_CRON_IST", "15 9 * * *")  # 09:15 IST daily

    # metrics labels
    env: str = os.getenv("ATLAS_ENV", "dev")

settings = Settings()
