from __future__ import annotations
import httpx, json
from typing import Optional, List, Dict
from ..config import settings

class SlackClient:
    def __init__(self, token: Optional[str]=None):
        self.token = token or settings.slack_bot_token
        self._headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"} if self.token else {}

    async def post_message(self, channel: str, text: str, blocks: Optional[List[Dict]]=None) -> None:
        # If no token is set, print to console (dev-friendly)
        if not self.token:
            print(f"[SLACK STUB] {channel}: {text}")
            if blocks:
                print(json.dumps(blocks, indent=2))
            return
        async with httpx.AsyncClient(timeout=30.0) as client:
            payload = {"channel": channel, "text": text}
            if blocks:
                payload["blocks"] = blocks
            r = await client.post("https://slack.com/api/chat.postMessage", headers=self._headers, json=payload)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                raise RuntimeError(f"Slack error: {data}")
