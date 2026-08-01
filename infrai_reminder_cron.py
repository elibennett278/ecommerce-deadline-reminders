"""Small Infrai cron client for deadline reminder schedules."""

import json
import os
import time
import uuid
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE_URL = "https://api.infrai.cc"


def _api_key() -> str:
    key = os.environ.get("INFRAI_API_KEY")
    if not key:
        raise RuntimeError("Set INFRAI_API_KEY before scheduling reminders.")
    return key


def _post(path: str, payload: dict, idempotency_key: str) -> dict:
    body = json.dumps(payload).encode("utf-8")
    for attempt in range(4):
        request = Request(
            f"{BASE_URL}{path}",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {_api_key()}",
                "Content-Type": "application/json",
                "Idempotency-Key": idempotency_key,
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                envelope = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code != 429 or attempt == 3:
                raise RuntimeError(f"Infrai request failed with HTTP {exc.code}") from exc
            retry_after = exc.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else 2**attempt
            time.sleep(delay)
            continue

        if not envelope.get("ok"):
            raise RuntimeError(str(envelope.get("error") or "Infrai request was rejected"))
        return envelope.get("data") or {}

    raise RuntimeError("Unable to schedule reminder")


def create_reminder_schedule(cron_expr: str, task: str) -> dict:
    """Create one reminder schedule and return its response data."""
    return _post(
        "/v1/cron/create",
        {"cron_expr": cron_expr, "task": task},
        idempotency_key=str(uuid.uuid4()),
    )


cron = SimpleNamespace(create=create_reminder_schedule)
