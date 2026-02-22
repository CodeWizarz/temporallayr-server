import asyncio
import json
import threading
import sys
import time
from fastapi.testclient import TestClient


def test_hook():
    from app.main import app

    client = TestClient(app)

    print("Testing hook natively offline")
    response = client.post(
        "/v1/ingest",
        headers={"Authorization": "Bearer dev-test-key"},
        json={
            "events": [
                {
                    "id": "mock-event",
                    "execution_id": "ST-01",
                    "timestamp": "2026-02-22T12:00:00Z",
                    "payload": {},
                }
            ]
        },
    )
    print("STATUS:", response.status_code)


test_hook()
time.sleep(3)
sys.exit(0)
