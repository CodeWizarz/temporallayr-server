"""
example_agent.py — Local test script for the /v1/live WebSocket feed.

Usage:
  1. Start the server:       uvicorn app.main:app --reload --port 8000
  2. Open wscat in another terminal:
       wscat -c "ws://localhost:8000/v1/live?tenant_id=demo-tenant&token=dev-temporallayr-key"
  3. Run this script:        python example_agent.py
  4. You should see a live JSON event appear in the wscat output immediately.
"""

import asyncio
import httpx

SERVER = "http://localhost:8000"
INGEST_API_KEY = "dev-test-key"  # matches TEMPORALLAYR_DEV_KEYS default
WS_TOKEN = (
    "dev-temporallayr-key"  # matches TEMPORALLAYR_API_KEY default (used by /v1/live)
)
TENANT_ID = "demo-tenant"


async def main():
    print(f"[example_agent] Sending test ingest event to {SERVER}/v1/ingest ...")
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            f"{SERVER}/v1/ingest",
            headers={"Authorization": f"Bearer {INGEST_API_KEY}"},
            json={
                "tenant_id": TENANT_ID,
                "events": [
                    {
                        "id": "live-test-01",
                        "execution_id": "LIVE-EXEC-01",
                        "timestamp": "2026-02-22T13:00:00Z",
                        "payload": {"step": "start", "agent": "example_agent"},
                    }
                ],
            },
        )
    print(f"[example_agent] Ingest response: {response.status_code} — {response.text}")
    print("[example_agent] Done. Check your wscat terminal for the live event.")


if __name__ == "__main__":
    asyncio.run(main())
