import asyncio
from fastapi.testclient import TestClient
from app.main import app
from app.core.event_stream import EventStream
import json

client = TestClient(app)


async def main():
    stream = EventStream()

    # Simulate consumer tracking
    global received
    received = 0

    async def consumer():
        global received
        try:
            async for event in stream.subscribe():
                print(f"Consumer received: {event}")
                received += 1
                if received >= 1:
                    break
        except Exception as e:
            print(f"Consumer exception: {e}")

    # Set up native stream task listener safely bypassing thread freezes
    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.1)

    # Fast ingest execution to test fire-and-forget publish
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

    # Needs a brief await so async flush inside fastAPI hits event publish hook natively
    await asyncio.sleep(2.5)

    # Wait for consumer safely
    task.cancel()  # Safe break

    if received > 0:
        print("SUCCESS: Event stream mapped ingestion gracefully!")
    else:
        print("FAILURE: Event stream hook failed!")


if __name__ == "__main__":
    asyncio.run(main())
