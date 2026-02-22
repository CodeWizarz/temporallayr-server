import asyncio
from app.services.alert_engine import process_incident
from datetime import datetime
import json


async def test_alert():
    print("Testing Alert Engine Isolation natively...")

    mock_payload = {
        "id": "test-uuid-001",
        "tenant_id": "dev-test-key",
        "failure_type": "runtime_error",
        "node_name": None,
        "summary": "Simulated error crash",
        "timestamp": str(datetime.now()),
    }

    # Process incident simulating the ingestion asynchronous detach layer
    await process_incident(mock_payload)

    # Allow background asyncio to_thread loops to dispatch cleanly
    print("Waiting for webhook resolution...")
    await asyncio.sleep(2)
    print("Done testing.")


if __name__ == "__main__":
    asyncio.run(test_alert())
