import asyncio
import os
from datetime import datetime, UTC
from typing import Dict, Any

# Overwrite environment to map demo structures gracefully
os.environ["TEMPORALLAYR_DEV_KEYS"] = "dev-test-key"

from app.services.ingestion_service import IngestionService


async def test_ingestion_incident():
    print("--- Test: Auto-Create Incident from Ingestion ---")

    service = IngestionService(max_batch_size=1, flush_interval=0.1)
    await service.start()

    failing_event = {
        "execution_id": "test-incident-exec-1",
        "graph": {
            "nodes": [
                {
                    "name": "CrashNode",
                    "created_at": datetime.now(UTC).isoformat(),
                    "metadata": {
                        "status": "failed",
                        "error": "Timeout accessing upstream service",
                    },
                }
            ]
        },
    }

    print("Triggering batch write synchronously natively...")

    # Bypass async queue background tasks to ensure logs print directly to the main thread securely
    await service._write_batch([{"tenant_id": "dev-test-key", "event": failing_event}])

    print(
        "\nIf [INCIDENT CREATED] test-incident-exec-1 printed above, the test passed!"
    )


if __name__ == "__main__":
    asyncio.run(test_ingestion_incident())


if __name__ == "__main__":
    asyncio.run(test_ingestion_incident())
