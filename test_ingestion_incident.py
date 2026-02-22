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

    print(
        "Triggering batch writes sequentially simulating duplicate anomaly metrics natively..."
    )

    from unittest.mock import AsyncMock, patch, MagicMock

    # Setup mocked PostgreSQL session arrays dynamically mapping hash lookups
    mock_incident = MagicMock()
    mock_incident.occurrence_count = 1
    mock_incident.timestamp = datetime.now(UTC)

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.side_effect = [None, mock_incident]

    mock_session = AsyncMock()
    mock_session.execute.return_value = mock_result

    mock_session_maker = MagicMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session

    with patch("app.core.database.async_session_maker", mock_session_maker):
        print("\n--- Event 1 (Should Create) ---")
        await service._write_batch(
            [{"tenant_id": "dev-test-key", "event": failing_event}]
        )

        print("\n--- Event 2 (Should Group) ---")
        await service._write_batch(
            [{"tenant_id": "dev-test-key", "event": failing_event}]
        )

    print(
        "\nIf [INCIDENT CREATED] followed by [INCIDENT GROUPED] printed above, the test passed!"
    )


if __name__ == "__main__":
    asyncio.run(test_ingestion_incident())


if __name__ == "__main__":
    asyncio.run(test_ingestion_incident())
