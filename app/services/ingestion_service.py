import asyncio
import logging
from typing import Any, Dict, List
from datetime import datetime, UTC

from app.services.storage_service import StorageService

logger = logging.getLogger("temporallayr.ingestion")


class IngestionService:
    def __init__(self, max_batch_size: int = 500, flush_interval: float = 2.0):
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval
        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._is_running = False
        self._storage = StorageService(max_retries=3, base_delay=1.0)

    async def start(self):
        """Start the background ingestion worker."""
        if not self._is_running:
            self._is_running = True
            logger.info("IngestionService background worker starting...")
            self._worker_task = asyncio.create_task(self._process_queue())

    async def stop(self):
        """Stop the background generic worker gracefully and flush remaining active items."""
        if self._is_running:
            self._is_running = False
            logger.info("IngestionService background worker stopping...")
            if self._worker_task:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass

            # Final flush
            remaining = []
            while not self._queue.empty():
                try:
                    remaining.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if remaining:
                await self._write_batch(remaining)
            logger.info("IngestionService stopped gracefully.")

    async def enqueue(self, tenant_id: str, events: List[Dict[str, Any]]):
        """Enqueue an array of loosely structured telemetry events mapped to a specific tenant."""
        for event in events:
            # Force server-side receipt timestamps
            event["_ingested_at"] = datetime.now(UTC).isoformat()
            await self._queue.put({"tenant_id": tenant_id, "event": event})

    async def _process_queue(self):
        """Background coroutine processing items into storage backend bindings."""
        batch = []
        last_flush = asyncio.get_event_loop().time()

        while self._is_running:
            try:
                # Calculate time remaining until next explicit flush trigger threshold
                now = asyncio.get_event_loop().time()
                timeout = max(0, self.flush_interval - (now - last_flush))

                # Wait for item or explicit flush timer iteration
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                    batch.append(item)
                    self._queue.task_done()
                except asyncio.TimeoutError:
                    pass  # Timer hit, proceed to explicit flush checks

                if (
                    len(batch) >= self.max_batch_size
                    or (asyncio.get_event_loop().time() - last_flush)
                    >= self.flush_interval
                ):
                    if batch:
                        await self._write_batch(batch)
                        batch.clear()
                    last_flush = asyncio.get_event_loop().time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                import traceback

                print(f"[FATAL] Worker exception: {e}")
                traceback.print_exc()
                logger.error(f"Error in background ingestion worker: {e}")

    async def _write_batch(self, batch: List[Dict[str, Any]]):
        """Write a batch of events reliably to secondary storage through structured backend routing.
        Stream publication is fire-and-forget and always fires, regardless of storage success.
        """
        from app.core.event_stream import EventStream
        from app.services.failure_detector import detect_execution_failure
        from app.core.database import async_session_maker
        from app.models.event import Incident
        from datetime import datetime, UTC

        stream = EventStream()

        # Publish to live stream immediately — non-blocking, independent of DB outcome
        for item in batch:
            event_payload = item.get("event", {})
            exec_id = event_payload.get("execution_id") or event_payload.get("id")
            tenant_id = item.get("tenant_id")

            if exec_id and tenant_id:
                asyncio.create_task(
                    stream.publish(
                        {
                            "type": "execution_ingested",
                            "execution_id": exec_id,
                            "tenant_id": tenant_id,
                            "timestamp": datetime.now(UTC).isoformat(),
                        }
                    )
                )

        # Persist to storage backend (best-effort; failure does not block the stream)
        logger.info(
            f"Dispatching {len(batch)} queued events into PostgreSQL storage backend natively..."
        )
        success = await self._storage.bulk_insert_events(batch)
        if not success:
            logger.error(
                "Failed persisting batch cleanly via storage backend layer boundaries. Continuing to failure detection gracefully."
            )

        # Explicitly map execution anomaly engine structurally validating stored boundaries
        incidents = []
        for item in batch:
            event_payload = item.get("event", {})
            # Natively bind tenant isolation tracing directly into payload for inspection
            if "tenant_id" not in event_payload and item.get("tenant_id"):
                event_payload["tenant_id"] = item.get("tenant_id")

            incident_data = await detect_execution_failure(event_payload)

            if incident_data:
                exec_id = incident_data["execution_id"]
                print(f"[INCIDENT CREATED] {exec_id}")

                # Transform robust chronological constraints tightly handling UTC conversions
                ts_str = incident_data.get("timestamp")
                try:
                    dt = (
                        datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if ts_str
                        else datetime.utcnow()
                    )
                except ValueError:
                    dt = datetime.utcnow()

                incidents.append(
                    Incident(
                        tenant_id=incident_data["tenant_id"],
                        execution_id=exec_id,
                        timestamp=dt,
                        failure_type=incident_data["failure_type"],
                        node_name=incident_data["node_name"],
                        summary=incident_data["summary"],
                    )
                )

        if incidents and async_session_maker:
            try:
                async with async_session_maker() as session:
                    session.add_all(incidents)
                    await session.commit()
            except Exception as e:
                logger.error(
                    f"Failed persisting localized incidents securely to database: {e}"
                )
