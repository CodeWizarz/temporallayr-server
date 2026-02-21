import asyncio
import logging
import json
from typing import Any, Dict, List
from datetime import datetime, UTC

logger = logging.getLogger("temporallayr.ingestion")


class IngestionService:
    def __init__(self, max_batch_size: int = 500, flush_interval: float = 2.0):
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval
        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._is_running = False

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
                logger.error(f"Error in background ingestion worker: {e}")

    async def _write_batch(self, batch: List[Dict[str, Any]]):
        """Write a batch of events reliably to secondary storage."""
        # TODO: Stubbed write integration binding. Hook this against actual GraphStore layer.
        logger.info(f"Flushing {len(batch)} events downstream to storage...")
        # Simulating IO latency
        await asyncio.sleep(0.05)
