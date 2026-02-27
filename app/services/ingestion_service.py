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
        self._queue: asyncio.Queue | None = None
        self._worker_task: asyncio.Task | None = None
        self._is_running = False
        self._storage = StorageService(max_retries=3, base_delay=1.0)

    async def start(self):
        """Start the background ingestion worker."""
        if not self._is_running:
            self._queue = asyncio.Queue(maxsize=10000)
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
                        success = await self._write_batch(batch)
                        if success:
                            batch.clear()
                        else:
                            logger.warning(
                                f"Batch write failed. Backing off for 5s and retaining {len(batch)} events."
                            )
                            await asyncio.sleep(5)
                    last_flush = asyncio.get_event_loop().time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                import traceback

                print(f"[FATAL] Worker exception: {e}")
                traceback.print_exc()
                logger.error(f"Error in background ingestion worker: {e}")
                await asyncio.sleep(1)  # Prevent rapid spin on generic crash

    async def _write_batch(self, batch: List[Dict[str, Any]]) -> bool:
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
        try:
            success = await asyncio.wait_for(
                self._storage.bulk_insert_events(batch), timeout=10.0
            )
            if not success:
                logger.error(
                    "Failed persisting batch cleanly via storage backend layer boundaries. Halting batch to preserve events."
                )
                return False
        except asyncio.TimeoutError:
            logger.error(
                "DB bulk insert timed out after 10s. Halting batch to preserve events."
            )
            return False

        from app.stream.stream_manager import stream_manager_v2
        from app.rules.engine import rule_engine

        # Explicitly map execution anomaly engine structurally validating stored boundaries
        for item in batch:
            event_payload = item.get("event", {})
            # Natively bind tenant isolation tracing directly into payload for inspection
            if "tenant_id" not in event_payload and item.get("tenant_id"):
                event_payload["tenant_id"] = item.get("tenant_id")

            # 1. Automatic Dynamic Detection Rules Engine (Enterprise Safety)
            rule_incident = None
            try:
                # 50ms timeout bounds isolating main arrays completely safely from generic DB locks
                result = await asyncio.wait_for(
                    rule_engine.evaluate_event(event_payload), timeout=0.05
                )
                if result and result.rule.actions.create_incident:
                    # Construct structural trace bridging engine maps organically
                    rule_incident = {
                        "tenant_id": event_payload["tenant_id"],
                        "execution_id": event_payload.get("execution_id")
                        or event_payload.get("id", "unknown"),
                        "timestamp": event_payload.get(
                            "_ingested_at", datetime.utcnow().isoformat()
                        ),
                        "failure_type": result.rule.condition.type,
                        "node_name": event_payload.get("node", "analyzer"),
                        "summary": f"Detected anomaly matching rule: {result.rule.name}",
                    }
                    print(
                        f"[RULE] triggered incident proactively logic='{result.rule.name}'"
                    )
                    asyncio.create_task(
                        stream_manager_v2.broadcast_event(
                            event_payload["tenant_id"],
                            {
                                "type": "rule_triggered",
                                "timestamp": event_payload.get(
                                    "_ingested_at", datetime.utcnow().isoformat()
                                ),
                                "payload": rule_incident,
                            },
                        )
                    )
            except asyncio.TimeoutError:
                logger.error("[RULE] evaluation timeout bounds exceeded safe.")
            except Exception as e:
                logger.error(f"[RULE] evaluation failed robustly natively: {e}")

            # 2. Legacy Base Anomaly Extractor
            incident_data = rule_incident or await detect_execution_failure(
                event_payload
            )

            # Use ingestion arrival times parsing correctly
            ts_str = event_payload.get("_ingested_at", datetime.utcnow().isoformat())

            is_incident = incident_data is not None

            # Unconditionally broadcast across real-time WebSockets isolated from core loops organically
            asyncio.create_task(
                stream_manager_v2.broadcast_event(
                    item.get("tenant_id"),
                    {
                        "type": "execution_graph",
                        "timestamp": ts_str,
                        "payload": event_payload,
                    },
                )
            )

            if is_incident:
                asyncio.create_task(
                    stream_manager_v2.broadcast_event(
                        item.get("tenant_id"),
                        {
                            "type": "incident_created",
                            "timestamp": ts_str,
                            "payload": incident_data,
                        },
                    )
                )

            if incident_data:
                exec_id = incident_data["execution_id"]

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

                import hashlib
                from datetime import timedelta
                from sqlalchemy import select

                # Natively map fingerprint bounds uniquely locking identical error paths
                fp_raw = f"{incident_data.get('failure_type', '')}:{incident_data.get('node_name', '')}"
                fingerprint = hashlib.sha256(fp_raw.encode("utf-8")).hexdigest()

                if async_session_maker:
                    try:
                        async with async_session_maker() as session:
                            # Search for an active incident mapped to this tenant within the last 24 hours natively
                            twenty_four_hours_ago = dt - timedelta(hours=24)

                            stmt = (
                                select(Incident)
                                .where(Incident.tenant_id == incident_data["tenant_id"])
                                .where(Incident.fingerprint == fingerprint)
                                .where(Incident.timestamp >= twenty_four_hours_ago)
                                .order_by(Incident.timestamp.desc())
                                .limit(1)
                            )

                            existing_incident = None
                            try:
                                result = await session.execute(stmt)
                                existing_incident = result.scalar_one_or_none()
                            except Exception as db_err:
                                logger.warning(
                                    f"DB offline/unreachable for incident grouping natively: {db_err}"
                                )

                            if existing_incident:
                                # Safe native isolation bounds mapping aggregates sequentially
                                existing_incident.occurrence_count += 1
                                existing_incident.timestamp = dt
                                print(f"[INCIDENT GROUPED] {fp_raw}")
                            else:
                                new_incident = Incident(
                                    tenant_id=incident_data["tenant_id"],
                                    execution_id=exec_id,
                                    timestamp=dt,
                                    failure_type=incident_data["failure_type"],
                                    node_name=incident_data["node_name"],
                                    summary=incident_data["summary"],
                                    fingerprint=fingerprint,
                                    occurrence_count=1,
                                )
                                session.add(new_incident)
                                print(f"[INCIDENT CREATED] {exec_id}")

                            await session.commit()

                            # Fire webhooks strictly on novel incidents natively isolating spam mappings
                            if not existing_incident:
                                from app.services.alert_engine import process_incident

                                payload = {
                                    "id": str(new_incident.id),
                                    "tenant_id": new_incident.tenant_id,
                                    "failure_type": new_incident.failure_type,
                                    "node_name": new_incident.node_name,
                                    "summary": new_incident.summary,
                                    "timestamp": new_incident.timestamp,
                                }
                                await process_incident(payload)

                    except Exception as e:
                        logger.error(
                            f"Failed persisting localized incidents securely to database: {e}"
                        )
                        print(
                            f"[INCIDENT OFFLINE] {exec_id} (Fingerprint: {fingerprint})"
                        )
                else:
                    print(f"[INCIDENT OFFLINE] {exec_id} (Fingerprint: {fingerprint})")

        return True
