import logging
import asyncio
from typing import List, Dict, Any
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from app.core.database import async_session_maker
from app.models.event import Event

logger = logging.getLogger("temporallayr.storage")


class StorageService:
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    async def bulk_insert_events(self, batch: List[Dict[str, Any]]) -> bool:
        """
        Execute high-throughput async DB batch inserts reliably explicitly backing off on transient PostgreSQL faults.

        Args:
            batch: List of dictionaries natively containing dict(tenant_id=str, event=dict)
        """
        if not batch or not async_session_maker:
            logger.warning(
                "Skipping bulk insert: Empty batch or uninitialized database engine."
            )
            return False

        # Transform raw structured batches mapping natively over target SQLAlchemy model entities tightly
        event_models = []
        for item in batch:
            tenant_id = item.get("tenant_id")
            event_data = item.get("event", {})

            # Map robust timestamp fallback extracting inner schemas defensively
            timestamp_str = event_data.get("_ingested_at")
            try:
                dt = (
                    datetime.fromisoformat(timestamp_str)
                    if timestamp_str
                    else datetime.utcnow()
                )
            except ValueError:
                dt = datetime.utcnow()

            event_models.append(
                Event(api_key=tenant_id, timestamp=dt, payload=event_data)
            )

        # Retry transient storage execution loop natively isolating background worker crashes cleanly
        for attempt in range(1, self.max_retries + 1):
            try:
                async with async_session_maker() as session:  # type: AsyncSession
                    session.add_all(event_models)
                    await session.commit()
                    logger.info(
                        f"Successfully persisted {len(event_models)} events to PostgreSQL backend."
                    )
                    return True
            except SQLAlchemyError as e:
                logger.error(
                    f"Database insertion failed (Attempt {attempt}/{self.max_retries}): {e}"
                )

                if attempt == self.max_retries:
                    logger.critical(
                        f"Exhausted db retry attempts dropping {len(event_models)} telemetry records."
                    )
                    # Prevent worker crash, bubble up handled failure logically
                    return False

                # Exponential backoff jitter fallback
                await asyncio.sleep(self.base_delay * (2 ** (attempt - 1)))
            except Exception as e:
                logger.error(
                    f"Unexpected execution error mapping storage transaction cleanly: {e}"
                )
                return False

        return False

    async def query_events(
        self,
        tenant_id: str,
        limit: int = 100,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute highly structured tenant event scans efficiently leveraging composite API key indexes defensively.
        """
        from sqlalchemy import select

        if not async_session_maker:
            logger.warning("Skipping event query: Uninitialized database engine.")
            return []

        # Bound explicit scan parameters natively
        stmt = select(Event.payload).where(Event.api_key == tenant_id)

        if from_time:
            stmt = stmt.where(Event.timestamp >= from_time)
        if to_time:
            stmt = stmt.where(Event.timestamp <= to_time)

        # Force sort mappings mapping chronological extraction optimally gracefully capping
        stmt = stmt.order_by(Event.timestamp.desc()).limit(limit)

        try:
            async with async_session_maker() as session:
                result = await session.execute(stmt)
                # Unpack scalar JSONB payload blocks directly cleanly
                return [row for row in result.scalars()]
        except SQLAlchemyError as e:
            logger.error(f"Failed extracting tenant query payload bounds natively: {e}")
            return []

    async def get_executions(
        self, tenant_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Extract high-level execution summaries natively."""
        from sqlalchemy import select

        if not async_session_maker:
            return []

        stmt = (
            select(Event)
            .where(Event.api_key == tenant_id)
            .order_by(Event.timestamp.desc())
            .limit(limit)
        )

        executions = []
        try:
            async with async_session_maker() as session:
                result = await session.execute(stmt)
                for event in result.scalars():
                    payload = event.payload or {}
                    # Attempt to extract id and node count from standard formats
                    exec_id = (
                        payload.get("execution_id")
                        or payload.get("id")
                        or str(event.id)
                    )
                    nodes = payload.get("nodes", [])
                    node_count = len(nodes) if isinstance(nodes, list) else 1

                    executions.append(
                        {
                            "id": exec_id,
                            "created_at": event.timestamp.isoformat()
                            if event.timestamp
                            else "",
                            "node_count": node_count,
                        }
                    )
        except SQLAlchemyError as e:
            logger.error(f"Failed extracting executions: {e}")
        except Exception as e:
            logger.error(f"Unexpected error extracting executions: {e}")

        return executions

    async def get_execution(
        self, tenant_id: str, execution_id: str
    ) -> Dict[str, Any] | None:
        """Extract a single full execution graph natively."""
        from sqlalchemy import select

        if not async_session_maker:
            return None

        # Optimization: filter query by looking at JSONB directly if possible, or scan bounds
        stmt = (
            select(Event)
            .where(Event.api_key == tenant_id)
            .order_by(Event.timestamp.desc())
            .limit(100)
        )
        try:
            async with async_session_maker() as session:
                result = await session.execute(stmt)
                for event in result.scalars():
                    payload = event.payload or {}
                    exec_id = (
                        payload.get("execution_id")
                        or payload.get("id")
                        or str(event.id)
                    )
                    if exec_id == execution_id:
                        return payload
        except SQLAlchemyError as e:
            logger.error(f"Failed extracting single execution: {e}")
        except Exception as e:
            logger.error(f"Unexpected error extracting single execution: {e}")

        return None
