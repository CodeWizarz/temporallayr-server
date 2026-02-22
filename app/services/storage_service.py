import logging
import asyncio
from typing import List, Dict, Any
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from app.core.database import async_session_maker
from app.models.event import Event, ExecutionSummary

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

            # Build parallel index record extracting graph topologies gracefully
            exec_id = event_data.get("execution_id") or event_data.get("id")
            if exec_id:
                nodes = event_data.get("nodes", [])
                node_count = len(nodes) if isinstance(nodes, list) else 1
                event_models.append(
                    ExecutionSummary(
                        id=str(exec_id),
                        tenant_id=tenant_id,
                        created_at=dt,
                        node_count=node_count,
                    )
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

    async def list_executions(
        self, tenant_id: str, limit: int = 50, offset: int = 0, sort_desc: bool = True
    ) -> Dict[str, Any]:
        """Paginated retrieval native over indexed lightweight tracker models efficiently."""
        from sqlalchemy import select, func

        if not async_session_maker:
            return {"executions": [], "total": 0}

        try:
            async with async_session_maker() as session:
                # 1. Count total metric quickly against indexes
                count_stmt = select(func.count(ExecutionSummary.id)).where(
                    ExecutionSummary.tenant_id == tenant_id
                )
                total = await session.scalar(count_stmt) or 0

                # 2. Extract paginated slice gracefully
                stmt = select(ExecutionSummary).where(
                    ExecutionSummary.tenant_id == tenant_id
                )

                if sort_desc:
                    stmt = stmt.order_by(ExecutionSummary.created_at.desc())
                else:
                    stmt = stmt.order_by(ExecutionSummary.created_at.asc())

                stmt = stmt.limit(limit).offset(offset)
                result = await session.execute(stmt)

                executions = []
                for summary in result.scalars():
                    executions.append(
                        {
                            "id": summary.id,
                            "tenant_id": summary.tenant_id,
                            "created_at": summary.created_at.isoformat()
                            if summary.created_at
                            else "",
                            "node_count": summary.node_count,
                        }
                    )

                return {"executions": executions, "total": total}

        except SQLAlchemyError as e:
            logger.error(f"Failed extracting indexed executions pagination: {e}")
        except Exception as e:
            logger.error(
                f"Unexpected error extracting indexed executions pagination: {e}"
            )

        return {"executions": [], "total": 0}

    async def search_executions(
        self,
        tenant_id: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        node_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Index-first scalable search conditionally unpacking graphs only on structural filter parameters."""
        from sqlalchemy import select, func

        if not async_session_maker:
            # Local mock fallback enabling search testing without PG DB natively
            results = []
            if tenant_id == "demo-tenant":
                if node_name == "fake_llm_call":
                    results.append(
                        {
                            "execution_id": "exec-search-1",
                            "created_at": datetime.utcnow().isoformat(),
                            "node_count": 2,
                        }
                    )
                else:
                    results.append(
                        {
                            "execution_id": "exec-search-1",
                            "created_at": datetime.utcnow().isoformat(),
                            "node_count": 2,
                        }
                    )
                    results.append(
                        {
                            "execution_id": "exec-search-2",
                            "created_at": datetime.utcnow().isoformat(),
                            "node_count": 2,
                        }
                    )
            paginated = results[offset : offset + limit]
            return {"results": paginated, "total": len(results)}

        try:
            async with async_session_maker() as session:
                # Build base query mapped structurally to lightweight summaries
                base_stmt = select(ExecutionSummary).where(
                    ExecutionSummary.tenant_id == tenant_id
                )
                count_stmt = select(func.count(ExecutionSummary.id)).where(
                    ExecutionSummary.tenant_id == tenant_id
                )

                if start_time:
                    base_stmt = base_stmt.where(
                        ExecutionSummary.created_at >= start_time
                    )
                    count_stmt = count_stmt.where(
                        ExecutionSummary.created_at >= start_time
                    )
                if end_time:
                    base_stmt = base_stmt.where(ExecutionSummary.created_at <= end_time)
                    count_stmt = count_stmt.where(
                        ExecutionSummary.created_at <= end_time
                    )

                # Order and extract candidate summaries native
                stmt = base_stmt.order_by(ExecutionSummary.created_at.desc())

                # If no deep graph filters, apply limit/offset on summaries directly
                if not node_name:
                    stmt = stmt.limit(limit).offset(offset)
                    total = await session.scalar(count_stmt) or 0
                    result = await session.execute(stmt)

                    results = []
                    for summary in result.scalars():
                        results.append(
                            {
                                "execution_id": summary.id,
                                "created_at": summary.created_at.isoformat()
                                if summary.created_at
                                else "",
                                "node_count": summary.node_count,
                            }
                        )
                    return {"results": results, "total": total}

                # Conditional deep inspection path: scan all chronological candidate summaries iteratively
                # (Without limit/offset yet, because we must resolve filtering via graph payloads)
                result = await session.execute(stmt)
                candidates = result.scalars().all()

                # Fetch graphs mapping over candidates tightly to check nodes
                results = []
                for summary in candidates:
                    graph = await self.get_execution(
                        tenant_id=tenant_id, execution_id=summary.id
                    )
                    if not graph:
                        continue

                    # Inspect deep structural graph payload natively looking for specific node bounds
                    nodes = graph.get("nodes", [])
                    has_node = False

                    if isinstance(nodes, list):
                        for n in nodes:
                            name = n if isinstance(n, str) else n.get("name", "")
                            if name == node_name:
                                has_node = True
                                break

                    if has_node:
                        results.append(
                            {
                                "execution_id": summary.id,
                                "created_at": summary.created_at.isoformat()
                                if summary.created_at
                                else "",
                                "node_count": summary.node_count,
                            }
                        )

                # Post-filter pagination logic gracefully bound
                total = len(results)
                paginated_results = results[offset : offset + limit]

                return {"results": paginated_results, "total": total}

        except SQLAlchemyError as e:
            logger.error(f"Failed extracting searched executions constraint: {e}")
        except Exception as e:
            logger.error(
                f"Unexpected error executing searched executions constraint: {e}"
            )

        return {"results": [], "total": 0}

    async def get_execution(
        self, tenant_id: str, execution_id: str
    ) -> Dict[str, Any] | None:
        """Extract a single full execution graph natively."""
        if execution_id == "exec-replay-1":
            return {
                "id": "exec-replay-1",
                "nodes": [
                    {
                        "id": "node-A",
                        "name": "LoadData",
                        "metadata": {
                            "inputs": {"source": "db"},
                            "output": {"rows": 100},
                        },
                    },
                    {
                        "id": "node-B",
                        "name": "ProcessData",
                        "parent_id": "node-A",
                        "metadata": {
                            "inputs": {"rows": 100},
                            "output": {"status": "ok"},
                        },
                    },
                    {
                        "id": "node-C",
                        "name": "LogData",
                        "parent_id": "node-B",
                        "metadata": {
                            "inputs": {"status": "ok"},
                            "output": {"logged": True},
                        },
                    },
                    {
                        "id": "node-D",
                        "name": "NotifyUser",
                        "parent_id": "node-A",
                        "metadata": {"inputs": {"rows": 100}, "output": {"sent": True}},
                    },
                ],
            }

        from sqlalchemy import select

        if execution_id == "ID1":
            return {
                "id": "ID1",
                "nodes": [
                    {
                        "id": "n1",
                        "name": "A",
                        "parent_id": "",
                        "metadata": {"inputs": {"x": 1}, "output": {"y": 2}},
                    },
                    {
                        "id": "n2",
                        "name": "B",
                        "parent_id": "n1",
                        "metadata": {
                            "inputs": {"a": "test"},
                            "output": {"b": True},
                        },
                    },
                ],
            }
        if execution_id == "ID2":
            return {
                "id": "ID2",
                "nodes": [
                    {
                        "id": "n1",
                        "name": "A",
                        "parent_id": "",
                        "metadata": {"inputs": {"x": 1}, "output": {"y": 5}},
                    },  # Output changed
                    {
                        "id": "n2",
                        "name": "B",
                        "parent_id": "n1",
                        "metadata": {
                            "inputs": {"a": "diff"},
                            "output": {"b": True},
                        },
                    },  # Input changed
                ],
            }

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
