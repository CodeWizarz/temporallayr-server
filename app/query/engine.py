import asyncio
import time
import logging
from typing import List, Dict, Any, Tuple
from sqlalchemy.future import select
from sqlalchemy import or_, and_, asc, desc, cast, String
from sqlalchemy.dialects.postgresql import JSONB

from app.core.database import async_session_maker
from app.models.event import Event, Incident
from app.query.models import MultiResourceQueryRequest, QueryResult

logger = logging.getLogger("temporallayr.query.engine")


class QueryEngine:
    """Enterprise Query Engine with strict timeout safeguards binding multitenant queries natively."""

    def __init__(self, default_timeout: float = 2.0, max_limit: int = 5000):
        self.default_timeout = default_timeout
        self.max_limit = max_limit

    async def _execute_with_safeguards(
        self, stmt, limit: int
    ) -> Tuple[List[Any], bool]:
        """Runs structurally complex SQL natively trapping timeouts accurately preserving app stability."""
        actual_limit = min(limit, self.max_limit)
        stmt = stmt.limit(actual_limit)

        start_time = time.time()
        is_partial = False
        results = []

        try:
            async with async_session_maker() as session:
                task = asyncio.create_task(session.execute(stmt))
                result = await asyncio.wait_for(task, timeout=self.default_timeout)
                results = list(result.scalars().all())

        except asyncio.TimeoutError:
            logger.warning(
                "[QUERY] Execution timeout safely bounded returning empty flags organically."
            )
            is_partial = True
        except Exception as e:
            logger.error(f"[QUERY] Execution failed natively safely: {e}")
            is_partial = True

        duration = time.time() - start_time
        if duration > 1.0:
            logger.warning(f"[QUERY] slow query detected >1s (took {duration:.2f}s)")

        return results, is_partial

    async def search_events(self, query: MultiResourceQueryRequest) -> QueryResult:
        """Search execution trace payloads directly checking boundaries natively."""
        stmt = select(Event).where(Event.tenant_id == query.tenant_id)

        # Apply strict query boundaries natively
        filters = query.filters
        if filters.execution_id:
            stmt = stmt.where(
                Event.payload.op("->>")("execution_id") == filters.execution_id
            )
        if filters.status:
            stmt = stmt.where(Event.payload.op("->>")("status") == filters.status)
        if filters.time_range:
            if filters.time_range.start:
                stmt = stmt.where(Event.timestamp >= filters.time_range.start)
            if filters.time_range.end:
                stmt = stmt.where(Event.timestamp <= filters.time_range.end)

        if query.search_text:
            text_filter = f"%{query.search_text}%"
            # Full text ILIKE match across the JSON payload dynamically
            stmt = stmt.where(cast(Event.payload, String).ilike(text_filter))

        # Node specific mapping (JSONB "@>") natively
        if filters.node_name:
            # Match executions dynamically containing a node named X
            node_match = [{"name": filters.node_name}]
            # We query if payload->'graph'->'nodes' contains this dict natively
            stmt = stmt.where(Event.payload["graph"]["nodes"].contains(node_match))

        # Apply sort boundaries natively
        if query.sort.field == "timestamp":
            if query.sort.direction == "desc":
                stmt = stmt.order_by(Event.timestamp.desc())
            else:
                stmt = stmt.order_by(Event.timestamp.asc())

        stmt = stmt.offset(query.offset)

        results, is_partial = await self._execute_with_safeguards(stmt, query.limit)

        logger.info(f"[QUERY] tenant={query.tenant_id} rows={len(results)}")
        warning = (
            "Partial results returned due to heavy query limits."
            if is_partial
            else None
        )

        # Hydrate JSON explicitly avoiding Pydantic ORM strict serialization issues
        data = [r.payload for r in results]
        return QueryResult(
            data=data, total=len(data), partial=is_partial, warning=warning
        )

    async def search_incidents(self, query: MultiResourceQueryRequest) -> QueryResult:
        """Search alert traces explicitly mapped over anomalies natively."""
        stmt = select(Incident).where(Incident.tenant_id == query.tenant_id)

        filters = query.filters
        if filters.incident_id:
            stmt = stmt.where(cast(Incident.id, String) == filters.incident_id)
        if filters.execution_id:
            stmt = stmt.where(Incident.execution_id == filters.execution_id)
        if filters.node_name:
            stmt = stmt.where(Incident.node_name == filters.node_name)
        if filters.fingerprint:
            stmt = stmt.where(Incident.fingerprint == filters.fingerprint)

        if filters.time_range:
            if filters.time_range.start:
                stmt = stmt.where(Incident.timestamp >= filters.time_range.start)
            if filters.time_range.end:
                stmt = stmt.where(Incident.timestamp <= filters.time_range.end)

        if query.search_text:
            stmt = stmt.where(Incident.summary.ilike(f"%{query.search_text}%"))

        if query.sort.direction == "desc":
            stmt = stmt.order_by(Incident.timestamp.desc())
        else:
            stmt = stmt.order_by(Incident.timestamp.asc())

        stmt = stmt.offset(query.offset)

        results, is_partial = await self._execute_with_safeguards(stmt, query.limit)
        logger.info(f"[QUERY] tenant={query.tenant_id} rows={len(results)}")

        data = [
            {
                "id": str(r.id),
                "execution_id": r.execution_id,
                "timestamp": r.timestamp.isoformat(),
                "failure_type": r.failure_type,
                "node_name": r.node_name,
                "summary": r.summary,
                "fingerprint": r.fingerprint,
                "occurrence_count": r.occurrence_count,
            }
            for r in results
        ]

        warning = "Partial results returned natively." if is_partial else None
        return QueryResult(
            data=data, total=len(data), partial=is_partial, warning=warning
        )

    async def search_nodes(self, query: MultiResourceQueryRequest) -> QueryResult:
        """Isolated wrapper delegating to search_events but specifically requesting node extracts.
        For architectural purity, we filter events by node and extract matching nodes physically.
        """
        # We parse the base executions and extract nodes explicitly bounding in memory to simulate
        # complex PostgreSQL lateral joins without destabilizing bounds organically.
        event_res = await self.search_events(query)

        extracted_nodes = []
        for ev in event_res.data:
            nodes = ev.get("graph", {}).get("nodes", [])
            for n in nodes:
                # Apply text search explicitly if it wasn't caught safely
                if query.filters.node_name and n.get("name") != query.filters.node_name:
                    continue
                extracted_nodes.append(n)
                if len(extracted_nodes) >= min(query.limit, self.max_limit):
                    break
            if len(extracted_nodes) >= min(query.limit, self.max_limit):
                break

        logger.info(f"[QUERY] tenant={query.tenant_id} rows={len(extracted_nodes)}")
        return QueryResult(
            data=extracted_nodes,
            total=len(extracted_nodes),
            partial=event_res.partial,
            warning=event_res.warning,
        )

    async def search_clusters(self, query: MultiResourceQueryRequest) -> QueryResult:
        """Search execution metadata flags natively finding cluster aggregates."""
        # Clusters are derived natively over "attributes.cluster_id" mapped into the payload.
        stmt = select(Event).where(Event.tenant_id == query.tenant_id)

        if query.filters.cluster_id:
            stmt = stmt.where(
                Event.payload.op("->>")("cluster_id") == query.filters.cluster_id
            )

        if query.time_range:
            if query.time_range.start:
                stmt = stmt.where(Event.timestamp >= query.time_range.start)
            if query.time_range.end:
                stmt = stmt.where(Event.timestamp <= query.time_range.end)

        stmt = stmt.offset(query.offset)
        results, is_partial = await self._execute_with_safeguards(stmt, query.limit)

        logger.info(f"[QUERY] tenant={query.tenant_id} rows={len(results)}")
        warning = "Partial results returned natively." if is_partial else None

        # Return exact cluster trace bounds organically
        data = [r.payload for r in results]
        return QueryResult(
            data=data, total=len(data), partial=is_partial, warning=warning
        )


query_engine = QueryEngine()
