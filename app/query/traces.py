import logging
from typing import Dict, Any, List, Optional
from uuid import UUID

from sqlalchemy.future import select
from sqlalchemy import desc

from app.core.database import async_session_maker
from app.models.event import Event

logger = logging.getLogger("temporallayr.query.traces")


async def get_trace(tenant_id: str, trace_id: str) -> Dict[str, Any]:
    """Retrieves a fully bounded execution generic graph organically fetching explicitly."""
    logger.info(f"[TRACE FETCH] tenant={tenant_id} trace_id={trace_id}")

    try:
        query_id = UUID(trace_id)
    except ValueError:
        return None

    async with async_session_maker() as session:
        query = select(Event).where(Event.id == query_id, Event.tenant_id == tenant_id)

        result = await session.execute(query)
        event = result.scalars().first()

        if not event:
            return None

        return {
            "id": str(event.id),
            "timestamp": event.timestamp.isoformat() if event.timestamp else None,
            "tenant_id": event.tenant_id,
            "payload": event.payload,
        }


async def list_traces(
    tenant_id: str, limit: int = 100, offset: int = 0, status: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Lists bounded executions sequentially natively masking trace bounds across limits correctly."""
    logger.info(
        f"[TRACE LIST] tenant={tenant_id} limit={limit} offset={offset} status={status}"
    )

    # Cap maximum limits structurally
    limit = min(max(1, limit), 100)
    offset = max(0, offset)

    async with async_session_maker() as session:
        query = select(Event).where(Event.tenant_id == tenant_id)

        if status:
            query = query.where(Event.payload["status"].astext == status)

        # Natively sorting descending tracking most recent executions automatically
        query = query.order_by(desc(Event.timestamp)).limit(limit).offset(offset)

        result = await session.execute(query)
        events = result.scalars().all()

        # Build optimized trace references masking payloads inherently mapping cleanly
        items = []
        for event in events:
            evt_status = event.payload.get("status", "UNKNOWN")
            is_error = evt_status == "FAILED"

            items.append(
                {
                    "id": str(event.id),
                    "timestamp": event.timestamp.isoformat()
                    if event.timestamp
                    else None,
                    "error": is_error,
                    "status": evt_status,
                }
            )

        return items
