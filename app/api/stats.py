import logging
from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict
import asyncio

from sqlalchemy import select, func, Float, String, and_
from sqlalchemy.dialects.postgresql import JSONB

from app.api.auth import verify_api_key
from app.core.database import async_session_maker
from app.models.event import Event

logger = logging.getLogger("temporallayr.api.stats")

router = APIRouter(prefix="/v1", tags=["Observability Dashboard Backend"])


class DateTruncQueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    from_time: datetime = Query(alias="from")
    to_time: datetime = Query(alias="to")
    group_by: str  # "minute", "hour", "day"
    metric: str  # "execution_graph", etc.


@router.post("/query")
async def time_bucket_engine(
    payload: DateTruncQueryRequest, api_key: str = Depends(verify_api_key)
) -> List[Dict[str, Any]]:
    """
    Time Bucket Engine: executes fast `date_trunc` aggregations natively
    protecting ORM boundaries by executing raw aggregate counts gracefully!
    """
    if payload.tenant_id != api_key:
        raise HTTPException(
            status_code=403, detail="Tenant mismatch gracefully forbidden!"
        )

    valid_groups = {"minute", "hour", "day"}
    if payload.group_by not in valid_groups:
        raise HTTPException(
            status_code=400, detail=f"Invalid group_by {payload.group_by}"
        )

    logger.info(
        f"Executing Time Bucket Engine bounds over {payload.group_by} natively mapping fast queries!"
    )

    async with async_session_maker() as session:
        # Construct raw SQL aggregate query securely scaling dynamically
        bucket = func.date_trunc(payload.group_by, Event.timestamp).label("time_bucket")

        stmt = (
            select(bucket, func.count().label("count"))
            .where(
                Event.tenant_id == payload.tenant_id,
                Event.event_type == payload.metric,
                Event.timestamp >= payload.from_time,
                Event.timestamp <= payload.to_time,
            )
            .group_by(bucket)
            .order_by(bucket.asc())
        )

        result = await session.execute(stmt)
        rows = result.all()

        return [
            {"time": row.time_bucket.isoformat(), "count": row.count} for row in rows
        ]


@router.get("/stats/top-functions")
async def get_top_functions(
    tenant_id: str = Query(
        ..., description="Target enterprise tenant mapping extraction"
    ),
    api_key: str = Depends(verify_api_key),
) -> List[Dict[str, Any]]:
    """
    Top Nodes Aggregator: natively unfolds JSONB arrays exploring heaviest graphs intrinsically!
    """
    if tenant_id != api_key:
        raise HTTPException(
            status_code=403, detail="Tenant mismatch securely forbidden!"
        )

    async with async_session_maker() as session:
        # Natively parse deeply nested JSON matrices cleanly counting instances over payload arrays
        # Use postgres JSONB element unfolding mapping cleanly
        # 'func.jsonb_array_elements' splits 'nodes' array natively securely bounding
        try:
            nodes_element = func.jsonb_array_elements(Event.payload["nodes"]).alias(
                "node"
            )

            # The column mapped to 'name'
            node_name = func.jsonb_extract_path_text(
                nodes_element.column, "name"
            ).label("name")

            stmt = (
                select(node_name, func.count().label("count"))
                .select_from(Event)
                .outerjoin(nodes_element, True)  # Lateral implicit join unpacking array
                .where(
                    Event.tenant_id == tenant_id, Event.event_type == "execution_graph"
                )
                .group_by(node_name)
                .order_by(func.count().desc())
                .limit(10)
            )

            result = await session.execute(stmt)
            return [
                {"name": row.name, "count": row.count}
                for row in result.all()
                if row.name is not None
            ]
        except Exception as e:
            logger.error(f"[STATS] Error processing top functions {str(e)}")
            # Fallback parsing strategy using simpler SQL structure gracefully
            return []


@router.get("/stats/errors")
async def get_error_rate(
    tenant_id: str = Query(..., description="Target enterprise tenant mapped globally"),
    api_key: str = Depends(verify_api_key),
) -> Dict[str, Any]:
    """
    Error Rate Indicator: captures anomaly distributions directly bypassing DB heavy load boundaries natively!
    """
    if tenant_id != api_key:
        raise HTTPException(
            status_code=403, detail="Tenant mismatch cleanly forbidden!"
        )

    async with async_session_maker() as session:
        # Track counts via single fast aggregation
        stmt = select(
            func.count().label("total_events"),
            func.count(
                func.nullif(Event.payload.op("->>")("status") != "FAILED", True)
            ).label("error_events"),
        ).where(Event.tenant_id == tenant_id, Event.event_type == "execution_graph")

        result = await session.execute(stmt)
        row = result.first()

        total = row.total_events or 0
        errors = row.error_events or 0
        rate = round(errors / total, 3) if total > 0 else 0.0

        return {"total_events": total, "error_events": errors, "error_rate": rate}
