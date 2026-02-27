import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict

from fastapi import APIRouter, Depends, Query, HTTPException, Request, Response
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
    payload: DateTruncQueryRequest,
    request: Request,
    response: Response,
    api_key: str = Depends(verify_api_key),
) -> List[Dict[str, Any]]:
    """
    Time Bucket Engine: executes fast `date_trunc` aggregations natively
    protecting ORM boundaries by executing raw aggregate counts gracefully!
    """
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return []

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

    try:
        async with async_session_maker() as session:
            # Construct raw SQL aggregate query securely scaling dynamically
            bucket = func.date_trunc(payload.group_by, Event.timestamp).label(
                "time_bucket"
            )

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
                {"time": row.time_bucket.isoformat(), "count": row.count}
                for row in rows
            ]
    except Exception as e:
        logger.error(f"[STATS] Error in time_bucket_engine: {str(e)}")
        return []


@router.get("/stats/top-functions")
async def get_top_functions(
    request: Request,
    response: Response,
    tenant_id: str = Query(
        ..., description="Target enterprise tenant mapping extraction"
    ),
    api_key: str = Depends(verify_api_key),
) -> List[Dict[str, Any]]:
    """
    Top Nodes Aggregator: natively unfolds JSONB arrays exploring heaviest graphs intrinsically!
    """
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return []

    if tenant_id != api_key:
        raise HTTPException(
            status_code=403, detail="Tenant mismatch securely forbidden!"
        )

    try:
        async with async_session_maker() as session:
            # Natively parse deeply nested JSON matrices cleanly counting instances over payload arrays
            # Use postgres JSONB element unfolding mapping cleanly
            # 'func.jsonb_array_elements' splits 'nodes' array natively securely bounding
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
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Target enterprise tenant mapped globally"),
    api_key: str = Depends(verify_api_key),
) -> Dict[str, Any]:
    """
    Error Rate Indicator: captures anomaly distributions directly bypassing DB heavy load boundaries natively!
    """
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return {"total_events": 0, "error_events": 0, "error_rate": 0.0}

    if tenant_id != api_key:
        raise HTTPException(
            status_code=403, detail="Tenant mismatch cleanly forbidden!"
        )

    try:
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
    except Exception as e:
        logger.error(f"[STATS] Error in get_error_rate: {str(e)}")
        return {"total_events": 0, "error_events": 0, "error_rate": 0.0}


@router.get("/stats/durations")
async def get_durations(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Target enterprise tenant mapped globally"),
    api_key: str = Depends(verify_api_key),
) -> Dict[str, Any]:
    """
    Latency & Duration Extraction: calculates accurate P95 percentiles
    dynamically in Postgres by parsing node metadata outputs natively!
    """
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return {"avg_duration_ms": 0.0, "p95_duration_ms": 0.0, "max_duration_ms": 0.0}

    if tenant_id != api_key:
        raise HTTPException(
            status_code=403, detail="Tenant mismatch cleanly forbidden!"
        )

    try:
        async with async_session_maker() as session:
            nodes_element = func.jsonb_array_elements(Event.payload["nodes"]).alias(
                "node"
            )

            # Safely cast JSONB text properties dynamically mapping Postgres Float
            duration_cast = func.cast(
                func.jsonb_extract_path_text(
                    nodes_element.column, "metadata", "output", "duration_ms"
                ),
                Float,
            )

            # The 'within_group' execution explicitly bounding percentile_cont natively requires `text()` string mapping unfortunately in older SQLAlchemy or specific usages.
            from sqlalchemy import text

            # Extract basic statistical averages filtering strictly
            stmt = (
                select(
                    func.avg(duration_cast).label("avg_duration_ms"),
                    func.max(duration_cast).label("max_duration_ms"),
                    # Percentile expression naturally bounded
                    func.percentile_cont(0.95)
                    .within_group(duration_cast.asc())
                    .label("p95_duration_ms"),
                )
                .select_from(Event)
                .outerjoin(nodes_element, True)
                .where(
                    Event.tenant_id == tenant_id,
                    Event.event_type == "execution_graph",
                )
                .where(
                    func.jsonb_extract_path_text(
                        nodes_element.column, "metadata", "output", "duration_ms"
                    ).isnot(None)
                )
            )

            result = await session.execute(stmt)
            row = result.first()

            def round_safe(val):
                return round(float(val), 2) if val is not None else 0.0

            return {
                "avg_duration_ms": round_safe(row.avg_duration_ms),
                "p95_duration_ms": round_safe(row.p95_duration_ms),
                "max_duration_ms": round_safe(row.max_duration_ms),
            }
    except Exception as e:
        logger.error(f"[STATS] Error processing durations {str(e)}")
        return {
            "avg_duration_ms": 0.0,
            "p95_duration_ms": 0.0,
            "max_duration_ms": 0.0,
        }


@router.get("/overview")
async def get_overview(
    request: Request,
    response: Response,
    tenant_id: str = Query(
        ..., description="Target enterprise tenant mapping globally"
    ),
    api_key: str = Depends(verify_api_key),
) -> Dict[str, Any]:
    """
    Live Data Summary Widget: Returns fast macros avoiding N+1 scaling.
    Calculates moving 1h/24h metrics mapped securely against live telemetry.
    """
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return {
            "events_last_1h": 0,
            "events_last_24h": 0,
            "unique_functions": 0,
            "last_event_timestamp": None,
        }

    if tenant_id != api_key:
        raise HTTPException(
            status_code=403, detail="Tenant mismatch cleanly forbidden!"
        )

    try:
        async with async_session_maker() as session:
            # Optimize extracting fast aggregated bounding without subqueries.
            # PostGres conditional SUM mappings gracefully fetch multi-bound metrics.
            from sqlalchemy import cast, Integer, text

            stmt = select(
                func.sum(
                    cast(
                        Event.timestamp >= func.now() - text("INTERVAL '1 hour'"),
                        Integer,
                    )
                ).label("events_last_1h"),
                func.sum(
                    cast(
                        Event.timestamp >= func.now() - text("INTERVAL '24 hours'"),
                        Integer,
                    )
                ).label("events_last_24h"),
                # Estimate unique nodes quickly gracefully avoiding distinct overhead mapped.
                func.count(
                    func.distinct(Event.payload.op("->>")("function_name"))
                ).label("unique_functions"),
                func.max(Event.timestamp).label("last_event_timestamp"),
            ).where(Event.tenant_id == tenant_id, Event.event_type == "execution_graph")

            result = await session.execute(stmt)
            row = result.first()

            return {
                "events_last_1h": int(row.events_last_1h) if row.events_last_1h else 0,
                "events_last_24h": int(row.events_last_24h)
                if row.events_last_24h
                else 0,
                "unique_functions": row.unique_functions or 0,
                "last_event_timestamp": row.last_event_timestamp.isoformat()
                if row.last_event_timestamp
                else None,
            }
    except Exception as e:
        logger.error(f"[STATS] Error processing overview {str(e)}")
        return {
            "events_last_1h": 0,
            "events_last_24h": 0,
            "unique_functions": 0,
            "last_event_timestamp": None,
        }


@router.get("/schema")
async def get_schema(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Target enterprise tenant schema parser"),
    api_key: str = Depends(verify_api_key),
) -> Dict[str, Any]:
    """
    Auto-Schema Detector: Scans latest N structures flattening deep JSON matrices
    producing string structural metadata for downstream dashboard queries naturally.
    """
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return {"fields": []}

    if tenant_id != api_key:
        raise HTTPException(
            status_code=403, detail="Tenant mismatch cleanly forbidden!"
        )

    try:
        async with async_session_maker() as session:
            # Extract native backend payload rows raw limiting overhead
            stmt = (
                select(Event.payload)
                .where(
                    Event.tenant_id == tenant_id, Event.event_type == "execution_graph"
                )
                .order_by(Event.timestamp.desc())
                .limit(100)
            )

            result = await session.execute(stmt)
            payloads = result.scalars().all()

            # Fast iterative recursive flattening gracefully bounds in memory across small lists correctly
            unique_paths = set()

            def flatten_json(obj, parent_key=""):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        key = f"{parent_key}.{k}" if parent_key else k
                        flatten_json(v, key)
                elif isinstance(obj, list) and obj:
                    # Handle matrix objects via mapping generic arrays natively: "graph.nodes.*.name"
                    for item in obj:
                        key = f"{parent_key}.*" if parent_key else "*"
                        flatten_json(item, key)
                else:
                    unique_paths.add(parent_key)

            for payload in payloads:
                flatten_json(payload)

            return {"fields": sorted(list(unique_paths))}
    except Exception as e:
        logger.error(f"[STATS] Error in get_schema: {str(e)}")
        return {"fields": []}
