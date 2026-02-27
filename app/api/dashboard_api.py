import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, Query, HTTPException, Request, Response
from sqlalchemy import select, func, text, desc, cast, Float, Integer

from app.api.auth import verify_api_key
from app.core.database import async_session_maker
from app.models.event import Event
from app.models.dashboard_api import (
    StandardDashboardResponse,
    ResponseMeta,
    DashboardSearchRequest,
    DashboardQueryRequest,
)

logger = logging.getLogger("temporallayr.api.dashboard_ext")

router = APIRouter(prefix="/v1/dashboard", tags=["Dashboard Ext"])


def wrap_response(
    start_time: float, data: Any = None, error: str = None, next_cursor: str = None
) -> StandardDashboardResponse:
    query_ms = round((time.perf_counter() - start_time) * 1000, 2)
    meta = ResponseMeta(query_ms=query_ms)
    if error:
        return StandardDashboardResponse(ok=False, error=error, meta=meta)
    return StandardDashboardResponse(
        ok=True, data=data, next_cursor=next_cursor, meta=meta
    )


@router.get("/ready", response_model=StandardDashboardResponse)
async def health_check():
    """Health Check querying internal Postgres states smoothly!"""
    start_time = time.perf_counter()
    status = {"status": "ready", "database": False, "ingest": True, "query": True}

    try:
        async with async_session_maker() as session:
            await session.execute(text("SELECT 1"))
            status["database"] = True
    except Exception:
        pass

    return wrap_response(start_time, data=status)


@router.post("/search", response_model=StandardDashboardResponse)
async def search_events(
    payload: DashboardSearchRequest,
    request: Request,
    response: Response,
    api_key: str = Depends(verify_api_key),
):
    """Cursor-bound UI pagination search effectively isolating OFFSET performance drag."""
    start_time = time.perf_counter()
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return wrap_response(start_time, data=[])

    if payload.tenant_id != api_key:
        return wrap_response(start_time, error="Tenant mismatch gracefully forbidden!")

    try:
        async with async_session_maker() as session:
            stmt = select(Event).where(Event.tenant_id == payload.tenant_id)

            if payload.time_from:
                stmt = stmt.where(Event.timestamp >= payload.time_from)
            if payload.time_to:
                stmt = stmt.where(Event.timestamp <= payload.time_to)

            if payload.contains:
                contains_str = payload.contains.replace("'", "''")
                stmt = stmt.where(text(f"payload::text ILIKE '%{contains_str}%'"))

            if payload.filters:
                for key, val in payload.filters.items():
                    if val is not None:
                        stmt = stmt.where(Event.payload.op("->>")(key) == str(val))

            # Cursor pagination mapping tuple comparisons cleanly!
            # Cursor Format: "1678888_uuid"
            if payload.cursor:
                try:
                    parts = payload.cursor.split("_")
                    if len(parts) == 2:
                        cursor_ts = datetime.fromisoformat(parts[0])
                        cursor_id = parts[1]
                        stmt = stmt.where(
                            (Event.timestamp < cursor_ts)
                            | ((Event.timestamp == cursor_ts) & (Event.id < cursor_id))
                        )
                except Exception as e:
                    logger.warning(f"Invalid cursor format: {e}")

            # Add 1 to limit checking next page logically
            limit_to_fetch = payload.limit + 1
            stmt = stmt.order_by(desc(Event.timestamp), desc(Event.id)).limit(
                limit_to_fetch
            )

            result = await session.execute(stmt)
            rows = result.scalars().all()

            next_cursor = None
            if len(rows) > payload.limit:
                last = rows[payload.limit - 1]
                next_cursor = f"{last.timestamp.isoformat()}_{str(last.id)}"
                rows = rows[: payload.limit]  # Slice correctly

            results = []
            for r in rows:
                event_data = {
                    "id": str(r.id),
                    "timestamp": r.timestamp.isoformat() if r.timestamp else None,
                    "event_type": r.event_type,
                    "payload": r.payload,
                }

                # Select bounding dynamically dropping excess metrics
                if payload.select:
                    event_data = {
                        k: v for k, v in event_data.items() if k in payload.select
                    }

                results.append(event_data)

        return wrap_response(start_time, data=results, next_cursor=next_cursor)
    except Exception as e:
        logger.error(f"[DASHBOARD_EXT] Error in search_events: {str(e)}")
        return wrap_response(start_time, data=[])


@router.post("/query", response_model=StandardDashboardResponse)
async def query_aggregation(
    payload: DashboardQueryRequest,
    request: Request,
    response: Response,
    api_key: str = Depends(verify_api_key),
):
    """Pipeline aggregation engine seamlessly mapping UI configurations natively into PG."""
    start_time = time.perf_counter()
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return wrap_response(start_time, data=[])

    if payload.tenant_id != api_key:
        return wrap_response(start_time, error="Tenant mismatch gracefully forbidden!")

    try:
        async with async_session_maker() as session:
            # Pipeline translator mapped dynamically
            selections = []
            groupings = []

            for stage in payload.pipeline:
                if stage.group_by:
                    validator = (
                        stage.group_by
                        if stage.group_by in {"minute", "hour", "day"}
                        else "hour"
                    )
                    bucket = func.date_trunc(validator, Event.timestamp).label(
                        "time_bucket"
                    )
                    selections.append(bucket)
                    groupings.append(bucket)
                if stage.count:
                    selections.append(func.count().label("count"))

            if not selections:
                return wrap_response(
                    start_time, error="Empty pipeline operations explicitly failed."
                )

            stmt = select(*selections).where(Event.tenant_id == payload.tenant_id)

            if payload.time_from:
                stmt = stmt.where(Event.timestamp >= payload.time_from)
            if payload.time_to:
                stmt = stmt.where(Event.timestamp <= payload.time_to)

            if groupings:
                stmt = stmt.group_by(*groupings)
                stmt = stmt.order_by(groupings[0].asc())

            result = await session.execute(stmt)
            rows = result.all()

            results = []
            for r in rows:
                item = {}
                if hasattr(r, "time_bucket"):
                    item["time_bucket"] = (
                        r.time_bucket.isoformat() if r.time_bucket else None
                    )
                item["count"] = getattr(r, "count", 0)
                results.append(item)

        return wrap_response(start_time, data=results)
    except Exception as e:
        logger.error(f"[DASHBOARD_EXT] Error in query_aggregation: {str(e)}")
        return wrap_response(start_time, data=[])


def _safe_wrap_sync(start_time: float, f):
    try:
        data = f()
        return wrap_response(start_time, data=data)
    except Exception as e:
        return wrap_response(start_time, error=str(e))


@router.get("/overview", response_model=StandardDashboardResponse)
async def wrapper_overview(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="dashboard tenant"),
    api_key: str = Depends(verify_api_key),
):
    start_time = time.perf_counter()
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return wrap_response(
            start_time,
            data={
                "events_last_1h": 0,
                "events_last_24h": 0,
                "unique_functions": 0,
                "last_event_timestamp": None,
            },
        )

    if tenant_id != api_key:
        return wrap_response(start_time, error="Tenant mismatch!")

    try:
        async with async_session_maker() as session:
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
                func.count(
                    func.distinct(Event.payload.op("->>")("function_name"))
                ).label("unique_functions"),
                func.max(Event.timestamp).label("last_event_timestamp"),
            ).where(Event.tenant_id == tenant_id, Event.event_type == "execution_graph")

            result = await session.execute(stmt)
            row = result.first()
            data = {
                "events_last_1h": int(row.events_last_1h) if row.events_last_1h else 0,
                "events_last_24h": int(row.events_last_24h)
                if row.events_last_24h
                else 0,
                "unique_functions": row.unique_functions or 0,
                "last_event_timestamp": row.last_event_timestamp.isoformat()
                if row.last_event_timestamp
                else None,
            }
        return wrap_response(start_time, data=data)
    except Exception as e:
        logger.error(f"[DASHBOARD_EXT] Error in wrapper_overview: {str(e)}")
        return wrap_response(
            start_time,
            data={
                "events_last_1h": 0,
                "events_last_24h": 0,
                "unique_functions": 0,
                "last_event_timestamp": None,
            },
        )


@router.get("/schema", response_model=StandardDashboardResponse)
async def wrapper_schema(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="dashboard tenant"),
    api_key: str = Depends(verify_api_key),
):
    start_time = time.perf_counter()
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return wrap_response(start_time, data={"fields": []})

    if tenant_id != api_key:
        return wrap_response(start_time, error="Tenant mismatch!")

    try:
        async with async_session_maker() as session:
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

            unique_paths = set()

            def flatten_json(obj, parent_key=""):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        key = f"{parent_key}.{k}" if parent_key else k
                        flatten_json(v, key)
                elif isinstance(obj, list) and obj:
                    for item in obj:
                        key = f"{parent_key}.*" if parent_key else "*"
                        flatten_json(item, key)
                else:
                    unique_paths.add(parent_key)

            for payload in payloads:
                flatten_json(payload)

            data = {"fields": sorted(list(unique_paths))}
        return wrap_response(start_time, data=data)
    except Exception as e:
        logger.error(f"[DASHBOARD_EXT] Error in wrapper_schema: {str(e)}")
        return wrap_response(start_time, data={"fields": []})


@router.get("/stats/top-functions", response_model=StandardDashboardResponse)
async def wrapper_top_functions(
    request: Request,
    response: Response,
    tenant_id: str = Query(...),
    api_key: str = Depends(verify_api_key),
):
    start_time = time.perf_counter()
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return wrap_response(start_time, data=[])

    if tenant_id != api_key:
        return wrap_response(start_time, error="Tenant mismatch!")

    try:
        async with async_session_maker() as session:
            nodes_element = func.jsonb_array_elements(Event.payload["nodes"]).alias(
                "node"
            )
            node_name = func.jsonb_extract_path_text(
                nodes_element.column, "name"
            ).label("name")

            stmt = (
                select(node_name, func.count().label("count"))
                .select_from(Event)
                .outerjoin(nodes_element, True)
                .where(
                    Event.tenant_id == tenant_id, Event.event_type == "execution_graph"
                )
                .group_by(node_name)
                .order_by(func.count().desc())
                .limit(10)
            )
            result = await session.execute(stmt)
            data = [
                {"name": row.name, "count": row.count}
                for row in result.all()
                if row.name is not None
            ]

        return wrap_response(start_time, data=data)
    except Exception as e:
        logger.error(f"[DASHBOARD_EXT] Error in wrapper_top_functions: {str(e)}")
        return wrap_response(start_time, data=[])


@router.get("/stats/errors", response_model=StandardDashboardResponse)
async def wrapper_errors(
    request: Request,
    response: Response,
    tenant_id: str = Query(...),
    api_key: str = Depends(verify_api_key),
):
    start_time = time.perf_counter()
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return wrap_response(
            start_time, data={"total_events": 0, "error_events": 0, "error_rate": 0.0}
        )

    if tenant_id != api_key:
        return wrap_response(start_time, error="Tenant mismatch!")

    try:
        async with async_session_maker() as session:
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

            data = {"total_events": total, "error_events": errors, "error_rate": rate}

        return wrap_response(start_time, data=data)
    except Exception as e:
        logger.error(f"[DASHBOARD_EXT] Error in wrapper_errors: {str(e)}")
        return wrap_response(
            start_time, data={"total_events": 0, "error_events": 0, "error_rate": 0.0}
        )


@router.get("/stats/durations", response_model=StandardDashboardResponse)
async def wrapper_durations(
    request: Request,
    response: Response,
    tenant_id: str = Query(...),
    api_key: str = Depends(verify_api_key),
):
    start_time = time.perf_counter()
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return wrap_response(
            start_time,
            data={
                "avg_duration_ms": 0.0,
                "p95_duration_ms": 0.0,
                "max_duration_ms": 0.0,
            },
        )

    if tenant_id != api_key:
        return wrap_response(start_time, error="Tenant mismatch!")

    try:
        async with async_session_maker() as session:
            nodes_element = func.jsonb_array_elements(Event.payload["nodes"]).alias(
                "node"
            )
            duration_cast = func.cast(
                func.jsonb_extract_path_text(
                    nodes_element.column, "metadata", "output", "duration_ms"
                ),
                Float,
            )

            stmt = (
                select(
                    func.avg(duration_cast).label("avg_duration_ms"),
                    func.max(duration_cast).label("max_duration_ms"),
                    func.percentile_cont(0.95)
                    .within_group(duration_cast.asc())
                    .label("p95_duration_ms"),
                )
                .select_from(Event)
                .outerjoin(nodes_element, True)
                .where(
                    Event.tenant_id == tenant_id, Event.event_type == "execution_graph"
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

            data = {
                "avg_duration_ms": round_safe(row.avg_duration_ms),
                "p95_duration_ms": round_safe(row.p95_duration_ms),
                "max_duration_ms": round_safe(row.max_duration_ms),
            }
        return wrap_response(start_time, data=data)
    except Exception as e:
        logger.error(f"[DASHBOARD_EXT] Error in wrapper_durations: {str(e)}")
        return wrap_response(
            start_time,
            data={
                "avg_duration_ms": 0.0,
                "p95_duration_ms": 0.0,
                "max_duration_ms": 0.0,
            },
        )
