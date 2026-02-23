import asyncio
import logging
from typing import Dict, Any, List

from app.query.engine import query_engine
from app.dashboard.service import dashboard_service
from app.query.models import MultiResourceQueryRequest

logger = logging.getLogger("temporallayr.query.runtime")


async def execute_saved_query(saved_query_id: str, tenant_id: str) -> Dict[str, Any]:
    """Dynamically converts a bound JSON Query structural layout into an execution stream cleanly over the backend."""
    # Note: dashboard_service list_saved_queries validates tenant mapping natively,
    # but we will just manually fetch the single query via DB. Oh wait, dashboard_service
    # doesn't have a `get_saved_query` natively right now. Let's add that or fetch it functionally.
    queries = await dashboard_service.list_saved_queries(tenant_id=tenant_id)
    target_query = next((q for q in queries if str(q.id) == saved_query_id), None)

    if not target_query:
        raise ValueError(
            f"SavedQuery {saved_query_id} not found or tenant isolation blocked access."
        )

    # Cast raw JSONB structural properties back onto Pydantic Models dynamically shielding bounds.
    raw_query = target_query.query_json

    # We enforce tenant_id strictly replacing whatever is originally there validating isolation statically
    raw_query["tenant_id"] = tenant_id

    # Time-Series aggregation bypass mapping
    query_type = raw_query.get("type", "raw")
    if query_type == "timeseries":
        from datetime import datetime
        from app.query.timeseries import aggregate_timeseries

        # Parse ISO strings into timezone-aware datetimes
        start_str = raw_query.get("start_time", "2026-01-01T00:00:00Z").replace(
            "Z", "+00:00"
        )
        end_str = raw_query.get("end_time", "2026-12-31T00:00:00Z").replace(
            "Z", "+00:00"
        )

        res = await aggregate_timeseries(
            tenant_id=tenant_id,
            start_time=datetime.fromisoformat(start_str),
            end_time=datetime.fromisoformat(end_str),
            interval_seconds=int(raw_query.get("interval_seconds", 3600)),
            metric=raw_query.get("metric", "execution_count"),
            filters=raw_query.get("filters", {}),
        )
        return res

    # If limit is not given natively, set safe default resolving over query blocks natively.
    if "limit" not in raw_query:
        raw_query["limit"] = 100

    mr_req = MultiResourceQueryRequest(**raw_query)

    # Dispatch structural mapping block securely via QueryEngine
    query_result = await query_engine.query(mr_req)

    return [d.model_dump() for d in query_result.data]


async def _run_panel_safe(panel, tenant_id: str) -> Dict[str, Any]:
    """Wraps panels running inherently handling cascading 10-second fault architectures."""
    panel_id_str = str(panel["panel_id"])
    query_id_str = str(panel["saved_query"]["id"])
    logger.info(f"[PANEL QUERY START] panel={panel_id_str} query={query_id_str}")

    try:
        data = await asyncio.wait_for(
            execute_saved_query(saved_query_id=query_id_str, tenant_id=tenant_id),
            timeout=10.0,
        )
        logger.info(f"[PANEL QUERY DONE] panel={panel_id_str} results={len(data)}")
        return {"panel_id": panel_id_str, "name": panel["name"], "data": data}
    except asyncio.TimeoutError:
        logger.error(f"[PANEL QUERY TIMEOUT] panel={panel_id_str} query={query_id_str}")
        return {
            "panel_id": panel_id_str,
            "name": panel["name"],
            "data": [],
            "error": "Query execution timed out after 10s organically.",
        }
    except Exception as e:
        logger.error(f"[PANEL QUERY FAULT] panel={panel_id_str} error={str(e)}")
        return {
            "panel_id": panel_id_str,
            "name": panel["name"],
            "data": [],
            "error": str(e),
        }


async def execute_dashboard(dashboard_id: str, tenant_id: str) -> Dict[str, Any]:
    """Generates structural mapped queries cascading asynchronously avoiding structural traps cleanly."""
    logger.info(f"[DASHBOARD RUN START] dashboard={dashboard_id} tenant={tenant_id}")

    dashboard_data = await dashboard_service.get_dashboard_with_panels(
        tenant_id=tenant_id, dashboard_id=dashboard_id
    )

    if not dashboard_data:
        raise ValueError(f"Dashboard {dashboard_id} not found natively")

    panels = dashboard_data.get("panels", [])

    # Run panels concurrently gathering organically catching local traps
    tasks = [_run_panel_safe(panel=p, tenant_id=tenant_id) for p in panels]
    panel_results = await asyncio.gather(*tasks)

    logger.info(
        f"[DASHBOARD RUN COMPLETE] dashboard={dashboard_id} completed_panels={len(panel_results)}"
    )

    return {"dashboard_id": dashboard_id, "panels": panel_results}
