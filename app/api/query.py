from fastapi import APIRouter, Depends, Request

from app.models.query import (
    QueryPayload,
    QueryResponse,
    DiffPayload,
    SearchRequest,
    CreateAlertRequest,
)

from app.query.models import QueryRequest

from app.api.auth import verify_api_key

import logging

logger = logging.getLogger("temporallayr.api.query")

router = APIRouter(tags=["Querying"])


def get_storage_service():
    from app.services.storage_service import StorageService

    # Singleton bound mappings mapping native downstream connections gracefully.
    return StorageService()


@router.post("/telemetry", response_model=QueryResponse, status_code=200)
async def query_telemetry_history(
    payload: QueryPayload,
    auth=Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    """
    Paginated telemetry extraction endpoint natively scanning backend bounded payloads defensively.
    """
    tenant_id = payload.tenant_id if hasattr(payload, "tenant_id") else "tenant_default"
    logger.info(
        f"Querying temporal traces mapping tenant={tenant_id} limit={payload.limit} bounds [{payload.from_time} -> {payload.to_time}]"
    )

    events = await storage.query_events(
        tenant_id=tenant_id,
        limit=payload.limit,
        from_time=payload.from_time,
        to_time=payload.to_time,
    )

    return QueryResponse(events=events)


@router.post("/query", status_code=200)
async def query_analytics(
    payload: QueryRequest,
    api_key: str = Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    """
    Production Analytics execution query exposing structural aggregations dynamically bounds safely.
    """
    from fastapi import HTTPException
    from app.query.service import query_events

    payload.tenant_id = api_key

    # Restrict enterprise queries over 1000 dynamically to protect DB overhead natively
    if payload.limit > 1000:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 1000")

    result = await query_events(payload, storage_engine=storage)

    if "error" in result:
        # Propagate strict bounding faults natively cleanly converting specific string payloads
        if result["error"] == "query timeout":
            return result
        raise HTTPException(status_code=500, detail=result["error"])

    # Returns native output format correctly: {"results": [...], "count": int}
    return result


@router.get("/executions")
async def get_executions(
    request: Request,
    tenant_id: str,
    limit: int = 50,
    offset: int = 0,
    api_key=Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    print(f"[INDEX QUERY] tenant={tenant_id} offset={offset}")
    result = await storage.list_executions(
        tenant_id=tenant_id, limit=limit, offset=offset
    )
    return result


@router.post("/search")
async def search_executions(
    request: Request,
    payload: SearchRequest,
    api_key: str = Depends(verify_api_key),
):
    # Enforce strict tenant isolation by mapping ID natively from token auth
    tenant_id = api_key
    print(f"[SEARCH] tenant={tenant_id} function={payload.function_name}")

    from app.services.search import search_executions as do_search_executions

    results = await do_search_executions(
        tenant_id=tenant_id,
        function_name=payload.function_name,
        start_time=payload.start_time,
        end_time=payload.end_time,
        limit=payload.limit,
        offset=payload.offset,
    )

    # Wrap in expected response dict shape
    return {"results": results}


@router.get("/incidents", response_model=None)
async def get_incidents(
    request: Request,
    limit: int = 50,
    offset: int = 0,
    api_key: str = Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    tenant_id = api_key
    print(f"[INCIDENTS QUERY] tenant={tenant_id} limit={limit} offset={offset}")

    results = await storage.list_incidents(
        tenant_id=tenant_id, limit=limit, offset=offset
    )
    return {"incidents": results}


@router.post("/alerts")
async def create_alert(
    request: Request,
    payload: CreateAlertRequest,
    api_key: str = Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    from fastapi import HTTPException

    tenant_id = api_key
    webhook_str = str(payload.webhook_url) if payload.webhook_url else None
    print(
        f"[ALERT CREATION] tenant={tenant_id} name={payload.name} webhook={webhook_str}"
    )

    success = await storage.create_alert_rule(
        tenant_id=tenant_id,
        name=payload.name,
        failure_type=payload.failure_type,
        node_name=payload.node_name,
        webhook_url=webhook_str,
    )

    if not success:
        raise HTTPException(
            status_code=500, detail="Failed persisting alert rule constraint natively."
        )

    return {"status": "ok"}


@router.get("/executions/{execution_id}")
async def get_execution(
    request: Request,
    execution_id: str,
    tenant_id: str,
    api_key=Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    print(f"[QUERY] tenant={tenant_id}")
    execution = await storage.get_execution(
        tenant_id=tenant_id, execution_id=execution_id
    )
    if not execution:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Execution not found")
    return execution


@router.post("/replay/{execution_id}")
async def replay_execution(
    request: Request,
    execution_id: str,
    tenant_id: str,
    api_key=Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    print(f"[REPLAY] execution={execution_id}")
    execution = await storage.get_execution(
        tenant_id=tenant_id, execution_id=execution_id
    )
    if not execution:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Execution not found")

    nodes = execution.get("nodes", [])

    # Structural topological sort mapping dependencies explicitly
    ordered_nodes = []
    visited = set()
    node_by_id = {}

    for n in nodes:
        node_id = n.get("id") or n.get("name")
        if node_id:
            node_by_id[node_id] = n

    def visit(node_id, current_path=None):
        if current_path is None:
            current_path = set()
        if node_id in visited:
            return
        if node_id in current_path:
            return  # Cycle fallback

        n = node_by_id.get(node_id)
        if not n:
            return

        parent_id = n.get("parent_id")
        if parent_id and parent_id in node_by_id:
            current_path.add(node_id)
            visit(parent_id, current_path)
            current_path.remove(node_id)

        visited.add(node_id)
        ordered_nodes.append(n)

    for n in nodes:
        node_id = n.get("id") or n.get("name")
        if node_id:
            visit(node_id)

    # Append any unidentified detached leaf topologies
    for n in nodes:
        node_id = n.get("id") or n.get("name")
        if not node_id:
            ordered_nodes.append(n)

    steps = []
    for n in ordered_nodes:
        meta = n.get("metadata", {})
        steps.append(
            {
                "node": n.get("name", "unknown"),
                "inputs": meta.get("inputs", {}),
                "output": meta.get("output", {}),
                "replayed": True,
            }
        )

    return {"execution_id": execution_id, "replayed": True, "steps": steps}


@router.post("/diff")
async def diff_executions(
    request: Request,
    payload: DiffPayload,
    api_key=Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    print(f"[DIFF] comparing {payload.execution_a} vs {payload.execution_b}")
    tenant_id = payload.tenant_id

    exec_a = await storage.get_execution(
        tenant_id=tenant_id, execution_id=payload.execution_a
    )
    exec_b = await storage.get_execution(
        tenant_id=tenant_id, execution_id=payload.execution_b
    )

    if not exec_a or not exec_b:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=404, detail="Execution not found or tenant mismatch"
        )

    nodes_a = exec_a.get("nodes", [])
    nodes_b = exec_b.get("nodes", [])

    def build_map(nodes):
        node_map = {}
        for n in nodes:
            name = n.get("name", "")
            parent_id = n.get("parent_id", "")
            key = f"{name}::{parent_id}"
            node_map[key] = n
        return node_map

    map_a = build_map(nodes_a)
    map_b = build_map(nodes_b)

    differences = []

    # Compare logical structural identities matching across branches
    intersection = set(map_a.keys()).intersection(set(map_b.keys()))

    for key in intersection:
        node_a = map_a[key]
        node_b = map_b[key]

        meta_a = node_a.get("metadata", {})
        meta_b = node_b.get("metadata", {})

        inputs_a = meta_a.get("inputs", {})
        outputs_a = meta_a.get("output", {})

        inputs_b = meta_b.get("inputs", {})
        outputs_b = meta_b.get("output", {})

        inputs_changed = inputs_a != inputs_b
        output_changed = outputs_a != outputs_b

        if inputs_changed or output_changed:
            differences.append(
                {
                    "node": node_a.get("name", "unknown"),
                    "inputs_changed": inputs_changed,
                    "output_changed": output_changed,
                    "old_output": outputs_a,
                    "new_output": outputs_b,
                }
            )

    return {
        "execution_a": payload.execution_a,
        "execution_b": payload.execution_b,
        "total_nodes_compared": len(intersection),
        "differences": differences,
    }


@router.get("/timeline/{execution_id}")
async def get_execution_timeline(
    request: Request,
    execution_id: str,
    tenant_id: str,
    api_key=Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    print(f"[TIMELINE] loaded execution {execution_id}")

    execution = await storage.get_execution(
        tenant_id=tenant_id, execution_id=execution_id
    )

    if not execution:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=404, detail="Execution not found or tenant mismatch"
        )

    nodes = execution.get("nodes", [])
    extracted_nodes = []

    for n in nodes:
        meta = n.get("metadata", {})
        extracted_nodes.append(
            {
                "node_id": n.get("id", ""),
                "name": n.get("name", "unknown"),
                "parent_id": n.get("parent_id"),
                "timestamp": n.get("created_at", ""),
                "inputs": meta.get("inputs", {}),
                "output": meta.get("output", {}),
            }
        )

    # Sort nodes by created_at ascending parsing chronological execution natively
    extracted_nodes.sort(key=lambda item: item["timestamp"] or "")

    timeline = []
    for idx, node_data in enumerate(extracted_nodes):
        event = {"order": idx}
        event.update(node_data)
        timeline.append(event)

    return {"execution_id": execution_id, "timeline": timeline}


# --- Advanced Multi-Resource Query Engine Endpoints ---

from app.query.models import QueryRequest, QueryResult
from app.query.engine import query_engine


@router.post("/query/events", response_model=QueryResult)
async def api_query_events(
    payload: QueryRequest,
    api_key: str = Depends(verify_api_key),
):
    """Scan and fetch real-time traces via dynamic JSON filtering structures."""
    payload.tenant_id = api_key
    return await query_engine.search_events(payload)


@router.post("/query/incidents", response_model=QueryResult)
async def api_query_incidents(
    payload: QueryRequest,
    api_key: str = Depends(verify_api_key),
):
    """Scan alert mappings natively fetching anomalies securely."""
    payload.tenant_id = api_key
    return await query_engine.search_incidents(payload)


@router.post("/query/clusters", response_model=QueryResult)
async def api_query_clusters(
    payload: QueryRequest,
    api_key: str = Depends(verify_api_key),
):
    """Resolve bounds fetching topological aggregates dynamically."""
    payload.tenant_id = api_key
    return await query_engine.search_clusters(payload)


@router.post("/query/nodes", response_model=QueryResult)
async def api_query_nodes(
    payload: QueryRequest,
    api_key: str = Depends(verify_api_key),
):
    """Extract individual JSONB leaf structures spanning deep graph traces."""
    payload.tenant_id = api_key
    return await query_engine.search_nodes(payload)
