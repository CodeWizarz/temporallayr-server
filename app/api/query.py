from fastapi import APIRouter, Depends, Request

from app.models.query import QueryPayload, QueryResponse, DiffPayload

from app.api.auth import verify_api_key

import logging

logger = logging.getLogger("temporallayr.api.query")

router = APIRouter(tags=["Querying"])


def get_storage_service():
    from app.services.storage_service import StorageService

    # Singleton bound mappings mapping native downstream connections gracefully.
    return StorageService()


@router.post("/query", response_model=QueryResponse, status_code=200)
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


@router.get("/search")
async def search_executions(
    request: Request,
    tenant_id: str,
    start_time: str | None = None,
    end_time: str | None = None,
    node_name: str | None = None,
    limit: int = 50,
    offset: int = 0,
    api_key=Depends(verify_api_key),
    storage=Depends(get_storage_service),
):
    print(f"[SEARCH] tenant={tenant_id} node={node_name}")
    from datetime import datetime

    start_dt = None
    if start_time:
        try:
            start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        except ValueError:
            pass

    end_dt = None
    if end_time:
        try:
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        except ValueError:
            pass

    result = await storage.search_executions(
        tenant_id=tenant_id,
        start_time=start_dt,
        end_time=end_dt,
        node_name=node_name,
        limit=limit,
        offset=offset,
    )
    return result


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
