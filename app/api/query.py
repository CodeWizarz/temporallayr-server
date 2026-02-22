from fastapi import APIRouter, Depends, Request

from app.models.query import QueryPayload, QueryResponse

from app.api.auth import verify_api_key

import logging

logger = logging.getLogger("temporallayr.api.query")

router = APIRouter(prefix="/v1", tags=["Querying"])


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
