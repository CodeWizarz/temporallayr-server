from fastapi import APIRouter, Depends, Request

from app.models.query import QueryPayload, QueryResponse

import logging

logger = logging.getLogger("temporallayr.api.query")

router = APIRouter(prefix="/v1", tags=["Querying"])


def get_storage_service():
    from app.services.storage_service import StorageService

    # Singleton bound mappings mapping native downstream connections gracefully.
    return StorageService()


@router.post("/query", response_model=QueryResponse, status_code=200)
async def query_telemetry_history(
    request: Request,
    payload: QueryPayload,
    storage=Depends(get_storage_service),
):
    """
    Paginated telemetry extraction endpoint natively scanning backend bounded payloads defensively.
    """
    logger.info(
        f"Querying temporal traces mapping tenant={request.state.api_key} limit={payload.limit} bounds [{payload.from_time} -> {payload.to_time}]"
    )

    events = await storage.query_events(
        tenant_id=request.state.api_key,
        limit=payload.limit,
        from_time=payload.from_time,
        to_time=payload.to_time,
    )

    return QueryResponse(events=events)
