from fastapi import APIRouter, Depends
from app.api.auth import verify_api_key

from app.models.ingestion import IngestionPayload
from app.services.ingestion_service import IngestionService
import logging

logger = logging.getLogger("temporallayr.api.ingest")

router = APIRouter(prefix="/v1", tags=["Ingestion"])


# Global singleton dependencies injected over route layouts dynamically per app instance.
def get_ingestion_service() -> IngestionService:
    from app.main import ingestion_service

    return ingestion_service


@router.post("/ingest")
async def ingest(
    payload: IngestionPayload,
    api_key=Depends(verify_api_key),
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Ingest arrays of execution context mappings parsing nested traces.
    Validated payloads are offloaded directly to asynchronous background queue dispatch handlers natively isolating critical IO.
    """
    logger.info(
        "INGEST_RECEIVED",
        extra={
            "event_count": len(payload.events),
            "tenant": payload.tenant_id if hasattr(payload, "tenant_id") else None,
        },
    )
    print(f"[INGEST RECEIVED] events={len(payload.events)}")

    if not payload.events:
        return {
            "status": "ok",
            "ingested": 0,
            "message": "No events provided in payload array.",
        }

    # Note: Tenant ID handling inside the enqueue can be null or a fixed tenant if not present
    tenant_id = payload.tenant_id if hasattr(payload, "tenant_id") else "tenant_default"
    await service.enqueue(tenant_id, payload.events)

    return {
        "status": "accepted",
        "ingested": len(payload.events),
        "message": "Events successfully queued for background flushing explicitly.",
    }
