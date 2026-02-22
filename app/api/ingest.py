from fastapi import APIRouter, Depends, Request

from app.models.ingestion import IngestionPayload
from app.services.ingestion_service import IngestionService
import logging

logger = logging.getLogger("temporallayr.api.ingest")

router = APIRouter(prefix="/v1", tags=["Ingestion"])


# Global singleton dependencies injected over route layouts dynamically per app instance.
def get_ingestion_service() -> IngestionService:
    from app.main import ingestion_service

    return ingestion_service


@router.post("/ingest", status_code=202)
async def ingest_telemetry_batch(
    request: Request,
    payload: IngestionPayload,
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Ingest arrays of execution context mappings parsing nested traces.
    Validated payloads are offloaded directly to asynchronous background queue dispatch handlers natively isolating critical IO.
    """
    if not payload.events:
        return {
            "status": "ok",
            "ingested": 0,
            "message": "No events provided in payload array.",
        }

    await service.enqueue(request.state.api_key, payload.events)

    return {
        "status": "accepted",
        "ingested": len(payload.events),
        "message": "Events successfully queued for background flushing explicitly.",
    }
