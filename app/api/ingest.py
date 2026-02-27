from fastapi import APIRouter, Request, Depends, Response
from app.api.auth import verify_api_key

from app.models.ingestion import IngestionPayload
from app.services.ingestion_service import IngestionService
import logging

logger = logging.getLogger("temporallayr.api.ingest")

router = APIRouter(tags=["Ingestion"])


# Global singleton dependencies injected over route layouts dynamically per app instance.
def get_ingestion_service() -> IngestionService:
    from app.main import ingestion_service

    return ingestion_service


@router.post("/ingest")
async def ingest(
    request: Request,
    response: Response,
    payload: IngestionPayload,
    service: IngestionService = Depends(get_ingestion_service),
):
    if getattr(request.app.state, "db_status", "unknown") != "connected":
        response.headers["X-DB-Status"] = getattr(
            request.app.state, "db_status", "unknown"
        )
        return {
            "status": "accepted",
            "ingested": len(payload.events),
            "message": "Database disconnected. Events dropped.",
        }
    """
    Ingest arrays of execution context mappings parsing nested traces.
    Accepts auth via Authorization: Bearer <key> header OR body api_key field.
    """
    # [DEBUG] Log raw auth inputs before validation
    print(
        f"[INGEST DEBUG]\n"
        f"  header_auth={request.headers.get('authorization') or request.headers.get('Authorization')}\n"
        f"  body_key={payload.api_key}"
    )

    # Auth: pass body key from parsed payload to avoid body double-read
    tenant_id = await verify_api_key(request, api_key_from_body=payload.api_key)

    logger.info(
        "INGEST_RECEIVED",
        extra={
            "event_count": len(payload.events),
            "tenant": tenant_id,
        },
    )
    print(f"[INGEST RECEIVED] events={len(payload.events)} tenant={tenant_id}")

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
