from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.models.ingestion import IngestionPayload
from app.services.ingestion_service import IngestionService
import logging

logger = logging.getLogger("temporallayr.api.ingest")

router = APIRouter(prefix="/v1", tags=["Ingestion"])
security = HTTPBearer()

# In-memory mock mapping for tenant configurations. Real implementation connects to DB.
MOCK_TENANT_STORE = {
    "sk_live_123456789": "tenant_alpha_001",
    "sk_test_987654321": "tenant_beta_002",
}


def verify_api_key(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> str:
    """Validate Bearer tokens against the internal storage mechanisms iteratively mapping Tenant IDs."""
    token = credentials.credentials
    tenant_id = MOCK_TENANT_STORE.get(token)

    if not tenant_id:
        logger.warning(
            f"Unauthorized ingestion attempt with masking token ending in '...{token[-4:] if len(token) > 4 else token}'"
        )
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired authentication credentials strictly blocked.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return tenant_id


# Global singleton dependencies injected over route layouts dynamically per app instance.
def get_ingestion_service() -> IngestionService:
    from app.main import ingestion_service

    return ingestion_service


@router.post("/ingest", status_code=202)
async def ingest_telemetry_batch(
    payload: IngestionPayload,
    tenant_id: str = Depends(verify_api_key),
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

    await service.enqueue(tenant_id, payload.events)

    return {
        "status": "accepted",
        "ingested": len(payload.events),
        "message": "Events successfully queued for background flushing explicitly.",
    }
