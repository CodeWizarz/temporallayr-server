from fastapi import APIRouter, Depends
from app.api.ingest import verify_api_key

router = APIRouter(tags=["Monitoring"])


@router.get("/handshake")
async def handshake(tenant_id: str = Depends(verify_api_key)):
    """SDK handshake endpoint for connectivity validation."""
    return {"status": "ok", "service": "temporallayr-server", "version": "1.0"}
