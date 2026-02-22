from fastapi import APIRouter, Depends
from app.api.auth import extract_api_key

router = APIRouter(tags=["Monitoring"])


@router.get("/handshake")
async def handshake(tenant_id: str = Depends(extract_api_key)):
    """SDK handshake endpoint for connectivity validation."""
    return {"status": "ok", "server": "temporallayr"}
