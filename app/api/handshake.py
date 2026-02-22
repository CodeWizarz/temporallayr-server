from fastapi import APIRouter, Depends
from app.core.auth import verify_api_key

router = APIRouter()


@router.get("/handshake")
async def handshake(auth=Depends(verify_api_key)):
    """SDK handshake endpoint for connectivity validation."""
    return {"status": "ok"}
