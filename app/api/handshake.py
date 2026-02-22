from fastapi import APIRouter, Request

router = APIRouter(tags=["Monitoring"])


@router.get("/handshake")
async def handshake(request: Request):
    """SDK handshake endpoint for connectivity validation."""
    return {"status": "ok", "server": "temporallayr"}
