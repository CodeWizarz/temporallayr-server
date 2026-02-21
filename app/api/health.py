from fastapi import APIRouter

router = APIRouter(tags=["Monitoring"])


@router.get("/health")
async def health_check():
    """Liveness probe reporting backend availability explicitly."""
    return {"status": "ok", "service": "temporallayr-server"}
