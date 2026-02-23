from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.core.database import engine

router = APIRouter(tags=["Monitoring"])


@router.get("/health")
async def health_check():
    """Liveness probe reporting backend availability explicitly."""
    try:
        if not engine:
            return JSONResponse(
                status_code=500, content={"status": "error", "db": "disconnected"}
            )

        async with engine.begin() as conn:
            pass
        return {"status": "ok", "db": "connected"}
    except Exception:
        return JSONResponse(
            status_code=500, content={"status": "error", "db": "disconnected"}
        )
