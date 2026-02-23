import os
from fastapi import APIRouter

router = APIRouter(tags=["Monitoring"])


@router.options("/health")
async def health_options():
    """Allow basic OPTIONS checks from load balancers and proxies."""
    return {"status": "ok"}


@router.get("/health")
async def health_check():
    """Liveness probe reporting backend availability. Keep it lightweight to satisfy Railway LB."""
    _DATABASE_URL = os.getenv("DATABASE_URL")
    if not _DATABASE_URL:
        return {"status": "ok", "db": "not configured"}

    # We no longer perform an explicit blocking asyncpg.connect() here.
    # The application gracefully handles DB outages at the route level.
    # We just need to signal to the Load Balancer that the Python process is alive.
    return {"status": "ok", "db": "configured"}
