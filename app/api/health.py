from fastapi import APIRouter, Request

router = APIRouter(tags=["Monitoring"])


@router.options("/health")
async def health_options():
    """Allow basic OPTIONS checks from load balancers and proxies."""
    return {"status": "ok"}


@router.get("/health")
async def health_check(request: Request):
    """Liveness probe reporting backend availability. Keep it lightweight to satisfy Railway LB."""
    db_status = getattr(request.app.state, "db_status", "unknown")

    return {"status": "ok", "db": db_status}
