from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["Monitoring"])


@router.options("/health")
async def health_options():
    """Allow basic OPTIONS checks from load balancers and proxies."""
    return {"status": "ok"}


@router.get("/health", response_class=JSONResponse, summary="Static Health Check")
async def health_check(request: Request):
    """
    Returns 200 instantly. Never touches the DB.
    """
    return {"status": "ok"}
