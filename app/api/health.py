import os
import asyncio
import asyncpg
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(tags=["Monitoring"])


@router.options("/health")
async def health_options():
    """Allow basic OPTIONS checks from load balancers and proxies."""
    return {"status": "ok"}


@router.get("/health")
async def health_check():
    """Liveness probe reporting backend availability explicitly."""
    try:
        _DATABASE_URL = os.getenv("DATABASE_URL")
        if not _DATABASE_URL:
            return {"status": "ok", "db": "not configured"}

        _asyncpg_url = _DATABASE_URL.replace("postgresql+asyncpg", "postgresql")
        _conn = None
        try:
            _conn = await asyncio.wait_for(asyncpg.connect(_asyncpg_url, timeout=5), timeout=6)
            await asyncio.wait_for(_conn.execute("SELECT 1"), timeout=2)
        finally:
            if _conn is not None:
                await _conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception:
        return JSONResponse(
            status_code=200, content={"status": "ok", "db": "unavailable"}
        )
