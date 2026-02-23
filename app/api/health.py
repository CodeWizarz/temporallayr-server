import os
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text

router = APIRouter(tags=["Monitoring"])


@router.get("/health")
async def health_check():
    """Liveness probe reporting backend availability explicitly."""
    try:
        _DATABASE_URL = os.getenv("DATABASE_URL")
        if not _DATABASE_URL:
            return {"status": "ok", "db": "not configured"}

        _sync_url = _DATABASE_URL.replace("postgresql+asyncpg", "postgresql")
        _engine = create_engine(
            _sync_url,
            pool_pre_ping=True,
            pool_timeout=5,
            connect_args={"connect_timeout": 5},
        )
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        _engine.dispose()
        return {"status": "ok", "db": "connected"}
    except Exception:
        return JSONResponse(
            status_code=200, content={"status": "ok", "db": "unavailable"}
        )
