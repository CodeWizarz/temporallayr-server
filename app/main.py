import logging
import sys
import asyncio
from app.config import (
    DATABASE_URL,
    PORT,
    API_KEY,
    TEMPORALLAYR_DEMO_API_KEY,
    TEMPORALLAYR_DEMO_TENANT,
)
import uvicorn
from contextlib import asynccontextmanager, suppress

import asyncpg
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.handshake import router as handshake_router
from app.api.health import router as health_router
from app.api import ingest, query, ws, stream, rules
from app.api.dashboard import router_dash as dashboard_router
from app.api.dashboard import router_sq as saved_query_router
from app.api.metrics import router as metrics_router
from app.api.traces import router as traces_router
from app.api.stats import router as stats_router
from app.api.dashboard_api import router as dashboard_ext_router
from app.core.middleware import RequestLoggingMiddleware
from app.services.ingestion_service import IngestionService

# Configure global structured json logging layouts targeting terminal stdout binds natively
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("temporallayr.main")

# Global singleton dependencies bindings initialized on app start boundaries
ingestion_service = IngestionService(max_batch_size=100, flush_interval=1.0)


class BackgroundTaskManager:
    def __init__(self):
        self.tasks = {}

    def start_supervised(self, name: str, coro_func, *args, **kwargs):
        async def supervisor():
            while True:
                try:
                    await coro_func(*args, **kwargs)
                    break  # If cleanly returns, stop supervising
                except asyncio.CancelledError:
                    logger.info(f"Task {name} cancelled cleanly.")
                    break
                except Exception as e:
                    logger.error(
                        f"Supervised task '{name}' crashed: {e}. Restarting in 5s..."
                    )
                    await asyncio.sleep(5)

        task = asyncio.create_task(supervisor(), name=name)
        self.tasks[name] = task
        return task

    async def stop_all(self):
        for name, task in self.tasks.items():
            task.cancel()
        if self.tasks:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.tasks.clear()


background_task_manager = BackgroundTaskManager()


async def _watchdog():
    import os
    import psutil

    process = psutil.Process(os.getpid())
    while True:
        mem_mb = process.memory_info().rss / 1024 / 1024
        db_connections = "disconnected"
        if getattr(app.state, "db_status", None) == "connected":
            try:
                from app.core.database import async_session_maker
                from sqlalchemy import text

                if async_session_maker:
                    async with async_session_maker() as session:
                        result = await session.execute(
                            text(
                                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                            )
                        )
                        db_connections = result.scalar()
            except Exception as e:
                db_connections = f"error: {e}"

        logger.info(
            f"I'm alive - Watchdog | RSS: {mem_mb:.1f}MB | DB_Conns: {db_connections}"
        )
        await asyncio.sleep(10)


async def _probe_database_with_retry(app: FastAPI, database_url: str) -> None:
    """Attempt DB connectivity in the background without crashing the server."""
    _asyncpg_url = database_url.replace("postgresql+asyncpg", "postgresql")

    app.state.db_status = "connecting"

    while True:
        try:
            conn = await asyncpg.connect(_asyncpg_url, timeout=5)
            if app.state.db_status != "connected":
                logger.info("Database strictly connected on boot/retry successfully.")
            app.state.db_status = "connected"
            await conn.close()
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            break
        except Exception as e:
            app.state.db_status = "disconnected"
            logger.warning(f"DB probe failed: {e}. Retrying in 5s...")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycle natively over FastAPI architectures."""
    try:
        logger.info("Initializing TemporalLayr Server components...")

        # Print ENV debug at startup
        print("DB:", bool(DATABASE_URL))
        print("API:", bool(API_KEY))
        print("PORT:", PORT)

        print("=== TEMPORALLAYR SERVER STARTED SUCCESSFULLY ===")
        print("Query API ready")
        print("Stats API ready")

        app.state.db_status = "unknown"

        background_task_manager.start_supervised("watchdog", _watchdog)

        if DATABASE_URL:
            background_task_manager.start_supervised(
                "db_probe", _probe_database_with_retry, app, DATABASE_URL
            )
        else:
            app.state.db_status = "not_configured"
            print("DATABASE_URL not set — skipping DB probe")

        # Bootstrap Background queues explicitly preventing dropped events during startup IO blocks
        # ingestion_service.start() is not async, we should wrap it if we want it supervised, but since it spawns its own asyncio loop task internally, we just call it.
        # Wait, ingestion_service.start() is async!
        background_task_manager.start_supervised(
            "ingestion_service", ingestion_service.start
        )

    except Exception as e:
        logger.error(f"Critical error during startup: {e}")

    yield

    try:
        logger.info("Tearing down TemporalLayr Server components securely...")
        await ingestion_service.stop()
        await background_task_manager.stop_all()
    except Exception as e:
        logger.error(f"Error during teardown: {e}")


app = FastAPI(
    title="TemporalLayr Ingestion Server",
    description="Production telemetry ingestion dispatch cluster resolving event arrays into backend isolation mechanisms.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(RequestLoggingMiddleware)


@app.middleware("http")
async def db_ready_middleware(request: Request, call_next):
    # Skip db check for core observability routes
    if request.url.path in ["/health", "/", "/favicon.ico", "/metrics", "/handshake"]:
        return await call_next(request)

    db_status = getattr(request.app.state, "db_status", "unknown")
    if db_status != "connected":
        return JSONResponse(
            status_code=503,
            content={"error": "database not ready", "status": "unavailable"},
        )
    return await call_next(request)


@app.middleware("http")
async def demo_mode_query_injector(request: Request, call_next):
    if (
        request.headers.get("x-api-key") == TEMPORALLAYR_DEMO_API_KEY
        and request.headers.get("x-tenant-id") == TEMPORALLAYR_DEMO_TENANT
    ):
        qs = request.scope.get("query_string", b"").decode()
        if "tenant_id=" not in qs:
            new_qs = qs + ("&" if qs else "") + f"tenant_id={TEMPORALLAYR_DEMO_TENANT}"
            request.scope["query_string"] = new_qs.encode()
    return await call_next(request)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all for demo
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key", "X-Tenant-ID", "*"],
)
app.include_router(handshake_router)
app.include_router(health_router)
# Mount routers
app.include_router(ingest.router, prefix="/v1")
app.include_router(query.router, prefix="/v1")
app.include_router(ws.router, prefix="/v1")
app.include_router(stream.router, prefix="/v1")
app.include_router(rules.router, prefix="/v1")
app.include_router(dashboard_router)
app.include_router(saved_query_router)
app.include_router(metrics_router)
app.include_router(traces_router)
app.include_router(stats_router)
app.include_router(dashboard_ext_router)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Graceful generic crash avoidance mechanism explicitly isolating top-level application failures natively."""
    logger.error(
        f"Unhandled Server Error routing request '{request.method} {request.url}': {exc}"
    )
    return JSONResponse(
        status_code=200,
        content={
            "ok": False,
            "data": [],
            "error": "Internal Server Exception (Degraded)",
        },
        headers={"X-Degraded-Status": "true"},
    )


# Removed deprecated @app.on_event("startup") in favor of lifespan context manager


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT, reload=True)
