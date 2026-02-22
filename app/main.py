import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.api.handshake import router as handshake_router
from app.api.health import router as health_router
from app.api.ingest import router as ingest_router
from app.api.query import router as query_router
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
ingestion_service = IngestionService(max_batch_size=1000, flush_interval=1.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycle natively over FastAPI architectures."""
    logger.info("Initializing TemporalLayr Server components...")
    print("=== TEMPORALLAYR SERVER STARTED SUCCESSFULLY ===")

    # Bootstrap Background queues explicitly preventing dropped events during startup IO blocks
    await ingestion_service.start()

    yield

    logger.info("Tearing down TemporalLayr Server components securely...")

    # Disconnect generic background instances mapping queue grace delays robustly
    await ingestion_service.stop()


app = FastAPI(
    title="TemporalLayr Ingestion Server",
    description="Production telemetry ingestion dispatch cluster resolving event arrays into backend isolation mechanisms.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(RequestLoggingMiddleware)
app.include_router(handshake_router)
app.include_router(health_router)
app.include_router(ingest_router)
app.include_router(query_router)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Graceful generic crash avoidance mechanism explicitly isolating top-level application failures natively."""
    logger.error(
        f"Unhandled Server Error routing request '{request.method} {request.url}': {exc}"
    )
    return JSONResponse(
        status_code=500,
        content={"status": "error", "message": "Internal Server Exception."},
    )


@app.on_event("startup")
async def startup():
    print("SERVER STARTED — ROUTES LOADED")
    for route in app.routes:
        print(route.path)
