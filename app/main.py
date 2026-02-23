import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.handshake import router as handshake_router
from app.api.health import router as health_router
from app.api import ingest, query, ws
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
    print("Query API ready")
    print("Stats API ready")

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


@app.middleware("http")
async def demo_mode_query_injector(request: Request, call_next):
    if (
        request.headers.get("x-api-key") == "demo-key"
        and request.headers.get("x-tenant-id") == "demo-tenant"
    ):
        qs = request.scope.get("query_string", b"").decode()
        if "tenant_id=" not in qs:
            new_qs = qs + ("&" if qs else "") + "tenant_id=demo-tenant"
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

from app.api import stream, rules
from app.api.dashboard import router_dash as dashboard_router
from app.api.dashboard import router_sq as saved_query_router
from app.api.metrics import router as metrics_router
from app.api.traces import router as traces_router
from app.api.stats import router as stats_router
from app.api.dashboard_api import router as dashboard_ext_router

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
        status_code=500,
        content={"status": "error", "message": "Internal Server Exception."},
    )


@app.on_event("startup")
async def startup():
    print("SERVER STARTED — ROUTES LOADED")
    print("TemporalLayr server started with demo tenant enabled")
    for route in app.routes:
        print(route.path)
