import time
import logging
import uuid
import json
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.responses import JSONResponse

logger = logging.getLogger("temporallayr.request")
auth_logger = logging.getLogger("temporallayr.auth")

MOCK_TENANT_STORE = {
    "sk_live_123456789": "tenant_alpha_001",
    "sk_test_987654321": "tenant_beta_002",
}


async def extract_api_key(request: Request, body_bytes: bytes):
    key = None
    if "x-api-key" in request.headers:
        key = request.headers.get("x-api-key")
    elif "authorization" in request.headers:
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            key = auth_header[7:]

    if not key and body_bytes:
        try:
            data = json.loads(body_bytes)
            key = data.get("api_key")
        except Exception:
            pass
    return key


class AuthMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        path = scope.get("path", "")
        if path not in ("/v1/ingest", "/handshake", "/v1/query"):
            return await self.app(scope, receive, send)

        request = Request(scope, receive)

        body_bytes = b""
        more_body = True
        while more_body:
            message = await receive()
            body_bytes += message.get("body", b"")
            more_body = message.get("more_body", False)

        received = False

        async def new_receive():
            nonlocal received
            if not received:
                received = True
                return {"type": "http.request", "body": body_bytes, "more_body": False}
            return {"type": "http.request", "body": b"", "more_body": False}

        key = await extract_api_key(request, body_bytes)

        if not key:
            res = JSONResponse(status_code=401, content={"detail": "Missing API key"})
            return await res(scope, new_receive, send)

        if key in ("dev-key", "test"):
            tenant_id = "tenant_dev_001"
        else:
            tenant_id = MOCK_TENANT_STORE.get(key)

        if not tenant_id:
            auth_logger.warning("Unauthorized attempt")
            res = JSONResponse(status_code=401, content={"detail": "Invalid API key"})
            return await res(scope, new_receive, send)

        auth_logger.info("Auth success for key %s", key[:6])
        request.state.api_key = key

        # update scope so downstream gets request.state
        scope["state"] = request.state._state

        return await self.app(scope, new_receive, send)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        """Request-scoped logging middleware extracting contextual request UUIDs securely."""
        request_id = str(uuid.uuid4())
        start_time = time.time()

        # Injects tracing context optionally attached to headers natively
        request.state.request_id = request_id

        logger.info(f"[{request_id}] START {request.method} {request.url.path}")

        try:
            response = await call_next(request)

            process_time = time.time() - start_time
            logger.info(
                f"[{request_id}] SUCCESS {response.status_code} in {process_time:.4f}s - {request.method} {request.url.path}"
            )

            # Surface telemetry correlation ID upstream via HTTP response headers dynamically
            response.headers["X-Request-ID"] = request_id
            return response

        except Exception as e:
            process_time = time.time() - start_time
            logger.error(
                f"[{request_id}] ERROR 500 in {process_time:.4f}s - {request.method} {request.url.path} (Error: {str(e)})"
            )
            raise
