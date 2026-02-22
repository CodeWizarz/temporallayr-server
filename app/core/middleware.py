import time
import logging
import uuid
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("temporallayr.request")


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
