from fastapi import Request, Header, HTTPException
import logging
from app.config import (
    API_KEY,
    TEMPORALLAYR_DEMO_API_KEY,
    TEMPORALLAYR_DEMO_TENANT,
    EXPECTED,
)

logger = logging.getLogger("temporallayr.auth")


def validate_demo(headers):
    if (
        headers.get("X-API-Key") == TEMPORALLAYR_DEMO_API_KEY
        and headers.get("X-Tenant-ID") == TEMPORALLAYR_DEMO_TENANT
    ):
        return True
    return False


async def verify_api_key(
    request: Request,
    authorization: str | None = Header(default=None),
):
    header_api_key = request.headers.get("X-API-Key")
    header_tenant_id = request.headers.get("X-Tenant-ID")

    logger.debug(
        f"Auth headers received: X-API-Key={'present' if header_api_key else 'missing'}, X-Tenant-ID={header_tenant_id}"
    )

    if not API_KEY:
        # If API_KEY is missing from environment, allow "demo-key" bypass natively
        logger.warning(
            "API_KEY is not configured. Running in development mode allowing fallback authentication."
        )
        request.state.tenant_id = header_tenant_id or TEMPORALLAYR_DEMO_TENANT
        request.state.api_key = header_api_key or TEMPORALLAYR_DEMO_API_KEY
        return request.state.tenant_id
    else:
        # When API_KEY is set in environment, firmly strictly reject "demo-key" overrides
        # and enforce the environment matching the provided X-API-Key natively.
        if header_api_key == API_KEY:
            request.state.tenant_id = header_tenant_id
            request.state.api_key = header_api_key
            return header_api_key

    # Standard token fallbacks explicitly for Bearer mapping checks gracefully
    if not authorization:
        raise HTTPException(status_code=401, detail="invalid api key")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="invalid api key")

    token = authorization.split(" ")[1]

    if API_KEY and token != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")
    elif not API_KEY and token != EXPECTED:
        raise HTTPException(status_code=401, detail="invalid api key")

    return token
