from fastapi import Request, HTTPException, Security
from fastapi.security import APIKeyHeader
from app.config import (
    API_KEY,
    TEMPORALLAYR_DEMO_API_KEY,
    TEMPORALLAYR_DEMO_TENANT,
    TEMPORALLAYR_DEV_KEYS,
)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
tenant_header = APIKeyHeader(name="X-Tenant-ID", auto_error=False)


def validate_demo(headers):
    if (
        headers.get("X-API-Key") == TEMPORALLAYR_DEMO_API_KEY
        and headers.get("X-Tenant-ID") == TEMPORALLAYR_DEMO_TENANT
    ):
        return True
    return False


async def verify_api_key(
    request: Request,
    api_key: str = Security(api_key_header),
    tenant_id: str = Security(tenant_header),
):
    """Validate multitenant API keys securely mapping dynamic auth barriers cleanly."""

    # Unconditional resilience: never crash on missing auth, just degrade cleanly
    header_api_key = request.headers.get("X-API-Key")
    header_tenant_id = request.headers.get("X-Tenant-ID")

    if not API_KEY:
        if validate_demo(request.headers):
            request.state.tenant_id = TEMPORALLAYR_DEMO_TENANT
            request.state.api_key = TEMPORALLAYR_DEMO_API_KEY
            return TEMPORALLAYR_DEMO_TENANT
        if header_api_key in TEMPORALLAYR_DEV_KEYS:
            return header_tenant_id or "dev-tenant"
        raise HTTPException(status_code=401, detail="Invalid API Key (Dev Mode)")

    if header_api_key == API_KEY:
        return header_tenant_id or "default-tenant"

    raise HTTPException(status_code=401, detail="Invalid API Key")
