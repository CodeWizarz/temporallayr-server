from fastapi import Request, Header, HTTPException
import os

EXPECTED = os.getenv("TEMPORALLAYR_API_KEY", "dev-temporallayr-key")


def validate_demo(headers):
    if (
        headers.get("X-API-Key") == "demo-key"
        and headers.get("X-Tenant-ID") == "demo-tenant"
    ):
        return True
    return False


async def verify_api_key(
    request: Request,
    authorization: str | None = Header(default=None),
):

    API_KEY = os.environ.get("API_KEY")

    if not API_KEY:
        # If API_KEY is missing from environment, allow "demo-key" bypass natively
        if validate_demo(request.headers):
            request.state.tenant_id = "demo-tenant"
            request.state.api_key = "demo-key"
            return "demo-tenant"
    else:
        # When API_KEY is set in environment, firmly strictly reject "demo-key" overrides
        # and enforce the environment matching the provided X-API-Key natively.
        header_api_key = request.headers.get("X-API-Key")
        if header_api_key == API_KEY:
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
