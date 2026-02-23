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

    if validate_demo(request.headers):
        request.state.tenant_id = "demo-tenant"
        request.state.api_key = "demo-key"
        return "demo-tenant"

    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization format")

    token = authorization.split(" ")[1]

    if token != EXPECTED:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return token
