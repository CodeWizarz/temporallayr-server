from fastapi import Request, Header, HTTPException
import os

EXPECTED = os.getenv("TEMPORALLAYR_API_KEY", "dev-temporallayr-key")


async def verify_api_key(
    request: Request,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
    x_tenant_id: str | None = Header(default=None),
):

    if x_api_key == "demo-key" and x_tenant_id == "demo-tenant":
        request.tenant = "demo-tenant"
        return "demo-tenant"

    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization format")

    token = authorization.split(" ")[1]

    if token != EXPECTED:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return token
