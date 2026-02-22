import logging
from fastapi import Request, HTTPException

logger = logging.getLogger("temporallayr.api.auth")

# In-memory mock mapping for tenant configurations. Real implementation connects to DB.
MOCK_TENANT_STORE = {
    "sk_live_123456789": "tenant_alpha_001",
    "sk_test_987654321": "tenant_beta_002",
}


async def extract_api_key(request: Request) -> str:
    """Extract API key from Header, Bearer token, or JSON body."""
    key = None

    # 1. Header: X-API-Key
    if "x-api-key" in request.headers:
        key = request.headers.get("x-api-key")

    # 2. Header: Authorization: Bearer <key>
    elif "authorization" in request.headers:
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            key = auth_header[7:]

    # 3. JSON body: { "api_key": "..." }
    if not key:
        try:
            body = await request.json()
            if isinstance(body, dict) and "api_key" in body:
                key = body.get("api_key")
        except Exception:
            pass

    if not key:
        raise HTTPException(
            status_code=401,
            detail="Authentication failed: Missing API key",
        )

    # Temporarily allow dev-key
    if key == "dev-key":
        tenant_id = "tenant_dev_001"
    else:
        tenant_id = MOCK_TENANT_STORE.get(key)

    if not tenant_id:
        logger.warning(
            f"Unauthorized attempt with masking token ending in '...{key[-4:] if len(key) > 4 else key}'"
        )
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired authentication credentials strictly blocked.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    logger.info("Auth success for key %s", key[:6])
    return tenant_id
