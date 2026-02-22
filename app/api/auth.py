from fastapi import Header, HTTPException
from typing import Optional

VALID_KEYS = {"demo-key", "test-key", "your-real-key"}


async def verify_api_key(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None),
):
    key = None

    # Support Bearer token (SDK default)
    if authorization and authorization.startswith("Bearer "):
        key = authorization.replace("Bearer ", "").strip()

    # Support x-api-key header
    if not key and x_api_key:
        key = x_api_key

    if not key or key not in VALID_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return key
