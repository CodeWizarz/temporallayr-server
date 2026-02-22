import os
from fastapi import Request, HTTPException


async def verify_api_key(request: Request, api_key_from_body: str | None = None):
    key = None

    # FORMAT A (preferred): Authorization header -> Bearer <key>
    # Note: request.headers is case-insensitive in FastAPI/Starlette
    authorization = request.headers.get("Authorization")

    if authorization and authorization.startswith("Bearer "):
        key = authorization.split(" ")[1]
    else:
        # FORMAT B (legacy fallback): Read from parsed JSON body
        key = api_key_from_body

    print(f"[AUTH CHECK] extracted_key={key}")

    allowed_keys = os.getenv("TEMPORALLAYR_DEV_KEYS", "dev-test-key").split(",")

    if key not in allowed_keys:
        raise HTTPException(status_code=401, detail="invalid api key")

    return key
