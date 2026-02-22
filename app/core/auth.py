from fastapi import Header, HTTPException
import os

EXPECTED = os.getenv("TEMPORALLAYR_API_KEY", "dev-temporallayr-key")


async def verify_api_key(authorization: str | None = Header(default=None)):

    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization format")

    token = authorization.split(" ")[1]

    if token != EXPECTED:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return True
