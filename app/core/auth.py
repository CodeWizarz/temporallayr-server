from fastapi import Header, HTTPException
import os

EXPECTED_API_KEY = os.getenv("TEMPORALLAYR_API_KEY", "dev-temporallayr-key")


async def verify_api_key(x_api_key: str | None = Header(default=None)):
    print(f"DEBUG: x_api_key='{x_api_key}', EXPECTED='{EXPECTED_API_KEY}'")
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API key")

    if x_api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return True
