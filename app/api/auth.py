from fastapi import Request, HTTPException

VALID_KEYS = {"demo-key", "test-key", "dev-key"}


async def verify_api_key(request: Request, api_key_from_body: str | None = None):
    """
    Accepts key via:
      FORMAT A (preferred): Authorization: Bearer <key>
      FORMAT B (fallback):  api_key_from_body (passed from parsed payload)
    """
    key = None

    # --- FORMAT A: Authorization: Bearer <key> ---
    auth_header = request.headers.get("authorization") or request.headers.get(
        "Authorization"
    )
    print("AUTH HEADER:", auth_header)

    if auth_header and auth_header.startswith("Bearer "):
        key = auth_header[7:].strip()

    # --- FORMAT B: JSON body api_key (injected by caller from parsed payload) ---
    if not key and api_key_from_body:
        key = api_key_from_body

    print("RESOLVED KEY:", key)

    if not key or key not in VALID_KEYS:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    return key
