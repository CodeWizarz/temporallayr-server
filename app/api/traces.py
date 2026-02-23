from fastapi import APIRouter, Depends, Query, HTTPException, Path
from typing import Dict, Any, Optional

from app.api.auth import verify_api_key
from app.query.traces import get_trace, list_traces

router = APIRouter(prefix="/v1/traces", tags=["traces"])


@router.get("")
async def list_recent_traces(
    limit: int = Query(
        50,
        description="Pagination bounds strictly capping payload responses",
        ge=1,
        le=100,
    ),
    offset: int = Query(0, description="Pagination displacement slice offset", ge=0),
    status: Optional[str] = Query(None, description="Optional status filtering"),
    api_key: str = Depends(verify_api_key),
) -> Dict[str, Any]:
    """Lists bounded executions sequentially natively masking trace bounds across limits correctly."""
    try:
        items = await list_traces(
            tenant_id=api_key, limit=limit, offset=offset, status=status
        )
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{trace_id}")
async def get_specific_trace(
    trace_id: str = Path(
        ...,
        description="Target execution trace graph ID natively tracking execution context",
    ),
    api_key: str = Depends(verify_api_key),
) -> Dict[str, Any]:
    """Retrieves a fully bounded execution generic graph organically fetching explicitly."""
    result = await get_trace(tenant_id=api_key, trace_id=trace_id)

    if not result:
        raise HTTPException(
            status_code=404,
            detail="Execution Trace not found securely matching current tenant mappings.",
        )

    return {"trace": result}
