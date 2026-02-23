from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Dict, Any, Optional
from datetime import datetime

from app.api.auth import verify_api_key
from app.query.timeseries import aggregate_timeseries

router = APIRouter(prefix="/v1/metrics", tags=["metrics"])


@router.get("/timeseries")
async def get_timeseries_metrics(
    start: str = Query(..., description="ISO 8601 start time"),
    end: str = Query(..., description="ISO 8601 end time"),
    interval: int = Query(..., description="Bucket interval in seconds"),
    metric: str = Query(..., description="Metric type to calculate"),
    api_key: str = Depends(verify_api_key),
) -> Dict[str, Any]:
    """Generates explicit time-series charts mapping streams organically across tenant grids natively."""
    tenant_id = api_key

    try:
        start_t = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_t = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ISO 8601 bounds.")

    if interval <= 0:
        raise HTTPException(status_code=400, detail="Interval must be > 0.")

    try:
        series = await aggregate_timeseries(
            tenant_id=tenant_id,
            start_time=start_t,
            end_time=end_t,
            interval_seconds=interval,
            metric=metric,
        )
        return {"series": series}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
