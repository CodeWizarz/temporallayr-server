import logging
import math
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from sqlalchemy.future import select

from app.core.database import async_session_maker
from app.models.event import Event

logger = logging.getLogger("temporallayr.query.timeseries")


def _compute_percentile(data: List[float], percentile: float) -> float:
    """Computes mathematically correct percentile over bounds safely."""
    if not data:
        return 0.0

    data.sort()
    k = (len(data) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)

    if f == c:
        return data[int(k)]

    d0 = data[int(f)]
    d1 = data[int(c)]
    return d0 + (d1 - d0) * (k - f)


async def aggregate_timeseries(
    tenant_id: str,
    start_time: datetime,
    end_time: datetime,
    interval_seconds: int,
    metric: str,
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Consumes highly-optimized execution streams grouping structural blocks naturally matching requested UI dimensions natively.
    """
    logger.info(
        f"[TIMESERIES QUERY START] tenant={tenant_id} metric={metric} start={start_time} end={end_time}"
    )

    # 1. Bootstrapping SQL limits dynamically resolving across streaming cursors effortlessly safely avoiding N+1 blocks
    query = (
        select(Event)
        .where(
            Event.tenant_id == tenant_id,
            Event.timestamp >= start_time,
            Event.timestamp <= end_time,
        )
        .order_by(Event.timestamp.asc())
    )

    # Optional filtering mapping bounds safely matching normal query engine logic organically
    if filters:
        for key, val in filters.items():
            query = query.where(Event.payload[key].astext == str(val))

    buckets: Dict[int, Dict[str, Any]] = {}
    total_events_processed = 0

    async with async_session_maker() as session:
        # Utilize yield_per dynamically resolving execution bounds into strictly memory-safe buffers (chunked by 5000 records organically)
        execution_stream = await session.stream(query.execution_options(yield_per=5000))

        async for row in execution_stream:
            event: Event = row[0]
            total_events_processed += 1

            # Map absolute timestamps into bounded epoch discrete grids natively
            if not event.timestamp.tzinfo:
                ts = event.timestamp.replace(tzinfo=timezone.utc).timestamp()
            else:
                ts = event.timestamp.timestamp()

            bucket_idx = int(ts // interval_seconds) * interval_seconds

            if bucket_idx not in buckets:
                buckets[bucket_idx] = {
                    "timestamp": datetime.fromtimestamp(
                        bucket_idx, tz=timezone.utc
                    ).isoformat(),
                    "count": 0,
                    "errors": 0,
                    "latencies": [],
                    "avg_duration": 0.0,
                    "error_rate": 0.0,
                }

            b = buckets[bucket_idx]
            b["count"] += 1
            status = event.payload.get("status", "UNKNOWN")
            if status == "FAILED":
                b["errors"] += 1

            metrics_payload = event.payload.get("metrics", {})
            duration_ms = metrics_payload.get("duration_ms", 0.0)
            b["latencies"].append(float(duration_ms))

    # 2. Materializing calculations neatly over streaming bins inherently correctly!
    final_series = []

    for _, b in sorted(buckets.items()):
        cnt = b["count"]

        # Calculate derived metrics structurally mapped
        b["error_rate"] = (b["errors"] / cnt * 100.0) if cnt > 0 else 0.0
        b["avg_duration"] = (sum(b["latencies"]) / cnt) if cnt > 0 else 0.0

        # We process requested metrics natively dynamically ensuring UI charting aligns structurally gracefully!
        res_data = {
            "timestamp": b["timestamp"],
            "count": cnt,
            "errors": b["errors"],
            "avg_duration": round(b["avg_duration"], 2),
        }

        if metric == "execution_count":
            res_data["value"] = cnt
        elif metric == "error_rate":
            res_data["value"] = round(b["error_rate"], 2)
        elif metric == "latency_avg":
            res_data["value"] = round(b["avg_duration"], 2)
        elif metric == "latency_p95":
            res_data["value"] = round(_compute_percentile(b["latencies"], 95.0), 2)
        else:
            # Default bounds cleanly resolving unknown variables explicitly securely.
            res_data["value"] = cnt

        final_series.append(res_data)

    logger.info(
        f"[TIMESERIES BUCKET COUNT] buckets={len(final_series)} events={total_events_processed}"
    )
    logger.info(f"[TIMESERIES COMPLETE] tenant={tenant_id} completed successfully.")

    return final_series
