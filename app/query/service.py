import asyncio
import logging
import time
from typing import Dict, Any

from app.query.models import QueryRequest
from app.services.storage_service import StorageService

logger = logging.getLogger("temporallayr.query.service")

# Singleton decoupled storage binding organically
storage_engine = StorageService()


async def query_events(
    request: QueryRequest, storage_engine: StorageService = None
) -> Dict[str, Any]:
    """
    Production Analytics Query Engine fetching bounds securely isolated via StorageService.
    Supports 5s strict server-side timeouts naturally guarding against DOS scans.
    """
    from app.query.service import storage_engine as global_storage

    engine = storage_engine or global_storage
    start_cpu_time = time.perf_counter()

    # Extract mappings
    filters_mapped = []
    if request.start_time:
        filters_mapped.append(f"start={request.start_time}")
    if request.end_time:
        filters_mapped.append(f"end={request.end_time}")
    if request.fingerprint:
        filters_mapped.append(f"fingerprint={request.fingerprint}")
    if request.event_type:
        filters_mapped.append(f"event_type={request.event_type}")

    filter_str = " ".join(filters_mapped) if filters_mapped else "none"

    logger.info(f"[QUERY] tenant={request.tenant_id} filters={filter_str}")

    try:
        # Wrap the whole IO engine natively capping at 5.0 seconds
        results = await asyncio.wait_for(
            engine.query_analytics_events(
                tenant_id=request.tenant_id,
                start_time=request.start_time,
                end_time=request.end_time,
                fingerprint=request.fingerprint,
                event_type=request.event_type,
                limit=request.limit,
                offset=request.offset,
                sort=request.sort,
            ),
            timeout=5.0,
        )

        duration_ms = int((time.perf_counter() - start_cpu_time) * 1000)

        logger.info(f"[QUERY] result_count={len(results)}")
        logger.info(f"[QUERY] duration_ms={duration_ms}")

        return {"results": results, "count": len(results)}

    except asyncio.TimeoutError:
        logger.warning(
            f"[QUERY] Exceeded 5s timeout limits natively tenant={request.tenant_id}"
        )
        return {"error": "query timeout"}
    except Exception as e:
        logger.error(f"[QUERY] Server failure extracting bindings natively: {e}")
        return {"error": "internal server error"}
