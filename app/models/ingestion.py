from typing import Any, Dict, List
from pydantic import BaseModel, Field


class IngestionPayload(BaseModel):
    api_key: str | None = Field(
        default=None,
        description="TemporalLayr API Key (optional if using Authorization: Bearer header)",
    )
    tenant_id: str | None = Field(
        default=None,
        description="Tenant identifier — used to route live events to the correct WebSocket subscriber",
    )
    events: List[Dict[str, Any]] = Field(
        ..., description="Array of structurally tracked telemetry payloads"
    )
