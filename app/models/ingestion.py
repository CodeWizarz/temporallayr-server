from typing import Any, Dict, List
from pydantic import BaseModel, Field


class IngestionPayload(BaseModel):
    api_key: str = Field(
        ..., description="TemporalLayr API Key identifying the target tenant"
    )
    events: List[Dict[str, Any]] = Field(
        ..., description="Array of structurally tracked telemetry payloads"
    )
