from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field


class QueryPayload(BaseModel):
    """Schema validating incoming POST requests for event queries natively."""

    api_key: str = Field(
        ..., description="TemporalLayr API Key identifying the target tenant"
    )
    limit: int = Field(
        100,
        ge=1,
        le=1000,
        description="Pagination bounds strictly capping payload responses",
    )
    from_time: Optional[datetime] = Field(
        None, alias="from", description="ISO8601 boundary starting queries"
    )
    to_time: Optional[datetime] = Field(
        None, alias="to", description="ISO8601 boundary ending queries"
    )

    class Config:
        populate_by_name = True


class QueryResponse(BaseModel):
    """Normalized response schema yielding structured event arrays safely."""

    events: List[Dict[str, Any]]
