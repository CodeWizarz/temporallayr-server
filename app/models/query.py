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


class SearchRequest(BaseModel):
    """Schema validating execution search payloads ensuring native constraint limits."""

    function_name: Optional[str] = Field(
        None,
        description="Exact node name string matching deeply executed graph functions",
    )
    start_time: Optional[datetime] = Field(
        None, description="ISO8601 boundary starting constraints"
    )
    end_time: Optional[datetime] = Field(
        None, description="ISO8601 boundary ending constraints"
    )
    limit: int = Field(50, ge=1, le=1000, description="Pagination slicing maximums")
    offset: int = Field(0, ge=0, description="Pagination displacement slice offset")


class DiffPayload(BaseModel):
    execution_a: str
    execution_b: str
    tenant_id: str = "demo-tenant"


class IncidentItem(BaseModel):
    """Schema tracking single executions isolated anomaly reports structurally."""

    id: str
    tenant_id: str
    execution_id: str
    timestamp: datetime
    failure_type: str
    node_name: Optional[str] = None
    summary: Optional[str] = None


class IncidentsResponse(BaseModel):
    """Response enclosing array of bound auto-generated incidents querying."""

    incidents: List[IncidentItem]
