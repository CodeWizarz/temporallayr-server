from typing import Optional, Literal
from datetime import datetime
from pydantic import BaseModel, Field


class TimeRange(BaseModel):
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class SortOption(BaseModel):
    field: str = "timestamp"
    direction: Literal["asc", "desc"] = "desc"


class QueryFilters(BaseModel):
    execution_id: Optional[str] = None
    node_name: Optional[str] = None
    fingerprint: Optional[str] = None
    incident_id: Optional[str] = None
    cluster_id: Optional[str] = None
    status: Optional[str] = None
    time_range: Optional[TimeRange] = None


class MultiResourceQueryRequest(BaseModel):
    tenant_id: str = ""
    filters: QueryFilters = Field(default_factory=QueryFilters)
    search_text: Optional[str] = None
    sort: SortOption = Field(default_factory=SortOption)
    limit: int = Field(default=100, le=5000)
    offset: int = Field(default=0, ge=0)


class QueryResult(BaseModel):
    data: list
    total: int
    partial: bool = False
    warning: Optional[str] = None


class QueryRequest(BaseModel):
    tenant_id: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    fingerprint: Optional[str] = None
    event_type: Optional[str] = None
    limit: int = Field(default=100)
    offset: int = Field(default=0)
    sort: Literal["asc", "desc"] = "desc"
