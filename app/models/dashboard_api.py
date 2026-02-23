from typing import Generic, TypeVar, Optional, List, Dict, Any
from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime

T = TypeVar("T")


class ResponseMeta(BaseModel):
    query_ms: float


class StandardDashboardResponse(BaseModel, Generic[T]):
    ok: bool = True
    data: Optional[T] = None
    meta: Optional[ResponseMeta] = None
    error: Optional[str] = None
    next_cursor: Optional[str] = None


class DashboardSearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    contains: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    time_from: Optional[datetime] = None
    time_to: Optional[datetime] = None
    limit: int = Field(50, ge=1, le=1000)
    cursor: Optional[str] = Field(
        None, description="cursor is formatted as {timestamp_iso}_{uuid}"
    )
    select: Optional[List[str]] = None


class PipelineStage(BaseModel):
    group_by: Optional[str] = None
    count: Optional[bool] = None


class DashboardQueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    pipeline: List[PipelineStage]
    time_from: Optional[datetime] = None
    time_to: Optional[datetime] = None
    filters: Optional[Dict[str, Any]] = None
