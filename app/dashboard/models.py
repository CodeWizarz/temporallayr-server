import uuid
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
from sqlalchemy import Column, String, DateTime, Index, Integer, Float, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.core.database import Base


# --- SQLAlchemy DB Models ---


class SavedQueryDB(Base):
    __tablename__ = "saved_queries"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    query_json = Column(JSONB, nullable=False)
    created_at = Column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )

    __table_args__ = (Index("ix_saved_queries_tenant", "tenant_id"),)


class DashboardDB(Base):
    __tablename__ = "dashboards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    created_at = Column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )

    __table_args__ = (Index("ix_dashboards_tenant", "tenant_id"),)


class PanelDB(Base):
    __tablename__ = "panels"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    dashboard_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dashboards.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    saved_query_id = Column(
        UUID(as_uuid=True), ForeignKey("saved_queries.id"), nullable=False
    )

    # UI grid representations natively
    position_x = Column(Float, nullable=False, default=0.0)
    position_y = Column(Float, nullable=False, default=0.0)
    width = Column(Float, nullable=False, default=1.0)
    height = Column(Float, nullable=False, default=1.0)

    __table_args__ = (Index("ix_panels_tenant_dashboard", "tenant_id", "dashboard_id"),)


# --- API Pydantic Schemas ---


class SavedQueryCreate(BaseModel):
    name: str
    query_json: Dict[str, Any]


class SavedQueryResponse(BaseModel):
    id: str
    tenant_id: str
    name: str
    query_json: Dict[str, Any]
    created_at: datetime

    class Config:
        from_attributes = True


class DashboardCreate(BaseModel):
    name: str


class PanelCreate(BaseModel):
    name: str
    saved_query_id: str
    position_x: float = 0.0
    position_y: float = 0.0
    width: float = 1.0
    height: float = 1.0


class PanelResponse(BaseModel):
    panel_id: str
    name: str
    saved_query: SavedQueryResponse
    position_x: float
    position_y: float
    width: float
    height: float

    class Config:
        from_attributes = True


class DashboardResponse(BaseModel):
    dashboard_id: str
    name: str
    panels: List[PanelResponse]
    created_at: datetime

    class Config:
        from_attributes = True


class DashboardListResponse(BaseModel):
    id: str
    tenant_id: str
    name: str
    created_at: datetime

    class Config:
        from_attributes = True
