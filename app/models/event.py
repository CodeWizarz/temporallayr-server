import uuid
from sqlalchemy import Column, String, DateTime, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.core.database import Base


class Event(Base):
    """Production telemetry event mapping structural storage backend tables natively."""

    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    api_key = Column(String, nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    payload = Column(JSONB, nullable=False)

    # Composite indexes optimizing multi-tenant temporal slice scans naturally
    __table_args__ = (Index("ix_events_api_key_timestamp", "api_key", "timestamp"),)
