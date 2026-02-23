import uuid
from sqlalchemy import Column, String, DateTime, Index, Integer, func
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.core.database import Base


class Event(Base):
    """Production telemetry event mapping structural storage backend tables natively."""

    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False, default="execution_graph", index=True)
    timestamp = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )
    payload = Column(JSONB, nullable=False)

    # Composite indexes optimizing multi-tenant temporal slice scans naturally
    # Plus GIN index supporting deep JSON payload traversing natively
    __table_args__ = (
        Index("idx_events_tenant_time", "tenant_id", timestamp.desc()),
        Index("ix_events_payload_gin", "payload", postgresql_using="gin"),
    )


class ExecutionSummary(Base):
    """Production execution index natively mapping full graph structural summaries."""

    __tablename__ = "execution_summaries"

    id = Column(String, primary_key=True, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, index=True)
    node_count = Column(Integer, nullable=False, default=1)

    __table_args__ = (Index("ix_execs_tenant_created", "tenant_id", "created_at"),)


class Incident(Base):
    """Production failure tracking isolated completely mapping incidents structurally."""

    __tablename__ = "incidents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    execution_id = Column(String, nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    failure_type = Column(String, nullable=False)
    node_name = Column(String, nullable=True)
    summary = Column(String, nullable=True)

    # Structural telemetry fingerprints natively bounding recurring anomaly alerts
    fingerprint = Column(String, nullable=False, index=True, default="")
    occurrence_count = Column(Integer, nullable=False, default=1)


class AlertRule(Base):
    """Production alert mapping constraints uniquely tracking notification parameters natively."""

    __tablename__ = "alert_rules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    failure_type = Column(String, nullable=False)
    node_name = Column(String, nullable=True)
    webhook_url = Column(String, nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
