import uuid
from typing import Dict, Any, Literal
from datetime import datetime, UTC
from pydantic import BaseModel, Field

from sqlalchemy import Column, String, Boolean, DateTime, Index, Integer
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.core.database import Base


class Rule(Base):
    """Database model storing enterprise detection rules natively."""

    __tablename__ = "detection_rules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    enabled = Column(Boolean, nullable=False, default=True)
    priority = Column(Integer, nullable=False, default=0)

    condition = Column(JSONB, nullable=False)
    actions = Column(JSONB, nullable=False)

    created_at = Column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    __table_args__ = (Index("ix_detection_rules_tenant", "tenant_id"),)


# --- Pydantic Schemas ---

RuleConditionType = Literal[
    "node_error_rate",
    "execution_latency",
    "divergence_detected",
    "cluster_anomaly",
    "custom_expression",
]


class RuleCondition(BaseModel):
    type: RuleConditionType
    parameters: Dict[str, Any] = Field(default_factory=dict)


class RuleActions(BaseModel):
    create_incident: bool = True
    severity: Literal["low", "medium", "high", "critical"] = "medium"
    notify: bool = False


class RuleSchema(BaseModel):
    id: uuid.UUID
    tenant_id: str
    name: str
    enabled: bool = True
    priority: int = 0
    condition: RuleCondition
    actions: RuleActions
    created_at: datetime


class RuleCreateRequest(BaseModel):
    name: str
    enabled: bool = True
    priority: int = 0
    condition: RuleCondition
    actions: RuleActions
