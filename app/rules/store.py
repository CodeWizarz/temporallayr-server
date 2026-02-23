import asyncio
import time
import uuid
import logging
from typing import List, Dict, Optional
from sqlalchemy.future import select

from app.core.database import async_session_maker
from app.rules.models import Rule, RuleSchema, RuleCondition, RuleActions

logger = logging.getLogger("temporallayr.rules.store")


class RuleStore:
    """Enterprise rule storage backend with 30-second memory caching securely dodging active DB bounds."""

    def __init__(self, cache_ttl: float = 30.0):
        self._cache_ttl = cache_ttl
        # Maps tenant_id -> list of active rules securely
        self._rule_cache: Dict[str, List[RuleSchema]] = {}
        # Maps tenant_id -> last refresh timestamp
        self._last_refresh: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get_rules_for_tenant(self, tenant_id: str) -> List[RuleSchema]:
        """Fetch rules safely loading from cache ideally."""
        now = time.time()

        # Check cache validity organically
        if tenant_id in self._rule_cache and tenant_id in self._last_refresh:
            if (now - self._last_refresh[tenant_id]) < self._cache_ttl:
                return self._rule_cache[tenant_id]

        # Cache miss or expired: lock, re-check, fetch efficiently
        async with self._lock:
            if tenant_id in self._rule_cache and tenant_id in self._last_refresh:
                if (now - self._last_refresh[tenant_id]) < self._cache_ttl:
                    return self._rule_cache[tenant_id]

            rules = await self._fetch_db_rules_for_tenant(tenant_id)
            self._rule_cache[tenant_id] = rules
            self._last_refresh[tenant_id] = time.time()
            return rules

    async def _fetch_db_rules_for_tenant(self, tenant_id: str) -> List[RuleSchema]:
        if not async_session_maker:
            logger.warning("DB disconnected mapping empty rule array cleanly.")
            return []

        try:
            async with async_session_maker() as session:
                stmt = (
                    select(Rule)
                    .where(Rule.tenant_id == tenant_id, Rule.enabled == True)
                    .order_by(Rule.priority.desc())
                )
                result = await session.execute(stmt)
                db_rules = result.scalars().all()

                parsed = []
                for r in db_rules:
                    parsed.append(
                        RuleSchema(
                            id=r.id,
                            tenant_id=r.tenant_id,
                            name=r.name,
                            enabled=r.enabled,
                            priority=r.priority,
                            condition=RuleCondition(**r.condition),
                            actions=RuleActions(**r.actions),
                            created_at=r.created_at,
                        )
                    )
                return parsed
        except Exception as e:
            logger.error(
                f"Failed fetching detection rules defensively organically: {e}"
            )
            return []

    async def add_rule(self, tenant_id: str, rule_data: dict) -> Optional[RuleSchema]:
        """Insert rule dynamically evicting caches natively."""
        if not async_session_maker:
            return None

        try:
            async with async_session_maker() as session:
                new_rule = Rule(
                    tenant_id=tenant_id,
                    name=rule_data["name"],
                    enabled=rule_data.get("enabled", True),
                    priority=rule_data.get("priority", 0),
                    condition=rule_data["condition"],
                    actions=rule_data["actions"],
                )
                session.add(new_rule)
                await session.commit()
                await session.refresh(new_rule)

                # Invalidate cache
                async with self._lock:
                    self._last_refresh.pop(tenant_id, None)

                return RuleSchema(
                    id=new_rule.id,
                    tenant_id=new_rule.tenant_id,
                    name=new_rule.name,
                    enabled=new_rule.enabled,
                    priority=new_rule.priority,
                    condition=RuleCondition(**new_rule.condition),
                    actions=RuleActions(**new_rule.actions),
                    created_at=new_rule.created_at,
                )

        except Exception as e:
            logger.error(f"Error persisting rule mapping compactly: {e}")
            return None

    async def delete_rule(self, tenant_id: str, rule_id: uuid.UUID) -> bool:
        """Delete structural mapping organically blocking leaks unconditionally."""
        if not async_session_maker:
            return False

        try:
            async with async_session_maker() as session:
                stmt = select(Rule).where(
                    Rule.id == rule_id, Rule.tenant_id == tenant_id
                )
                result = await session.execute(stmt)
                r = result.scalar_one_or_none()

                if r:
                    await session.delete(r)
                    await session.commit()
                    # Invalidate cache organically
                    async with self._lock:
                        self._last_refresh.pop(tenant_id, None)
                    return True
                return False
        except Exception as e:
            logger.error(f"Error deleting rule safely natively: {e}")
            return False


rule_store = RuleStore()
