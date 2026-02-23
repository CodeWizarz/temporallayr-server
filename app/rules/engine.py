import logging
from typing import Dict, Any, Optional

from app.rules.models import RuleSchema
from app.rules.store import rule_store

logger = logging.getLogger("temporallayr.rules.engine")


class TriggerResult:
    def __init__(self, rule: RuleSchema, event: Dict[str, Any]):
        self.rule = rule
        self.event = event


class RuleEngine:
    """Enterprise evaluating structure isolated securely catching execution triggers mapping robust evaluations."""

    async def evaluate_event(self, event: Dict[str, Any]) -> Optional[TriggerResult]:
        tenant_id = event.get("tenant_id")
        if not tenant_id:
            return None

        try:
            rules = await rule_store.get_rules_for_tenant(tenant_id)
            if not rules:
                return None

            sorted_rules = sorted(rules, key=lambda r: r.priority, reverse=True)
            evaluated_count = 0

            for rule in sorted_rules:
                if not rule.enabled:
                    continue

                evaluated_count += 1
                is_triggered = await self._evaluate_condition(rule, event)

                if is_triggered:
                    logger.info(f"[RULE] triggered rule={rule.id} name='{rule.name}'")
                    logger.info(f"[RULE] evaluated {evaluated_count} rules")
                    return TriggerResult(rule=rule, event=event)

            # If no triggers match
            logger.info(f"[RULE] evaluated {evaluated_count} rules")
            return None

        except Exception as e:
            logger.error(f"[RULE] evaluation failed safe: {e}")
            return None

    async def _evaluate_condition(
        self, rule: RuleSchema, event: Dict[str, Any]
    ) -> bool:
        """Core parsing structurally checking telemetry bounds safely."""
        cond_type = rule.condition.type
        params = rule.condition.parameters

        try:
            if cond_type == "execution_latency":
                duration = event.get("duration", 0)
                threshold = params.get("threshold", 0)
                return duration > threshold

            elif cond_type == "divergence_detected":
                metadata = event.get("metadata", {})
                if isinstance(metadata, dict):
                    return metadata.get("diverged") is True
                return False

            elif cond_type == "node_error_rate":
                # In a fully stateless single-event pass, if 'error' metadata exists, we trigger it instantly
                # simulating a rate spike for this node natively if count is 1
                threshold = params.get("threshold", 1)
                if threshold <= 1:
                    metadata = event.get("metadata", {})
                    meta_str = str(metadata).lower()
                    if "error" in meta_str or "exception" in meta_str:
                        return True
                return False

            elif cond_type == "cluster_anomaly":
                # Match size spike or specific isolation flags mapping
                cluster_size = event.get("cluster_size", 0)
                threshold = params.get("threshold", 100)
                return cluster_size > threshold

            elif cond_type == "custom_expression":
                # Safely evaluate primitive numeric constraints purely on event scalars natively
                # e.g parameters{"field": "duration", "operator": ">", "value": 500} mapped loosely
                # Or just generic simple match
                field_name = params.get("field", "")
                if field_name and field_name in event:
                    try:
                        val = float(event[field_name])
                        target = float(params.get("value", 0))
                        return val > target
                    except (ValueError, TypeError):
                        pass
                return False

            return False

        except Exception as e:
            logger.error(f"Evaluating structurally failed safe internally: {e}")
            return False


rule_engine = RuleEngine()
