import unittest
import asyncio
from fastapi.testclient import TestClient
from app.main import app
from app.rules.engine import rule_engine


# Natively map TestClient bypassing direct DB initialization for REST assertions initially
class TestDetectionRules(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.tenant_id = "dev-test-key"
        self.headers = {"Authorization": f"Bearer {self.tenant_id}"}

        # Mock DB bindings to ensure integration tests run strictly in isolation
        from unittest.mock import patch, MagicMock
        from app.rules.models import RuleSchema

        # Mock add_rule safely returning dict shapes natively simulating DB returns
        self.mock_add_rule = patch("app.api.rules.rule_store.add_rule").start()
        import uuid

        self.rule_uuid = uuid.uuid4()
        self.mock_add_rule.return_value = RuleSchema(
            id=self.rule_uuid,
            tenant_id=self.tenant_id,
            name="High CPU Latency Anomaly",
            enabled=True,
            priority=100,
            condition={
                "type": "custom_expression",
                "parameters": {"field": "duration", "value": 2000},
            },
            actions={"create_incident": True, "severity": "high", "notify": True},
            created_at="2026-01-01T00:00:00Z",
        )

        self.mock_get_rules = patch(
            "app.api.rules.rule_store.get_rules_for_tenant"
        ).start()
        self.mock_get_rules.return_value = [self.mock_add_rule.return_value]

        # The engine also hits rule_store
        self.mock_engine_get_rules = patch(
            "app.rules.engine.rule_store.get_rules_for_tenant"
        ).start()
        self.mock_engine_get_rules.return_value = [self.mock_add_rule.return_value]

        self.mock_delete_rule = patch("app.api.rules.rule_store.delete_rule").start()

        def mock_delete_side_effect(tenant_id, rule_id):
            self.mock_get_rules.return_value = []
            self.mock_engine_get_rules.return_value = []
            return True

        self.mock_delete_rule.side_effect = mock_delete_side_effect

    def tearDown(self):
        from unittest.mock import patch

        patch.stopall()
        self.loop.close()

    def test_rule_lifecycle_and_engine_trigger(self):
        # 1. Create a Custom Expression Rule
        rule_payload = {
            "name": "High CPU Latency Anomaly",
            "enabled": True,
            "priority": 100,
            "condition": {
                "type": "custom_expression",
                "parameters": {"field": "duration", "value": 2000},
            },
            "actions": {"create_incident": True, "severity": "high", "notify": True},
        }

        response = self.client.post(
            "/v1/rules", json=rule_payload, headers=self.headers
        )
        if response.status_code != 201:
            print("Validation Error:", response.json())
        self.assertEqual(response.status_code, 201)

        rule_data = response.json()
        self.assertIn("id", rule_data)
        rule_id = rule_data["id"]

        # 2. Verify rule was created effectively fetching from REST securely
        get_resp = self.client.get("/v1/rules", headers=self.headers)
        self.assertEqual(get_resp.status_code, 200)
        rules_list = get_resp.json()
        self.assertTrue(any(r["id"] == rule_id for r in rules_list))

        # 3. Trigger Engine Manually Natively Bypassing Full Ingestion Overheads
        # A duration of 3000 mapping the "duration" custom_expression conditionally natively > 2000
        anomalous_event = {
            "tenant_id": self.tenant_id,
            "execution_id": "exec-trigger-test",
            "duration": 3500,
            "metadata": {"diverged": False},
        }

        trigger_result = self.loop.run_until_complete(
            rule_engine.evaluate_event(anomalous_event)
        )

        # Assert triggers mapped correctly
        import uuid

        self.assertIsNotNone(trigger_result)
        self.assertEqual(trigger_result.rule.id, uuid.UUID(rule_id))
        self.assertTrue(trigger_result.rule.actions.create_incident)

        # 4. Clean up the rule aggressively ensuring testing isolates
        del_resp = self.client.delete(f"/v1/rules/{rule_id}", headers=self.headers)
        self.assertEqual(del_resp.status_code, 204)

        # Verify eviction mapping
        get_after_del = self.client.get("/v1/rules", headers=self.headers)
        rules_after = get_after_del.json()
        self.assertFalse(any(r["id"] == rule_id for r in rules_after))


if __name__ == "__main__":
    unittest.main()
