import unittest
import asyncio
from fastapi.testclient import TestClient

from app.main import app
from app.models.event import Event


class TestQueryEngine(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.tenant_id = "dev-test-key"
        self.headers = {"Authorization": f"Bearer {self.tenant_id}"}

    def tearDown(self):
        self.loop.close()

    def test_query_engine_node_search(self):
        """Simulate ingesting traces efficiently scaling DB bounds organically."""
        from unittest.mock import patch
        import datetime
        import uuid

        mock_execute = patch(
            "app.query.engine.QueryEngine._execute_with_safeguards"
        ).start()

        mock_event = Event(
            id=uuid.uuid4(),
            tenant_id=self.tenant_id,
            timestamp=datetime.datetime.utcnow(),
            payload={
                "execution_id": "exec-123",
                "graph": {
                    "nodes": [
                        {"name": "fake_llm_call", "output": 20},
                        {"name": "other_node", "output": 10},
                    ]
                },
            },
        )

        mock_execute.return_value = ([mock_event], False)

        # Query matching exact node
        payload = {"filters": {"node_name": "fake_llm_call"}}

        resp = self.client.post("/v1/query/nodes", json=payload, headers=self.headers)
        if resp.status_code != 200:
            print("JSON Validation errors natively mapped bounds:", resp.json())
        self.assertEqual(resp.status_code, 200)

        data = resp.json()
        nodes = data.get("data", [])

        # Asserts structural node extraction dynamically bypassed extraneous nodes organically
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]["name"], "fake_llm_call")
        self.assertEqual(nodes[0]["output"], 20)
        self.assertFalse(data.get("partial"))

        patch.stopall()
