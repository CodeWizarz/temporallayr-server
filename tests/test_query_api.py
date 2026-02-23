import unittest
import asyncio
from datetime import datetime, UTC, timedelta
from fastapi.testclient import TestClient

from app.main import app
from app.services.storage_service import StorageService
from app.api.query import get_storage_service


GLOBAL_MOCK_DB = []
for i in range(10):
    GLOBAL_MOCK_DB.append(
        {
            "api_key": "dev-test-key",
            "timestamp": datetime.now(UTC) - timedelta(minutes=i),
            "payload": {
                "id": f"exec-{i}",
                "type": "execution_graph" if i % 2 == 0 else "incident_created",
                "fingerprint": f"fp-{i % 3}",
            },
        }
    )


class TestQueryAPI(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.tenant_id = "dev-test-key"

        # Inject mock storage override
        self.original_storage = get_storage_service

        class MockStorage(StorageService):
            async def query_analytics_events(
                self,
                tenant_id,
                start_time=None,
                end_time=None,
                fingerprint=None,
                event_type=None,
                limit=100,
                offset=0,
                sort="desc",
            ):
                # Fast dummy filter mappings
                results = []
                for item in GLOBAL_MOCK_DB:
                    if item["api_key"] != tenant_id:
                        continue
                    if fingerprint and item["payload"]["fingerprint"] != fingerprint:
                        continue
                    if event_type and item["payload"]["type"] != event_type:
                        continue
                    results.append(item["payload"])
                return results[offset : offset + limit]

        app.dependency_overrides[get_storage_service] = MockStorage

    def tearDown(self):
        app.dependency_overrides.clear()

    def test_query_analytics_pagination(self):
        """Validate /v1/query organically resolving bounds gracefully."""
        # Test simple query matching everything
        res = self.client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"limit": 100},
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["count"], 10)

        # Test limit 4 natively
        res2 = self.client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"limit": 4},
        )
        self.assertEqual(res2.json()["count"], 4)

    def test_query_analytics_fingerprint(self):
        """Ensure precise topological text matching applies intelligently."""
        res = self.client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"fingerprint": "fp-0"},
        )
        self.assertEqual(res.status_code, 200)
        self.assertTrue(res.json()["count"] > 0)
        self.assertEqual(res.json()["results"][0]["fingerprint"], "fp-0")

    def test_query_analytics_limit_boundary(self):
        """Validate HTTP 400 safely triggering over 1000 limit mappings natively."""
        res = self.client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"limit": 2000},
        )
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["detail"], "Limit cannot exceed 1000")
