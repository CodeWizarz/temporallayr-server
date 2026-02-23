import unittest
import asyncio
from unittest.mock import patch, MagicMock
from types import SimpleNamespace
from fastapi.testclient import TestClient

from app.main import app
from app.api.auth import verify_api_key


class MockDashboardServiceRuntime:
    async def list_saved_queries(self, tenant_id):
        return [
            SimpleNamespace(
                id="sq-1", tenant_id=tenant_id, query_json={"event_type": "event_a"}
            ),
            SimpleNamespace(
                id="sq-2", tenant_id=tenant_id, query_json={"event_type": "event_b"}
            ),
        ]

    async def get_dashboard_with_panels(self, tenant_id, dashboard_id):
        if dashboard_id == "dash-1":
            return {
                "dashboard_id": "dash-1",
                "panels": [
                    {
                        "panel_id": "p-1",
                        "name": "Panel 1",
                        "saved_query": {"id": "sq-1"},
                    },
                    {
                        "panel_id": "p-2",
                        "name": "Panel 2",
                        "saved_query": {"id": "sq-2"},
                    },
                    {
                        "panel_id": "p-3",
                        "name": "Panel Fail",
                        "saved_query": {"id": "sq-fail"},
                    },
                    {
                        "panel_id": "p-4",
                        "name": "Panel Timeout",
                        "saved_query": {"id": "sq-timeout"},
                    },
                ],
            }
        return None


class MockQueryEngine:
    async def query(self, request):
        event_val = request.model_dump().get("event_type", "event_a")
        return SimpleNamespace(
            data=[SimpleNamespace(model_dump=lambda: {"mock_data": f"res_{event_val}"})]
        )


class TestDashboardRuntime(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import app.query.runtime as runtime

        self.original_dash_service = runtime.dashboard_service
        self.original_query_engine = runtime.query_engine

        runtime.dashboard_service = MockDashboardServiceRuntime()
        runtime.query_engine = MockQueryEngine()

        from fastapi import Request

        async def mock_verify(request: Request):
            return "tenant-1"

        app.dependency_overrides[verify_api_key] = mock_verify
        self.client = TestClient(app)

    async def asyncTearDown(self):
        import app.query.runtime as runtime

        runtime.dashboard_service = self.original_dash_service
        runtime.query_engine = self.original_query_engine
        app.dependency_overrides.clear()

    async def test_execute_saved_query_success(self):
        from app.query.runtime import execute_saved_query

        res = await execute_saved_query("sq-1", "tenant-1")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["mock_data"], "res_event_a")

    @patch("app.query.runtime.execute_saved_query")
    async def test_execute_dashboard_concurrently_with_failures(self, mock_execute):
        # We'll mock the internal execute_saved_query block so it dynamically mimics structural failures perfectly
        async def mock_execute_function(saved_query_id, tenant_id):
            if saved_query_id == "sq-1":
                return [{"data": "success-1"}]
            if saved_query_id == "sq-2":
                return [{"data": "success-2"}]
            if saved_query_id == "sq-fail":
                raise ValueError("Simulated DB layer break")
            if saved_query_id == "sq-timeout":
                raise asyncio.TimeoutError("Simulated 10s wait")

        mock_execute.side_effect = mock_execute_function

        from app.query.runtime import execute_dashboard

        res = await execute_dashboard("dash-1", "tenant-1")

        self.assertEqual(res["dashboard_id"], "dash-1")
        self.assertEqual(len(res["panels"]), 4)

        # Verify successes
        p1 = next(p for p in res["panels"] if p["panel_id"] == "p-1")
        self.assertEqual(p1["data"][0]["data"], "success-1")

        p2 = next(p for p in res["panels"] if p["panel_id"] == "p-2")
        self.assertEqual(p2["data"][0]["data"], "success-2")

        # Verify caught exceptions securely mapping to inner API structure blocking cascading execution break natively!
        p3 = next(p for p in res["panels"] if p["panel_id"] == "p-3")
        self.assertEqual(p3["data"], [])
        self.assertTrue("Simulated DB layer break" in p3["error"])

        # Verify 10s Timeout bounds
        p4 = next(p for p in res["panels"] if p["panel_id"] == "p-4")
        self.assertEqual(p4["data"], [])
        self.assertTrue("timed out after 10s" in p4["error"])

    def test_run_dashboard_http_endpoint(self):
        # Full integration bounding fast native requests securely via TestClient
        with patch("app.query.runtime.execute_saved_query") as mock_execute:

            async def mock_execute_function(saved_query_id, tenant_id):
                return [{"data": "simulated_http_pass"}]

            mock_execute.side_effect = mock_execute_function

            res = self.client.get(
                "/v1/dashboard/dash-1/run", headers={"Authorization": "Bearer tenant-1"}
            )

            self.assertEqual(res.status_code, 200)
            json_payload = res.json()
            self.assertEqual(json_payload["dashboard_id"], "dash-1")
            self.assertEqual(len(json_payload["panels"]), 4)
            self.assertEqual(
                json_payload["panels"][0]["data"][0]["data"], "simulated_http_pass"
            )

    def test_run_dashboard_http_endpoint_404(self):
        res = self.client.get(
            "/v1/dashboard/dash-9999/run", headers={"Authorization": "Bearer tenant-1"}
        )
        self.assertEqual(res.status_code, 404)
        self.assertTrue("not found natively" in res.json()["detail"])
