import unittest
from fastapi.testclient import TestClient
from types import SimpleNamespace

from app.main import app
from app.api.dashboard import dashboard_service


# To sidestep SQLAlchemy dependencies cleanly, we mock the dashboard_service entirely
class MockDashboardService:
    def __init__(self):
        self.queries = {}
        self.dashboards = {}
        self.panels = []

    async def create_saved_query(self, tenant_id, name, query_json):
        q_id = f"q-{len(self.queries)}"
        q = {
            "id": q_id,
            "tenant_id": tenant_id,
            "name": name,
            "query_json": query_json,
            "created_at": "2026-01-01T00:00:00Z",
        }
        self.queries[q_id] = q
        return SimpleNamespace(**q)

    async def list_saved_queries(self, tenant_id):
        res = []
        for q in self.queries.values():
            if q["tenant_id"] == tenant_id:
                res.append(SimpleNamespace(**q))
        return res

    async def create_dashboard(self, tenant_id, name):
        d_id = f"d-{len(self.dashboards)}"
        d = {
            "id": d_id,
            "tenant_id": tenant_id,
            "name": name,
            "created_at": "2026-01-01T00:00:00Z",
        }
        self.dashboards[d_id] = d
        return SimpleNamespace(**d)

    async def list_dashboards(self, tenant_id):
        res = []
        for d in self.dashboards.values():
            if d["tenant_id"] == tenant_id:
                res.append(SimpleNamespace(**d))
        return res

    async def add_panel_to_dashboard(
        self, tenant_id, dashboard_id, saved_query_id, name, pos_x, pos_y, width, height
    ):
        if saved_query_id not in self.queries:
            raise ValueError("SavedQuery not found.")

        q = self.queries[saved_query_id]
        if q["tenant_id"] != tenant_id:
            raise PermissionError(
                "Access denied: cross-tenant bounding constraint violation safely."
            )

        p_id = f"p-{len(self.panels)}"
        p = {
            "id": p_id,
            "tenant_id": tenant_id,
            "dashboard_id": dashboard_id,
            "name": name,
            "saved_query_id": saved_query_id,
            "position_x": pos_x,
            "position_y": pos_y,
            "width": width,
            "height": height,
        }
        self.panels.append(p)
        return SimpleNamespace(**p)

    async def get_dashboard_with_panels(self, tenant_id, dashboard_id):
        if dashboard_id not in self.dashboards:
            return None
        d = self.dashboards[dashboard_id]
        if d["tenant_id"] != tenant_id:
            return None

        panels_map = []
        for p in self.panels:
            if p["dashboard_id"] == dashboard_id and p["tenant_id"] == tenant_id:
                q = self.queries[p["saved_query_id"]]
                panels_map.append(
                    {
                        "panel_id": p["id"],
                        "name": p["name"],
                        "position_x": p["position_x"],
                        "position_y": p["position_y"],
                        "width": p["width"],
                        "height": p["height"],
                        "saved_query": q,
                    }
                )

        return {
            "dashboard_id": d["id"],
            "name": d["name"],
            "created_at": d["created_at"],
            "panels": panels_map,
        }


class TestDashboardBackend(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.tenant_id = "tenant-a"
        self.tenant_b = "tenant-b"

        # Override the API dependency organically
        from app.api import dashboard

        self.original_service = dashboard.dashboard_service
        self.mock_service = MockDashboardService()
        dashboard.dashboard_service = self.mock_service

        from app.api.auth import verify_api_key
        from fastapi import Request

        async def mock_verify(request: Request):
            auth = request.headers.get("Authorization")
            if auth and auth.startswith("Bearer "):
                return auth.split(" ")[1]
            return None

        app.dependency_overrides[verify_api_key] = mock_verify

    def tearDown(self):
        from app.api import dashboard
        from app.api.auth import verify_api_key

        dashboard.dashboard_service = self.original_service
        app.dependency_overrides.pop(verify_api_key, None)

    def test_saved_query_lifecycle(self):
        """Testing mapping query JSON boundaries accurately."""
        res = self.client.post(
            "/v1/saved-query",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"name": "Error Query", "query_json": {"status": "error"}},
        )
        self.assertEqual(res.status_code, 201, res.json())
        q_id = res.json()["id"]

        res = self.client.get(
            "/v1/saved-query", headers={"Authorization": f"Bearer {self.tenant_id}"}
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(len(res.json()), 1)

    def test_dashboard_lifecycle(self):
        """Creates panel layouts linking to Saved Queries safely resolving Cross-Tenant traps natively."""
        # 1. Tenant A setup
        q_res = self.client.post(
            "/v1/saved-query",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"name": "A", "query_json": {}},
        )
        q_id = q_res.json()["id"]

        d_res = self.client.post(
            "/v1/dashboard",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"name": "Dash A"},
        )
        d_id = d_res.json()["id"]

        # 2. Add panel
        p_res = self.client.post(
            f"/v1/dashboard/{d_id}/panel",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"name": "Panel A", "saved_query_id": q_id, "width": 2, "height": 2},
        )
        self.assertEqual(p_res.status_code, 201)

        # 3. Retrieve nested grid
        get_res = self.client.get(
            f"/v1/dashboard/{d_id}",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
        )
        self.assertEqual(get_res.status_code, 200)
        self.assertEqual(len(get_res.json()["panels"]), 1)
        self.assertEqual(get_res.json()["panels"][0]["saved_query"]["id"], q_id)

    def test_dashboard_cross_tenant_isolation(self):
        """Tests security bounds validating 403 blocks."""
        # Tenant B creates a query
        q_res = self.client.post(
            "/v1/saved-query",
            headers={"Authorization": f"Bearer {self.tenant_b}"},
            json={"name": "B", "query_json": {}},
        )
        q_id = q_res.json()["id"]

        # Tenant A tries creating a panel appending Tenant B's saved query (Should 403 block natively)
        d_res = self.client.post(
            "/v1/dashboard",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"name": "Dash A"},
        )
        d_id = d_res.json()["id"]

        p_res = self.client.post(
            f"/v1/dashboard/{d_id}/panel",
            headers={"Authorization": f"Bearer {self.tenant_id}"},
            json={"name": "Malicious Panel", "saved_query_id": q_id},
        )
        self.assertEqual(p_res.status_code, 403)
        self.assertTrue("forbidden" in p_res.json()["detail"].lower())
