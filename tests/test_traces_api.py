import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from fastapi.testclient import TestClient
import uuid

from app.main import app
from app.api.auth import verify_api_key


class MockAsyncResult:
    def __init__(self, data):
        self.data = data

    def scalars(self):
        return self

    def first(self):
        return self.data[0] if self.data else None

    def all(self):
        return self.data


class MockAsyncSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def execute(self, query):
        return self._mock_result

    def set_mock_result(self, data):
        self._mock_result = MockAsyncResult(data)


class TestTracesAPI(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Override the verify API mapping cleanly
        from fastapi import Request

        async def mock_verify(request: Request):
            return "tenant-traces-test"

        app.dependency_overrides[verify_api_key] = mock_verify
        self.client = TestClient(app)

    async def asyncTearDown(self):
        app.dependency_overrides.clear()

    @patch("app.query.traces.async_session_maker")
    def test_get_trace_success(self, mock_async_session):
        from app.models.event import Event

        trace_id = str(uuid.uuid4())
        mock_event = Event(
            id=uuid.UUID(trace_id),
            tenant_id="tenant-traces-test",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            payload={"foo": "bar", "status": "COMPLETED"},
        )

        mock_session_instance = MockAsyncSession()
        mock_session_instance.set_mock_result([mock_event])
        mock_async_session.return_value = mock_session_instance

        res = self.client.get(
            f"/v1/traces/{trace_id}",
            headers={"Authorization": "Bearer tenant-traces-test"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()["trace"]
        self.assertEqual(data["id"], trace_id)
        self.assertEqual(data["tenant_id"], "tenant-traces-test")
        self.assertEqual(data["payload"]["foo"], "bar")

    @patch("app.query.traces.async_session_maker")
    def test_get_trace_not_found(self, mock_async_session):
        trace_id = str(uuid.uuid4())

        mock_session_instance = MockAsyncSession()
        mock_session_instance.set_mock_result([])  # Empty result
        mock_async_session.return_value = mock_session_instance

        res = self.client.get(
            f"/v1/traces/{trace_id}",
            headers={"Authorization": "Bearer tenant-traces-test"},
        )

        self.assertEqual(res.status_code, 404)

    def test_get_trace_invalid_uuid(self):
        res = self.client.get(
            "/v1/traces/invalid-uuid",
            headers={"Authorization": "Bearer tenant-traces-test"},
        )
        self.assertEqual(res.status_code, 404)  # It returns None, API raises 404

    @patch("app.query.traces.async_session_maker")
    def test_list_traces(self, mock_async_session):
        from app.models.event import Event

        e1_id = uuid.uuid4()
        e2_id = uuid.uuid4()

        mock_events = [
            Event(
                id=e1_id,
                tenant_id="tenant-traces-test",
                timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
                payload={"status": "FAILED"},
            ),
            Event(
                id=e2_id,
                tenant_id="tenant-traces-test",
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                payload={"status": "COMPLETED"},
            ),
        ]

        mock_session_instance = MockAsyncSession()
        mock_session_instance.set_mock_result(mock_events)
        mock_async_session.return_value = mock_session_instance

        res = self.client.get(
            "/v1/traces?limit=10",
            headers={"Authorization": "Bearer tenant-traces-test"},
        )

        self.assertEqual(res.status_code, 200)
        items = res.json()["items"]
        self.assertEqual(len(items), 2)

        self.assertEqual(items[0]["id"], str(e1_id))
        self.assertTrue(items[0]["error"])

        self.assertEqual(items[1]["id"], str(e2_id))
        self.assertFalse(items[1]["error"])
