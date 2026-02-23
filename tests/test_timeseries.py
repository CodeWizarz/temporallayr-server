import unittest
import asyncio
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from fastapi.testclient import TestClient

from app.main import app
from app.api.auth import verify_api_key


class MockAsyncResult:
    def __init__(self, data):
        self.data = data

    def __aiter__(self):
        self.it = iter(self.data)
        return self

    async def __anext__(self):
        try:
            return next(self.it)
        except StopIteration:
            raise StopAsyncIteration


class MockAsyncSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def stream(self, query):
        # We will dynamically mock DB loops based on query contents mapped globally via patches.
        # But for test isolation we can return a configurable stream.
        return self._mock_stream

    def set_mock_stream(self, data):
        self._mock_stream = MockAsyncResult(data)


class TestTimeSeriesEngine(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Override the verify API mapping cleanly
        from fastapi import Request

        async def mock_verify(request: Request):
            return "tenant-metrics"

        app.dependency_overrides[verify_api_key] = mock_verify
        self.client = TestClient(app)

    async def asyncTearDown(self):
        app.dependency_overrides.clear()

    @patch("app.query.timeseries.async_session_maker")
    async def test_timeseries_bucket_groupings(self, mock_async_session):
        from app.query.timeseries import aggregate_timeseries
        from app.models.event import Event

        # Crafting 3 distinct timestamped events sequentially crossing bounds
        # Event 1: 00:00:10 (Bucket 00:00:00)
        # Event 2: 00:00:15 (Bucket 00:00:00) - Error
        # Event 3: 00:01:05 (Bucket 00:01:00)
        t_base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()

        events = [
            (
                Event(
                    tenant_id="tenant-metrics",
                    timestamp=datetime.fromtimestamp(t_base + 10, tz=timezone.utc),
                    payload={"status": "COMPLETED", "metrics": {"duration_ms": 100.0}},
                ),
            ),
            (
                Event(
                    tenant_id="tenant-metrics",
                    timestamp=datetime.fromtimestamp(t_base + 15, tz=timezone.utc),
                    payload={"status": "FAILED", "metrics": {"duration_ms": 300.0}},
                ),
            ),
            (
                Event(
                    tenant_id="tenant-metrics",
                    timestamp=datetime.fromtimestamp(t_base + 65, tz=timezone.utc),
                    payload={"status": "COMPLETED", "metrics": {"duration_ms": 200.0}},
                ),
            ),
        ]

        mock_session_instance = MockAsyncSession()
        mock_session_instance.set_mock_stream(events)
        mock_async_session.return_value = mock_session_instance

        # Test 1: Execution Count Metric (60s buckets)
        start_t = datetime.fromtimestamp(t_base, tz=timezone.utc)
        end_t = datetime.fromtimestamp(t_base + 120, tz=timezone.utc)

        res1 = await aggregate_timeseries(
            "tenant-metrics", start_t, end_t, 60, "execution_count"
        )

        self.assertEqual(len(res1), 2)
        self.assertEqual(res1[0]["count"], 2)  # Bucket 0: 2 events
        self.assertEqual(res1[0]["value"], 2)  # metric == execution_count
        self.assertEqual(res1[1]["count"], 1)  # Bucket 1: 1 event

        # Test 2: Checking Error Rates dynamically bounding 50% cleanly over buckets correctly
        res2 = await aggregate_timeseries(
            "tenant-metrics", start_t, end_t, 60, "error_rate"
        )
        self.assertEqual(res2[0]["value"], 50.0)
        self.assertEqual(res2[1]["value"], 0.0)

        # Test 3: Checking Latency Averages dynamically
        res3 = await aggregate_timeseries(
            "tenant-metrics", start_t, end_t, 60, "latency_avg"
        )
        self.assertEqual(res3[0]["value"], 200.0)  # (100+300)/2
        self.assertEqual(res3[1]["value"], 200.0)

    def test_compute_percentile(self):
        from app.query.timeseries import _compute_percentile

        data = [15.0, 20.0, 35.0, 40.0, 50.0]  # sorted length 5

        # p50 = index 2 = 35.0
        p50 = _compute_percentile(data, 50)
        self.assertEqual(p50, 35.0)

        # Empty boundary natively yields zeros dynamically
        self.assertEqual(_compute_percentile([], 90), 0.0)

    @patch("app.query.timeseries.async_session_maker")
    def test_metrics_api_endpoint(self, mock_async_session):
        mock_session_instance = MockAsyncSession()
        mock_session_instance.set_mock_stream(
            []
        )  # Return empty natively bypassing mock limits
        mock_async_session.return_value = mock_session_instance

        payload = {
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            "interval": 3600,
            "metric": "latency_p95",
        }

        res = self.client.get(
            "/v1/metrics/timeseries",
            headers={"Authorization": "Bearer tenant-metrics"},
            params=payload,
        )

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["series"], [])

    def test_metrics_api_invalid_bounds(self):
        # Tracking bad intervals rejecting with HTTP 400 safely.
        res = self.client.get(
            "/v1/metrics/timeseries",
            headers={"Authorization": "Bearer tenant-metrics"},
            params={
                "start": "invalid_date",
                "end": "invalid_date",
                "interval": 0,
                "metric": "invalid",
            },
        )
        self.assertEqual(res.status_code, 400)
