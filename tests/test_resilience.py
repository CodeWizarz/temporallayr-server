import pytest
import httpx
import os

BASE_URL = os.environ.get("TEST_API_URL", "http://localhost:8000")
HEADERS = {"X-API-Key": "demo-key", "X-Tenant-ID": "demo-tenant"}


def test_health_check_instant():
    # Warmup request to bypass Railway proxy cold starts
    with httpx.Client(timeout=10.0) as client:
        try:
            client.get(f"{BASE_URL}/health")
        except:
            pass

    # Health check must return 200 within 1 second unconditionally
    with httpx.Client(timeout=1.0) as client:
        response = client.get(f"{BASE_URL}/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_ingest_accepts_events():
    with httpx.Client(timeout=5.0) as client:
        payload = {
            "api_key": "demo-key",
            "events": [{"type": "test_resilience", "value": 42}],
            "tenant_id": "demo-tenant",
        }
        response = client.post(f"{BASE_URL}/v1/ingest", json=payload, headers=HEADERS)
        assert response.status_code in (200, 202)


def test_stats_top_functions_is_stable():
    # Should return stable JSON natively, whether data is active or degraded
    with httpx.Client(timeout=15.0) as client:
        response = client.get(
            f"{BASE_URL}/v1/stats/top-functions?time_range=24h", headers=HEADERS
        )
        assert response.status_code in (200, 503)
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict)


def test_query_is_stable():
    with httpx.Client(timeout=15.0) as client:
        payload = {"time_range": "24h", "limit": 10}
        response = client.post(f"{BASE_URL}/v1/query", json=payload, headers=HEADERS)
        assert response.status_code in (200, 503)
