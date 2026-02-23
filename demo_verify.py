import httpx
import json

BASE_URL = "http://localhost:8000"
API_KEY = "dev-test-key"

HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def verify_dashboard_backend():
    print("=== Running VC Demo Validations ===")

    with httpx.Client(base_url=BASE_URL, headers=HEADERS, timeout=10.0) as client:
        # TEST 1: Time Bucket Engine
        try:
            print("[TEST] Fetching DateTrunc Time Bucket Engine (POST /v1/query)...")
            res = client.post(
                "/v1/query",
                json={
                    "tenant_id": API_KEY,
                    "from": "2026-01-01T00:00:00Z",
                    "to": "2026-01-02T00:00:00Z",
                    "group_by": "hour",
                    "metric": "execution_graph",
                },
            )
            if res.status_code == 200:
                print("  => SUCCESS:", json.dumps(res.json(), indent=2))
            else:
                print(f"  => FAILED: HTTP {res.status_code} {res.text}")
        except Exception as e:
            print("  => ERROR:", str(e))

        # TEST 2: Top Functions
        try:
            print(
                "\n[TEST] Fetching Top Nodes JSON Array (GET /v1/stats/top-functions)..."
            )
            res = client.get("/v1/stats/top-functions", params={"tenant_id": API_KEY})
            if res.status_code == 200:
                print("  => SUCCESS:", json.dumps(res.json(), indent=2))
            else:
                print(f"  => FAILED: HTTP {res.status_code} {res.text}")
        except Exception as e:
            print("  => ERROR:", str(e))

        # TEST 3: Error Rate Indicator
        try:
            print("\n[TEST] Fetching Error Rate Aggregation (GET /v1/stats/errors)...")
            res = client.get("/v1/stats/errors", params={"tenant_id": API_KEY})
            if res.status_code == 200:
                print("  => SUCCESS:", json.dumps(res.json(), indent=2))
            else:
                print(f"  => FAILED: HTTP {res.status_code} {res.text}")
        except Exception as e:
            print("  => ERROR:", str(e))


if __name__ == "__main__":
    verify_dashboard_backend()
