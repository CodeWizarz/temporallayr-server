import httpx
import time
import argparse

HEADERS = {
    "X-API-Key": "demo-key",
    "X-Tenant-ID": "demo-tenant",
    "Content-Type": "application/json",
}


def test_endpoint(client, base_url, method, path, data=None):
    url = f"{base_url}{path}"
    print(f"Testing {method} {url}...")
    try:
        if method == "GET":
            response = client.get(url, headers=HEADERS)
        elif method == "POST":
            response = client.post(url, headers=HEADERS, json=data)

        print(f"  Status: {response.status_code}")
        try:
            print(f"  Response: {response.json()}")
        except Exception:
            print(f"  Response (text): {response.text[:200]}")

        header_degraded = response.headers.get("x-degraded-status")
        if header_degraded:
            print(f"  [!] Received degraded status header.")

        return response.status_code
    except Exception as e:
        print(f"  [ERROR] {e}")
        return 500


def run_tests(base_url):
    print(f"--- RUNNING FULL SYSTEM TESTS AGAINST {base_url} ---")
    with httpx.Client(timeout=15.0) as client:
        test_endpoint(client, base_url, "GET", "/health")
        test_endpoint(client, base_url, "GET", "/v1/stats/errors?tenant_id=demo-tenant")
        test_endpoint(
            client,
            base_url,
            "POST",
            "/v1/query/events",
            data={"tenant_id": "demo-tenant", "limit": 10},
        )
        test_endpoint(
            client,
            base_url,
            "POST",
            "/v1/ingest",
            data={"events": [], "api_key": "demo-key", "tenant_id": "demo-tenant"},
        )
    print("---------------------------------")


def poll_health(base_url):
    print(f"Polling {base_url}/health every 10 seconds for 5 minutes...")
    with httpx.Client(timeout=15.0) as client:
        for _ in range(30):
            test_endpoint(client, base_url, "GET", "/health")
            time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url",
        default="https://temporallayr-server-production.up.railway.app",
        help="Base URL to test",
    )
    parser.add_argument(
        "--poll", action="store_true", help="Poll health endpoint for 5 minutes"
    )
    args = parser.parse_args()

    url = args.url.rstrip("/")

    if args.poll:
        poll_health(url)
    else:
        run_tests(url)
