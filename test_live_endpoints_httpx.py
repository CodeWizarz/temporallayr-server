import httpx
import time
import argparse

BASE_URL = "https://temporallayr-server-production.up.railway.app"
HEADERS = {
    "X-API-Key": "demo-key",
    "X-Tenant-ID": "demo-tenant",
    "Content-Type": "application/json",
}


def test_endpoint(client, method, path, data=None):
    url = f"{BASE_URL}{path}"
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
            print(f"  Response (text): {response.text}")

        header_degraded = response.headers.get("x-degraded-status")
        if header_degraded:
            print(f"  [!] Received degraded status header.")

        return response.status_code
    except Exception as e:
        print(f"  [ERROR] {e}")
        return 500


def run_tests():
    print("--- RUNNING FULL SYSTEM TESTS ---")
    with httpx.Client(timeout=15.0) as client:
        test_endpoint(client, "GET", "/health")
        test_endpoint(client, "GET", "/v1/stats/errors?time_range=24h")
        test_endpoint(client, "POST", "/v1/query", data={"query": "#test"})
        test_endpoint(client, "POST", "/v1/ingest", data={"events": []})
    print("---------------------------------")


def poll_health():
    print("Polling /health every 10 seconds for 5 minutes...")
    with httpx.Client(timeout=15.0) as client:
        for _ in range(30):
            test_endpoint(client, "GET", "/health")
            time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--poll", action="store_true", help="Poll health endpoint for 5 minutes"
    )
    args = parser.parse_args()

    if args.poll:
        poll_health()
    else:
        run_tests()
