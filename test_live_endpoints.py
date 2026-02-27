import urllib.request
import urllib.error
import urllib.parse
import json

BASE_URL = "https://temporallayr-server-production.up.railway.app"
API_KEY = "demo-key"
TENANT_ID = "demo-tenant"

endpoints = [
    {
        "name": "Health Check",
        "method": "GET",
        "url": f"{BASE_URL}/health",
        "payload": None,
    },
    {
        "name": "Top Functions",
        "method": "GET",
        "url": f"{BASE_URL}/v1/stats/top-functions?tenant_id={TENANT_ID}",
        "payload": None,
    },
    {
        "name": "Dashboard Overview",
        "method": "GET",
        "url": f"{BASE_URL}/v1/dashboard/overview?tenant_id={TENANT_ID}",
        "payload": None,
    },
    {
        "name": "Query Search",
        "method": "POST",
        "url": f"{BASE_URL}/v1/dashboard/search",
        "payload": {"tenant_id": TENANT_ID, "limit": 10},
    },
    {
        "name": "Ingest Event",
        "method": "POST",
        "url": f"{BASE_URL}/v1/ingest",
        "payload": {"api_key": API_KEY, "tenant_id": TENANT_ID, "events": []},
    },
]


def run_tests():
    print(f"Testing Live Endpoints at {BASE_URL}...\n")
    headers = {
        "X-API-Key": API_KEY,
        "X-Tenant-ID": TENANT_ID,
        "Content-Type": "application/json",
    }

    for ep in endpoints:
        print(f"--- {ep['name']} ({ep['method']} {ep['url']}) ---")
        try:
            data = None
            if ep["payload"] is not None:
                data = json.dumps(ep["payload"]).encode("utf-8")

            req = urllib.request.Request(
                ep["url"], data=data, headers=headers, method=ep["method"]
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                status = response.getcode()
                body = response.read().decode("utf-8")
                res_headers = dict(response.getheaders())

                print(f"STATUS: {status}")
                print(
                    f"X-DB-Status Header: {res_headers.get('X-DB-Status', 'Not Present')}"
                )
                try:
                    parsed_body = json.loads(body)
                    print(
                        f"BODY: {json.dumps(parsed_body, indent=2)[:200]}..."
                    )  # truncated
                except json.JSONDecodeError:
                    print(f"BODY: {body[:200]}")
        except urllib.error.HTTPError as e:
            print(f"CRITICAL ERROR: {e.code} - {e.reason}")
            print(f"BODY: {e.read().decode('utf-8')[:200]}")
        except urllib.error.URLError as e:
            print(f"NETWORK ERROR: {e.reason}")
        print("\n")


if __name__ == "__main__":
    run_tests()
