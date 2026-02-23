import httpx
import json

BASE_URL = "http://localhost:8000"
API_KEY = "demo-tenant"

HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def verify_vc_demo_backend():
    print("=== VC Demo Validation Script ===")

    success = True

    with httpx.Client(base_url=BASE_URL, headers=HEADERS, timeout=10.0) as client:
        # TEST 1: Advanced Search ILIKE
        print("\n[TEST] POST /v1/search (ILIKE 'fake_llm_call')")
        try:
            res = client.post(
                "/v1/search",
                json={
                    "tenant_id": API_KEY,
                    "limit": 5,
                    "contains": "fake_llm_call",
                    "filters": {"status": "SUCCESS"},
                },
            )
            if res.status_code == 200 and len(res.json().get("results", [])) > 0:
                print("  => SUCCESS")
            else:
                print(f"  => FAILED: HTTP {res.status_code}")
                success = False
        except Exception as e:
            print("  => ERROR:", str(e))
            success = False

        # TEST 2: Durations Extraction
        print("\n[TEST] GET /v1/stats/durations (Percentiles)")
        try:
            res = client.get("/v1/stats/durations", params={"tenant_id": API_KEY})
            if res.status_code == 200 and "p95_duration_ms" in res.json():
                print("  => SUCCESS:", json.dumps(res.json(), indent=2))
            else:
                print(f"  => FAILED: HTTP {res.status_code}")
                success = False
        except Exception as e:
            print("  => ERROR:", str(e))
            success = False

        # TEST 3: Overview Macro Dashboard
        print("\n[TEST] GET /v1/overview")
        try:
            res = client.get("/v1/overview", params={"tenant_id": API_KEY})
            if res.status_code == 200 and res.json().get("events_last_24h", 0) > 0:
                print("  => SUCCESS:", json.dumps(res.json(), indent=2))
            else:
                print(f"  => FAILED: HTTP {res.status_code}")
                success = False
        except Exception as e:
            print("  => ERROR:", str(e))
            success = False

        # TEST 4: Schema Detector
        print("\n[TEST] GET /v1/schema")
        try:
            res = client.get("/v1/schema", params={"tenant_id": API_KEY})
            if res.status_code == 200 and len(res.json().get("fields", [])) > 0:
                print(
                    "  => SUCCESS:", json.dumps(res.json().get("fields")[:5], indent=2)
                )
            else:
                print(f"  => FAILED: HTTP {res.status_code}")
                success = False
        except Exception as e:
            print("  => ERROR:", str(e))
            success = False

    if success:
        print("\n=== TEMPORALLAYR DEMO READY ===")
    else:
        print("\n=== DEMO VALIDATION FAILED ===")
        exit(1)


if __name__ == "__main__":
    verify_vc_demo_backend()
