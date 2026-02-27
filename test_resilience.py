from fastapi.testclient import TestClient
from app.main import app
import json

client = TestClient(app)

print("Testing /health endpoint...")
response = client.get("/health")
print("Response status code:", response.status_code)
print("Response JSON:", json.dumps(response.json(), indent=2))

print("\nTesting /v1/query endpoint without DB...")
response2 = client.post(
    "/v1/stats/top-functions?tenant_id=demo-tenant",
    headers={"X-API-Key": "demo-key", "X-Tenant-ID": "demo-tenant"},
)
print("Response status code:", response2.status_code)
print("Response text:", response2.text)
