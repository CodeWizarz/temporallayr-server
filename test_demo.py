from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)
response = client.get(
    "/v1/stats/top-functions",
    headers={"X-API-Key": "demo-key", "X-Tenant-ID": "demo-tenant"},
)
print(response.status_code)
print(response.json())
