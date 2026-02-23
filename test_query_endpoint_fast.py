import unittest
from fastapi.testclient import TestClient
from app.api.query import router
from fastapi import FastAPI, Depends


# Mock verify_api_key
async def mock_verify_api_key():
    return "demo-tenant"


app = FastAPI()

# We override the dependency in the router but the easiest way is to override in the app
app.dependency_overrides[Depends] = mock_verify_api_key
app.include_router(router)


class TestQueryEndpointIntegration(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.headers = {"Authorization": "Bearer dev-test-key"}

    def test_valid_query(self):
        response = self.client.post(
            "/query",
            json={"query": 'tenant == "demo"', "limit": 50},
            headers=self.headers,
        )
        # If the endpoint doesn't support the raw test exactly without storage dependencies, we mock storage.
        # But let's just assert the parser works and it hits the engine.
        self.assertEqual(response.status_code, 200)

    def test_invalid_query_400(self):
        response = self.client.post(
            "/query", json={"query": "tenant ==", "limit": 50}, headers=self.headers
        )
        self.assertEqual(response.status_code, 400)
        self.assertTrue("Missing value" in response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
