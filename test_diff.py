import asyncio
from fastapi.testclient import TestClient
from app.main import app
import json

client = TestClient(app)


def test_diff():
    # Test diffing execution graphs natively
    response = client.post(
        "/v1/diff",
        headers={"Authorization": "Bearer dev-test-key"},
        json={"execution_a": "ID1", "execution_b": "ID2"},
    )
    print("STATUS:", response.status_code)
    try:
        data = response.json()
        print("RESPONSE JSON:")
        print(json.dumps(data, indent=2))

        # Verify inputs changed output correctly
        diffs = data.get("differences", [])
        if data.get("total_nodes_compared") == 2 and len(diffs) == 2:
            print("SUCCESS: Diff engine works!")
        else:
            print("FAILURE: Diff engine is incorrect!")

    except Exception as e:
        print("ERROR:", e)


if __name__ == "__main__":
    test_diff()
