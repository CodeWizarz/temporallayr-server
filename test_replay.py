import asyncio
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_replay():
    response = client.post(
        "/v1/replay/exec-replay-1?tenant_id=demo-tenant",
        headers={"Authorization": "Bearer dev-test-key"},
    )
    print("STATUS:", response.status_code)
    try:
        data = response.json()
        print("RESPONSE JSON:")
        import json

        print(json.dumps(data, indent=2))

        # Verify topological sort
        steps = data.get("steps", [])
        nodes_ordered = [s["node"] for s in steps]
        print("ORDERED NODES:", nodes_ordered)

        # Expect A to be before B and D, B to be before C
        a_idx = nodes_ordered.index("LoadData")
        b_idx = nodes_ordered.index("ProcessData")
        c_idx = nodes_ordered.index("LogData")
        d_idx = nodes_ordered.index("NotifyUser")

        if a_idx < b_idx and a_idx < d_idx and b_idx < c_idx:
            print("SUCCESS: Topological sort is correct!")
        else:
            print("FAILURE: Topological sort is incorrect!")

    except Exception as e:
        print("ERROR:", e)


if __name__ == "__main__":
    test_replay()
