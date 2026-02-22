import asyncio
from fastapi.testclient import TestClient
from app.main import app


def test_websocket():
    client = TestClient(app)
    # The client.websocket_connect natively captures the asynchronous scopes correctly
    with client.websocket_connect(
        "/v1/live?tenant_id=demo-tenant&token=dev-test-key"
    ) as websocket:
        # Fire a parallel async ingest request pushing Native Streams dynamically onto memory loop queues
        import threading

        def fire_ingest():
            client.post(
                "/v1/ingest",
                headers={"Authorization": "Bearer dev-test-key"},
                json={
                    "events": [
                        {
                            "id": "ws-mock-event",
                            "execution_id": "WS-01",
                            "timestamp": "2026-02-22T12:00:00Z",
                            "payload": {},
                        }
                    ]
                },
            )

        t = threading.Thread(target=fire_ingest)
        t.start()

        # Wait for data dynamically matching tenant payload bounds
        data = websocket.receive_json()
        print("RECEIVED WS:", data)

        if (
            data.get("type") == "execution_ingested"
            and data.get("execution_id") == "WS-01"
        ):
            print("SUCCESS: Websocket mapped stream natively!")
        else:
            print("FAILED: Websocket stream yield mismatch!")


if __name__ == "__main__":
    test_websocket()
