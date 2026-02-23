import unittest
import asyncio
import json
from fastapi.testclient import TestClient
from app.main import app
from app.stream.manager import stream_manager


class TestStream(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def test_websocket_stream_integration(self):
        with self.client.websocket_connect(
            "/v1/stream?api_key=demo-tenant"
        ) as websocket:
            subscription = {
                "tenant_id": "demo-tenant",
                "filters": {"node": "payment_gateway", "incident_only": False},
            }
            websocket.send_text(json.dumps(subscription))

            conn_msg = websocket.receive_json()
            self.assertEqual(conn_msg, {"type": "subscribed", "status": "ok"})

            matching_event = {
                "execution_id": "test-exec-1",
                "tenant_id": "demo-tenant",
                "node": "payment_gateway",
                "status": "completed",
                "incident_flag": False,
                "timestamp": "2026-01-01T00:00:00Z",
            }

            self.loop.run_until_complete(stream_manager.publish_event(matching_event))

            received = websocket.receive_json()
            self.assertEqual(received["execution_id"], "test-exec-1")
            self.assertEqual(received["node"], "payment_gateway")

            non_matching = {
                "execution_id": "test-exec-2",
                "tenant_id": "demo-tenant",
                "node": "different_node",
                "status": "completed",
                "incident_flag": False,
                "timestamp": "2026-01-01T00:00:00Z",
            }
            self.loop.run_until_complete(stream_manager.publish_event(non_matching))

            matching_incident = {
                "execution_id": "test-exec-3",
                "tenant_id": "demo-tenant",
                "node": "payment_gateway",
                "status": "failure",
                "incident_flag": True,
                "timestamp": "2026-01-01T00:00:00Z",
            }
            self.loop.run_until_complete(
                stream_manager.publish_event(matching_incident)
            )

            received_2 = websocket.receive_json()
            self.assertEqual(received_2["execution_id"], "test-exec-3")
            self.assertEqual(received_2["incident_flag"], True)

    def test_websocket_incident_only_filter(self):
        with self.client.websocket_connect(
            "/v1/stream?api_key=dev-test-key"
        ) as websocket:
            subscription = {
                "tenant_id": "dev-test-key",
                "filters": {"incident_only": True},
            }
            websocket.send_text(json.dumps(subscription))
            self.assertEqual(
                websocket.receive_json(), {"type": "subscribed", "status": "ok"}
            )

            normal_evt = {
                "tenant_id": "dev-test-key",
                "incident_flag": False,
                "execution_id": "evt-1",
            }
            self.loop.run_until_complete(stream_manager.publish_event(normal_evt))

            incident_evt = {
                "tenant_id": "dev-test-key",
                "incident_flag": True,
                "execution_id": "evt-2",
            }
            self.loop.run_until_complete(stream_manager.publish_event(incident_evt))

            received = websocket.receive_json()
            self.assertEqual(received["execution_id"], "evt-2")

    def test_websocket_auth_rejection(self):
        from fastapi.websockets import WebSocketDisconnect

        with self.assertRaises(WebSocketDisconnect) as context:
            with self.client.websocket_connect("/v1/stream?api_key=invalid"):
                pass
        self.assertEqual(context.exception.code, 1008)


if __name__ == "__main__":
    unittest.main()
