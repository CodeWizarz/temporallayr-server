import unittest
import asyncio
from fastapi.testclient import TestClient
from fastapi.websockets import WebSocket

from app.main import app
from app.api.stream import stream_manager_v2


class TestStreaming(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.tenant_id = "dev-test-key"

    def tearDown(self):
        self.loop.close()

    def test_websocket_stream_delivery(self):
        """Simulate connecting WebSockets natively and broadcasting payloads through the API endpoints correctly."""

        # Test connecting and receiving the initial loop heartbeat or trace cleanly
        with self.client.websocket_connect(
            f"/v1/stream/ws?api_key={self.tenant_id}"
        ) as websocket:
            # Inject a manual broadcast event as if the Ingestion Service pushed it natively
            event_payload = {
                "type": "execution_graph",
                "timestamp": "2026-02-23T10:00:00Z",
                "payload": {"id": "exec-123"},
            }

            # Manually push resolving asynchronous context safely mapping over the TestClient event loop directly
            async def mock_broadcast():
                # Yield context allocating the WS accept fully securely
                await asyncio.sleep(0.01)
                await stream_manager_v2.broadcast_event(self.tenant_id, event_payload)

            self.loop.run_until_complete(mock_broadcast())

            # Await receiving the JSON organically
            data = websocket.receive_json()

            self.assertEqual(data["type"], "execution_graph")
            self.assertEqual(data["payload"]["id"], "exec-123")

            # Test disconnection map structurally
            websocket.close()

        # Ensure client got dropped elegantly
        self.assertNotIn(self.tenant_id, stream_manager_v2._clients)
