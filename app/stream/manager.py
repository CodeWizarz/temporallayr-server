import asyncio
import logging
import json
from typing import Dict, Any, Optional
from fastapi import WebSocket

logger = logging.getLogger("temporallayr.stream")


class StreamManager:
    """
    Enterprise-grade execution stream manager. Handles WebSocket subscriptions dynamically natively filtering streams loosely without blocking DB operations.
    """

    def __init__(self):
        # Maps WebSocket connection instance natively to dict containing tenant and filter constraints dynamically
        # { websocket: {"tenant_id": "...", "filters": {...}, "queue": asyncio.Queue(1000), "task": asyncio.Task}}
        self.active_connections: Dict[WebSocket, Dict[str, Any]] = {}

    async def subscribe(self, websocket: WebSocket, tenant_id: str, filters: dict):
        """Bind connection queues internally allocating overflow handlers."""

        # Max backlog cap = 1000 dynamically protecting memory per subscriber
        queue = asyncio.Queue(maxsize=1000)

        task = asyncio.create_task(self._process_queue_for_subscriber(websocket, queue))

        self.active_connections[websocket] = {
            "tenant_id": tenant_id,
            "filters": filters or {},
            "queue": queue,
            "task": task,
        }

        # Initial heartbeat immediately confirms subscription bound cleanly
        try:
            await websocket.send_json({"type": "subscribed", "status": "ok"})
        except Exception:
            await self.unsubscribe(websocket)
            return

        logger.info(f"[STREAM] subscriber connected tenant={tenant_id}")

        # Start automatic heartbeat ping loops holding connections open securely
        asyncio.create_task(self._heartbeat_loop(websocket))

    async def _heartbeat_loop(self, websocket: WebSocket):
        """Keep-alive loops ensuring network traversal timeouts are bypassed natively."""
        try:
            while websocket in self.active_connections:
                await asyncio.sleep(20)
                if websocket in self.active_connections:
                    await websocket.send_json({"type": "ping"})
        except Exception:
            await self.unsubscribe(websocket)

    async def unsubscribe(self, websocket: WebSocket):
        """Tear down individual isolated websocket instances and loops reliably."""
        if websocket in self.active_connections:
            conn_data = self.active_connections.pop(websocket)
            if "task" in conn_data:
                conn_data["task"].cancel()
            try:
                await websocket.close()
            except Exception:
                pass
            logger.info("[STREAM] subscriber disconnected")

    async def _process_queue_for_subscriber(
        self, websocket: WebSocket, queue: asyncio.Queue
    ):
        """Isolated pump draining messages into subscriber safely avoiding blocking the overarching publish function."""
        try:
            while websocket in self.active_connections:
                item = await queue.get()
                try:
                    await websocket.send_json(item)
                except Exception:
                    # Network IO fault isolates socket disconnect safely
                    await self.unsubscribe(websocket)
                    break
        except asyncio.CancelledError:
            pass

    async def publish_event(self, event: Dict[str, Any]):
        """
        Evaluate and broadcast the lightweight execution summary (tenant_id, node, incident, execution_id, timestamp)
        natively against connected subscriber rule sets asynchronously.
        """
        tenant_id = event.get("tenant_id")
        if not tenant_id:
            return

        broadcast_count = 0
        dead_sockets = []

        for ws, conn_data in self.active_connections.items():
            if conn_data["tenant_id"] != tenant_id:
                continue

            filters = conn_data.get("filters", {})
            event_node = event.get("node", "")
            event_incident = event.get("incident_flag", False)

            # Check constraints
            if "node" in filters and filters["node"] != event_node:
                continue

            if filters.get("incident_only") is True and not event_incident:
                continue

            queue = conn_data["queue"]
            if queue.full():
                # Rate limit drop mechanism enforcing safety
                try:
                    queue.get_nowait()  # Drop oldest explicitly
                    logger.warning("[STREAM] subscriber overflow handled")
                except asyncio.QueueEmpty:
                    pass

            try:
                queue.put_nowait(event)
                broadcast_count += 1
            except asyncio.QueueFull:
                pass

        if broadcast_count > 0:
            logger.info(f"[STREAM] event broadcast count={broadcast_count}")


# Singleton mapping dynamic connections broadly across ingestion systems
stream_manager = StreamManager()
