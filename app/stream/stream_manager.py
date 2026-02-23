import asyncio
import logging
from typing import Dict, List, Any
from fastapi import WebSocket

logger = logging.getLogger("temporallayr.stream.manager")


class StreamManager:
    """Enterprise StreamManager V2 dynamically bounding 100 msg/sec topologies natively."""

    def __init__(self):
        # tenant_id -> list of active connections
        self._clients: Dict[str, List[WebSocket]] = {}
        # WebSocket -> execution queue bound securely (limit 100)
        self._client_queues: Dict[WebSocket, asyncio.Queue] = {}
        # WebSocket -> active sender task
        self._client_tasks: Dict[WebSocket, asyncio.Task] = {}

    async def register_client(self, tenant_id: str, websocket: WebSocket):
        """Bind arrays organically accepting natively mapping structural allocations."""
        await websocket.accept()

        if tenant_id not in self._clients:
            self._clients[tenant_id] = []
        self._clients[tenant_id].append(websocket)

        # Max capacity 100 enforcing rate limits securely natively.
        queue = asyncio.Queue(maxsize=100)
        self._client_queues[websocket] = queue

        # Spawn dedicated sender loop safely executing completely decoupled from ingestion
        task = asyncio.create_task(self._sender_loop(websocket, queue))
        self._client_tasks[websocket] = task

        logger.info(f"[STREAM] client connected tenant={tenant_id}")

    async def remove_client(self, websocket: WebSocket):
        """Drop references safely resolving bounds without memory leaks."""
        # Find tenant logically resolving cleanup efficiently
        found_tenant = None
        for t_id, sockets in self._clients.items():
            if websocket in sockets:
                found_tenant = t_id
                sockets.remove(websocket)
                if not sockets:
                    del self._clients[t_id]
                break

        if websocket in self._client_tasks:
            self._client_tasks[websocket].cancel()
            del self._client_tasks[websocket]

        if websocket in self._client_queues:
            del self._client_queues[websocket]

        logger.info("[STREAM] client disconnected")

    async def broadcast_event(self, tenant_id: str, event: Dict[str, Any]):
        """Publish cleanly mapped JSON structures into multitenant bounds safely."""
        sockets = self._clients.get(tenant_id, [])
        if not sockets:
            return

        broadcast_count = 0
        for ws in sockets:
            queue = self._client_queues.get(ws)
            if not queue:
                continue

            # Rate protection: drop oldest buffered elements natively scaling loads safely.
            if queue.full():
                try:
                    queue.get_nowait()
                    logger.warning(
                        "[STREAM] rate limit exceeded! Dropped oldest bounded trace proactively."
                    )
                except asyncio.QueueEmpty:
                    pass

            try:
                queue.put_nowait(event)
                broadcast_count += 1
            except asyncio.QueueFull:
                pass

        if broadcast_count > 0:
            logger.info(f"[STREAM] event broadcast count={broadcast_count}")

    async def _sender_loop(self, websocket: WebSocket, queue: asyncio.Queue):
        """Dynamically isolate delivery resolving I/O blocking gracefully natively."""
        try:
            while True:
                event = await queue.get()
                await websocket.send_json(event)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(
                f"[STREAM] delivery bound errored cleanly resolving trace: {e}"
            )
            await self.remove_client(websocket)


stream_manager_v2 = StreamManager()
