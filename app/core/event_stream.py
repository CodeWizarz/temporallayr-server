import asyncio
from typing import Any, AsyncGenerator

# Global singleton queue
_global_queue: asyncio.Queue | None = None


def _get_queue() -> asyncio.Queue:
    global _global_queue
    if _global_queue is None:
        _global_queue = asyncio.Queue()
    return _global_queue


class EventStream:
    """Async event streaming system natively pushing to queues dynamically."""

    async def publish(self, event: Any) -> None:
        """Push an event into the global singleton queue."""
        q = _get_queue()
        await q.put(event)
        print("[STREAM] event published")

    async def subscribe(self) -> AsyncGenerator[Any, None]:
        """Async generator yielding events forever. Resilient against consumer disconnects."""
        q = _get_queue()
        try:
            while True:
                event = await q.get()
                yield event
        except asyncio.CancelledError:
            # Client disconnected securely without crashing the backend loop
            pass
        except Exception as e:
            # Fallback capturing unexpected consumer iterations
            print(f"[STREAM] Consumer disconnected unexpectedly: {e}")
