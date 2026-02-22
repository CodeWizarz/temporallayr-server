import asyncio
from typing import Any, AsyncGenerator

# Fan-out pub/sub: each subscriber gets its own queue.
# Events published here are broadcast to ALL active subscribers independently.
_subscribers: list[asyncio.Queue] = []


class EventStream:
    """Async fan-out pub/sub event stream.

    publish() broadcasts to every connected subscriber.
    subscribe() registers a per-client queue and yields events forever.
    When the subscriber exits (e.g., WebSocket disconnect), its queue is
    automatically removed — no memory leaks, no server crashes.
    """

    async def publish(self, event: Any) -> None:
        """Broadcast an event to all active subscribers."""
        for q in list(_subscribers):  # snapshot to avoid mutation during iteration
            await q.put(event)
        print("[STREAM] event published")

    async def subscribe(self) -> AsyncGenerator[Any, None]:
        """Async generator: register a subscriber queue, yield events, clean up on exit."""
        q: asyncio.Queue = asyncio.Queue()
        _subscribers.append(q)
        try:
            while True:
                event = await q.get()
                yield event
        except asyncio.CancelledError:
            # Client disconnected cleanly
            pass
        except Exception as e:
            print(f"[STREAM] subscriber error: {e}")
        finally:
            try:
                _subscribers.remove(q)
            except ValueError:
                pass  # Already removed; safe to ignore
