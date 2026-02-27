import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from app.core.event_stream import EventStream
from app.config import EXPECTED

router = APIRouter()


@router.websocket("/live")
async def websocket_live_execution(
    websocket: WebSocket, tenant_id: str = Query(...), token: str = Query(...)
):
    await websocket.accept()

    # Authenticate token explicitly since standard HTTP headers are unavailable on initial WS handshakes reliably over browser websockets
    if token != EXPECTED:
        await websocket.close(code=1008, reason="Invalid API Key")
        return

    print("[LIVE] client connected")
    stream = EventStream()

    try:
        async for event in stream.subscribe():
            if event.get("tenant_id") == tenant_id:
                try:
                    await websocket.send_json(event)
                except Exception:
                    # Trapped exception pushing explicitly means socket severed dirty
                    break
    except WebSocketDisconnect:
        # Expected clean disconnection
        pass
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[LIVE] error: {e}")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
