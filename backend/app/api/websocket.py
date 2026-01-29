"""WebSocket endpoint for real-time updates."""

import asyncio
import logging
from datetime import datetime
from typing import Any

import orjson
from fastapi import WebSocket, WebSocketDisconnect
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _orjson_dumps(obj: Any) -> str:
    """Serialize object to JSON string using orjson."""
    return orjson.dumps(obj, default=str).decode("utf-8")


class WebSocketMessage(BaseModel):
    """WebSocket message format."""

    type: str  # "signal", "mae_update", "outcome", "status"
    data: dict[str, Any]
    timestamp: datetime

    def to_json(self) -> str:
        """Serialize to JSON string using orjson for performance."""
        return _orjson_dumps(self.model_dump())


class ConnectionManager:
    """Manage WebSocket connections and broadcasts."""

    def __init__(self):
        self._connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        """Accept a new WebSocket connection."""
        await websocket.accept()
        async with self._lock:
            self._connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self._connections)}")

    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove a disconnected WebSocket."""
        async with self._lock:
            if websocket in self._connections:
                self._connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self._connections)}")

    async def broadcast(self, message: WebSocketMessage) -> None:
        """Broadcast message to all connected clients."""
        if not self._connections:
            return

        message_text = message.to_json()
        disconnected = []

        async with self._lock:
            for websocket in self._connections:
                try:
                    await websocket.send_text(message_text)
                except Exception as e:
                    logger.warning(f"Failed to send message: {e}")
                    disconnected.append(websocket)

            # Remove disconnected websockets
            for ws in disconnected:
                self._connections.remove(ws)

    async def send_signal(self, signal_data: dict) -> None:
        """Broadcast a new signal."""
        message = WebSocketMessage(
            type="signal",
            data=signal_data,
            timestamp=datetime.utcnow(),
        )
        await self.broadcast(message)

    async def send_mae_update(
        self,
        signal_id: str,
        mae_ratio: float,
        mfe_ratio: float,
    ) -> None:
        """Broadcast MAE update for a signal."""
        message = WebSocketMessage(
            type="mae_update",
            data={
                "signal_id": signal_id,
                "mae_ratio": mae_ratio,
                "mfe_ratio": mfe_ratio,
            },
            timestamp=datetime.utcnow(),
        )
        await self.broadcast(message)

    async def send_outcome(
        self,
        signal_id: str,
        outcome: str,
        exit_price: float,
    ) -> None:
        """Broadcast signal outcome (TP/SL hit)."""
        message = WebSocketMessage(
            type="outcome",
            data={
                "signal_id": signal_id,
                "outcome": outcome,
                "exit_price": exit_price,
            },
            timestamp=datetime.utcnow(),
        )
        await self.broadcast(message)

    async def send_status(self, status_data: dict) -> None:
        """Broadcast system status update."""
        message = WebSocketMessage(
            type="status",
            data=status_data,
            timestamp=datetime.utcnow(),
        )
        await self.broadcast(message)

    @property
    def connection_count(self) -> int:
        """Get number of active connections."""
        return len(self._connections)


# Global connection manager
manager = ConnectionManager()


async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates.

    Messages sent to clients:
    - signal: New trading signal
    - mae_update: MAE ratio update for active signal
    - outcome: Signal outcome (TP or SL hit)
    - status: System status update

    Message format:
    {
        "type": "signal",
        "data": {...},
        "timestamp": "2024-01-01T00:00:00"
    }
    """
    await manager.connect(websocket)

    try:
        # Send initial status
        await websocket.send_text(_orjson_dumps({
            "type": "connected",
            "data": {"message": "Connected to MSR Retest Capture"},
            "timestamp": datetime.utcnow().isoformat(),
        }))

        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Wait for messages (ping/pong, subscriptions, etc.)
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=60.0,
                )

                # Handle incoming message
                try:
                    message = orjson.loads(data)
                    await handle_client_message(websocket, message)
                except orjson.JSONDecodeError:
                    await websocket.send_text(_orjson_dumps({
                        "type": "error",
                        "data": {"message": "Invalid JSON"},
                        "timestamp": datetime.utcnow().isoformat(),
                    }))

            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                await websocket.send_text(_orjson_dumps({
                    "type": "ping",
                    "data": {},
                    "timestamp": datetime.utcnow().isoformat(),
                }))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await manager.disconnect(websocket)


async def handle_client_message(websocket: WebSocket, message: dict) -> None:
    """Handle incoming message from client."""
    msg_type = message.get("type", "")

    if msg_type == "ping":
        await websocket.send_text(_orjson_dumps({
            "type": "pong",
            "data": {},
            "timestamp": datetime.utcnow().isoformat(),
        }))
    elif msg_type == "subscribe":
        # Handle subscription requests (e.g., specific symbols)
        symbols = message.get("data", {}).get("symbols", [])
        await websocket.send_text(_orjson_dumps({
            "type": "subscribed",
            "data": {"symbols": symbols},
            "timestamp": datetime.utcnow().isoformat(),
        }))
    else:
        await websocket.send_text(_orjson_dumps({
            "type": "error",
            "data": {"message": f"Unknown message type: {msg_type}"},
            "timestamp": datetime.utcnow().isoformat(),
        }))
