"""Binance WebSocket client for real-time K-line data using picows."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Awaitable

from picows import ws_connect, WSFrame, WSTransport, WSListener, WSMsgType, WSCloseCode

from app.models import Kline

logger = logging.getLogger(__name__)

# Type alias for kline callback
KlineCallback = Callable[[Kline], Awaitable[None]]


class BinanceKlineListener(WSListener):
    """picows listener for Binance K-line WebSocket stream."""

    def __init__(
        self,
        callbacks: dict[str, list[KlineCallback]],
        on_connected: Callable[[], None],
        on_disconnected: Callable[[], None],
        loop: asyncio.AbstractEventLoop,
    ):
        self._callbacks = callbacks
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._transport: WSTransport | None = None
        # Store loop at init time to avoid race condition
        # picows callbacks may run from different threads
        self._loop = loop

    def on_ws_connected(self, transport: WSTransport):
        """Called when WebSocket connection is established."""
        self._transport = transport
        logger.info("picows: K-line WebSocket connected")

        # Subscribe to all registered streams
        if self._callbacks:
            self._send_subscribe(list(self._callbacks.keys()))

        self._on_connected()

    def on_ws_disconnected(self, transport: WSTransport):
        """Called when WebSocket is disconnected."""
        logger.info("picows: K-line WebSocket disconnected")
        self._transport = None
        self._on_disconnected()

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        """Called when a new frame is received."""
        if frame.msg_type == WSMsgType.TEXT:
            payload = frame.get_payload_as_utf8_text()
            self._handle_message(payload)
        elif frame.msg_type == WSMsgType.PING:
            transport.send_pong(frame.get_payload_as_bytes())

    def _send_subscribe(self, streams: list[str]) -> None:
        """Send subscription request."""
        if not self._transport:
            return

        msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": int(datetime.now().timestamp() * 1000),
        }
        self._transport.send(WSMsgType.TEXT, json.dumps(msg).encode())
        logger.info(f"Subscribed to kline streams: {streams}")

    def send_subscribe(self, streams: list[str]) -> None:
        """Public method to subscribe to additional streams."""
        self._send_subscribe(streams)

    def _handle_message(self, message: str) -> None:
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            # Ignore subscription confirmations
            if "result" in data or "id" in data:
                return

            # Handle kline data
            if "e" in data and data["e"] == "kline":
                self._process_kline(data)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse kline message: {e}")
        except Exception as e:
            logger.error(f"Error handling kline message: {e}")

    def _process_kline(self, data: dict) -> None:
        """Process K-line data and call callbacks."""
        kline_data = data["k"]
        symbol = data["s"]
        interval = kline_data["i"]

        kline = Kline(
            symbol=symbol,
            timeframe=interval,
            timestamp=datetime.fromtimestamp(
                kline_data["t"] / 1000, tz=timezone.utc
            ),
            open=Decimal(kline_data["o"]),
            high=Decimal(kline_data["h"]),
            low=Decimal(kline_data["l"]),
            close=Decimal(kline_data["c"]),
            volume=Decimal(kline_data["v"]),
            is_closed=kline_data["x"],
        )

        # Call all registered callbacks
        stream_name = f"{symbol.lower()}@kline_{interval}"
        if stream_name in self._callbacks:
            for callback in self._callbacks[stream_name]:
                # Schedule async callback in the event loop
                if self._loop:
                    asyncio.run_coroutine_threadsafe(
                        self._safe_callback(callback, kline), self._loop
                    )

    async def _safe_callback(self, callback: KlineCallback, kline: Kline) -> None:
        """Safely execute async callback."""
        try:
            await callback(kline)
        except Exception as e:
            logger.error(f"Kline callback error: {e}")

    def disconnect(self) -> None:
        """Disconnect the WebSocket."""
        if self._transport:
            self._transport.send_close(WSCloseCode.OK)
            self._transport.disconnect()


class BinanceKlineWebSocket:
    """WebSocket client for Binance Futures K-line streams using picows."""

    WS_URL = "wss://fstream.binance.com/ws"

    def __init__(self):
        self._callbacks: dict[str, list[KlineCallback]] = {}
        self._running = False
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0
        self._task: asyncio.Task | None = None
        self._listener: BinanceKlineListener | None = None
        self._connected = asyncio.Event()
        self._disconnected = asyncio.Event()

    async def subscribe(
        self,
        symbol: str,
        timeframe: str,
        callback: KlineCallback,
    ) -> None:
        """
        Subscribe to K-line updates for a symbol.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            timeframe: K-line interval (e.g., "5m")
            callback: Async function to call with each Kline update
        """
        stream_name = f"{symbol.lower()}@kline_{timeframe}"
        if stream_name not in self._callbacks:
            self._callbacks[stream_name] = []
        self._callbacks[stream_name].append(callback)

        # Send subscribe message if already connected
        if self._listener and self._connected.is_set():
            self._listener.send_subscribe([stream_name])

    async def start(self) -> None:
        """Start the WebSocket connection and message processing."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the WebSocket connection."""
        self._running = False
        if self._listener:
            self._listener.disconnect()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def _on_connected(self) -> None:
        """Called when connection is established."""
        self._connected.set()
        self._disconnected.clear()
        self._reconnect_delay = 1.0

    def _on_disconnected(self) -> None:
        """Called when connection is lost."""
        self._connected.clear()
        self._disconnected.set()

    async def _run(self) -> None:
        """Main WebSocket loop with reconnection."""
        while self._running:
            try:
                await self._connect_and_process()
            except Exception as e:
                logger.error(f"picows kline error: {e}")

            if self._running:
                logger.info(
                    f"Reconnecting kline WS in {self._reconnect_delay} seconds..."
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )

    async def _connect_and_process(self) -> None:
        """Connect to WebSocket and wait for disconnection."""
        self._disconnected.clear()

        # Capture event loop here (in async context) to pass to listener
        # This ensures callbacks can schedule coroutines safely
        loop = asyncio.get_running_loop()

        def listener_factory():
            self._listener = BinanceKlineListener(
                callbacks=self._callbacks,
                on_connected=self._on_connected,
                on_disconnected=self._on_disconnected,
                loop=loop,
            )
            return self._listener

        logger.info(f"Connecting to {self.WS_URL}")
        transport, _ = await ws_connect(
            listener_factory,
            self.WS_URL,
            enable_auto_ping=True,
            auto_ping_idle_timeout=30,
            auto_ping_reply_timeout=10,
        )

        # Wait until disconnected
        await self._disconnected.wait()
