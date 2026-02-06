"""Binance WebSocket client for real-time aggregated trade data using picows."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Awaitable

from picows import ws_connect, WSFrame, WSTransport, WSListener, WSMsgType, WSCloseCode

from app.models import AggTrade

logger = logging.getLogger(__name__)

# Type alias for trade callback
AggTradeCallback = Callable[[AggTrade], Awaitable[None]]


class BinanceAggTradeListener(WSListener):
    """picows listener for Binance aggTrade WebSocket stream."""

    def __init__(
        self,
        callbacks: dict[str, list[AggTradeCallback]],
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
        logger.info("picows: AggTrade WebSocket connected")

        # Subscribe to all registered streams
        if self._callbacks:
            self._send_subscribe(list(self._callbacks.keys()))

        self._on_connected()

    def on_ws_disconnected(self, transport: WSTransport):
        """Called when WebSocket is disconnected."""
        logger.info("picows: AggTrade WebSocket disconnected")
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
        logger.info(f"Subscribed to aggTrade streams: {streams}")

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

            # Handle aggTrade data
            if "e" in data and data["e"] == "aggTrade":
                self._process_aggtrade(data)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse aggTrade message: {e}")
        except Exception as e:
            logger.error(f"Error handling aggTrade message: {e}")

    def _process_aggtrade(self, data: dict) -> None:
        """Process aggregated trade data and call callbacks."""
        symbol = data["s"]

        trade = AggTrade(
            symbol=symbol,
            agg_trade_id=data["a"],
            price=Decimal(data["p"]),
            quantity=Decimal(data["q"]),
            timestamp=datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc),
            is_buyer_maker=data["m"],
        )

        # Call all registered callbacks
        stream_name = f"{symbol.lower()}@aggTrade"
        if stream_name in self._callbacks:
            for callback in self._callbacks[stream_name]:
                # Schedule async callback in the event loop
                if self._loop:
                    asyncio.run_coroutine_threadsafe(
                        self._safe_callback(callback, trade), self._loop
                    )

    async def _safe_callback(self, callback: AggTradeCallback, trade: AggTrade) -> None:
        """Safely execute async callback."""
        try:
            await callback(trade)
        except Exception as e:
            logger.error(f"AggTrade callback error: {e}")

    def disconnect(self) -> None:
        """Disconnect the WebSocket."""
        if self._transport:
            self._transport.send_close(WSCloseCode.OK)
            self._transport.disconnect()


class BinanceAggTradeWebSocket:
    """WebSocket client for Binance Futures aggregated trade streams using picows."""

    WS_URL = "wss://fstream.binance.com/ws"

    def __init__(self):
        self._callbacks: dict[str, list[AggTradeCallback]] = {}
        self._running = False
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0
        self._task: asyncio.Task | None = None
        self._listener: BinanceAggTradeListener | None = None
        self._connected = asyncio.Event()
        self._disconnected = asyncio.Event()

    async def subscribe(
        self,
        symbol: str,
        callback: AggTradeCallback,
    ) -> None:
        """
        Subscribe to aggregated trade updates for a symbol.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            callback: Async function to call with each AggTrade update

        Note: Duplicate callbacks are ignored to prevent accumulation on reconnect.
        """
        stream_name = f"{symbol.lower()}@aggTrade"
        if stream_name not in self._callbacks:
            self._callbacks[stream_name] = []

        # Prevent duplicate callbacks (same callback object)
        if callback not in self._callbacks[stream_name]:
            self._callbacks[stream_name].append(callback)

        # Send subscribe message if already connected
        if self._listener and self._connected.is_set():
            self._listener.send_subscribe([stream_name])

    def unsubscribe(self, symbol: str, callback: AggTradeCallback | None = None) -> None:
        """
        Unsubscribe from aggregated trade updates.

        Args:
            symbol: Trading pair
            callback: Specific callback to remove, or None to remove all
        """
        stream_name = f"{symbol.lower()}@aggTrade"
        if stream_name in self._callbacks:
            if callback is None:
                del self._callbacks[stream_name]
            elif callback in self._callbacks[stream_name]:
                self._callbacks[stream_name].remove(callback)
                if not self._callbacks[stream_name]:
                    del self._callbacks[stream_name]

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
                logger.error(f"picows aggTrade error: {e}")

            if self._running:
                logger.info(
                    f"Reconnecting aggTrade WS in {self._reconnect_delay} seconds..."
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
            self._listener = BinanceAggTradeListener(
                callbacks=self._callbacks,
                on_connected=self._on_connected,
                on_disconnected=self._on_disconnected,
                loop=loop,
            )
            return self._listener

        logger.info(f"Connecting aggTrade WS to {self.WS_URL}")
        transport, _ = await ws_connect(
            listener_factory,
            self.WS_URL,
            enable_auto_ping=True,
            auto_ping_idle_timeout=30,
            auto_ping_reply_timeout=10,
        )

        # Wait until disconnected
        await self._disconnected.wait()
