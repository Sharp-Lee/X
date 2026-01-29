"""Data collection service for K-lines and aggregated trades."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from app.clients import BinanceRestClient, BinanceKlineWebSocket, BinanceAggTradeWebSocket
from app.config import get_settings
from app.models import Kline, KlineBuffer, AggTrade
from app.storage import KlineRepository, AggTradeRepository
from app.storage import price_cache

logger = logging.getLogger(__name__)


class DataCollector:
    """Service for collecting and managing market data."""

    def __init__(self):
        self.settings = get_settings()
        self.rest_client = BinanceRestClient(
            api_key=self.settings.binance_api_key,
            api_secret=self.settings.binance_api_secret,
        )
        self.kline_ws = BinanceKlineWebSocket()
        self.aggtrade_ws = BinanceAggTradeWebSocket()
        self.kline_repo = KlineRepository()
        self.aggtrade_repo = AggTradeRepository()

        # In-memory buffers for each symbol
        self._kline_buffers: dict[str, KlineBuffer] = {}

        # Callbacks for new data
        self._kline_callbacks: list = []
        self._aggtrade_callbacks: list = []

    def on_kline(self, callback) -> None:
        """Register callback for new kline data."""
        self._kline_callbacks.append(callback)

    def on_aggtrade(self, callback) -> None:
        """Register callback for new aggtrade data."""
        self._aggtrade_callbacks.append(callback)

    def get_kline_buffer(self, symbol: str) -> KlineBuffer | None:
        """Get the kline buffer for a symbol."""
        key = f"{symbol}_{self.settings.timeframe}"
        return self._kline_buffers.get(key)

    async def sync_historical_klines(
        self,
        symbol: str,
        timeframe: str,
        lookback_hours: int = 24,
    ) -> int:
        """
        Sync historical K-lines from Binance.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            lookback_hours: How many hours of history to fetch

        Returns:
            Number of K-lines synced
        """
        logger.info(f"Syncing historical klines for {symbol} {timeframe}")

        # Check last timestamp in database
        last_ts = await self.kline_repo.get_last_timestamp(symbol, timeframe)

        if last_ts:
            start_time = last_ts
            logger.info(f"Resuming from {start_time}")
        else:
            start_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
            logger.info(f"Starting fresh from {start_time}")

        end_time = datetime.now(timezone.utc)

        # Fetch klines
        klines = await self.rest_client.get_all_klines(
            symbol=symbol,
            interval=timeframe,
            start_time=start_time,
            end_time=end_time,
        )

        if klines:
            # Save to database
            await self.kline_repo.save_batch(klines)

            # Update buffer
            key = f"{symbol}_{timeframe}"
            if key not in self._kline_buffers:
                self._kline_buffers[key] = KlineBuffer(
                    symbol=symbol, timeframe=timeframe
                )
            for kline in klines:
                self._kline_buffers[key].add(kline)

            logger.info(f"Synced {len(klines)} klines for {symbol}")

        return len(klines)

    async def _handle_kline(self, kline: Kline) -> None:
        """Handle incoming kline from WebSocket."""
        # Update buffer
        key = f"{kline.symbol}_{kline.timeframe}"
        if key not in self._kline_buffers:
            self._kline_buffers[key] = KlineBuffer(
                symbol=kline.symbol, timeframe=kline.timeframe
            )
        self._kline_buffers[key].add(kline)

        # Save closed klines to database
        if kline.is_closed:
            await self.kline_repo.save(kline)
            logger.debug(f"Saved closed kline: {kline.symbol} {kline.timestamp}")

        # Notify callbacks
        for callback in self._kline_callbacks:
            try:
                await callback(kline)
            except Exception as e:
                logger.error(f"Kline callback error: {e}")

    async def _handle_aggtrade(self, trade: AggTrade) -> None:
        """Handle incoming aggTrade from WebSocket."""
        # Update price cache (non-blocking, best-effort)
        await price_cache.update_price(
            symbol=trade.symbol,
            price=float(trade.price),
            timestamp=trade.timestamp.timestamp(),
            volume=float(trade.quantity),
        )

        # Notify callbacks (for position tracking)
        for callback in self._aggtrade_callbacks:
            try:
                await callback(trade)
            except Exception as e:
                logger.error(f"AggTrade callback error: {e}")

    async def start(self) -> None:
        """Start data collection for all configured symbols."""
        symbols = self.settings.symbols
        timeframe = self.settings.timeframe

        logger.info(f"Starting data collection for {symbols} on {timeframe}")

        # Sync historical data first
        for symbol in symbols:
            await self.sync_historical_klines(symbol, timeframe, lookback_hours=48)

        # Subscribe to WebSocket streams
        for symbol in symbols:
            await self.kline_ws.subscribe(symbol, timeframe, self._handle_kline)
            await self.aggtrade_ws.subscribe(symbol, self._handle_aggtrade)

        # Start WebSocket connections
        await self.kline_ws.start()
        await self.aggtrade_ws.start()

        logger.info("Data collection started")

    async def stop(self) -> None:
        """Stop data collection."""
        await self.kline_ws.stop()
        await self.aggtrade_ws.stop()
        await self.rest_client.close()
        logger.info("Data collection stopped")
