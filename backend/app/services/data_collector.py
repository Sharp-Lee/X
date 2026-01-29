"""Data collection service for K-lines and aggregated trades.

This service collects market data from Binance:
- Only subscribes to 1m K-lines via WebSocket
- Uses KlineAggregator to generate higher timeframes (3m, 5m, 15m, 30m) locally
- This reduces WebSocket connections from 25 to 5 (80% reduction)
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from app.clients import BinanceRestClient, BinanceKlineWebSocket, BinanceAggTradeWebSocket
from app.config import get_settings
from app.models import Kline, KlineBuffer, AggTrade, FastKline, kline_to_fast, fast_to_kline
from app.storage import KlineRepository, AggTradeRepository
from app.storage import price_cache
from app.services.kline_aggregator import KlineAggregator

logger = logging.getLogger(__name__)


class DataCollector:
    """Service for collecting and managing market data.

    Uses 1m K-line aggregation strategy:
    - Only subscribes to 1m K-lines via WebSocket
    - Aggregates to higher timeframes (3m, 5m, 15m, 30m) locally
    - Reduces WebSocket connections by 80%
    """

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

        # In-memory buffers for each symbol/timeframe
        self._kline_buffers: dict[str, KlineBuffer] = {}

        # K-line aggregator for generating higher timeframes from 1m
        # Filter out 1m from target timeframes (it comes directly from WebSocket)
        aggregated_timeframes = [
            tf for tf in self.settings.timeframes if tf != "1m"
        ]
        self._aggregator = KlineAggregator(target_timeframes=aggregated_timeframes)
        self._aggregator.on_aggregated_kline(self._handle_aggregated_kline)

        # Callbacks for new data
        self._kline_callbacks: list = []
        self._aggtrade_callbacks: list = []

    def on_kline(self, callback) -> None:
        """Register callback for new kline data."""
        self._kline_callbacks.append(callback)

    def on_aggtrade(self, callback) -> None:
        """Register callback for new aggtrade data."""
        self._aggtrade_callbacks.append(callback)

    def get_kline_buffer(self, symbol: str, timeframe: str | None = None) -> KlineBuffer | None:
        """Get the kline buffer for a symbol and timeframe.

        Args:
            symbol: Trading pair
            timeframe: K-line interval (defaults to first configured timeframe)
        """
        if timeframe is None:
            timeframe = self.settings.timeframes[0] if self.settings.timeframes else "5m"
        key = f"{symbol}_{timeframe}"
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
        """Handle incoming 1m kline from WebSocket.

        This method:
        1. Updates the 1m buffer
        2. Saves closed 1m klines to database
        3. Feeds klines to the aggregator for higher timeframe generation
        4. Notifies registered callbacks
        """
        # Update buffer for 1m
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

        # Feed to aggregator for higher timeframe generation
        if kline.timeframe == "1m":
            fast_kline = kline_to_fast(kline)
            await self._aggregator.add_1m_kline(fast_kline)

        # Notify callbacks
        for callback in self._kline_callbacks:
            try:
                await callback(kline)
            except Exception as e:
                logger.error(f"Kline callback error: {e}")

    async def _handle_aggregated_kline(self, fast_kline: FastKline) -> None:
        """Handle aggregated kline from the aggregator.

        This method is called when a higher timeframe kline (3m, 5m, 15m, 30m)
        is completed by the aggregator.
        """
        # Convert to Pydantic model
        kline = fast_to_kline(fast_kline)

        # Update buffer for this timeframe
        key = f"{kline.symbol}_{kline.timeframe}"
        if key not in self._kline_buffers:
            self._kline_buffers[key] = KlineBuffer(
                symbol=kline.symbol, timeframe=kline.timeframe
            )
        self._kline_buffers[key].add(kline)

        # Save to database
        await self.kline_repo.save(kline)
        logger.debug(f"Saved aggregated kline: {kline.symbol} {kline.timeframe} {kline.timestamp}")

        # Notify callbacks
        for callback in self._kline_callbacks:
            try:
                await callback(kline)
            except Exception as e:
                logger.error(f"Aggregated kline callback error: {e}")

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
        """Start data collection for all configured symbols.

        Uses 1m K-line aggregation strategy:
        - Only subscribes to 1m K-lines via WebSocket
        - Aggregates to higher timeframes locally
        - Reduces WebSocket connections by 80%
        """
        symbols = self.settings.symbols
        timeframes = self.settings.timeframes

        logger.info(f"Starting data collection for {symbols}")
        logger.info(f"Target timeframes: {timeframes} (using 1m aggregation)")

        # Sync historical 1m data for aggregation prefill
        for symbol in symbols:
            # Always sync 1m data (base for aggregation)
            await self.sync_historical_klines(symbol, "1m", lookback_hours=48)

            # Prefill aggregator with recent 1m klines
            await self._prefill_aggregator(symbol)

            # Also sync historical data for other timeframes (for complete history)
            for timeframe in timeframes:
                if timeframe != "1m":
                    await self.sync_historical_klines(symbol, timeframe, lookback_hours=48)

        # Subscribe to WebSocket streams - ONLY 1m klines
        for symbol in symbols:
            # Only subscribe to 1m klines - aggregator generates higher timeframes
            await self.kline_ws.subscribe(symbol, "1m", self._handle_kline)
            # AggTrade is independent of timeframe
            await self.aggtrade_ws.subscribe(symbol, self._handle_aggtrade)

        # Start WebSocket connections
        await self.kline_ws.start()
        await self.aggtrade_ws.start()

        ws_count = len(symbols)  # 1m klines per symbol
        aggws_count = len(symbols)  # aggTrade per symbol
        logger.info(f"Data collection started with {ws_count} kline + {aggws_count} aggTrade streams")
        logger.info(f"WebSocket reduction: {len(symbols) * len(timeframes)} -> {ws_count} kline streams")

    async def _prefill_aggregator(self, symbol: str) -> None:
        """Prefill the aggregator with historical 1m klines.

        This ensures aggregation is properly aligned when real-time data starts.
        """
        # Get the buffer for 1m klines
        buffer = self.get_kline_buffer(symbol, "1m")
        if buffer is None or len(buffer) == 0:
            logger.debug(f"No 1m klines to prefill aggregator for {symbol}")
            return

        # Convert to FastKline and prefill
        fast_klines = [kline_to_fast(k) for k in buffer.klines]
        self._aggregator.prefill_from_history(symbol, fast_klines)
        logger.info(f"Prefilled aggregator for {symbol} with {len(fast_klines)} 1m klines")

    async def stop(self) -> None:
        """Stop data collection."""
        await self.kline_ws.stop()
        await self.aggtrade_ws.stop()
        await self.rest_client.close()
        logger.info("Data collection stopped")
