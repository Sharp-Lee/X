"""Data collection service for K-lines and aggregated trades.

This service collects market data from Binance:
- Only subscribes to 1m K-lines via WebSocket
- Uses KlineAggregator to generate higher timeframes (3m, 5m, 15m, 30m) locally
- This reduces WebSocket connections from 25 to 5 (80% reduction)

Startup flow with data integrity:
1. Initialize in buffering mode (queue incoming WebSocket data)
2. Detect and backfill K-line gaps from database
3. Restore buffer state for each symbol/timeframe
4. Replay missed K-lines from last checkpoint
5. Flush WebSocket buffer and switch to live mode
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from app.clients import BinanceRestClient, BinanceKlineWebSocket, BinanceAggTradeWebSocket
from app.config import get_settings
from app.models import Kline, KlineBuffer, AggTrade, FastKline, ProcessingState, kline_to_fast, fast_to_kline
from app.storage import KlineRepository, AggTradeRepository, ProcessingStateRepository
from app.storage import price_cache
from app.services.kline_aggregator import KlineAggregator, TIMEFRAME_MINUTES
from app.services.kline_replay import KlineReplayService

logger = logging.getLogger(__name__)

# Expected K-line interval in minutes for each timeframe
TIMEFRAME_TO_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}


class DataCollector:
    """Service for collecting and managing market data.

    Uses 1m K-line aggregation strategy:
    - Only subscribes to 1m K-lines via WebSocket
    - Aggregates to higher timeframes (3m, 5m, 15m, 30m) locally
    - Reduces WebSocket connections by 80%

    Data integrity features:
    - Buffering mode during startup (prevents race conditions)
    - Gap detection and backfill
    - K-line replay for state recovery
    - Checkpoint mechanism for crash recovery
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
        self.state_repo = ProcessingStateRepository()

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

        # Buffering mode: queue WebSocket data during startup/replay
        self._buffering_mode = True
        self._ws_buffer: dict[str, list[Kline]] = {}  # symbol -> queued 1m klines
        self._buffer_lock = asyncio.Lock()  # Protects _buffering_mode and _ws_buffer

        # Replay service for state recovery
        self._replay_service = KlineReplayService(self.kline_repo, self.state_repo)

        # Signal generator reference (set by App during initialization)
        self._signal_generator = None

    def on_kline(self, callback) -> None:
        """Register callback for new kline data."""
        self._kline_callbacks.append(callback)

    def on_aggtrade(self, callback) -> None:
        """Register callback for new aggtrade data."""
        self._aggtrade_callbacks.append(callback)

    def set_signal_generator(self, signal_generator) -> None:
        """Set the signal generator for replay processing.

        Args:
            signal_generator: SignalGenerator instance
        """
        self._signal_generator = signal_generator

    @property
    def is_buffering(self) -> bool:
        """Check if in buffering mode (fast, non-blocking read).

        Note: This is a direct read without lock. For critical sections where
        consistency is required, use get_buffering_status() instead.
        This is acceptable for the common case (checking in callbacks) because:
        1. Python's GIL makes single attribute reads atomic
        2. The worst case is processing one extra kline during mode transition
        """
        return self._buffering_mode

    async def get_buffering_status(self) -> bool:
        """Get buffering status with lock protection (thread-safe).

        Use this method when you need guaranteed consistency, such as
        when making decisions that depend on the exact mode.
        """
        async with self._buffer_lock:
            return self._buffering_mode

    @property
    def is_replaying(self) -> bool:
        """Check if replay is in progress."""
        return self._replay_service.is_replaying

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

        new_count = 0
        if klines:
            # Save to database
            await self.kline_repo.save_batch(klines)
            new_count = len(klines)
            logger.info(f"Synced {new_count} new klines for {symbol} {timeframe}")

        # IMPORTANT: Load historical data from database into buffer
        # This ensures we have enough data for indicator calculation (needs 50+ klines)
        await self._load_buffer_from_db(symbol, timeframe, limit=200)

        return new_count

    async def _load_buffer_from_db(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
    ) -> int:
        """
        Load klines from database into the buffer.

        This ensures the buffer has enough historical data for indicator calculation,
        even if the database already had data from previous runs.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            limit: Maximum number of klines to load

        Returns:
            Number of klines loaded into buffer
        """
        key = f"{symbol}_{timeframe}"

        # Create buffer if not exists
        if key not in self._kline_buffers:
            self._kline_buffers[key] = KlineBuffer(symbol=symbol, timeframe=timeframe)

        # Load from database
        klines = await self.kline_repo.get_latest(symbol, timeframe, limit=limit)

        if klines:
            # Clear and refill buffer with historical data
            buffer = self._kline_buffers[key]
            buffer.klines.clear()
            for kline in klines:
                buffer.add(kline)
            logger.info(f"Loaded {len(klines)} klines into buffer for {symbol} {timeframe}")

        return len(klines) if klines else 0

    async def _handle_kline(self, kline: Kline) -> None:
        """Handle incoming 1m kline from WebSocket.

        In buffering mode (during startup/replay):
        - Queue klines for later processing

        In live mode:
        1. Updates the 1m buffer
        2. Saves closed 1m klines to database
        3. Feeds klines to the aggregator for higher timeframe generation
        4. Notifies registered callbacks
        """
        # Use lock to prevent race condition during mode transition
        async with self._buffer_lock:
            if self._buffering_mode:
                symbol = kline.symbol
                if symbol not in self._ws_buffer:
                    self._ws_buffer[symbol] = []
                # Only buffer closed klines (open klines will be superseded)
                if kline.is_closed:
                    self._ws_buffer[symbol].append(kline)
                return

        # Live mode: normal processing (outside lock to avoid blocking)
        await self._process_kline_live(kline)

    async def _process_kline_live(self, kline: Kline) -> None:
        """Process a kline in live mode (not buffering)."""
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

            # Update processing state checkpoint
            await self.state_repo.update_last_processed(
                kline.symbol, "1m", kline.timestamp
            )

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
        """Start data collection with data integrity checks.

        Startup flow:
        1. Initialize in buffering mode
        2. Start WebSocket connections (data is queued)
        3. Check/initialize processing state
        4. Detect and backfill K-line gaps
        5. Restore buffer state for each symbol
        6. Replay missed K-lines (if any)
        7. Flush WebSocket buffer and go live
        """
        symbols = self.settings.symbols
        timeframes = self.settings.timeframes

        logger.info("=" * 60)
        logger.info("STARTUP PHASE 1: Initialize")
        logger.info("=" * 60)
        self._buffering_mode = True

        # Subscribe to WebSocket streams - ONLY 1m klines
        # Data will be buffered until we go live
        for symbol in symbols:
            await self.kline_ws.subscribe(symbol, "1m", self._handle_kline)
            await self.aggtrade_ws.subscribe(symbol, self._handle_aggtrade)

        # Start WebSocket connections (data goes to buffer)
        await self.kline_ws.start()
        await self.aggtrade_ws.start()
        logger.info("WebSocket connections started (buffering mode)")

        logger.info("=" * 60)
        logger.info("STARTUP PHASE 2: Check Processing State")
        logger.info("=" * 60)

        # Check for crashed replays that need recovery
        pending_states = await self._replay_service.check_pending_recovery()
        if pending_states:
            logger.warning(f"Found {len(pending_states)} pending states from crashed replay")
            for state in pending_states:
                logger.warning(f"  - {state.symbol} {state.timeframe}: last={state.last_processed_time}")

        for symbol in symbols:
            state = await self.state_repo.get_state(symbol, "1m")

            if state is None:
                # First run: sync historical data and initialize state
                logger.info(f"First run for {symbol}: syncing historical data")
                await self.sync_historical_klines(symbol, "1m", lookback_hours=48)

                # Also sync higher timeframes
                for timeframe in timeframes:
                    if timeframe != "1m":
                        await self.sync_historical_klines(symbol, timeframe, lookback_hours=48)

                # Get the last kline timestamp as initial checkpoint
                last_ts = await self.kline_repo.get_last_timestamp(symbol, "1m")
                if last_ts:
                    await self._replay_service.initialize_state(
                        symbol=symbol,
                        system_start_time=datetime.now(timezone.utc),
                        initial_kline_time=last_ts,
                    )
            else:
                logger.info(
                    f"Resuming {symbol}: last_processed={state.last_processed_time}, "
                    f"system_start={state.system_start_time}"
                )

        logger.info("=" * 60)
        logger.info("STARTUP PHASE 3: Gap Detection & Backfill")
        logger.info("=" * 60)

        for symbol in symbols:
            state = await self.state_repo.get_state(symbol, "1m")
            if state is None:
                continue  # First run, no gaps to check

            # Check for gaps from last checkpoint to now (not from system_start)
            # This is efficient for long-running systems - only checks downtime period
            gaps = await self.detect_gaps(
                symbol, "1m",
                start_time=state.last_processed_time,
                end_time=datetime.now(timezone.utc),
            )

            if gaps:
                logger.warning(f"Detected {len(gaps)} gaps for {symbol} 1m")
                filled = await self.backfill_gaps(symbol, "1m", gaps)
                logger.info(f"Backfilled {filled} klines for {symbol}")

                # Also check higher timeframes from checkpoint
                for timeframe in timeframes:
                    if timeframe != "1m":
                        tf_gaps = await self.detect_gaps(
                            symbol, timeframe,
                            start_time=state.last_processed_time,
                            end_time=datetime.now(timezone.utc),
                        )
                        if tf_gaps:
                            await self.backfill_gaps(symbol, timeframe, tf_gaps)

        logger.info("=" * 60)
        logger.info("STARTUP PHASE 4: Buffer Restoration")
        logger.info("=" * 60)

        for symbol in symbols:
            state = await self.state_repo.get_state(symbol, "1m")
            if state:
                # Restore buffers to state at checkpoint
                await self.restore_buffers_for_replay(symbol, state.last_processed_time)
            else:
                # Load recent klines for indicators
                for timeframe in timeframes:
                    await self._load_buffer_from_db(symbol, timeframe, limit=200)

            # Prefill aggregator
            await self._prefill_aggregator(symbol)

        logger.info("=" * 60)
        logger.info("STARTUP PHASE 5: Replay")
        logger.info("=" * 60)

        for symbol in symbols:
            state = await self.state_repo.get_state(symbol, "1m")
            if state is None:
                logger.info(f"No replay needed for {symbol} (first run)")
                continue

            # Check if there are klines to replay
            last_kline_ts = await self.kline_repo.get_last_timestamp(symbol, "1m")
            if last_kline_ts and last_kline_ts > state.last_processed_time:
                replayed = await self._replay_service.replay_from_checkpoint(
                    symbol=symbol,
                    checkpoint_time=state.last_processed_time,
                    aggregator=self._aggregator,
                    buffers=self._kline_buffers,
                    signal_generator=self._signal_generator,
                    timeframes=timeframes,
                )
                logger.info(f"Replayed {replayed} klines for {symbol}")
            else:
                logger.info(f"No replay needed for {symbol} (up to date)")

        logger.info("=" * 60)
        logger.info("STARTUP PHASE 6: Go Live")
        logger.info("=" * 60)

        # Process buffered WebSocket data and switch to live mode
        buffered = await self.flush_buffer_and_go_live()
        logger.info(f"Processed {buffered} buffered klines, now in LIVE mode")

        ws_count = len(symbols)
        logger.info(f"Data collection started with {ws_count} kline streams")
        logger.info(f"System ready for signal generation")
        logger.info("=" * 60)

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

    async def detect_gaps(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[tuple[datetime, datetime]]:
        """Detect gaps in K-line data.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            start_time: Start of range to check (defaults to first kline)
            end_time: End of range to check (defaults to now)

        Returns:
            List of (gap_start, gap_end) tuples
        """
        interval_minutes = TIMEFRAME_TO_MINUTES.get(timeframe, 1)
        interval = timedelta(minutes=interval_minutes)

        # Get time range
        if start_time is None:
            start_time = await self.kline_repo.get_first_timestamp(symbol, timeframe)
            if start_time is None:
                return []  # No data yet

        if end_time is None:
            end_time = datetime.now(timezone.utc)

        # Get all timestamps
        timestamps = await self.kline_repo.get_all_timestamps(
            symbol, timeframe, start_time, end_time
        )

        if len(timestamps) < 2:
            return []

        gaps = []
        for i in range(1, len(timestamps)):
            expected = timestamps[i - 1] + interval
            actual = timestamps[i]

            # Allow small tolerance (30 seconds) for timestamp drift
            if actual > expected + timedelta(seconds=30):
                gaps.append((expected, actual))

        return gaps

    async def backfill_gaps(
        self,
        symbol: str,
        timeframe: str,
        gaps: list[tuple[datetime, datetime]],
    ) -> int:
        """Backfill K-line gaps from Binance REST API.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            gaps: List of (gap_start, gap_end) tuples

        Returns:
            Total number of klines backfilled
        """
        total_filled = 0

        for gap_start, gap_end in gaps:
            logger.info(f"Backfilling gap for {symbol} {timeframe}: {gap_start} to {gap_end}")

            klines = await self.rest_client.get_all_klines(
                symbol=symbol,
                interval=timeframe,
                start_time=gap_start,
                end_time=gap_end,
            )

            if klines:
                await self.kline_repo.save_batch(klines)
                total_filled += len(klines)
                logger.info(f"Backfilled {len(klines)} klines for gap")

        return total_filled

    async def flush_buffer_and_go_live(self) -> int:
        """Process buffered WebSocket data and switch to live mode.

        Thread-safe: Uses lock to prevent race condition with _handle_kline.
        The mode switch happens AFTER all buffered data is processed.

        Returns:
            Number of buffered klines processed
        """
        total_processed = 0

        # Step 1: Snapshot and clear buffer while holding lock
        # New klines arriving during processing will still be buffered
        async with self._buffer_lock:
            buffer_snapshot = {
                symbol: list(klines) for symbol, klines in self._ws_buffer.items()
            }
            self._ws_buffer.clear()

        # Step 2: Process snapshot outside lock (allows new klines to buffer)
        for symbol, klines in buffer_snapshot.items():
            # Sort by timestamp
            sorted_klines = sorted(klines, key=lambda k: k.timestamp)

            # Get the last processed timestamp to avoid duplicates
            state = await self.state_repo.get_state(symbol, "1m")
            last_processed = state.last_processed_time if state else None

            for kline in sorted_klines:
                # Skip if already processed during replay
                if last_processed and kline.timestamp <= last_processed:
                    continue

                await self._process_kline_live(kline)
                total_processed += 1

        # Step 3: Final flush - process any klines that arrived during Step 2
        async with self._buffer_lock:
            remaining_snapshot = {
                symbol: list(klines) for symbol, klines in self._ws_buffer.items()
            }
            self._ws_buffer.clear()
            # NOW switch to live mode - all future klines go directly to live processing
            self._buffering_mode = False

        # Process remaining klines
        for symbol, klines in remaining_snapshot.items():
            sorted_klines = sorted(klines, key=lambda k: k.timestamp)
            state = await self.state_repo.get_state(symbol, "1m")
            last_processed = state.last_processed_time if state else None

            for kline in sorted_klines:
                if last_processed and kline.timestamp <= last_processed:
                    continue
                await self._process_kline_live(kline)
                total_processed += 1

        logger.info(f"Flushed WebSocket buffer: {total_processed} klines processed")
        return total_processed

    async def restore_buffers_for_replay(
        self,
        symbol: str,
        checkpoint_time: datetime,
    ) -> None:
        """Restore all buffers to state at checkpoint time.

        Args:
            symbol: Trading pair
            checkpoint_time: Restore to this point in time
        """
        timeframes = self.settings.timeframes

        for timeframe in timeframes:
            buffer = await self._replay_service.restore_buffer_state(
                symbol, timeframe, checkpoint_time, limit=200
            )
            key = f"{symbol}_{timeframe}"
            self._kline_buffers[key] = buffer

    async def stop(self) -> None:
        """Stop data collection."""
        await self.kline_ws.stop()
        await self.aggtrade_ws.stop()
        await self.rest_client.close()
        logger.info("Data collection stopped")
