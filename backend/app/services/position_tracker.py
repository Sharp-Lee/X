"""Position tracker for monitoring active signals and calculating MAE.

Uses hot path models (FastSignal, FastTrade) internally for performance,
converts to cold path models (SignalRecord, AggTrade) for storage and callbacks.

Includes Redis caching for fast access and persistence across restarts.
"""

import asyncio
import logging
from typing import Callable, Awaitable

from app.models import (
    AggTrade,
    Outcome,
    SignalRecord,
    FastSignal,
    FastTrade,
    signal_to_fast,
    fast_to_signal,
    aggtrade_to_fast,
)
from app.storage import SignalRepository
from app.storage import signal_cache

logger = logging.getLogger(__name__)

# Type alias for outcome callback (receives cold path SignalRecord)
OutcomeCallback = Callable[[SignalRecord, Outcome], Awaitable[None]]

# Note: Cache updates are now synced with DB updates (same interval)
# to ensure consistency. Previously cache was updated less frequently,
# causing staleness issues.


class PositionTracker:
    """
    Track active positions and update MAE based on real-time trade data.

    Internally uses FastSignal and FastTrade (hot path models) for performance.
    Converts to SignalRecord when persisting or calling external callbacks.

    Uses Redis cache for:
    - Fast startup (load from cache instead of DB)
    - Persistence across restarts
    - Reduced database load

    This service:
    1. Maintains a list of active signals (as FastSignal)
    2. Updates MAE ratio as prices move against the position
    3. Detects when TP or SL is hit
    4. Records outcomes
    """

    def __init__(
        self,
        update_interval: float = 1.0,
        signal_repo: SignalRepository | None = None,
    ):
        """
        Args:
            update_interval: Minimum interval (seconds) between DB updates for MAE
            signal_repo: Optional signal repository (for testing)
        """
        self.signal_repo = signal_repo or SignalRepository()
        self.update_interval = update_interval

        # Active signals by symbol (hot path: FastSignal)
        self._active_signals: dict[str, list[FastSignal]] = {}

        # Last update time for each signal (to throttle DB/cache writes)
        self._last_db_update: dict[str, float] = {}

        # Callbacks for outcome events
        self._outcome_callbacks: list[OutcomeCallback] = []

        # Lock for thread safety
        self._lock = asyncio.Lock()

        # Track cache usage for metrics
        self._cache_hits = 0
        self._cache_misses = 0

    def on_outcome(self, callback: OutcomeCallback) -> None:
        """Register callback for outcome events (TP/SL hit).

        Note: Duplicate callbacks are ignored.
        """
        if callback not in self._outcome_callbacks:
            self._outcome_callbacks.append(callback)

    def off_outcome(self, callback: OutcomeCallback) -> None:
        """Unregister callback for outcome events."""
        if callback in self._outcome_callbacks:
            self._outcome_callbacks.remove(callback)

    async def load_active_signals(self) -> None:
        """Load all active signals from cache or database.

        Tries to load from Redis cache first for fast startup.
        Falls back to database if cache is unavailable or empty.
        Syncs loaded signals to cache for next startup.
        """
        async with self._lock:
            self._active_signals.clear()

            # Try to load from cache first
            cached_signals = await signal_cache.get_all_signals()

            if cached_signals:
                # Cache hit - use cached data
                self._cache_hits += 1
                for fast_signal in cached_signals:
                    if fast_signal.symbol not in self._active_signals:
                        self._active_signals[fast_signal.symbol] = []
                    self._active_signals[fast_signal.symbol].append(fast_signal)

                total = sum(len(s) for s in self._active_signals.values())
                logger.info(f"Loaded {total} active signals from cache")
            else:
                # Cache miss - load from database
                self._cache_misses += 1
                signals = await self.signal_repo.get_active()

                for signal in signals:
                    fast_signal = signal_to_fast(signal)
                    if fast_signal.symbol not in self._active_signals:
                        self._active_signals[fast_signal.symbol] = []
                    self._active_signals[fast_signal.symbol].append(fast_signal)

                total = sum(len(s) for s in self._active_signals.values())
                logger.info(f"Loaded {total} active signals from database")

                # Sync to cache for next startup
                # Note: We're already holding the lock, so directly access _active_signals
                if total > 0:
                    all_signals = [
                        signal
                        for signals in self._active_signals.values()
                        for signal in signals
                    ]
                    await signal_cache.sync_from_db(all_signals)

    async def add_signal(self, signal: SignalRecord) -> None:
        """Add a new signal to track.

        Args:
            signal: Cold path SignalRecord (converted to FastSignal internally)
        """
        async with self._lock:
            fast_signal = signal_to_fast(signal)
            if fast_signal.symbol not in self._active_signals:
                self._active_signals[fast_signal.symbol] = []
            self._active_signals[fast_signal.symbol].append(fast_signal)

            # Cache the signal
            await signal_cache.cache_signal(fast_signal)

            logger.info(f"Tracking new signal: {fast_signal.id} ({fast_signal.symbol})")

    async def process_trade(self, trade: AggTrade) -> None:
        """
        Process an incoming trade and update relevant signals.

        Args:
            trade: The aggregated trade data (cold path, converted internally)
        """
        # Convert to hot path for fast processing
        fast_trade = aggtrade_to_fast(trade)
        symbol = fast_trade.symbol
        price = fast_trade.price
        timestamp = fast_trade.timestamp

        # Collect data for updates outside lock
        signals_for_db_update: list[FastSignal] = []
        signals_with_outcome: list[FastSignal] = []

        async with self._lock:
            if symbol not in self._active_signals:
                return

            signals_to_remove = []
            now = asyncio.get_event_loop().time()

            for fast_signal in self._active_signals[symbol]:
                # Check for outcome (TP or SL hit) - hot path
                outcome_changed = fast_signal.check_outcome(price, timestamp)

                if outcome_changed:
                    # Signal hit TP or SL - collect for processing outside lock
                    signals_with_outcome.append(fast_signal)
                    signals_to_remove.append(fast_signal)
                else:
                    # Update MAE - hot path (very fast)
                    fast_signal.update_mae(price)

                    # Throttle updates - DB and cache are updated together for consistency
                    last_update = self._last_db_update.get(fast_signal.id, 0)

                    # Collect for database AND cache update (synced)
                    if now - last_update >= self.update_interval:
                        signals_for_db_update.append(fast_signal)
                        self._last_db_update[fast_signal.id] = now

            # Remove closed signals
            for fast_signal in signals_to_remove:
                self._active_signals[symbol].remove(fast_signal)
                if fast_signal.id in self._last_db_update:
                    del self._last_db_update[fast_signal.id]

        # Process outcomes OUTSIDE lock (DB and callbacks are slow)
        for fast_signal in signals_with_outcome:
            await self._handle_outcome(fast_signal)

        # Update database AND cache OUTSIDE lock (slow I/O)
        # Both are updated together to ensure consistency
        for fast_signal in signals_for_db_update:
            try:
                await self._update_signal_mae(fast_signal)
                # Update cache immediately after successful DB update
                await signal_cache.update_signal(fast_signal)
            except Exception as e:
                logger.warning(f"Failed to update MAE/cache for {fast_signal.id}: {e}")

    async def _handle_outcome(self, fast_signal: FastSignal) -> None:
        """Handle signal outcome (TP or SL hit)."""
        outcome_str = fast_signal.outcome
        direction_name = "LONG" if fast_signal.direction == 1 else "SHORT"

        logger.info(
            f"Signal {fast_signal.id} hit {outcome_str.upper()}: "
            f"{fast_signal.symbol} {direction_name} "
            f"entry={fast_signal.entry_price} exit={fast_signal.outcome_price}"
        )

        # Remove from cache
        await signal_cache.remove_signal(fast_signal.id, fast_signal.symbol)

        # Convert to cold path for database update
        signal_record = fast_to_signal(fast_signal)
        outcome = Outcome(outcome_str)

        # Update database
        await self.signal_repo.update_outcome(
            signal_id=signal_record.id,
            mae_ratio=signal_record.mae_ratio,
            mfe_ratio=signal_record.mfe_ratio,
            outcome=outcome,
            outcome_time=signal_record.outcome_time,
            outcome_price=signal_record.outcome_price,
        )

        # Notify callbacks (with cold path model)
        for callback in self._outcome_callbacks:
            try:
                await callback(signal_record, outcome)
            except Exception as e:
                logger.error(f"Outcome callback error: {e}")

    async def _update_signal_mae(self, fast_signal: FastSignal) -> None:
        """Update signal MAE in database.

        Raises:
            Exception: If database update fails (caller should handle)
        """
        # Convert to cold path for database update
        signal_record = fast_to_signal(fast_signal)

        await self.signal_repo.update_outcome(
            signal_id=signal_record.id,
            mae_ratio=signal_record.mae_ratio,
            mfe_ratio=signal_record.mfe_ratio,
            outcome=signal_record.outcome,
            outcome_time=signal_record.outcome_time,
            outcome_price=signal_record.outcome_price,
            max_atr=signal_record.max_atr,
        )

    async def update_max_atr(self, symbol: str, timeframe: str, current_atr: float) -> None:
        """Update max_atr for all active signals matching symbol and timeframe.

        Called when a new kline closes and ATR is recalculated.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            current_atr: Current ATR value
        """
        async with self._lock:
            if symbol not in self._active_signals:
                return

            for fast_signal in self._active_signals[symbol]:
                if fast_signal.timeframe == timeframe and fast_signal.outcome == "active":
                    fast_signal.update_max_atr(current_atr)

    async def get_active_signals(self, symbol: str | None = None) -> list[SignalRecord]:
        """Get all active signals, optionally filtered by symbol.

        Returns cold path SignalRecord models for API compatibility.

        Note: This is now async to properly acquire lock for thread safety.
        """
        async with self._lock:
            if symbol:
                fast_signals = list(self._active_signals.get(symbol, []))
            else:
                fast_signals = [
                    signal
                    for signals in self._active_signals.values()
                    for signal in signals
                ]
        # Convert to cold path for external use (outside lock)
        return [fast_to_signal(s) for s in fast_signals]

    async def get_active_fast_signals(self, symbol: str | None = None) -> list[FastSignal]:
        """Get all active signals as FastSignal (hot path).

        Use this for internal processing where performance matters.

        Note: This is now async to properly acquire lock for thread safety.
        """
        async with self._lock:
            if symbol:
                return list(self._active_signals.get(symbol, []))
            return [
                signal
                for signals in self._active_signals.values()
                for signal in signals
            ]

    def get_signal_status(self, signal_id: str) -> dict | None:
        """Get current status of a tracked signal."""
        for signals in self._active_signals.values():
            for fast_signal in signals:
                if fast_signal.id == signal_id:
                    return {
                        "id": fast_signal.id,
                        "symbol": fast_signal.symbol,
                        "direction": "LONG" if fast_signal.direction == 1 else "SHORT",
                        "entry_price": fast_signal.entry_price,
                        "tp_price": fast_signal.tp_price,
                        "sl_price": fast_signal.sl_price,
                        "mae_ratio": fast_signal.mae_ratio,
                        "mfe_ratio": fast_signal.mfe_ratio,
                        "outcome": fast_signal.outcome,
                    }
        return None

    @property
    def active_count(self) -> int:
        """Get total number of active signals."""
        return sum(len(s) for s in self._active_signals.values())

    @property
    def cache_stats(self) -> dict:
        """Get cache hit/miss statistics."""
        total = self._cache_hits + self._cache_misses
        hit_rate = self._cache_hits / total if total > 0 else 0
        return {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "hit_rate": hit_rate,
        }
