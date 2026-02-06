"""K-line aggregator for generating higher timeframes from 1-minute data.

This module aggregates 1-minute K-lines into higher timeframes (3m, 5m, 15m, 30m)
locally, reducing WebSocket connections from 25 to 5 (80% reduction).

Aggregation rules:
- 3m: Triggered when minute % 3 == 0 and kline is closed
- 5m: Triggered when minute % 5 == 0 and kline is closed
- 15m: Triggered when minute % 15 == 0 and kline is closed
- 30m: Triggered when minute % 30 == 0 and kline is closed
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from app.models import FastKline

logger = logging.getLogger(__name__)

# Timeframe to minutes mapping
TIMEFRAME_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
}


@dataclass
class AggregationBuffer:
    """Buffer for accumulating 1m klines for aggregation.

    Uses period boundary alignment to ensure correct aggregation
    even after restarts or when there are gaps in the data.
    """

    symbol: str
    timeframe: str
    period_minutes: int
    klines_1m: list[FastKline] = field(default_factory=list)

    def _get_period_start(self, timestamp: float) -> float:
        """Get the period start timestamp for a given kline timestamp."""
        period_seconds = self.period_minutes * 60
        return (int(timestamp) // period_seconds) * period_seconds

    def add(self, kline: FastKline) -> FastKline | None:
        """Add a 1m kline to the buffer.

        Uses period boundary alignment:
        - Checks if kline belongs to current period
        - Aggregates when kline ends on period boundary
        - Handles gaps by discarding incomplete periods

        Args:
            kline: A closed 1m kline

        Returns:
            Aggregated kline if the period is complete, None otherwise
        """
        period_seconds = self.period_minutes * 60

        # Get the period this kline belongs to
        kline_period_start = self._get_period_start(kline.timestamp)

        # If buffer has klines from a different period, reset
        if self.klines_1m:
            first_period_start = self._get_period_start(self.klines_1m[0].timestamp)
            if first_period_start != kline_period_start:
                # New period started, discard old incomplete period
                if len(self.klines_1m) < self.period_minutes:
                    logger.debug(
                        f"Discarding incomplete period for {self.symbol} {self.timeframe}: "
                        f"had {len(self.klines_1m)}/{self.period_minutes} klines"
                    )
                self.klines_1m.clear()

        self.klines_1m.append(kline)

        # Check if this kline ends on a period boundary
        kline_end_time = kline.timestamp + 60
        if int(kline_end_time) % period_seconds == 0:
            # Period boundary reached
            if len(self.klines_1m) == self.period_minutes:
                # Complete period, aggregate
                return self._aggregate()
            else:
                # Incomplete period (gap in data), discard and log
                logger.warning(
                    f"Incomplete period at boundary for {self.symbol} {self.timeframe}: "
                    f"expected {self.period_minutes}, got {len(self.klines_1m)} klines"
                )
                self.klines_1m.clear()

        return None

    def _aggregate(self) -> FastKline:
        """Aggregate buffered 1m klines into a single higher timeframe kline."""
        klines = self.klines_1m[: self.period_minutes]

        aggregated = FastKline(
            symbol=self.symbol,
            timeframe=self.timeframe,
            timestamp=klines[0].timestamp,  # Period start time
            open=klines[0].open,  # First kline's open
            high=max(k.high for k in klines),  # Highest high
            low=min(k.low for k in klines),  # Lowest low
            close=klines[-1].close,  # Last kline's close
            volume=sum(k.volume for k in klines),  # Sum of volumes
            is_closed=True,
        )

        # Clear the buffer (remove aggregated klines)
        self.klines_1m = self.klines_1m[self.period_minutes :]

        return aggregated

    def reset(self) -> None:
        """Reset the buffer."""
        self.klines_1m.clear()


class KlineAggregator:
    """Aggregates 1-minute K-lines into higher timeframes.

    This class receives 1m klines and produces aggregated klines for
    3m, 5m, 15m, and 30m timeframes.

    Usage:
        aggregator = KlineAggregator()
        aggregator.on_aggregated_kline(my_callback)

        # Feed 1m klines
        for kline in klines_1m:
            await aggregator.add_1m_kline(kline)
    """

    def __init__(self, target_timeframes: list[str] | None = None):
        """Initialize the aggregator.

        Args:
            target_timeframes: List of timeframes to aggregate to.
                              Defaults to ["3m", "5m", "15m", "30m"]
        """
        if target_timeframes is None:
            target_timeframes = ["3m", "5m", "15m", "30m"]

        # Filter out 1m (no aggregation needed) and validate
        self.target_timeframes = [
            tf for tf in target_timeframes if tf in TIMEFRAME_MINUTES and tf != "1m"
        ]

        # Buffers: {symbol: {timeframe: AggregationBuffer}}
        self._buffers: dict[str, dict[str, AggregationBuffer]] = defaultdict(dict)

        # Callbacks for aggregated klines
        self._callbacks: list[Callable[[FastKline], Awaitable[None]]] = []

        # Current 1m kline for each symbol (for real-time updates)
        self._current_1m: dict[str, FastKline] = {}

        logger.info(f"KlineAggregator initialized for timeframes: {self.target_timeframes}")

    def on_aggregated_kline(
        self, callback: Callable[[FastKline], Awaitable[None]]
    ) -> None:
        """Register a callback for aggregated klines.

        Args:
            callback: Async function called with each aggregated kline
        """
        self._callbacks.append(callback)

    def _ensure_buffers(self, symbol: str) -> None:
        """Ensure buffers exist for a symbol."""
        if symbol not in self._buffers:
            self._buffers[symbol] = {}

        for timeframe in self.target_timeframes:
            if timeframe not in self._buffers[symbol]:
                self._buffers[symbol][timeframe] = AggregationBuffer(
                    symbol=symbol,
                    timeframe=timeframe,
                    period_minutes=TIMEFRAME_MINUTES[timeframe],
                )

    def _get_period_start(self, timestamp: float, period_minutes: int) -> float:
        """Get the period start timestamp for alignment.

        Args:
            timestamp: Unix timestamp in seconds
            period_minutes: Period in minutes

        Returns:
            Aligned period start timestamp
        """
        period_seconds = period_minutes * 60
        return (int(timestamp) // period_seconds) * period_seconds

    def _should_aggregate(self, kline: FastKline, period_minutes: int) -> bool:
        """Check if this 1m kline completes an aggregation period.

        A period is complete when:
        1. The kline is closed
        2. The kline's timestamp + 60s would cross into a new period

        Args:
            kline: The 1m kline
            period_minutes: Target period in minutes

        Returns:
            True if this kline completes a period
        """
        if not kline.is_closed:
            return False

        # The 1m kline's end time (start + 60 seconds)
        kline_end_time = kline.timestamp + 60

        # Check if this ends on a period boundary
        period_seconds = period_minutes * 60
        return int(kline_end_time) % period_seconds == 0

    async def add_1m_kline(self, kline: FastKline) -> list[FastKline]:
        """Add a 1-minute kline and generate aggregated klines if periods complete.

        Args:
            kline: A 1-minute kline (can be open or closed)

        Returns:
            List of completed aggregated klines (empty if none completed)
        """
        if kline.timeframe != "1m":
            logger.warning(f"Ignoring non-1m kline: {kline.timeframe}")
            return []

        symbol = kline.symbol
        self._ensure_buffers(symbol)

        # Store current 1m kline for real-time queries
        self._current_1m[symbol] = kline

        # Only process closed klines for aggregation
        if not kline.is_closed:
            return []

        aggregated_klines: list[FastKline] = []

        # Try to aggregate for each target timeframe
        for timeframe in self.target_timeframes:
            period_minutes = TIMEFRAME_MINUTES[timeframe]
            buffer = self._buffers[symbol][timeframe]

            # Add to buffer and check for completed aggregation
            result = buffer.add(kline)
            if result is not None:
                aggregated_klines.append(result)
                logger.debug(
                    f"Aggregated {symbol} {timeframe} kline at {result.timestamp}"
                )

        # Notify callbacks
        for aggregated in aggregated_klines:
            for callback in self._callbacks:
                try:
                    await callback(aggregated)
                except Exception as e:
                    logger.error(f"Aggregated kline callback error: {e}")

        return aggregated_klines

    def get_current_1m(self, symbol: str) -> FastKline | None:
        """Get the current (possibly open) 1m kline for a symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Current 1m kline or None if not available
        """
        return self._current_1m.get(symbol)

    def get_partial_kline(
        self, symbol: str, timeframe: str
    ) -> FastKline | None:
        """Get the current partial (incomplete) kline for a timeframe.

        This is useful for displaying real-time data before a period closes.

        Args:
            symbol: Trading pair symbol
            timeframe: Target timeframe

        Returns:
            Partial aggregated kline or None if no data
        """
        if symbol not in self._buffers or timeframe not in self._buffers[symbol]:
            return None

        buffer = self._buffers[symbol][timeframe]
        if not buffer.klines_1m:
            return None

        klines = buffer.klines_1m
        return FastKline(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=klines[0].timestamp,
            open=klines[0].open,
            high=max(k.high for k in klines),
            low=min(k.low for k in klines),
            close=klines[-1].close,
            volume=sum(k.volume for k in klines),
            is_closed=False,  # Partial kline
        )

    def reset(self, symbol: str | None = None) -> None:
        """Reset aggregation buffers.

        Args:
            symbol: Reset only this symbol's buffers, or all if None
        """
        if symbol is not None:
            if symbol in self._buffers:
                for buffer in self._buffers[symbol].values():
                    buffer.reset()
                self._current_1m.pop(symbol, None)
        else:
            for sym_buffers in self._buffers.values():
                for buffer in sym_buffers.values():
                    buffer.reset()
            self._current_1m.clear()

    def prefill_from_history(
        self, symbol: str, klines_1m: list[FastKline]
    ) -> None:
        """Prefill aggregation buffers from historical 1m klines.

        This should be called at startup to ensure aggregation is
        properly aligned from the first real-time kline.

        Only prefills INCOMPLETE periods. If the last kline ends exactly
        on a period boundary, the buffer stays empty (period already complete).

        Args:
            symbol: Trading pair symbol
            klines_1m: Historical 1m klines (must be closed, sorted by timestamp)
        """
        self._ensure_buffers(symbol)

        for timeframe in self.target_timeframes:
            period_minutes = TIMEFRAME_MINUTES[timeframe]
            period_seconds = period_minutes * 60
            buffer = self._buffers[symbol][timeframe]
            buffer.reset()

            if not klines_1m:
                continue

            # Check if the last kline ends on a period boundary
            last_kline = klines_1m[-1]
            last_kline_end_time = last_kline.timestamp + 60

            if int(last_kline_end_time) % period_seconds == 0:
                # Last kline completes a period, buffer should be empty
                # The completed period was already processed historically
                logger.debug(
                    f"Last kline for {symbol} {timeframe} ends on boundary, "
                    f"buffer stays empty"
                )
                continue

            # Get the period that the last kline belongs to
            period_start = self._get_period_start(last_kline.timestamp, period_minutes)

            # Add klines that belong to the current incomplete period
            for kline in klines_1m:
                if kline.timestamp >= period_start:
                    buffer.klines_1m.append(kline)

            logger.debug(
                f"Prefilled {symbol} {timeframe} with {len(buffer.klines_1m)} klines "
                f"(period starts at {period_start})"
            )

        logger.info(
            f"Prefilled aggregation buffers for {symbol} with {len(klines_1m)} 1m klines"
        )
