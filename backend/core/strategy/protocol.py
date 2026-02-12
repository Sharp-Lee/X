"""Strategy protocol defining the interface all strategies must implement.

This module provides:
- ProcessResult: Standard return type from strategy processing
- Strategy: Runtime-checkable Protocol that strategies must satisfy
- Type aliases for callback functions used by strategies
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Awaitable, Callable, Protocol, runtime_checkable

from core.models.signal import Outcome, SignalRecord, StreakTracker
from core.models.kline import Kline, KlineBuffer


# ---------------------------------------------------------------------------
# Callback type aliases (matching what SignalGenerator uses)
# ---------------------------------------------------------------------------
SignalCallback = Callable[[SignalRecord], Awaitable[None]]
SaveSignalCallback = Callable[[SignalRecord], Awaitable[None]]
SaveStreakCallback = Callable[[str, str, StreakTracker], Awaitable[None]]
LoadStreaksCallback = Callable[[], Awaitable[dict[str, StreakTracker]]]
LoadActiveSignalsCallback = Callable[[], Awaitable[list[SignalRecord]]]


# ---------------------------------------------------------------------------
# ProcessResult: standard return value from process_kline
# ---------------------------------------------------------------------------
@dataclass
class ProcessResult:
    """Result of processing a kline.

    Attributes:
        signal: Generated signal, if any.
        atr: Current ATR value (for updating max_atr of active signals).
        metadata: Optional strategy-specific data (e.g., indicator snapshots).
    """

    signal: SignalRecord | None = None
    atr: float | None = None
    metadata: dict | None = None


# ---------------------------------------------------------------------------
# Strategy Protocol
# ---------------------------------------------------------------------------
@runtime_checkable
class Strategy(Protocol):
    """Protocol that all trading strategies must implement.

    Strategies are responsible for:
    1. Processing closed klines and generating signals
    2. Recording outcomes and managing streak state
    3. Managing per-symbol position locks
    """

    @property
    def name(self) -> str:
        """Unique strategy identifier (e.g., 'msr_retest_capture')."""
        ...

    @property
    def version(self) -> str:
        """Strategy version string (e.g., '1.0.0')."""
        ...

    @property
    def required_indicators(self) -> list[str]:
        """List of indicator names this strategy requires.

        Example: ['ema50', 'atr', 'fib_382', 'fib_500', 'fib_618', 'vwap']
        """
        ...

    async def init(self) -> None:
        """Initialize strategy state (load streaks, active positions, etc.)."""
        ...

    async def process_kline(
        self,
        kline: Kline,
        buffer: KlineBuffer,
    ) -> ProcessResult:
        """Process a closed kline and optionally generate a signal.

        Args:
            kline: The closed kline to process.
            buffer: Buffer of recent klines for indicator calculation.

        Returns:
            ProcessResult with signal (if generated) and current ATR.
        """
        ...

    async def record_outcome(
        self,
        outcome: Outcome,
        symbol: str | None = None,
        timeframe: str | None = None,
    ) -> None:
        """Record a signal outcome and update internal state.

        Args:
            outcome: The outcome (TP or SL).
            symbol: Symbol of the closed position.
            timeframe: Timeframe of the closed position.
        """
        ...

    def release_position(self, symbol: str, timeframe: str) -> None:
        """Release position lock for a symbol/timeframe combination."""
        ...

    def on_signal(self, callback: SignalCallback) -> None:
        """Register a callback for new signals."""
        ...

    def off_signal(self, callback: SignalCallback) -> None:
        """Unregister a callback for new signals."""
        ...
