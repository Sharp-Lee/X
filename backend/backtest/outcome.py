"""Kline-based outcome determination for backtesting.

Determines signal outcomes (TP/SL) using 1m kline high/low instead of
aggtrades. For TP=2.0 ATR / SL=8.84 ATR, single 1m kline simultaneously
hitting both TP and SL requires range > 10.84 ATR — probability << 0.01%.

Rules:
- LONG: high >= tp_price → TP, low <= sl_price → SL
- SHORT: low <= tp_price → TP, high >= sl_price → SL
- Both hit same kline → SL (pessimistic assumption)
- MAE/MFE updated using kline high/low on every 1m kline
- Timeout after configurable hours → signal stays ACTIVE
"""

from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Awaitable, Callable

from core.models.signal import Direction, Outcome, SignalRecord

if TYPE_CHECKING:
    from core.models.kline import Kline

logger = logging.getLogger(__name__)

OnOutcomeCallback = Callable[[SignalRecord, Outcome], Awaitable[None]]


class OutcomeTracker:
    """Track active signals and determine outcomes from 1m klines."""

    def __init__(
        self,
        timeout_hours: int = 24,
        on_outcome: OnOutcomeCallback | None = None,
    ):
        self.timeout_hours = timeout_hours
        self._on_outcome = on_outcome
        self._active_signals: list[SignalRecord] = []
        self._resolved_count = 0

    def add_signal(self, signal: SignalRecord) -> None:
        """Add a new signal to track."""
        self._active_signals.append(signal)

    async def check_kline(self, kline: Kline) -> None:
        """Check all active signals against a 1m kline.

        For each active signal matching kline.symbol:
        1. Check timeout
        2. Update MAE/MFE using kline high and low
        3. Check if TP or SL is hit
        """
        if not self._active_signals:
            return

        to_remove: list[SignalRecord] = []
        timeout_delta = timedelta(hours=self.timeout_hours)

        for signal in self._active_signals:
            if signal.symbol != kline.symbol:
                continue

            # Check timeout — release position lock so new signals can generate
            if kline.timestamp - signal.signal_time >= timeout_delta:
                to_remove.append(signal)
                if self._on_outcome:
                    await self._on_outcome(signal, Outcome.ACTIVE)
                continue

            # Update MAE/MFE using both extremes
            # update_mae handles both adverse and favorable tracking
            if signal.direction == Direction.LONG:
                signal.update_mae(kline.low)   # adverse first
                signal.update_mae(kline.high)  # favorable
            else:
                signal.update_mae(kline.high)  # adverse first
                signal.update_mae(kline.low)   # favorable

            # Check outcome
            outcome = self._check_outcome(signal, kline)
            if outcome is not None:
                to_remove.append(signal)
                if self._on_outcome:
                    await self._on_outcome(signal, outcome)
                self._resolved_count += 1

        for signal in to_remove:
            self._active_signals.remove(signal)

    def _check_outcome(self, signal: SignalRecord, kline: Kline) -> Outcome | None:
        """Check if a signal hits TP or SL on this kline.

        Pessimistic rule: if both TP and SL are hit in the same kline,
        the outcome is SL.
        """
        tp_hit = False
        sl_hit = False

        if signal.direction == Direction.LONG:
            tp_hit = kline.high >= signal.tp_price
            sl_hit = kline.low <= signal.sl_price
        else:  # SHORT
            tp_hit = kline.low <= signal.tp_price
            sl_hit = kline.high >= signal.sl_price

        if tp_hit and sl_hit:
            # Pessimistic: SL wins
            signal.outcome = Outcome.SL
            signal.outcome_time = kline.timestamp
            signal.outcome_price = signal.sl_price
            return Outcome.SL
        elif tp_hit:
            signal.outcome = Outcome.TP
            signal.outcome_time = kline.timestamp
            signal.outcome_price = signal.tp_price
            return Outcome.TP
        elif sl_hit:
            signal.outcome = Outcome.SL
            signal.outcome_time = kline.timestamp
            signal.outcome_price = signal.sl_price
            return Outcome.SL

        return None

    def update_atr(self, symbol: str, timeframe: str, current_atr: float) -> None:
        """Update max_atr for active signals of a given symbol/timeframe."""
        atr_decimal = Decimal(str(current_atr))
        for signal in self._active_signals:
            if (
                signal.symbol == symbol
                and signal.timeframe == timeframe
                and signal.outcome == Outcome.ACTIVE
                and atr_decimal > signal.max_atr
            ):
                signal.max_atr = atr_decimal

    def finalize(self) -> None:
        """Clear remaining active signals (they stay with outcome=ACTIVE)."""
        remaining = len(self._active_signals)
        if remaining > 0:
            logger.info(f"Finalizing {remaining} unresolved signals (remain ACTIVE)")
        self._active_signals.clear()

    @property
    def active_count(self) -> int:
        return len(self._active_signals)

    @property
    def resolved_count(self) -> int:
        return self._resolved_count
