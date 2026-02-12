"""EMA Crossover strategy implementation.

Simple trend-following strategy:
- Fast EMA(20) crosses above Slow EMA(50) -> LONG
- Fast EMA(20) crosses below Slow EMA(50) -> SHORT

TP/SL based on ATR:
- TP = entry +/- 2.0 * ATR
- SL = entry -/+ 4.0 * ATR

This module is pure business logic with no I/O dependencies.
"""

import logging
import math
from decimal import Decimal

from core.indicators import ema, atr
from core.models import (
    Direction,
    Kline,
    KlineBuffer,
    Outcome,
    SignalRecord,
    StreakTracker,
)
from core.strategy.ema_crossover.models import (
    EmaCrossoverConfig,
    EMA_CROSSOVER_STRATEGY_NAME,
)
from core.strategy.protocol import (
    ProcessResult,
    SaveSignalCallback,
    SaveStreakCallback,
    SignalCallback,
    LoadStreaksCallback,
    LoadActiveSignalsCallback,
)
from core.strategy.registry import register_strategy

logger = logging.getLogger(__name__)


def _is_nan(value) -> bool:
    """Check if a value is NaN (handles Decimal and float)."""
    if value is None:
        return True
    if isinstance(value, Decimal):
        return value.is_nan()
    if isinstance(value, float):
        return math.isnan(value)
    return str(value) == "NaN"


@register_strategy("ema_crossover")
class EmaCrossoverStrategy:
    """EMA Crossover trend-following strategy.

    Signal Logic:
    - LONG: Fast EMA crosses above Slow EMA (bullish crossover)
    - SHORT: Fast EMA crosses below Slow EMA (bearish crossover)

    TP/SL:
    - TP = entry +/- tp_atr_mult * ATR
    - SL = entry -/+ sl_atr_mult * ATR

    Position Management:
    - One active signal per symbol/timeframe at a time
    """

    def __init__(
        self,
        config: EmaCrossoverConfig | None = None,
        save_signal: SaveSignalCallback | None = None,
        save_streak: SaveStreakCallback | None = None,
        load_streaks: LoadStreaksCallback | None = None,
        load_active_signals: LoadActiveSignalsCallback | None = None,
    ):
        self.config = config or EmaCrossoverConfig()

        self.fast_period = self.config.fast_period
        self.slow_period = self.config.slow_period
        self.atr_period = self.config.atr_period
        self.tp_atr_mult = self.config.tp_atr_mult
        self.sl_atr_mult = self.config.sl_atr_mult

        self._save_signal = save_signal
        self._save_streak = save_streak
        self._load_streaks = load_streaks
        self._load_active_signals = load_active_signals

        self._callbacks: list[SignalCallback] = []
        self._initialized = False

        self._streak_trackers: dict[str, StreakTracker] = {}
        self._active_positions: dict[str, bool] = {}

        # Previous EMA values for crossover detection (per symbol_timeframe)
        self._prev_fast_ema: dict[str, Decimal] = {}
        self._prev_slow_ema: dict[str, Decimal] = {}

    # ------------------------------------------------------------------
    # Strategy Protocol properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return EMA_CROSSOVER_STRATEGY_NAME

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def required_indicators(self) -> list[str]:
        return [f"ema{self.fast_period}", f"ema{self.slow_period}", "atr"]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """Initialize streak trackers and active positions."""
        if self._initialized:
            return

        if self._load_streaks:
            cached_trackers = await self._load_streaks()
            if cached_trackers:
                self._streak_trackers = cached_trackers
                logger.info(
                    f"EMA Crossover: loaded {len(cached_trackers)} streak trackers"
                )

        if self._load_active_signals:
            active_signals = await self._load_active_signals()
            for signal in active_signals:
                symbol_key = f"{signal.symbol}_{signal.timeframe}"
                self._active_positions[symbol_key] = True
            logger.info(
                f"EMA Crossover: loaded {len(active_signals)} active positions"
            )

        self._initialized = True

    def _get_streak(self, symbol: str, timeframe: str) -> StreakTracker:
        """Get or create a streak tracker for a symbol/timeframe pair."""
        key = f"{symbol}_{timeframe}"
        if key not in self._streak_trackers:
            self._streak_trackers[key] = StreakTracker()
        return self._streak_trackers[key]

    def on_signal(self, callback: SignalCallback) -> None:
        """Register callback for new signals."""
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def off_signal(self, callback: SignalCallback) -> None:
        """Unregister callback for new signals."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    def detect_crossover(
        self,
        kline: Kline,
        fast_ema_value: Decimal,
        slow_ema_value: Decimal,
        atr_value: Decimal,
    ) -> SignalRecord | None:
        """Detect EMA crossover and generate signal.

        Args:
            kline: Current closed kline
            fast_ema_value: Current fast EMA value
            slow_ema_value: Current slow EMA value
            atr_value: Current ATR value

        Returns:
            SignalRecord if crossover detected, None otherwise
        """
        symbol_key = f"{kline.symbol}_{kline.timeframe}"

        # Only one active position per symbol/timeframe
        if self._active_positions.get(symbol_key, False):
            return None

        prev_fast = self._prev_fast_ema.get(symbol_key)
        prev_slow = self._prev_slow_ema.get(symbol_key)

        # Store current values for next comparison
        self._prev_fast_ema[symbol_key] = fast_ema_value
        self._prev_slow_ema[symbol_key] = slow_ema_value

        # Need previous values to detect crossover
        if prev_fast is None or prev_slow is None:
            return None

        entry_price = kline.close
        tp_distance = atr_value * self.tp_atr_mult
        sl_distance = atr_value * self.sl_atr_mult

        signal = None

        # Bullish crossover: fast was below slow, now fast is above slow
        if prev_fast <= prev_slow and fast_ema_value > slow_ema_value:
            tp_price = entry_price + tp_distance
            sl_price = entry_price - sl_distance

            streak = self._get_streak(kline.symbol, kline.timeframe)
            signal = SignalRecord(
                strategy=EMA_CROSSOVER_STRATEGY_NAME,
                symbol=kline.symbol,
                timeframe=kline.timeframe,
                signal_time=kline.timestamp,
                direction=Direction.LONG,
                entry_price=entry_price,
                tp_price=tp_price,
                sl_price=sl_price,
                atr_at_signal=atr_value,
                max_atr=atr_value,
                streak_at_signal=streak.current_streak,
            )
            logger.info(
                f"EMA LONG: {kline.symbol} @ {entry_price} "
                f"fast={fast_ema_value} slow={slow_ema_value} ATR={atr_value}"
            )

        # Bearish crossover: fast was above slow, now fast is below slow
        elif prev_fast >= prev_slow and fast_ema_value < slow_ema_value:
            tp_price = entry_price - tp_distance
            sl_price = entry_price + sl_distance

            streak = self._get_streak(kline.symbol, kline.timeframe)
            signal = SignalRecord(
                strategy=EMA_CROSSOVER_STRATEGY_NAME,
                symbol=kline.symbol,
                timeframe=kline.timeframe,
                signal_time=kline.timestamp,
                direction=Direction.SHORT,
                entry_price=entry_price,
                tp_price=tp_price,
                sl_price=sl_price,
                atr_at_signal=atr_value,
                max_atr=atr_value,
                streak_at_signal=streak.current_streak,
            )
            logger.info(
                f"EMA SHORT: {kline.symbol} @ {entry_price} "
                f"fast={fast_ema_value} slow={slow_ema_value} ATR={atr_value}"
            )

        return signal

    async def process_kline(
        self,
        kline: Kline,
        buffer: KlineBuffer,
    ) -> ProcessResult:
        """Process a closed kline and generate signal if crossover detected.

        Args:
            kline: The closed kline
            buffer: Buffer containing recent klines for indicator calculation

        Returns:
            ProcessResult with signal (if generated) and current ATR
        """
        if not kline.is_closed:
            return ProcessResult(signal=None, atr=None)

        # Need enough history for slow EMA
        min_required = max(self.slow_period, self.atr_period) + 1
        if len(buffer) < min_required:
            return ProcessResult(signal=None, atr=None)

        # Extract price data from buffer
        klines = buffer.klines
        highs = [k.high for k in klines]
        lows = [k.low for k in klines]
        closes = [k.close for k in klines]

        # Calculate indicators using existing functions
        fast_ema_values = ema(closes, self.fast_period)
        slow_ema_values = ema(closes, self.slow_period)
        atr_values = atr(highs, lows, closes, self.atr_period)

        fast_ema_value = fast_ema_values[-1]
        slow_ema_value = slow_ema_values[-1]
        atr_value = atr_values[-1]

        # Skip if any indicator is NaN
        if any(_is_nan(v) for v in [fast_ema_value, slow_ema_value, atr_value]):
            return ProcessResult(signal=None, atr=None)

        atr_float = float(atr_value) if not _is_nan(atr_value) else None

        # Detect crossover
        signal = self.detect_crossover(
            kline, fast_ema_value, slow_ema_value, atr_value
        )

        if signal:
            symbol_key = f"{signal.symbol}_{signal.timeframe}"

            # Persist signal via callback
            if self._save_signal:
                try:
                    await self._save_signal(signal)
                except Exception as e:
                    logger.error(
                        f"Failed to save EMA signal {signal.id}: {e}. "
                        "Signal will NOT be tracked."
                    )
                    return ProcessResult(signal=None, atr=atr_float)

            # Mark position as active
            self._active_positions[symbol_key] = True

            # Notify callbacks
            for callback in self._callbacks:
                try:
                    await callback(signal)
                except Exception as e:
                    logger.error(f"EMA signal callback error: {e}")

        return ProcessResult(signal=signal, atr=atr_float)

    async def record_outcome(
        self,
        outcome: Outcome,
        symbol: str | None = None,
        timeframe: str | None = None,
    ) -> None:
        """Record a signal outcome and update streak tracker."""
        if symbol and timeframe:
            tracker = self._get_streak(symbol, timeframe)
            tracker.record_outcome(outcome)

            if self._save_streak:
                await self._save_streak(symbol, timeframe, tracker)

            logger.debug(
                f"EMA streak {symbol}_{timeframe}: {tracker.current_streak} "
                f"(wins={tracker.total_wins}, losses={tracker.total_losses})"
            )

            # Release position lock
            symbol_key = f"{symbol}_{timeframe}"
            if symbol_key in self._active_positions:
                del self._active_positions[symbol_key]

    def release_position(self, symbol: str, timeframe: str) -> None:
        """Release position lock for a symbol/timeframe combination."""
        symbol_key = f"{symbol}_{timeframe}"
        if symbol_key in self._active_positions:
            del self._active_positions[symbol_key]
            logger.debug(f"Released EMA position lock for {symbol_key}")
