"""MSR Retest Capture strategy implementation.

This module contains the MsrStrategy class (formerly SignalGenerator),
which implements the MSR Retest Capture trading strategy.

This module is pure business logic with no I/O dependencies.
All persistence operations are injected via callbacks, making it
usable by both the live trading system and the backtesting system.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Awaitable

from core.atr_tracker import AtrPercentileTracker
from core.indicators import IndicatorCalculator
from core.models import (
    Direction,
    Kline,
    KlineBuffer,
    Outcome,
    SignalRecord,
    StreakTracker,
)
from core.models.config import SignalFilterConfig, StrategyConfig
from core.strategy.msr.level_manager import LevelManager, _is_nan
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


# Backward compatibility alias
ProcessKlineResult = ProcessResult


@register_strategy("msr_retest_capture")
class MsrStrategy:
    """
    Generate trading signals based on the MSR Retest Capture strategy.

    Strategy Logic:
    - Uptrend (close > ema50) + Touch support + Bullish reversal -> Short
    - Downtrend (close < ema50) + Touch resistance + Bearish reversal -> Long

    TP/SL:
    - TP distance = ATR x 2 (with math.max/min limits based on high/low)
    - SL distance = ATR x 2 x 4.42

    Risk Management:
    - Maximum risk per trade: 2.53% of equity
    - Only one position per symbol at a time

    All I/O operations are injected via callbacks:
    - save_signal: Persist a new signal (e.g., to database)
    - save_streak: Persist streak tracker state (e.g., to Redis)
    - load_streaks: Load all streak trackers at startup
    - load_active_signals: Load active (open) signals at startup
    """

    MIN_SCORE_THRESHOLD = Decimal("1.0")

    def __init__(
        self,
        config: StrategyConfig,
        save_signal: SaveSignalCallback | None = None,
        save_streak: SaveStreakCallback | None = None,
        load_streaks: LoadStreaksCallback | None = None,
        load_active_signals: LoadActiveSignalsCallback | None = None,
        filters: dict[str, SignalFilterConfig] | None = None,
        atr_tracker: AtrPercentileTracker | None = None,
    ):
        self.config = config
        self.indicator_calc = IndicatorCalculator(
            ema_period=config.ema_period,
            fib_period=config.fib_period,
            atr_period=config.atr_period,
        )
        self.level_manager = LevelManager(touch_tolerance=config.touch_tolerance)
        self._streak_trackers: dict[str, StreakTracker] = {}

        self.tp_atr_mult = config.tp_atr_mult
        self.sl_atr_mult = config.sl_atr_mult

        # Injected callbacks (None = no-op, e.g., in backtesting mode)
        self._save_signal = save_signal
        self._save_streak = save_streak
        self._load_streaks = load_streaks
        self._load_active_signals = load_active_signals

        self._callbacks: list[SignalCallback] = []
        self._initialized = False

        # Track active positions per symbol (Pine Script: strategy.position_size == 0)
        self._active_positions: dict[str, bool] = {}

        # Signal quality filters (keyed by "SYMBOL_TIMEFRAME")
        # When None, all signals pass (backward compatible with backtest).
        self._filters = filters
        self._atr_tracker = atr_tracker

        if filters:
            logger.info(
                "Signal filters enabled: %s",
                ", ".join(
                    f"{f.symbol} {f.timeframe} streak[{f.streak_lo},{f.streak_hi}] "
                    f"ATR>{f.atr_pct_threshold:.0%}"
                    for f in filters.values()
                    if f.enabled
                ),
            )

    # ------------------------------------------------------------------
    # Strategy Protocol properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "msr_retest_capture"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def required_indicators(self) -> list[str]:
        return ["ema50", "atr", "fib_382", "fib_500", "fib_618", "vwap"]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """Initialize per-symbol/timeframe streak trackers and active positions."""
        if self._initialized:
            return

        # Load streaks via callback
        if self._load_streaks:
            cached_trackers = await self._load_streaks()
            if cached_trackers:
                self._streak_trackers = cached_trackers
                logger.info(
                    f"Loaded {len(cached_trackers)} streak trackers: "
                    + ", ".join(
                        f"{k}={v.current_streak}" for k, v in cached_trackers.items()
                    )
                )
            else:
                logger.info("No streak trackers found, will build from outcomes")

        # Load active positions via callback
        if self._load_active_signals:
            active_signals = await self._load_active_signals()
            for signal in active_signals:
                symbol_key = f"{signal.symbol}_{signal.timeframe}"
                self._active_positions[symbol_key] = True
            logger.info(f"Loaded {len(active_signals)} active positions")

        self._initialized = True

    def _get_streak(self, symbol: str, timeframe: str) -> StreakTracker:
        """Get or create a streak tracker for a symbol/timeframe pair."""
        key = f"{symbol}_{timeframe}"
        if key not in self._streak_trackers:
            self._streak_trackers[key] = StreakTracker()
        return self._streak_trackers[key]

    def on_signal(self, callback: SignalCallback) -> None:
        """Register callback for new signals.

        Note: Duplicate callbacks are ignored.
        """
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def off_signal(self, callback: SignalCallback) -> None:
        """Unregister callback for new signals."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def calculate_tp_sl(
        self,
        direction: Direction,
        entry_price: Decimal,
        atr_value: Decimal,
        high: Decimal,
        low: Decimal,
    ) -> tuple[Decimal, Decimal]:
        """
        Calculate take profit and stop loss prices.

        Strategy uses "wide stop, narrow take profit" design:
        - TP distance = ATR x tp_mult (narrow), with math.max/min limits
        - SL distance = ATR x sl_mult (wide, = tp_mult x 4.42)

        Pine Script logic:
        - LONG: tp = min(entry + tp_distance, high + atr)
        - SHORT: tp = max(entry - tp_distance, low - atr)

        Returns:
            Tuple of (tp_price, sl_price)
        """
        tp_distance = atr_value * self.tp_atr_mult
        sl_distance = atr_value * self.sl_atr_mult

        if direction == Direction.LONG:
            # Pine Script: math.min(entryPrice + narrowDistance, high + atr)
            tp_raw = entry_price + tp_distance
            tp_limit = high + atr_value
            tp_price = min(tp_raw, tp_limit)
            sl_price = entry_price - sl_distance
        else:  # SHORT
            # Pine Script: math.max(entryPrice - narrowDistance, low - atr)
            tp_raw = entry_price - tp_distance
            tp_limit = low - atr_value
            tp_price = max(tp_raw, tp_limit)
            sl_price = entry_price + sl_distance

        return tp_price, sl_price

    def detect_signal(
        self,
        kline: Kline,
        prev_kline: Kline | None,
        indicators: dict,
    ) -> SignalRecord | None:
        """
        Detect if current kline produces a signal.

        Args:
            kline: Current (closed) kline
            prev_kline: Previous kline (for checking if prev touched level)
            indicators: Dict of indicator values

        Returns:
            SignalRecord if signal detected, None otherwise
        """
        close = kline.close
        open_price = kline.open
        high = kline.high
        low = kline.low
        prev_low = prev_kline.low if prev_kline else low
        prev_high = prev_kline.high if prev_kline else high

        ema50 = indicators["ema50"]
        atr_value = indicators["atr"]
        fib_382 = indicators["fib_382"]
        fib_500 = indicators["fib_500"]
        fib_618 = indicators["fib_618"]
        vwap_value = indicators["vwap"]

        # Skip if any indicator is NaN (not enough data)
        if any(_is_nan(v) for v in [ema50, atr_value, fib_382, fib_500, fib_618, vwap_value]):
            return None

        # Pine Script: strategy.position_size == 0
        # Only allow one active position per symbol
        symbol_key = f"{kline.symbol}_{kline.timeframe}"
        if self._active_positions.get(symbol_key, False):
            return None

        # Get support/resistance levels (using already validated indicators)
        support_levels, resistance_levels = self.level_manager.get_levels(
            close,
            fib_382,
            fib_500,
            fib_618,
            vwap_value,
        )

        # Get nearest levels
        nearest_support, nearest_resistance = self.level_manager.get_nearest_levels(
            close, support_levels, resistance_levels
        )

        # Calculate scores
        support_score, support_count = self.level_manager.calculate_level_score(
            close, support_levels, is_support=True
        )
        resistance_score, resistance_count = self.level_manager.calculate_level_score(
            close, resistance_levels, is_support=False
        )

        # Determine trend
        uptrend = close > ema50
        downtrend = close < ema50

        # Check for bullish/bearish candle
        is_bullish = close > open_price
        is_bearish = close < open_price

        signal = None

        # SHORT signal: Uptrend + Touch support + Bullish reversal
        # Logic: Price touched support and reversed up, expect it to retest support
        if (
            uptrend
            and support_count >= 1
            and support_score >= self.MIN_SCORE_THRESHOLD
            and nearest_support is not None
        ):
            touched_support = low <= nearest_support or prev_low <= nearest_support
            if touched_support and is_bullish:
                tp_price, sl_price = self.calculate_tp_sl(
                    Direction.SHORT, close, atr_value, high, low
                )

                streak = self._get_streak(kline.symbol, kline.timeframe)
                signal = SignalRecord(
                    symbol=kline.symbol,
                    timeframe=kline.timeframe,
                    signal_time=kline.timestamp,
                    direction=Direction.SHORT,
                    entry_price=close,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    atr_at_signal=atr_value,
                    max_atr=atr_value,  # Initialize max_atr with atr_at_signal
                    streak_at_signal=streak.current_streak,
                )
                logger.info(
                    f"SHORT signal: {kline.symbol} @ {close} "
                    f"TP={tp_price} SL={sl_price} ATR={atr_value}"
                )

        # LONG signal: Downtrend + Touch resistance + Bearish reversal
        # Logic: Price touched resistance and reversed down, expect it to retest resistance
        elif (
            downtrend
            and resistance_count >= 1
            and resistance_score >= self.MIN_SCORE_THRESHOLD
            and nearest_resistance is not None
        ):
            touched_resistance = (
                high >= nearest_resistance or prev_high >= nearest_resistance
            )
            if touched_resistance and is_bearish:
                tp_price, sl_price = self.calculate_tp_sl(
                    Direction.LONG, close, atr_value, high, low
                )

                streak = self._get_streak(kline.symbol, kline.timeframe)
                signal = SignalRecord(
                    symbol=kline.symbol,
                    timeframe=kline.timeframe,
                    signal_time=kline.timestamp,
                    direction=Direction.LONG,
                    entry_price=close,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    atr_at_signal=atr_value,
                    max_atr=atr_value,  # Initialize max_atr with atr_at_signal
                    streak_at_signal=streak.current_streak,
                )
                logger.info(
                    f"LONG signal: {kline.symbol} @ {close} "
                    f"TP={tp_price} SL={sl_price} ATR={atr_value}"
                )

        return signal

    def _passes_filter(self, signal: SignalRecord, atr_value: float) -> bool:
        """Check whether *signal* passes the quality filters.

        Returns ``True`` (pass) when:
        - No filters are configured (backward compatible).
        - The signal's symbol/timeframe has an enabled filter entry AND
          both the streak and ATR-percentile checks pass.

        Returns ``False`` (reject) when:
        - Filters are configured but this symbol/timeframe is absent.
        - The filter entry is disabled.
        - streak_at_signal is outside [streak_lo, streak_hi].
        - ATR percentile is at or below the threshold (or data is insufficient).
        """
        if self._filters is None:
            return True  # no filters configured -> all signals pass

        key = f"{signal.symbol}_{signal.timeframe}"
        fc = self._filters.get(key)
        if fc is None or not fc.enabled:
            return False  # not in the portfolio

        # --- streak filter ---
        if not (fc.streak_lo <= signal.streak_at_signal <= fc.streak_hi):
            logger.debug(
                "Filter REJECT %s %s: streak=%d not in [%d,%d]",
                signal.symbol,
                signal.timeframe,
                signal.streak_at_signal,
                fc.streak_lo,
                fc.streak_hi,
            )
            return False

        # --- ATR percentile filter ---
        if fc.atr_pct_threshold > 0:
            if self._atr_tracker is None:
                # Configuration error: threshold set but no tracker injected.
                # Reject for safety to avoid unfiltered trading.
                logger.warning(
                    "Filter REJECT %s %s: atr_pct_threshold=%.2f but no ATR tracker",
                    signal.symbol,
                    signal.timeframe,
                    fc.atr_pct_threshold,
                )
                return False
            pct = self._atr_tracker.get_percentile(
                signal.symbol, signal.timeframe, atr_value
            )
            if pct is None:
                # Not enough data -> reject for safety
                logger.debug(
                    "Filter REJECT %s %s: ATR history insufficient",
                    signal.symbol,
                    signal.timeframe,
                )
                return False
            if pct <= fc.atr_pct_threshold:
                logger.debug(
                    "Filter REJECT %s %s: atr_pct=%.2f <= %.2f",
                    signal.symbol,
                    signal.timeframe,
                    pct,
                    fc.atr_pct_threshold,
                )
                return False

        return True

    async def process_kline(
        self,
        kline: Kline,
        buffer: KlineBuffer,
    ) -> ProcessResult:
        """
        Process a closed kline and generate signal if conditions are met.

        Args:
            kline: The closed kline
            buffer: Buffer containing recent klines for indicator calculation

        Returns:
            ProcessResult with signal (if generated) and current ATR
        """
        if not kline.is_closed:
            return ProcessResult(signal=None, atr=None)

        # Need enough history for indicators
        if len(buffer) < 50:
            return ProcessResult(signal=None, atr=None)

        # Get OHLCV data from buffer
        klines = buffer.klines
        opens = [k.open for k in klines]
        highs = [k.high for k in klines]
        lows = [k.low for k in klines]
        closes = [k.close for k in klines]
        volumes = [k.volume for k in klines]

        # Calculate indicators
        indicators = self.indicator_calc.calculate_latest(
            opens, highs, lows, closes, volumes
        )

        if indicators is None:
            return ProcessResult(signal=None, atr=None)

        # Extract ATR for updating max_atr of active signals.
        # Note: Decimal("0") is falsy and Decimal("NaN") is truthy in Python,
        # so we must use explicit None + NaN checks instead of truthiness.
        raw_atr = indicators["atr"]
        if raw_atr is not None and not _is_nan(raw_atr):
            atr_value = float(raw_atr)
        else:
            atr_value = None

        # Track ATR history for percentile calculation (every closed kline,
        # not just signal klines -- the expanding window must reflect the full
        # market, not a biased subset).
        if atr_value is not None and self._atr_tracker is not None:
            self._atr_tracker.update(kline.symbol, kline.timeframe, atr_value)

        # Get previous kline for level touch detection
        prev_kline = klines[-2] if len(klines) >= 2 else None

        # Detect signal
        signal = self.detect_signal(kline, prev_kline, indicators)

        if signal:
            # Apply signal quality filters (streak + ATR percentile).
            # atr_value is guaranteed non-None here because detect_signal()
            # returns None when any indicator (including ATR) is NaN.
            if not self._passes_filter(signal, atr_value):
                return ProcessResult(signal=None, atr=atr_value)

            symbol_key = f"{signal.symbol}_{signal.timeframe}"

            # Persist signal via callback
            if self._save_signal:
                try:
                    await self._save_signal(signal)
                except Exception as e:
                    logger.error(
                        f"Failed to save signal {signal.id}: {e}. "
                        "Signal will NOT be tracked."
                    )
                    return ProcessResult(signal=None, atr=atr_value)

            # Mark position as active only after successful save
            # (Pine Script: strategy.position_size != 0)
            self._active_positions[symbol_key] = True

            # Notify callbacks (PositionTracker.add_signal, WebSocket broadcast)
            for callback in self._callbacks:
                try:
                    await callback(signal)
                except Exception as e:
                    logger.error(f"Signal callback error: {e}")

        return ProcessResult(signal=signal, atr=atr_value)

    async def record_outcome(
        self, outcome: Outcome, symbol: str | None = None, timeframe: str | None = None
    ) -> None:
        """Record a signal outcome and update per-symbol/timeframe streak tracker.

        Args:
            outcome: The outcome (TP or SL)
            symbol: Symbol of the closed position (to release position lock)
            timeframe: Timeframe of the closed position
        """
        # Update per-symbol/timeframe streak
        if symbol and timeframe:
            tracker = self._get_streak(symbol, timeframe)
            tracker.record_outcome(outcome)

            # Persist streak via callback
            if self._save_streak:
                await self._save_streak(symbol, timeframe, tracker)

            logger.debug(
                f"Updated streak for {symbol}_{timeframe}: {tracker.current_streak} "
                f"(wins={tracker.total_wins}, losses={tracker.total_losses})"
            )

            # Release position lock (Pine Script: allow new position after close)
            symbol_key = f"{symbol}_{timeframe}"
            if symbol_key in self._active_positions:
                del self._active_positions[symbol_key]
                logger.debug(f"Released position lock for {symbol_key}")

    def release_position(self, symbol: str, timeframe: str) -> None:
        """Release position lock for a symbol/timeframe combination.

        Call this when a position is closed externally (e.g., by position tracker).
        """
        symbol_key = f"{symbol}_{timeframe}"
        if symbol_key in self._active_positions:
            del self._active_positions[symbol_key]
            logger.debug(f"Released position lock for {symbol_key}")


# Backward compatibility alias
SignalGenerator = MsrStrategy
