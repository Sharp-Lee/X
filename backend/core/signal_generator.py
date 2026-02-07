"""Signal generator implementing the MSR Retest Capture strategy.

This module is pure business logic with no I/O dependencies.
All persistence operations are injected via callbacks, making it
usable by both the live trading system and the backtesting system.
"""

import logging
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Awaitable

from core.indicators import IndicatorCalculator
from core.models import (
    Direction,
    Kline,
    KlineBuffer,
    Outcome,
    SignalRecord,
    StreakTracker,
)
from core.models.config import StrategyConfig

logger = logging.getLogger(__name__)

# Type aliases for callbacks
SignalCallback = Callable[[SignalRecord], Awaitable[None]]
SaveSignalCallback = Callable[[SignalRecord], Awaitable[None]]
SaveStreakCallback = Callable[[str, str, StreakTracker], Awaitable[None]]
LoadStreaksCallback = Callable[[], Awaitable[dict[str, StreakTracker]]]
LoadActiveSignalsCallback = Callable[[], Awaitable[list[SignalRecord]]]


@dataclass
class ProcessKlineResult:
    """Result of processing a kline."""
    signal: SignalRecord | None  # Generated signal, if any
    atr: float | None  # Current ATR value (for updating max_atr of active signals)


class LevelManager:
    """Manages support and resistance levels based on indicators."""

    def __init__(self, touch_tolerance: Decimal = Decimal("0.001")):
        """
        Args:
            touch_tolerance: Tolerance for price touching levels (as ratio)
        """
        self.touch_tolerance = touch_tolerance

    def get_levels(
        self,
        close: Decimal,
        fib_382: Decimal,
        fib_500: Decimal,
        fib_618: Decimal,
        vwap_value: Decimal,
    ) -> tuple[list[Decimal], list[Decimal]]:
        """
        Classify levels as support or resistance based on current price.

        Returns:
            Tuple of (support_levels, resistance_levels)
        """
        support_levels = []
        resistance_levels = []

        for level in [fib_382, fib_500, fib_618, vwap_value]:
            if close < level:
                resistance_levels.append(level)
            else:
                support_levels.append(level)

        return support_levels, resistance_levels

    def get_nearest_levels(
        self,
        close: Decimal,
        support_levels: list[Decimal],
        resistance_levels: list[Decimal],
    ) -> tuple[Decimal | None, Decimal | None]:
        """
        Get the nearest support and resistance levels.

        Returns:
            Tuple of (nearest_support, nearest_resistance)
        """
        nearest_support = None
        nearest_resistance = None

        for level in support_levels:
            if level < close:
                if nearest_support is None or level > nearest_support:
                    nearest_support = level

        for level in resistance_levels:
            if level > close:
                if nearest_resistance is None or level < nearest_resistance:
                    nearest_resistance = level

        return nearest_support, nearest_resistance

    def calculate_level_score(
        self,
        price: Decimal,
        levels: list[Decimal],
        is_support: bool,
    ) -> tuple[Decimal, int]:
        """
        Calculate score based on proximity to levels.

        Returns:
            Tuple of (score, count)
        """
        score = Decimal("0")
        count = 0

        for level in levels:
            if (is_support and level < price) or (not is_support and level > price):
                dist = abs(price - level) / price * 100
                score += Decimal("1") / (Decimal("1") + dist)
                count += 1

        return score, count

    def is_touching_level(
        self,
        price: Decimal,
        level: Decimal,
    ) -> bool:
        """Check if price is touching a level within tolerance.

        Note: This method is not used in the current MSR Retest Capture strategy,
        which uses exact price comparison (low <= support) matching the Pine Script.
        This method is available for alternative strategies that need tolerance-based
        level detection.
        """
        tolerance = level * self.touch_tolerance
        return abs(price - level) <= tolerance


def _is_nan(value) -> bool:
    """Check if a value is NaN (handles Decimal and float).

    Args:
        value: Value to check (Decimal, float, or None)

    Returns:
        True if value is NaN or None
    """
    if value is None:
        return True
    if isinstance(value, Decimal):
        return value.is_nan()
    if isinstance(value, float):
        return math.isnan(value)
    # For string comparison (legacy support)
    return str(value) == "NaN"


class SignalGenerator:
    """
    Generate trading signals based on the MSR Retest Capture strategy.

    Strategy Logic:
    - Uptrend (close > ema50) + Touch support + Bullish reversal → Short
    - Downtrend (close < ema50) + Touch resistance + Bearish reversal → Long

    TP/SL:
    - TP distance = ATR × 2 (with math.max/min limits based on high/low)
    - SL distance = ATR × 2 × 4.42

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
        - TP distance = ATR × tp_mult (narrow), with math.max/min limits
        - SL distance = ATR × sl_mult (wide, = tp_mult × 4.42)

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

    async def process_kline(
        self,
        kline: Kline,
        buffer: KlineBuffer,
    ) -> ProcessKlineResult:
        """
        Process a closed kline and generate signal if conditions are met.

        Args:
            kline: The closed kline
            buffer: Buffer containing recent klines for indicator calculation

        Returns:
            ProcessKlineResult with signal (if generated) and current ATR
        """
        if not kline.is_closed:
            return ProcessKlineResult(signal=None, atr=None)

        # Need enough history for indicators
        if len(buffer) < 50:
            return ProcessKlineResult(signal=None, atr=None)

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
            return ProcessKlineResult(signal=None, atr=None)

        # Extract ATR for updating max_atr of active signals
        atr_value = float(indicators["atr"]) if indicators["atr"] else None

        # Get previous kline for level touch detection
        prev_kline = klines[-2] if len(klines) >= 2 else None

        # Detect signal
        signal = self.detect_signal(kline, prev_kline, indicators)

        if signal:
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
                    return ProcessKlineResult(signal=None, atr=atr_value)

            # Mark position as active only after successful save
            # (Pine Script: strategy.position_size != 0)
            self._active_positions[symbol_key] = True

            # Notify callbacks (PositionTracker.add_signal, WebSocket broadcast)
            for callback in self._callbacks:
                try:
                    await callback(signal)
                except Exception as e:
                    logger.error(f"Signal callback error: {e}")

        return ProcessKlineResult(signal=signal, atr=atr_value)

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
