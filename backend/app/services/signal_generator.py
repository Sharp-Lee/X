"""Signal generator implementing the MSR Retest Capture strategy."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Callable, Awaitable

from app.config import get_settings
from app.core import IndicatorCalculator
from app.models import (
    Direction,
    Kline,
    KlineBuffer,
    Outcome,
    SignalRecord,
    StreakTracker,
)
from app.storage import SignalRepository
from app.storage import streak_cache

logger = logging.getLogger(__name__)

# Type alias for signal callback
SignalCallback = Callable[[SignalRecord], Awaitable[None]]


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
        """Check if price is touching a level within tolerance."""
        tolerance = level * self.touch_tolerance
        return abs(price - level) <= tolerance


class SignalGenerator:
    """
    Generate trading signals based on the MSR Retest Capture strategy.

    Strategy Logic:
    - Uptrend (close > ema50) + Touch support + Bullish reversal → Short
    - Downtrend (close < ema50) + Touch resistance + Bearish reversal → Long

    TP/SL:
    - TP distance = ATR × 2
    - SL distance = ATR × 2 × 4.42
    """

    MIN_SCORE_THRESHOLD = Decimal("1.0")

    def __init__(self):
        settings = get_settings()
        self.indicator_calc = IndicatorCalculator(
            ema_period=settings.ema_period,
            fib_period=settings.fib_period,
            atr_period=settings.atr_period,
        )
        self.level_manager = LevelManager()
        self.streak_tracker = StreakTracker()
        self.signal_repo = SignalRepository()

        self.tp_atr_mult = Decimal(str(settings.tp_atr_mult))
        self.sl_atr_mult = Decimal(str(settings.sl_atr_mult))

        self._callbacks: list[SignalCallback] = []
        self._initialized = False

    async def init(self) -> None:
        """Initialize streak tracker from cache or database."""
        if self._initialized:
            return

        # Try to load from cache first
        cached_tracker = await streak_cache.load_streak()
        if cached_tracker:
            self.streak_tracker = cached_tracker
            logger.info(
                f"Loaded streak from cache: {cached_tracker.current_streak} "
                f"(wins={cached_tracker.total_wins}, losses={cached_tracker.total_losses})"
            )
        else:
            # Fall back to database query for historical stats
            stats = await self.signal_repo.get_stats()
            self.streak_tracker.total_wins = stats.get("tp_count", 0)
            self.streak_tracker.total_losses = stats.get("sl_count", 0)
            # Sync to cache
            await streak_cache.save_streak(self.streak_tracker)
            logger.info(
                f"Loaded streak from database: "
                f"wins={self.streak_tracker.total_wins}, "
                f"losses={self.streak_tracker.total_losses}"
            )

        self._initialized = True

    def on_signal(self, callback: SignalCallback) -> None:
        """Register callback for new signals."""
        self._callbacks.append(callback)

    def calculate_tp_sl(
        self,
        direction: Direction,
        entry_price: Decimal,
        atr_value: Decimal,
    ) -> tuple[Decimal, Decimal]:
        """
        Calculate take profit and stop loss prices.

        Strategy uses "wide stop, narrow take profit" design:
        - TP distance = ATR × tp_mult (narrow)
        - SL distance = ATR × sl_mult (wide, = tp_mult × 4.42)

        Returns:
            Tuple of (tp_price, sl_price)
        """
        tp_distance = atr_value * self.tp_atr_mult
        sl_distance = atr_value * self.sl_atr_mult

        if direction == Direction.LONG:
            tp_price = entry_price + tp_distance
            sl_price = entry_price - sl_distance
        else:  # SHORT
            tp_price = entry_price - tp_distance
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

        # Skip if indicators not ready
        if str(ema50) == "NaN" or str(atr_value) == "NaN":
            return None

        # Get support/resistance levels
        support_levels, resistance_levels = self.level_manager.get_levels(
            close,
            indicators["fib_382"],
            indicators["fib_500"],
            indicators["fib_618"],
            indicators["vwap"],
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
                    Direction.SHORT, close, atr_value
                )
                signal = SignalRecord(
                    symbol=kline.symbol,
                    timeframe=kline.timeframe,
                    signal_time=kline.timestamp,
                    direction=Direction.SHORT,
                    entry_price=close,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    atr_at_signal=atr_value,
                    streak_at_signal=self.streak_tracker.current_streak,
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
                    Direction.LONG, close, atr_value
                )
                signal = SignalRecord(
                    symbol=kline.symbol,
                    timeframe=kline.timeframe,
                    signal_time=kline.timestamp,
                    direction=Direction.LONG,
                    entry_price=close,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    atr_at_signal=atr_value,
                    streak_at_signal=self.streak_tracker.current_streak,
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
    ) -> SignalRecord | None:
        """
        Process a closed kline and generate signal if conditions are met.

        Args:
            kline: The closed kline
            buffer: Buffer containing recent klines for indicator calculation

        Returns:
            SignalRecord if signal generated, None otherwise
        """
        if not kline.is_closed:
            return None

        # Need enough history for indicators
        if len(buffer) < 50:
            return None

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
            return None

        # Get previous kline for level touch detection
        prev_kline = klines[-2] if len(klines) >= 2 else None

        # Detect signal
        signal = self.detect_signal(kline, prev_kline, indicators)

        if signal:
            # Save to database
            await self.signal_repo.save(signal)

            # Notify callbacks
            for callback in self._callbacks:
                try:
                    await callback(signal)
                except Exception as e:
                    logger.error(f"Signal callback error: {e}")

        return signal

    async def record_outcome(self, outcome: Outcome) -> None:
        """Record a signal outcome and update streak tracker."""
        self.streak_tracker.record_outcome(outcome)
        # Save to cache
        await streak_cache.save_streak(self.streak_tracker)
        logger.debug(
            f"Updated streak: {self.streak_tracker.current_streak} "
            f"(wins={self.streak_tracker.total_wins}, losses={self.streak_tracker.total_losses})"
        )
