"""Tests for EMA Crossover strategy."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from core.models.signal import Direction, Outcome, SignalRecord
from core.models.kline import Kline, KlineBuffer
from core.strategy.ema_crossover import (
    EmaCrossoverStrategy,
    EmaCrossoverConfig,
    EmaSignalRecord,
    EMA_CROSSOVER_STRATEGY_NAME,
)
from core.strategy import list_strategies, create_strategy, Strategy
from core.strategy.protocol import ProcessResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_kline(
    symbol: str = "BTCUSDT",
    timeframe: str = "5m",
    minute: int = 0,
    close: Decimal = Decimal("50000"),
    high: Decimal | None = None,
    low: Decimal | None = None,
) -> Kline:
    """Create a test kline."""
    if high is None:
        high = close + Decimal("50")
    if low is None:
        low = close - Decimal("50")
    return Kline(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=datetime(2024, 1, 1, minute // 60, minute % 60, tzinfo=timezone.utc),
        open=close - Decimal("10"),
        high=high,
        low=low,
        close=close,
        volume=Decimal("100"),
        is_closed=True,
    )


def _make_buffer(n: int = 60, base_price: float = 50000.0) -> KlineBuffer:
    """Create a buffer with n klines showing a gentle uptrend."""
    buffer = KlineBuffer(symbol="BTCUSDT", timeframe="5m")
    for i in range(n):
        price = Decimal(str(base_price + i * 10))
        kline = Kline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=datetime(2024, 1, 1, i // 60, i % 60, tzinfo=timezone.utc),
            open=price - Decimal("5"),
            high=price + Decimal("50"),
            low=price - Decimal("50"),
            close=price,
            volume=Decimal("100"),
            is_closed=True,
        )
        buffer.add(kline)
    return buffer


# ---------------------------------------------------------------------------
# Registry / Protocol tests
# ---------------------------------------------------------------------------

class TestRegistry:
    """Test that EMA Crossover is properly registered."""

    def test_registered_in_strategy_list(self):
        strategies = list_strategies()
        assert "ema_crossover" in strategies

    def test_create_strategy_by_name(self):
        config = EmaCrossoverConfig()
        strategy = create_strategy("ema_crossover", config=config)
        assert strategy.name == "ema_crossover"
        assert strategy.version == "1.0.0"

    def test_satisfies_strategy_protocol(self):
        strategy = EmaCrossoverStrategy()
        assert isinstance(strategy, Strategy)

    def test_required_indicators(self):
        strategy = EmaCrossoverStrategy()
        indicators = strategy.required_indicators
        assert "ema20" in indicators
        assert "ema50" in indicators
        assert "atr" in indicators


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestEmaSignalRecord:
    """Test EmaSignalRecord model."""

    def test_default_strategy_name(self):
        record = EmaSignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49000"),
        )
        assert record.strategy == "ema_crossover"
        assert record.id != ""

    def test_extra_fields(self):
        record = EmaSignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49000"),
            ema_fast=Decimal("50100"),
            ema_slow=Decimal("49900"),
            atr_at_signal=Decimal("200"),
        )
        assert record.ema_fast == Decimal("50100")
        assert record.ema_slow == Decimal("49900")
        assert record.atr_at_signal == Decimal("200")


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------

class TestEmaCrossoverConfig:
    """Test strategy configuration."""

    def test_default_config(self):
        config = EmaCrossoverConfig()
        assert config.fast_period == 20
        assert config.slow_period == 50
        assert config.atr_period == 9
        assert config.tp_atr_mult == Decimal("2.0")
        assert config.sl_atr_mult == Decimal("4.0")

    def test_custom_config(self):
        config = EmaCrossoverConfig(fast_period=10, slow_period=30)
        assert config.fast_period == 10
        assert config.slow_period == 30


# ---------------------------------------------------------------------------
# Crossover detection tests
# ---------------------------------------------------------------------------

class TestCrossoverDetection:
    """Test EMA crossover detection logic."""

    def test_no_signal_on_first_kline(self):
        """First kline has no previous EMA -> no signal."""
        strategy = EmaCrossoverStrategy()
        kline = _make_kline(minute=0)

        signal = strategy.detect_crossover(
            kline,
            fast_ema_value=Decimal("50100"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )
        assert signal is None

    def test_bullish_crossover(self):
        """Fast EMA crosses above slow EMA -> LONG signal."""
        strategy = EmaCrossoverStrategy()
        kline1 = _make_kline(minute=0)
        kline2 = _make_kline(minute=1)

        # First kline: fast below slow (no signal, sets prev values)
        strategy.detect_crossover(
            kline1,
            fast_ema_value=Decimal("49900"),  # fast below slow
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )

        # Second kline: fast crosses above slow -> LONG
        signal = strategy.detect_crossover(
            kline2,
            fast_ema_value=Decimal("50100"),  # fast above slow
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )

        assert signal is not None
        assert signal.direction == Direction.LONG
        assert signal.strategy == "ema_crossover"

    def test_bearish_crossover(self):
        """Fast EMA crosses below slow EMA -> SHORT signal."""
        strategy = EmaCrossoverStrategy()
        kline1 = _make_kline(minute=0)
        kline2 = _make_kline(minute=1)

        # First: fast above slow
        strategy.detect_crossover(
            kline1,
            fast_ema_value=Decimal("50100"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )

        # Second: fast crosses below slow -> SHORT
        signal = strategy.detect_crossover(
            kline2,
            fast_ema_value=Decimal("49900"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )

        assert signal is not None
        assert signal.direction == Direction.SHORT

    def test_no_crossover_when_same_side(self):
        """No signal when fast stays above slow (no crossover)."""
        strategy = EmaCrossoverStrategy()
        kline1 = _make_kline(minute=0)
        kline2 = _make_kline(minute=1)

        # Both klines: fast above slow
        strategy.detect_crossover(
            kline1,
            fast_ema_value=Decimal("50100"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )

        signal = strategy.detect_crossover(
            kline2,
            fast_ema_value=Decimal("50200"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )

        assert signal is None

    def test_tp_sl_calculation_long(self):
        """Verify TP/SL for LONG signal."""
        strategy = EmaCrossoverStrategy()
        kline1 = _make_kline(minute=0, close=Decimal("50000"))
        kline2 = _make_kline(minute=1, close=Decimal("50000"))

        strategy.detect_crossover(
            kline1,
            fast_ema_value=Decimal("49900"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("100"),
        )

        signal = strategy.detect_crossover(
            kline2,
            fast_ema_value=Decimal("50100"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("100"),
        )

        assert signal is not None
        # TP = entry + 2.0 * ATR = 50000 + 200 = 50200
        assert signal.tp_price == Decimal("50200")
        # SL = entry - 4.0 * ATR = 50000 - 400 = 49600
        assert signal.sl_price == Decimal("49600")

    def test_tp_sl_calculation_short(self):
        """Verify TP/SL for SHORT signal."""
        strategy = EmaCrossoverStrategy()
        kline1 = _make_kline(minute=0, close=Decimal("50000"))
        kline2 = _make_kline(minute=1, close=Decimal("50000"))

        strategy.detect_crossover(
            kline1,
            fast_ema_value=Decimal("50100"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("100"),
        )

        signal = strategy.detect_crossover(
            kline2,
            fast_ema_value=Decimal("49900"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("100"),
        )

        assert signal is not None
        # TP = entry - 2.0 * ATR = 50000 - 200 = 49800
        assert signal.tp_price == Decimal("49800")
        # SL = entry + 4.0 * ATR = 50000 + 400 = 50400
        assert signal.sl_price == Decimal("50400")

    def test_position_lock_blocks_second_signal(self):
        """After a signal, same symbol/timeframe cannot generate another."""
        strategy = EmaCrossoverStrategy()
        kline1 = _make_kline(minute=0)
        kline2 = _make_kline(minute=1)
        kline3 = _make_kline(minute=2)

        # Setup: fast below slow
        strategy.detect_crossover(
            kline1,
            fast_ema_value=Decimal("49900"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )

        # Bullish crossover -> signal generated
        signal1 = strategy.detect_crossover(
            kline2,
            fast_ema_value=Decimal("50100"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )
        assert signal1 is not None

        # Simulate position being active
        strategy._active_positions["BTCUSDT_5m"] = True

        # Another crossover attempt -> blocked by position lock
        signal2 = strategy.detect_crossover(
            kline3,
            fast_ema_value=Decimal("49800"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )
        assert signal2 is None

    def test_position_lock_released_after_outcome(self):
        """After recording outcome, new signals are allowed."""
        strategy = EmaCrossoverStrategy()
        strategy._active_positions["BTCUSDT_5m"] = True

        # Release position
        strategy.release_position("BTCUSDT", "5m")
        assert "BTCUSDT_5m" not in strategy._active_positions

    def test_crossover_at_equality(self):
        """When prev fast == prev slow, crossing above still triggers LONG."""
        strategy = EmaCrossoverStrategy()
        kline1 = _make_kline(minute=0)
        kline2 = _make_kline(minute=1)

        # Equal EMAs
        strategy.detect_crossover(
            kline1,
            fast_ema_value=Decimal("50000"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )

        # Fast goes above -> LONG
        signal = strategy.detect_crossover(
            kline2,
            fast_ema_value=Decimal("50100"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("200"),
        )
        assert signal is not None
        assert signal.direction == Direction.LONG


# ---------------------------------------------------------------------------
# process_kline integration tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestProcessKline:
    """Integration tests for process_kline."""

    async def test_non_closed_kline_returns_early(self):
        strategy = EmaCrossoverStrategy()
        buffer = _make_buffer()
        kline = Kline(
            symbol="BTCUSDT", timeframe="5m",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            open=Decimal("50000"), high=Decimal("50050"),
            low=Decimal("49950"), close=Decimal("50005"),
            volume=Decimal("100"), is_closed=False,
        )
        result = await strategy.process_kline(kline, buffer)
        assert result.signal is None
        assert result.atr is None

    async def test_insufficient_buffer_returns_early(self):
        strategy = EmaCrossoverStrategy()
        buffer = _make_buffer(n=10)
        kline = buffer.klines[-1]
        result = await strategy.process_kline(kline, buffer)
        assert result.signal is None
        assert result.atr is None

    async def test_process_kline_returns_atr(self):
        """process_kline always returns ATR when enough data."""
        strategy = EmaCrossoverStrategy()
        buffer = _make_buffer(n=60)
        kline = buffer.klines[-1]
        result = await strategy.process_kline(kline, buffer)
        assert result.atr is not None
        assert result.atr > 0

    async def test_signal_saved_via_callback(self):
        """When signal is generated, save_signal callback is called."""
        save_signal = AsyncMock()
        strategy = EmaCrossoverStrategy(save_signal=save_signal)
        buffer = _make_buffer(n=60)
        kline = buffer.klines[-1]

        # Force a crossover by mocking detect_crossover
        fake_signal = SignalRecord(
            strategy="ema_crossover",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50200"),
            sl_price=Decimal("49600"),
        )

        with patch.object(strategy, "detect_crossover", return_value=fake_signal):
            result = await strategy.process_kline(kline, buffer)

        assert result.signal is fake_signal
        save_signal.assert_called_once_with(fake_signal)

    async def test_callback_notified_on_signal(self):
        """on_signal callbacks fire when signal is generated."""
        callback = AsyncMock()
        save_signal = AsyncMock()
        strategy = EmaCrossoverStrategy(save_signal=save_signal)
        strategy.on_signal(callback)

        buffer = _make_buffer(n=60)
        kline = buffer.klines[-1]

        fake_signal = SignalRecord(
            strategy="ema_crossover",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50200"),
            sl_price=Decimal("49600"),
        )

        with patch.object(strategy, "detect_crossover", return_value=fake_signal):
            await strategy.process_kline(kline, buffer)

        callback.assert_called_once_with(fake_signal)

    async def test_save_failure_discards_signal(self):
        """When save_signal raises, signal is discarded."""
        save_signal = AsyncMock(side_effect=RuntimeError("DB down"))
        strategy = EmaCrossoverStrategy(save_signal=save_signal)

        buffer = _make_buffer(n=60)
        kline = buffer.klines[-1]

        fake_signal = SignalRecord(
            strategy="ema_crossover",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50200"),
            sl_price=Decimal("49600"),
        )

        with patch.object(strategy, "detect_crossover", return_value=fake_signal):
            result = await strategy.process_kline(kline, buffer)

        assert result.signal is None
        assert strategy._active_positions.get("BTCUSDT_5m", False) is False


# ---------------------------------------------------------------------------
# Outcome recording tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestRecordOutcome:
    """Test outcome recording and streak tracking."""

    async def test_record_outcome_updates_streak(self):
        strategy = EmaCrossoverStrategy()

        await strategy.record_outcome(Outcome.TP, "BTCUSDT", "5m")
        streak = strategy._get_streak("BTCUSDT", "5m")
        assert streak.current_streak == 1
        assert streak.total_wins == 1

        await strategy.record_outcome(Outcome.SL, "BTCUSDT", "5m")
        assert streak.current_streak == -1
        assert streak.total_losses == 1

    async def test_record_outcome_releases_position_lock(self):
        strategy = EmaCrossoverStrategy()
        strategy._active_positions["BTCUSDT_5m"] = True

        await strategy.record_outcome(Outcome.TP, "BTCUSDT", "5m")
        assert "BTCUSDT_5m" not in strategy._active_positions

    async def test_record_outcome_calls_save_streak(self):
        save_streak = AsyncMock()
        strategy = EmaCrossoverStrategy(save_streak=save_streak)

        await strategy.record_outcome(Outcome.TP, "BTCUSDT", "5m")
        save_streak.assert_called_once()


# ---------------------------------------------------------------------------
# Custom config tests
# ---------------------------------------------------------------------------

class TestCustomConfig:
    """Test with non-default configuration."""

    def test_custom_periods_affect_indicators(self):
        config = EmaCrossoverConfig(fast_period=10, slow_period=30)
        strategy = EmaCrossoverStrategy(config=config)
        assert strategy.fast_period == 10
        assert strategy.slow_period == 30
        assert "ema10" in strategy.required_indicators
        assert "ema30" in strategy.required_indicators

    def test_custom_tp_sl_multipliers(self):
        config = EmaCrossoverConfig(
            tp_atr_mult=Decimal("3.0"),
            sl_atr_mult=Decimal("6.0"),
        )
        strategy = EmaCrossoverStrategy(config=config)
        kline1 = _make_kline(minute=0, close=Decimal("50000"))
        kline2 = _make_kline(minute=1, close=Decimal("50000"))

        strategy.detect_crossover(
            kline1,
            fast_ema_value=Decimal("49900"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("100"),
        )

        signal = strategy.detect_crossover(
            kline2,
            fast_ema_value=Decimal("50100"),
            slow_ema_value=Decimal("50000"),
            atr_value=Decimal("100"),
        )

        assert signal is not None
        # TP = 50000 + 3.0 * 100 = 50300
        assert signal.tp_price == Decimal("50300")
        # SL = 50000 - 6.0 * 100 = 49400
        assert signal.sl_price == Decimal("49400")
