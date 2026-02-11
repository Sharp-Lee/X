"""Tests for signal quality filtering (streak + ATR percentile)."""

import asyncio
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from core.atr_tracker import AtrPercentileTracker
from core.models.config import SignalFilterConfig, PORTFOLIO_B
from core.models.signal import Direction, Outcome, SignalRecord
from core.models.kline import Kline, KlineBuffer
from core.signal_generator import SignalGenerator, ProcessKlineResult
from core.models.config import StrategyConfig


# ---------------------------------------------------------------------------
# AtrPercentileTracker
# ---------------------------------------------------------------------------

class TestAtrPercentileTracker:
    """Tests for AtrPercentileTracker."""

    def test_returns_none_before_min_samples(self):
        tracker = AtrPercentileTracker(min_samples=10)
        for i in range(1, 10):  # 9 values (skip 0 since it's invalid)
            tracker.update("BTC", "5m", float(i))
        assert tracker.get_percentile("BTC", "5m", 5.0) is None

    def test_returns_value_at_min_samples(self):
        tracker = AtrPercentileTracker(min_samples=10)
        for i in range(1, 11):
            tracker.update("BTC", "5m", float(i))
        pct = tracker.get_percentile("BTC", "5m", 5.0)
        assert pct is not None
        assert 0.0 < pct < 1.0

    def test_percentile_semantics(self):
        """Percentile = fraction of values <= current (empirical CDF)."""
        tracker = AtrPercentileTracker(min_samples=5)
        for i in range(1, 11):
            tracker.update("X", "1m", float(i))

        assert tracker.get_percentile("X", "1m", 5.0) == pytest.approx(0.5)
        assert tracker.get_percentile("X", "1m", 10.0) == pytest.approx(1.0)
        assert tracker.get_percentile("X", "1m", 1.0) == pytest.approx(0.1)

    def test_percentile_with_duplicates(self):
        tracker = AtrPercentileTracker(min_samples=5)
        for _ in range(10):
            tracker.update("X", "1m", 100.0)
        assert tracker.get_percentile("X", "1m", 100.0) == pytest.approx(1.0)
        assert tracker.get_percentile("X", "1m", 50.0) == pytest.approx(0.0)

    def test_independent_per_symbol_timeframe(self):
        tracker = AtrPercentileTracker(min_samples=5)
        for i in range(1, 11):
            tracker.update("BTC", "5m", float(i))
        assert tracker.get_percentile("ETH", "5m", 5.0) is None
        assert tracker.get_percentile("BTC", "15m", 5.0) is None

    def test_is_ready(self):
        tracker = AtrPercentileTracker(min_samples=10)
        assert not tracker.is_ready("BTC", "5m")
        for i in range(1, 11):
            tracker.update("BTC", "5m", float(i))
        assert tracker.is_ready("BTC", "5m")

    def test_get_count(self):
        tracker = AtrPercentileTracker(min_samples=5)
        assert tracker.get_count("BTC", "5m") == 0
        tracker.update("BTC", "5m", 1.0)
        tracker.update("BTC", "5m", 2.0)
        assert tracker.get_count("BTC", "5m") == 2

    def test_bulk_load(self):
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTC", "5m", [1.0, 2.0, 3.0, 4.0, 5.0])
        assert tracker.get_count("BTC", "5m") == 5
        assert tracker.is_ready("BTC", "5m")

    def test_bulk_load_then_update(self):
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTC", "5m", [1.0, 2.0, 3.0, 4.0, 5.0])
        tracker.update("BTC", "5m", 6.0)
        assert tracker.get_count("BTC", "5m") == 6
        assert tracker.get_percentile("BTC", "5m", 6.0) == pytest.approx(1.0)

    # --- New tests from review ---

    def test_nan_silently_skipped(self):
        """NaN values must not enter the tracker."""
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTC", "5m", [1.0, 2.0, 3.0, 4.0, 5.0])
        tracker.update("BTC", "5m", float("nan"))
        assert tracker.get_count("BTC", "5m") == 5  # NaN not counted

    def test_negative_silently_skipped(self):
        """Negative values must not enter the tracker."""
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTC", "5m", [1.0, 2.0, 3.0, 4.0, 5.0])
        tracker.update("BTC", "5m", -1.0)
        assert tracker.get_count("BTC", "5m") == 5

    def test_zero_silently_skipped(self):
        """Zero ATR must not enter the tracker (ATR is always positive)."""
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.update("BTC", "5m", 0.0)
        assert tracker.get_count("BTC", "5m") == 0

    def test_inf_silently_skipped(self):
        """Infinite values must not enter the tracker."""
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTC", "5m", [1.0, 2.0, 3.0, 4.0, 5.0])
        tracker.update("BTC", "5m", float("inf"))
        assert tracker.get_count("BTC", "5m") == 5

    def test_bulk_load_filters_invalid(self):
        """bulk_load should filter out NaN, negative, zero, inf."""
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTC", "5m", [
            1.0, float("nan"), -5.0, 0.0, 2.0, float("inf"), 3.0, 4.0, 5.0
        ])
        assert tracker.get_count("BTC", "5m") == 5  # only 1,2,3,4,5

    def test_max_history_cap(self):
        """History should not exceed max_history."""
        tracker = AtrPercentileTracker(min_samples=5, max_history=100)
        for i in range(1, 201):
            tracker.update("BTC", "5m", float(i))
        assert tracker.get_count("BTC", "5m") == 100
        # Most recent 100 values: 101..200
        # So value 150 should be at percentile 0.5 (50 out of 100 are <= 150)
        assert tracker.get_percentile("BTC", "5m", 150.0) == pytest.approx(0.5)

    def test_bulk_load_respects_max_history(self):
        """bulk_load with more than max_history should keep only the most recent."""
        tracker = AtrPercentileTracker(min_samples=5, max_history=50)
        tracker.bulk_load("BTC", "5m", [float(i) for i in range(1, 201)])
        assert tracker.get_count("BTC", "5m") == 50


# ---------------------------------------------------------------------------
# SignalFilterConfig
# ---------------------------------------------------------------------------

class TestSignalFilterConfig:
    """Tests for SignalFilterConfig model."""

    def test_key_property(self):
        fc = SignalFilterConfig(symbol="BTCUSDT", timeframe="15m")
        assert fc.key == "BTCUSDT_15m"

    def test_portfolio_b_has_5_strategies(self):
        assert len(PORTFOLIO_B) == 5

    def test_portfolio_b_keys_unique(self):
        keys = [f.key for f in PORTFOLIO_B]
        assert len(keys) == len(set(keys))

    def test_portfolio_b_all_enabled(self):
        for f in PORTFOLIO_B:
            assert f.enabled is True

    def test_portfolio_b_valid_ranges(self):
        """All configs must have streak_lo <= streak_hi and valid thresholds."""
        for f in PORTFOLIO_B:
            assert f.streak_lo <= f.streak_hi
            assert 0.0 <= f.atr_pct_threshold < 1.0
            assert f.position_qty > 0


# ---------------------------------------------------------------------------
# SignalGenerator._passes_filter (unit tests)
# ---------------------------------------------------------------------------

def _make_signal(symbol="BTCUSDT", timeframe="5m", streak=0):
    """Helper to create a minimal SignalRecord for filter testing."""
    return SignalRecord(
        symbol=symbol,
        timeframe=timeframe,
        signal_time=datetime.now(timezone.utc),
        direction=Direction.LONG,
        entry_price=Decimal("50000"),
        tp_price=Decimal("50500"),
        sl_price=Decimal("49000"),
        streak_at_signal=streak,
    )


class TestPassesFilter:
    """Tests for SignalGenerator._passes_filter."""

    def _make_generator(self, filters=None, atr_tracker=None):
        return SignalGenerator(
            config=StrategyConfig(),
            filters=filters,
            atr_tracker=atr_tracker,
        )

    def test_no_filters_all_pass(self):
        gen = self._make_generator(filters=None)
        assert gen._passes_filter(_make_signal(), 100.0) is True

    def test_symbol_not_in_filters_rejected(self):
        filters = {
            "XRPUSDT_30m": SignalFilterConfig(
                symbol="XRPUSDT", timeframe="30m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_generator(filters=filters)
        assert gen._passes_filter(_make_signal(symbol="BTCUSDT", timeframe="5m"), 100.0) is False

    def test_disabled_filter_rejected(self):
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m", enabled=False,
            )
        }
        gen = self._make_generator(filters=filters)
        assert gen._passes_filter(_make_signal(), 100.0) is False

    def test_streak_in_range_passes(self):
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_generator(filters=filters)
        assert gen._passes_filter(_make_signal(streak=0), 100.0) is True
        assert gen._passes_filter(_make_signal(streak=3), 100.0) is True
        assert gen._passes_filter(_make_signal(streak=2), 100.0) is True

    def test_streak_out_of_range_rejected(self):
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_generator(filters=filters)
        assert gen._passes_filter(_make_signal(streak=-1), 100.0) is False
        assert gen._passes_filter(_make_signal(streak=4), 100.0) is False
        assert gen._passes_filter(_make_signal(streak=10), 100.0) is False

    def test_atr_pct_above_threshold_passes(self):
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTCUSDT", "5m", [float(i) for i in range(1, 11)])
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=-999, streak_hi=999, atr_pct_threshold=0.80,
            )
        }
        gen = self._make_generator(filters=filters, atr_tracker=tracker)
        assert gen._passes_filter(_make_signal(), 9.0) is True

    def test_atr_pct_at_threshold_rejected(self):
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTCUSDT", "5m", [float(i) for i in range(1, 11)])
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=-999, streak_hi=999, atr_pct_threshold=0.90,
            )
        }
        gen = self._make_generator(filters=filters, atr_tracker=tracker)
        assert gen._passes_filter(_make_signal(), 9.0) is False

    def test_atr_pct_below_threshold_rejected(self):
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTCUSDT", "5m", [float(i) for i in range(1, 11)])
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=-999, streak_hi=999, atr_pct_threshold=0.80,
            )
        }
        gen = self._make_generator(filters=filters, atr_tracker=tracker)
        assert gen._passes_filter(_make_signal(), 5.0) is False

    def test_atr_insufficient_data_rejected(self):
        tracker = AtrPercentileTracker(min_samples=200)
        tracker.bulk_load("BTCUSDT", "5m", [float(i) for i in range(1, 11)])
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=-999, streak_hi=999, atr_pct_threshold=0.50,
            )
        }
        gen = self._make_generator(filters=filters, atr_tracker=tracker)
        assert gen._passes_filter(_make_signal(), 5.0) is False

    def test_atr_threshold_zero_skips_check(self):
        """When atr_pct_threshold=0.0, ATR check is skipped entirely."""
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_generator(filters=filters, atr_tracker=None)
        assert gen._passes_filter(_make_signal(streak=1), 100.0) is True

    def test_combined_streak_and_atr(self):
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTCUSDT", "5m", [float(i) for i in range(1, 11)])
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.60,
            )
        }
        gen = self._make_generator(filters=filters, atr_tracker=tracker)
        assert gen._passes_filter(_make_signal(streak=2), 8.0) is True
        assert gen._passes_filter(_make_signal(streak=5), 8.0) is False
        assert gen._passes_filter(_make_signal(streak=2), 3.0) is False

    def test_portfolio_b_realistic(self):
        tracker = AtrPercentileTracker(min_samples=5)
        tracker.bulk_load("BTCUSDT", "5m", [float(i) for i in range(1, 101)])
        btc_5m = next(f for f in PORTFOLIO_B if f.key == "BTCUSDT_5m")
        filters = {btc_5m.key: btc_5m}
        gen = self._make_generator(filters=filters, atr_tracker=tracker)
        assert gen._passes_filter(_make_signal(streak=0), 95.0) is True
        assert gen._passes_filter(_make_signal(streak=0), 10.0) is False
        assert gen._passes_filter(_make_signal(streak=5), 95.0) is False

    # --- New tests from review ---

    def test_atr_threshold_with_no_tracker_rejects(self):
        """When threshold > 0 but no tracker injected, reject for safety."""
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.90,
            )
        }
        gen = self._make_generator(filters=filters, atr_tracker=None)
        assert gen._passes_filter(_make_signal(streak=1), 100.0) is False

    def test_inverted_streak_range_rejects_all(self):
        """streak_lo > streak_hi should reject every signal."""
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=5, streak_hi=2, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_generator(filters=filters)
        for s in range(-5, 10):
            assert gen._passes_filter(_make_signal(streak=s), 100.0) is False


# ---------------------------------------------------------------------------
# Integration: process_kline with filters (async tests)
#
# These tests mock detect_signal() to deterministically return a known
# SignalRecord, so filter acceptance/rejection is exercised reliably.
# ---------------------------------------------------------------------------

def _make_kline_buffer(symbol="BTCUSDT", timeframe="5m", n=60):
    """Create a KlineBuffer with n klines for integration testing."""
    buffer = KlineBuffer(symbol=symbol, timeframe=timeframe)
    for i in range(n):
        price = Decimal("50000") + Decimal(str(i * 10))
        kline = Kline(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=datetime(2024, 1, 1, i // 60, i % 60, tzinfo=timezone.utc),
            open=price,
            high=price + Decimal("50"),
            low=price - Decimal("50"),
            close=price + Decimal("5"),
            volume=Decimal("100"),
            is_closed=True,
        )
        buffer.add(kline)
    return buffer


# Fake indicator dict that process_kline expects from calculate_latest.
# ATR=Decimal("100") so atr_value=100.0 after float conversion.
_FAKE_INDICATORS = {
    "ema50": Decimal("50000"),
    "atr": Decimal("100"),
    "fib_382": Decimal("49000"),
    "fib_500": Decimal("48500"),
    "fib_618": Decimal("48000"),
    "vwap": Decimal("50000"),
}


@pytest.mark.asyncio
class TestProcessKlineWithFilter:
    """Integration tests: process_kline() with signal filters enabled.

    All tests mock detect_signal() to return a known SignalRecord so the
    filter-acceptance and filter-rejection paths are exercised deterministically.
    """

    def _make_gen(self, *, filters=None, atr_tracker=None, save_signal=None):
        """Create a SignalGenerator with mocked indicator calculator."""
        gen = SignalGenerator(
            config=StrategyConfig(),
            save_signal=save_signal,
            filters=filters,
            atr_tracker=atr_tracker,
        )
        # Mock indicator calculator to always return valid indicators
        gen.indicator_calc.calculate_latest = lambda *a, **kw: _FAKE_INDICATORS
        return gen

    async def test_filtered_signal_not_saved(self):
        """When filter rejects a signal, save_signal must NOT be called."""
        save_signal = AsyncMock()
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters, save_signal=save_signal)

        # Signal with streak=5 (outside 0~3 range) -> filtered out
        fake_signal = _make_signal(streak=5)

        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=fake_signal):
            result = await gen.process_kline(kline, buffer)

        save_signal.assert_not_called()
        assert result.signal is None
        assert result.atr == pytest.approx(100.0)

    async def test_accepted_signal_is_saved(self):
        """When filter accepts a signal, save_signal IS called."""
        save_signal = AsyncMock()
        tracker = AtrPercentileTracker(min_samples=5)
        # Load values 1..200; ATR=100 -> percentile = 100/200 = 0.50
        tracker.bulk_load("BTCUSDT", "5m", [float(i) for i in range(1, 201)])
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=5, atr_pct_threshold=0.40,
            )
        }
        gen = self._make_gen(filters=filters, atr_tracker=tracker, save_signal=save_signal)

        fake_signal = _make_signal(streak=2)
        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=fake_signal):
            result = await gen.process_kline(kline, buffer)

        save_signal.assert_called_once_with(fake_signal)
        assert result.signal is fake_signal
        assert result.atr == pytest.approx(100.0)

    async def test_filtered_signal_does_not_set_position_lock(self):
        """Filtered signals must not consume the position slot."""
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters)

        # streak=5 -> outside range -> rejected
        fake_signal = _make_signal(streak=5)
        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=fake_signal):
            result = await gen.process_kline(kline, buffer)

        assert result.signal is None
        assert gen._active_positions.get("BTCUSDT_5m", False) is False

    async def test_accepted_signal_sets_position_lock(self):
        """Accepted signals must set the position lock."""
        save_signal = AsyncMock()
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=5, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters, save_signal=save_signal)

        fake_signal = _make_signal(streak=2)
        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=fake_signal):
            result = await gen.process_kline(kline, buffer)

        assert result.signal is fake_signal
        assert gen._active_positions.get("BTCUSDT_5m") is True

    async def test_filtered_signal_does_not_trigger_callbacks(self):
        """on_signal callbacks must not fire for filtered-out signals."""
        callback = AsyncMock()
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters)
        gen.on_signal(callback)

        fake_signal = _make_signal(streak=5)
        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=fake_signal):
            result = await gen.process_kline(kline, buffer)

        assert result.signal is None
        callback.assert_not_called()

    async def test_accepted_signal_triggers_callbacks(self):
        """on_signal callbacks fire for accepted signals."""
        callback = AsyncMock()
        save_signal = AsyncMock()
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=5, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters, save_signal=save_signal)
        gen.on_signal(callback)

        fake_signal = _make_signal(streak=2)
        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=fake_signal):
            result = await gen.process_kline(kline, buffer)

        assert result.signal is fake_signal
        callback.assert_called_once_with(fake_signal)

    async def test_atr_tracker_updated_on_every_kline(self):
        """ATR tracker must grow with every closed kline, not just signals."""
        tracker = AtrPercentileTracker(min_samples=5)
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=0, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters, atr_tracker=tracker)

        buffer = _make_kline_buffer(n=60)
        kline = buffer.klines[-1]

        count_before = tracker.get_count("BTCUSDT", "5m")

        # detect_signal returns None — no signal, but ATR should still be tracked
        with patch.object(gen, "detect_signal", return_value=None):
            result = await gen.process_kline(kline, buffer)

        assert tracker.get_count("BTCUSDT", "5m") == count_before + 1
        assert result.signal is None
        assert result.atr == pytest.approx(100.0)

    async def test_atr_tracker_updated_even_when_signal_filtered(self):
        """ATR update happens BEFORE filter check, so it always runs."""
        tracker = AtrPercentileTracker(min_samples=5)
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=0, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters, atr_tracker=tracker)

        buffer = _make_kline_buffer(n=60)
        kline = buffer.klines[-1]
        count_before = tracker.get_count("BTCUSDT", "5m")

        # Signal with streak=5 -> filtered, but ATR must still be tracked
        fake_signal = _make_signal(streak=5)
        with patch.object(gen, "detect_signal", return_value=fake_signal):
            await gen.process_kline(kline, buffer)

        assert tracker.get_count("BTCUSDT", "5m") == count_before + 1

    async def test_no_filters_passes_all_signals(self):
        """With filters=None, all detected signals pass through unfiltered."""
        save_signal = AsyncMock()
        callback = AsyncMock()
        gen = self._make_gen(filters=None, save_signal=save_signal)
        gen.on_signal(callback)

        fake_signal = _make_signal(streak=99)
        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=fake_signal):
            result = await gen.process_kline(kline, buffer)

        assert result.signal is fake_signal
        save_signal.assert_called_once_with(fake_signal)
        callback.assert_called_once_with(fake_signal)

    async def test_no_detect_signal_returns_none_without_calling_filter(self):
        """When detect_signal returns None, _passes_filter must not be called."""
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=3, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters)

        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=None), \
             patch.object(gen, "_passes_filter") as mock_filter:
            result = await gen.process_kline(kline, buffer)

        mock_filter.assert_not_called()
        assert result.signal is None

    async def test_save_signal_exception_returns_none(self):
        """When save_signal raises, the signal is discarded safely."""
        save_signal = AsyncMock(side_effect=RuntimeError("DB down"))
        callback = AsyncMock()
        filters = {
            "BTCUSDT_5m": SignalFilterConfig(
                symbol="BTCUSDT", timeframe="5m",
                streak_lo=0, streak_hi=5, atr_pct_threshold=0.0,
            )
        }
        gen = self._make_gen(filters=filters, save_signal=save_signal)
        gen.on_signal(callback)

        fake_signal = _make_signal(streak=2)
        buffer = _make_kline_buffer()
        kline = buffer.klines[-1]

        with patch.object(gen, "detect_signal", return_value=fake_signal):
            result = await gen.process_kline(kline, buffer)

        # Signal passes filter but save fails -> no position lock, no callbacks
        assert result.signal is None
        assert gen._active_positions.get("BTCUSDT_5m", False) is False
        callback.assert_not_called()

    async def test_non_closed_kline_returns_early(self):
        """Non-closed klines skip all processing."""
        gen = self._make_gen()
        buffer = _make_kline_buffer()
        kline = Kline(
            symbol="BTCUSDT", timeframe="5m",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            open=Decimal("50000"), high=Decimal("50050"),
            low=Decimal("49950"), close=Decimal("50005"),
            volume=Decimal("100"), is_closed=False,
        )
        result = await gen.process_kline(kline, buffer)
        assert result.signal is None
        assert result.atr is None

    async def test_insufficient_buffer_returns_early(self):
        """Buffer with fewer than 50 klines returns early."""
        gen = self._make_gen()
        buffer = _make_kline_buffer(n=10)  # only 10 klines
        kline = buffer.klines[-1]
        result = await gen.process_kline(kline, buffer)
        assert result.signal is None
        assert result.atr is None
