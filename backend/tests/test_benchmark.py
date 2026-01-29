"""Tests for performance benchmark script.

Verifies that benchmark functions work correctly and performance
meets minimum thresholds.
"""

import pytest
import time
from datetime import datetime, timezone
from decimal import Decimal

from app.models import (
    AggTrade,
    Direction,
    Kline,
    Outcome,
    SignalRecord,
    FastKline,
    FastSignal,
    FastTrade,
)
from app.models.converters import signal_to_fast, aggtrade_to_fast


class TestPerformanceThresholds:
    """Test that performance meets minimum thresholds."""

    def test_fast_signal_creation_faster_than_pydantic(self):
        """FastSignal creation should be faster than SignalRecord."""
        now = datetime.now(timezone.utc)
        iterations = 1000

        # Time Pydantic creation
        start = time.perf_counter()
        for _ in range(iterations):
            SignalRecord(
                symbol="BTCUSDT",
                timeframe="1m",
                signal_time=now,
                direction=Direction.LONG,
                entry_price=Decimal("50000"),
                tp_price=Decimal("50500"),
                sl_price=Decimal("49500"),
                streak_at_signal=0,
            )
        pydantic_time = time.perf_counter() - start

        # Time dataclass creation
        start = time.perf_counter()
        for _ in range(iterations):
            FastSignal(
                id="test",
                symbol="BTCUSDT",
                timeframe="1m",
                signal_time=now.timestamp(),
                direction=1,
                entry_price=50000.0,
                tp_price=50500.0,
                sl_price=49500.0,
                streak_at_signal=0,
            )
        fast_time = time.perf_counter() - start

        # FastSignal should be at least 2x faster
        speedup = pydantic_time / fast_time
        assert speedup >= 2.0, f"Expected at least 2x speedup, got {speedup:.1f}x"

    def test_fast_trade_creation_faster_than_pydantic(self):
        """FastTrade creation should be faster than AggTrade."""
        now = datetime.now(timezone.utc)
        iterations = 1000

        # Time Pydantic creation
        start = time.perf_counter()
        for _ in range(iterations):
            AggTrade(
                symbol="BTCUSDT",
                agg_trade_id=123456,
                price=Decimal("50000"),
                quantity=Decimal("1.5"),
                timestamp=now,
                is_buyer_maker=True,
            )
        pydantic_time = time.perf_counter() - start

        # Time dataclass creation
        start = time.perf_counter()
        for _ in range(iterations):
            FastTrade(
                symbol="BTCUSDT",
                agg_trade_id=123456,
                price=50000.0,
                quantity=1.5,
                timestamp=now.timestamp(),
                is_buyer_maker=True,
            )
        fast_time = time.perf_counter() - start

        # FastTrade should be at least 2x faster
        speedup = pydantic_time / fast_time
        assert speedup >= 2.0, f"Expected at least 2x speedup, got {speedup:.1f}x"

    def test_mae_update_hot_path_faster(self):
        """Hot path MAE update should be faster than cold path."""
        now = datetime.now(timezone.utc)
        iterations = 1000

        cold_signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=now,
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49500"),
            streak_at_signal=0,
        )

        hot_signal = signal_to_fast(cold_signal)

        test_prices_decimal = [Decimal(str(p)) for p in range(49800, 50200, 50)]
        test_prices_float = [float(p) for p in range(49800, 50200, 50)]

        # Time cold path
        start = time.perf_counter()
        for _ in range(iterations):
            for price in test_prices_decimal:
                cold_signal.update_mae(price)
        cold_time = time.perf_counter() - start

        # Time hot path
        start = time.perf_counter()
        for _ in range(iterations):
            for price in test_prices_float:
                hot_signal.update_mae(price)
        hot_time = time.perf_counter() - start

        # Hot path should be at least 1.5x faster
        speedup = cold_time / hot_time
        assert speedup >= 1.5, f"Expected at least 1.5x speedup, got {speedup:.1f}x"

    def test_conversion_overhead_acceptable(self):
        """Model conversion overhead should be reasonable."""
        now = datetime.now(timezone.utc)
        iterations = 1000

        cold_signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=now,
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49500"),
            streak_at_signal=0,
        )

        # Conversion should be fast enough that it doesn't negate hot path benefits
        start = time.perf_counter()
        for _ in range(iterations):
            signal_to_fast(cold_signal)
        conversion_time = time.perf_counter() - start

        # Conversion time per operation
        time_per_conversion_us = (conversion_time / iterations) * 1_000_000

        # Should be under 10μs per conversion
        assert time_per_conversion_us < 10, f"Conversion too slow: {time_per_conversion_us:.1f}μs"


class TestOrjsonPerformance:
    """Test orjson performance."""

    def test_orjson_faster_than_json(self):
        """orjson should be faster than standard json."""
        try:
            import orjson
        except ImportError:
            pytest.skip("orjson not installed")

        import json
        from dataclasses import asdict

        hot_signal = FastSignal(
            id="test",
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=1704067200.0,
            direction=1,
            entry_price=50000.0,
            tp_price=50500.0,
            sl_price=49500.0,
            streak_at_signal=0,
        )

        data = asdict(hot_signal)
        iterations = 1000

        # Time standard json
        start = time.perf_counter()
        for _ in range(iterations):
            json.dumps(data)
        json_time = time.perf_counter() - start

        # Time orjson
        start = time.perf_counter()
        for _ in range(iterations):
            orjson.dumps(data)
        orjson_time = time.perf_counter() - start

        # orjson should be at least 2x faster
        speedup = json_time / orjson_time
        assert speedup >= 2.0, f"Expected at least 2x speedup, got {speedup:.1f}x"
