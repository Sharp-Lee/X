"""Tests for technical indicators."""

import pytest
from decimal import Decimal

from core.indicators import (
    ema,
    sma,
    atr,
    highest,
    lowest,
    fibonacci_levels,
    IndicatorCalculator,
)


class TestEMA:
    """Tests for EMA calculation."""

    def test_ema_basic(self):
        """Test basic EMA calculation."""
        values = [Decimal(str(i)) for i in range(1, 11)]  # 1-10
        result = ema(values, 5)

        # First 4 values should be NaN
        assert str(result[0]) == "NaN"
        assert str(result[3]) == "NaN"

        # 5th value should be SMA of first 5 = (1+2+3+4+5)/5 = 3
        assert result[4] == Decimal("3")

        # Subsequent values should be EMA
        assert result[5] > result[4]  # EMA should increase

    def test_ema_insufficient_data(self):
        """Test EMA with insufficient data."""
        values = [Decimal("100"), Decimal("101"), Decimal("102")]
        result = ema(values, 10)

        assert len(result) == 3
        assert all(str(v) == "NaN" for v in result)


class TestSMA:
    """Tests for SMA calculation."""

    def test_sma_basic(self):
        """Test basic SMA calculation."""
        values = [Decimal(str(i)) for i in range(1, 11)]  # 1-10
        result = sma(values, 3)

        # First 2 values should be NaN
        assert str(result[0]) == "NaN"
        assert str(result[1]) == "NaN"

        # 3rd value should be (1+2+3)/3 = 2
        assert result[2] == Decimal("2")

        # 4th value should be (2+3+4)/3 = 3
        assert result[3] == Decimal("3")


class TestATR:
    """Tests for ATR calculation."""

    def test_atr_constant_range(self):
        """Test ATR with constant range candles."""
        # All candles have range of 2 (high - low)
        highs = [Decimal("102")] * 20
        lows = [Decimal("100")] * 20
        closes = [Decimal("101")] * 20

        result = atr(highs, lows, closes, 9)

        # With constant range, ATR should approach 2
        assert abs(float(result[-1]) - 2.0) < 0.01

    def test_atr_insufficient_data(self):
        """Test ATR with insufficient data."""
        highs = [Decimal("102")] * 5
        lows = [Decimal("100")] * 5
        closes = [Decimal("101")] * 5

        result = atr(highs, lows, closes, 9)

        assert len(result) == 5
        assert all(str(v) == "NaN" for v in result)


class TestHighestLowest:
    """Tests for highest/lowest calculations."""

    def test_highest_basic(self):
        """Test highest value over period."""
        values = [Decimal(str(i)) for i in [1, 3, 2, 5, 4, 6, 3, 8, 7]]
        result = highest(values, 3)

        # First 2 should be NaN
        assert str(result[0]) == "NaN"
        assert str(result[1]) == "NaN"

        # highest([1,3,2]) = 3
        assert result[2] == Decimal("3")

        # highest([3,2,5]) = 5
        assert result[3] == Decimal("5")

        # highest([2,5,4]) = 5
        assert result[4] == Decimal("5")

    def test_lowest_basic(self):
        """Test lowest value over period."""
        values = [Decimal(str(i)) for i in [5, 3, 4, 1, 6, 2, 7, 3, 8]]
        result = lowest(values, 3)

        # lowest([5,3,4]) = 3
        assert result[2] == Decimal("3")

        # lowest([3,4,1]) = 1
        assert result[3] == Decimal("1")


class TestFibonacciLevels:
    """Tests for Fibonacci level calculations."""

    def test_fibonacci_levels_basic(self):
        """Test Fibonacci level calculations."""
        # Simple case: high=110, low=100, range=10
        highs = [Decimal("110")] * 15
        lows = [Decimal("100")] * 15

        fib_382, fib_500, fib_618 = fibonacci_levels(highs, lows, 9)

        # fib_382 = 110 - 10 * 0.382 = 106.18
        assert abs(float(fib_382[-1]) - 106.18) < 0.01

        # fib_500 = 110 - 10 * 0.5 = 105
        assert fib_500[-1] == Decimal("105")

        # fib_618 = 110 - 10 * 0.618 = 103.82
        assert abs(float(fib_618[-1]) - 103.82) < 0.01


class TestIndicatorCalculator:
    """Tests for IndicatorCalculator class."""

    def test_calculate_latest(self):
        """Test calculating indicators for latest bar."""
        # Generate enough test data
        n = 60
        opens = [Decimal(str(100 + i * 0.1)) for i in range(n)]
        closes = [Decimal(str(100 + i * 0.1 + 0.05)) for i in range(n)]
        highs = [c + Decimal("0.5") for c in closes]
        lows = [c - Decimal("0.5") for c in closes]
        volumes = [Decimal("1000")] * n

        calc = IndicatorCalculator(ema_period=50, fib_period=9, atr_period=9)
        result = calc.calculate_latest(opens, highs, lows, closes, volumes)

        assert result is not None
        assert "ema50" in result
        assert "fib_382" in result
        assert "fib_500" in result
        assert "fib_618" in result
        assert "vwap" in result
        assert "atr" in result

        # All values should be valid (not NaN)
        assert str(result["ema50"]) != "NaN"
        assert str(result["atr"]) != "NaN"

    def test_calculate_latest_insufficient_data(self):
        """Test with insufficient data."""
        calc = IndicatorCalculator(ema_period=50)
        opens = [Decimal("100")] * 10
        highs = [Decimal("101")] * 10
        lows = [Decimal("99")] * 10
        closes = [Decimal("100")] * 10
        volumes = [Decimal("1000")] * 10

        result = calc.calculate_latest(opens, highs, lows, closes, volumes)

        assert result is None
