"""Technical indicators for signal generation.

This module provides two implementations:
1. TA-Lib based (fast, C library) - used when TA-Lib is available
2. Pure Python/NumPy fallback - used when TA-Lib is not installed

The TA-Lib version is 10-50x faster for indicator calculations.
"""

from decimal import Decimal
from typing import Sequence

import numpy as np

# Try to import TA-Lib implementations
try:
    from core.indicators.talib_indicators import (
        ema as _talib_ema,
        sma as _talib_sma,
        atr as _talib_atr,
        vwap as _talib_vwap,
        highest as _talib_highest,
        lowest as _talib_lowest,
        fibonacci_levels as _talib_fibonacci_levels,
        TalibIndicatorCalculator,
    )
    _TALIB_AVAILABLE = True
except ImportError:
    _TALIB_AVAILABLE = False


# =============================================================================
# NumPy-based fallback implementations
# =============================================================================

def _numpy_ema(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """Calculate EMA using pure NumPy (fallback)."""
    if len(values) < period:
        return [Decimal("NaN")] * len(values)

    arr = np.array([float(v) for v in values], dtype=np.float64)
    multiplier = 2.0 / (period + 1)

    result = np.empty_like(arr)
    result[:period - 1] = np.nan
    result[period - 1] = np.mean(arr[:period])

    for i in range(period, len(arr)):
        result[i] = arr[i] * multiplier + result[i - 1] * (1 - multiplier)

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def _numpy_sma(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """Calculate SMA using pure NumPy (fallback)."""
    if len(values) < period:
        return [Decimal("NaN")] * len(values)

    arr = np.array([float(v) for v in values], dtype=np.float64)
    result = np.empty_like(arr)
    result[:period - 1] = np.nan

    for i in range(period - 1, len(arr)):
        result[i] = np.mean(arr[i - period + 1 : i + 1])

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def _numpy_highest(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """Calculate highest using pure NumPy (fallback)."""
    if len(values) < period:
        return [Decimal("NaN")] * len(values)

    arr = np.array([float(v) for v in values], dtype=np.float64)
    result = np.empty_like(arr)
    result[:period - 1] = np.nan

    for i in range(period - 1, len(arr)):
        result[i] = np.max(arr[i - period + 1 : i + 1])

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def _numpy_lowest(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """Calculate lowest using pure NumPy (fallback)."""
    if len(values) < period:
        return [Decimal("NaN")] * len(values)

    arr = np.array([float(v) for v in values], dtype=np.float64)
    result = np.empty_like(arr)
    result[:period - 1] = np.nan

    for i in range(period - 1, len(arr)):
        result[i] = np.min(arr[i - period + 1 : i + 1])

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def _numpy_true_range(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
) -> list[Decimal]:
    """Calculate True Range using pure Python (fallback)."""
    n = len(highs)
    if n == 0:
        return []

    result = [highs[0] - lows[0]]

    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i - 1])
        lc = abs(lows[i] - closes[i - 1])
        result.append(max(hl, hc, lc))

    return result


def _numpy_atr(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> list[Decimal]:
    """Calculate ATR using pure NumPy (fallback)."""
    tr = _numpy_true_range(highs, lows, closes)

    if len(tr) < period:
        return [Decimal("NaN")] * len(tr)

    tr_arr = np.array([float(v) for v in tr], dtype=np.float64)
    result = np.empty_like(tr_arr)
    result[:period - 1] = np.nan
    result[period - 1] = np.mean(tr_arr[:period])

    alpha = 1.0 / period
    for i in range(period, len(tr_arr)):
        result[i] = alpha * tr_arr[i] + (1 - alpha) * result[i - 1]

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def _numpy_vwap(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    volumes: Sequence[Decimal],
) -> list[Decimal]:
    """Calculate VWAP using pure Python (fallback)."""
    if len(closes) == 0:
        return []

    result = []
    cum_vol = Decimal("0")
    cum_pv = Decimal("0")

    for i in range(len(closes)):
        tp = (highs[i] + lows[i] + closes[i]) / 3
        cum_vol += volumes[i]
        cum_pv += tp * volumes[i]

        if cum_vol > 0:
            result.append(cum_pv / cum_vol)
        else:
            result.append(closes[i])

    return result


def _numpy_fibonacci_levels(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    period: int = 9,
) -> tuple[list[Decimal], list[Decimal], list[Decimal]]:
    """Calculate Fibonacci levels using NumPy fallback."""
    hh = _numpy_highest(highs, period)
    ll = _numpy_lowest(lows, period)

    fib_382 = []
    fib_500 = []
    fib_618 = []

    for i in range(len(highs)):
        if i < period - 1:
            fib_382.append(Decimal("NaN"))
            fib_500.append(Decimal("NaN"))
            fib_618.append(Decimal("NaN"))
        else:
            h = hh[i]
            l = ll[i]
            range_size = h - l
            fib_382.append(h - range_size * Decimal("0.382"))
            fib_500.append(h - range_size * Decimal("0.500"))
            fib_618.append(h - range_size * Decimal("0.618"))

    return fib_382, fib_500, fib_618


# =============================================================================
# Public API - uses TA-Lib when available, falls back to NumPy
# =============================================================================

def ema(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """
    Calculate Exponential Moving Average.

    Uses TA-Lib when available for better performance.

    Args:
        values: Sequence of price values
        period: EMA period

    Returns:
        List of EMA values (same length as input, with NaN for initial values)
    """
    if _TALIB_AVAILABLE:
        return _talib_ema(values, period)
    return _numpy_ema(values, period)


def sma(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """
    Calculate Simple Moving Average.

    Uses TA-Lib when available for better performance.

    Args:
        values: Sequence of price values
        period: SMA period

    Returns:
        List of SMA values
    """
    if _TALIB_AVAILABLE:
        return _talib_sma(values, period)
    return _numpy_sma(values, period)


def highest(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """
    Calculate highest value over lookback period.

    Uses TA-Lib when available for better performance.

    Args:
        values: Sequence of values (typically highs)
        period: Lookback period

    Returns:
        List of highest values
    """
    if _TALIB_AVAILABLE:
        return _talib_highest(values, period)
    return _numpy_highest(values, period)


def lowest(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """
    Calculate lowest value over lookback period.

    Uses TA-Lib when available for better performance.

    Args:
        values: Sequence of values (typically lows)
        period: Lookback period

    Returns:
        List of lowest values
    """
    if _TALIB_AVAILABLE:
        return _talib_lowest(values, period)
    return _numpy_lowest(values, period)


def true_range(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
) -> list[Decimal]:
    """
    Calculate True Range.

    TR = max(high - low, abs(high - prev_close), abs(low - prev_close))

    Args:
        highs: Sequence of high prices
        lows: Sequence of low prices
        closes: Sequence of close prices

    Returns:
        List of True Range values
    """
    return _numpy_true_range(highs, lows, closes)


def atr(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> list[Decimal]:
    """
    Calculate Average True Range (ATR).

    Uses TA-Lib when available for better performance.
    Uses RMA (Relative Moving Average) / Wilder's smoothing.

    Args:
        highs: Sequence of high prices
        lows: Sequence of low prices
        closes: Sequence of close prices
        period: ATR period

    Returns:
        List of ATR values
    """
    if _TALIB_AVAILABLE:
        return _talib_atr(highs, lows, closes, period)
    return _numpy_atr(highs, lows, closes, period)


def vwap(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    volumes: Sequence[Decimal],
) -> list[Decimal]:
    """
    Calculate Volume Weighted Average Price (VWAP).

    Uses TA-Lib optimized version when available.

    Note: This is a simple cumulative VWAP. In TradingView, VWAP resets daily.
    For intraday use, you may need to reset at session boundaries.

    Args:
        highs: Sequence of high prices
        lows: Sequence of low prices
        closes: Sequence of close prices
        volumes: Sequence of volumes

    Returns:
        List of VWAP values
    """
    if _TALIB_AVAILABLE:
        return _talib_vwap(highs, lows, closes, volumes)
    return _numpy_vwap(highs, lows, closes, volumes)


def fibonacci_levels(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    period: int = 9,
) -> tuple[list[Decimal], list[Decimal], list[Decimal]]:
    """
    Calculate Fibonacci retracement levels based on lookback period.

    Uses TA-Lib when available for better performance.

    fib_618 = highest - (highest - lowest) * 0.618
    fib_500 = highest - (highest - lowest) * 0.500
    fib_382 = highest - (highest - lowest) * 0.382

    Args:
        highs: Sequence of high prices
        lows: Sequence of low prices
        period: Lookback period for highest/lowest

    Returns:
        Tuple of (fib_382, fib_500, fib_618) lists
    """
    if _TALIB_AVAILABLE:
        return _talib_fibonacci_levels(highs, lows, period)
    return _numpy_fibonacci_levels(highs, lows, period)


# =============================================================================
# IndicatorCalculator class
# =============================================================================

class IndicatorCalculator:
    """Calculator for all technical indicators needed by the strategy.

    Uses TA-Lib when available for better performance.
    """

    def __init__(
        self,
        ema_period: int = 50,
        fib_period: int = 9,
        atr_period: int = 9,
    ):
        self.ema_period = ema_period
        self.fib_period = fib_period
        self.atr_period = atr_period

        # Use TA-Lib calculator if available
        if _TALIB_AVAILABLE:
            self._talib_calc = TalibIndicatorCalculator(
                ema_period=ema_period,
                fib_period=fib_period,
                atr_period=atr_period,
            )
        else:
            self._talib_calc = None

    def calculate_all(
        self,
        opens: list[Decimal],
        highs: list[Decimal],
        lows: list[Decimal],
        closes: list[Decimal],
        volumes: list[Decimal],
    ) -> dict:
        """
        Calculate all indicators for the given OHLCV data.

        Args:
            opens: List of open prices
            highs: List of high prices
            lows: List of low prices
            closes: List of close prices
            volumes: List of volumes

        Returns:
            Dict with all indicator values at each index
        """
        if self._talib_calc:
            return self._talib_calc.calculate_all(opens, highs, lows, closes, volumes)

        ema50 = ema(closes, self.ema_period)
        fib_382, fib_500, fib_618 = fibonacci_levels(highs, lows, self.fib_period)
        vwap_values = vwap(highs, lows, closes, volumes)
        atr_values = atr(highs, lows, closes, self.atr_period)
        hh = highest(highs, self.fib_period)
        ll = lowest(lows, self.fib_period)

        return {
            "ema50": ema50,
            "fib_382": fib_382,
            "fib_500": fib_500,
            "fib_618": fib_618,
            "vwap": vwap_values,
            "atr": atr_values,
            "highest": hh,
            "lowest": ll,
        }

    def calculate_latest(
        self,
        opens: list[Decimal],
        highs: list[Decimal],
        lows: list[Decimal],
        closes: list[Decimal],
        volumes: list[Decimal],
    ) -> dict | None:
        """
        Calculate indicators for the latest bar only.

        Args:
            opens: List of open prices (need enough history)
            highs: List of high prices
            lows: List of low prices
            closes: List of close prices
            volumes: List of volumes

        Returns:
            Dict with indicator values for the latest bar, or None if not enough data
        """
        if self._talib_calc:
            return self._talib_calc.calculate_latest(opens, highs, lows, closes, volumes)

        min_len = max(self.ema_period, self.fib_period, self.atr_period)
        if len(closes) < min_len:
            return None

        all_indicators = self.calculate_all(opens, highs, lows, closes, volumes)

        return {
            "ema50": all_indicators["ema50"][-1],
            "fib_382": all_indicators["fib_382"][-1],
            "fib_500": all_indicators["fib_500"][-1],
            "fib_618": all_indicators["fib_618"][-1],
            "vwap": all_indicators["vwap"][-1],
            "atr": all_indicators["atr"][-1],
            "highest": all_indicators["highest"][-1],
            "lowest": all_indicators["lowest"][-1],
        }


def is_talib_available() -> bool:
    """Check if TA-Lib is available."""
    return _TALIB_AVAILABLE
