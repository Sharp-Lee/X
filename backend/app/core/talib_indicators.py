"""Technical indicators using TA-Lib for high performance."""

from decimal import Decimal
from typing import Sequence

import numpy as np
import talib


def ema(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """
    Calculate Exponential Moving Average using TA-Lib.

    Args:
        values: Sequence of price values
        period: EMA period

    Returns:
        List of EMA values (NaN for initial values)
    """
    if len(values) < period:
        return [Decimal("NaN")] * len(values)

    arr = np.array([float(v) for v in values], dtype=np.float64)
    result = talib.EMA(arr, timeperiod=period)

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def sma(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """
    Calculate Simple Moving Average using TA-Lib.

    Args:
        values: Sequence of price values
        period: SMA period

    Returns:
        List of SMA values
    """
    if len(values) < period:
        return [Decimal("NaN")] * len(values)

    arr = np.array([float(v) for v in values], dtype=np.float64)
    result = talib.SMA(arr, timeperiod=period)

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def highest(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """
    Calculate highest value over lookback period using TA-Lib.

    Args:
        values: Sequence of values (typically highs)
        period: Lookback period

    Returns:
        List of highest values
    """
    if len(values) < period:
        return [Decimal("NaN")] * len(values)

    arr = np.array([float(v) for v in values], dtype=np.float64)
    result = talib.MAX(arr, timeperiod=period)

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def lowest(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """
    Calculate lowest value over lookback period using TA-Lib.

    Args:
        values: Sequence of values (typically lows)
        period: Lookback period

    Returns:
        List of lowest values
    """
    if len(values) < period:
        return [Decimal("NaN")] * len(values)

    arr = np.array([float(v) for v in values], dtype=np.float64)
    result = talib.MIN(arr, timeperiod=period)

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def atr(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    period: int = 14,
) -> list[Decimal]:
    """
    Calculate Average True Range (ATR) using TA-Lib.

    Uses Wilder's smoothing (RMA) internally.

    Args:
        highs: Sequence of high prices
        lows: Sequence of low prices
        closes: Sequence of close prices
        period: ATR period

    Returns:
        List of ATR values
    """
    n = len(highs)
    if n < period:
        return [Decimal("NaN")] * n

    high_arr = np.array([float(v) for v in highs], dtype=np.float64)
    low_arr = np.array([float(v) for v in lows], dtype=np.float64)
    close_arr = np.array([float(v) for v in closes], dtype=np.float64)

    result = talib.ATR(high_arr, low_arr, close_arr, timeperiod=period)

    return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in result]


def vwap(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    closes: Sequence[Decimal],
    volumes: Sequence[Decimal],
) -> list[Decimal]:
    """
    Calculate Volume Weighted Average Price (VWAP).

    Note: TA-Lib doesn't have VWAP, so we implement it manually.
    This is a cumulative VWAP. For daily reset, handle externally.

    Args:
        highs: Sequence of high prices
        lows: Sequence of low prices
        closes: Sequence of close prices
        volumes: Sequence of volumes

    Returns:
        List of VWAP values
    """
    if len(closes) == 0:
        return []

    # Convert to numpy for faster computation
    high_arr = np.array([float(v) for v in highs], dtype=np.float64)
    low_arr = np.array([float(v) for v in lows], dtype=np.float64)
    close_arr = np.array([float(v) for v in closes], dtype=np.float64)
    vol_arr = np.array([float(v) for v in volumes], dtype=np.float64)

    # Typical price = (high + low + close) / 3
    tp = (high_arr + low_arr + close_arr) / 3

    # Cumulative calculations
    cum_vol = np.cumsum(vol_arr)
    cum_pv = np.cumsum(tp * vol_arr)

    # Avoid division by zero
    with np.errstate(divide='ignore', invalid='ignore'):
        result = np.where(cum_vol > 0, cum_pv / cum_vol, close_arr)

    return [Decimal(str(v)) for v in result]


def fibonacci_levels(
    highs: Sequence[Decimal],
    lows: Sequence[Decimal],
    period: int = 9,
) -> tuple[list[Decimal], list[Decimal], list[Decimal]]:
    """
    Calculate Fibonacci retracement levels based on lookback period.

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
    n = len(highs)
    if n < period:
        nan_list = [Decimal("NaN")] * n
        return nan_list, nan_list.copy(), nan_list.copy()

    high_arr = np.array([float(v) for v in highs], dtype=np.float64)
    low_arr = np.array([float(v) for v in lows], dtype=np.float64)

    # Use TA-Lib for highest/lowest
    hh = talib.MAX(high_arr, timeperiod=period)
    ll = talib.MIN(low_arr, timeperiod=period)

    # Calculate Fibonacci levels
    range_size = hh - ll
    fib_382_arr = hh - range_size * 0.382
    fib_500_arr = hh - range_size * 0.500
    fib_618_arr = hh - range_size * 0.618

    def to_decimal_list(arr):
        return [Decimal(str(v)) if not np.isnan(v) else Decimal("NaN") for v in arr]

    return to_decimal_list(fib_382_arr), to_decimal_list(fib_500_arr), to_decimal_list(fib_618_arr)


class TalibIndicatorCalculator:
    """Calculator for all technical indicators using TA-Lib."""

    def __init__(
        self,
        ema_period: int = 50,
        fib_period: int = 9,
        atr_period: int = 9,
    ):
        self.ema_period = ema_period
        self.fib_period = fib_period
        self.atr_period = atr_period

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
