"""ATR percentile tracker for signal quality filtering.

Maintains a per-(symbol, timeframe) rolling window of historical ATR
values and computes the empirical CDF (fraction of values <= query).

In backtesting, ``atr_pct = df.groupby(...).rank(pct=True)`` is computed
over the entire dataset.  In live trading we use a large rolling window
(default 10,000 observations).  Because ATR distributions are stationary
(no long-term drift), the rolling percentile converges quickly and
closely approximates the backtest semantics.

The empirical CDF and pandas ``rank(pct=True)`` are effectively identical
for continuous data (ATR has 8+ decimal precision, ties are negligible).
"""

from __future__ import annotations

import logging
import math
from collections import deque

import numpy as np

logger = logging.getLogger(__name__)

# Default cap on history per symbol/timeframe.
# 10,000 × 8 bytes = 80 KB per pair; 25 pairs = 2 MB total.
# At 1m klines this is ~7 days of history; at 5m it is ~35 days.
DEFAULT_MAX_HISTORY = 10_000


class AtrPercentileTracker:
    """Track ATR values per symbol/timeframe and compute percentile ranks.

    Parameters
    ----------
    min_samples : int
        Minimum number of ATR observations before percentile calculation
        is considered reliable.  Before this threshold is reached,
        ``get_percentile()`` returns ``None`` (meaning "not enough data,
        do not filter").
    max_history : int
        Maximum observations to keep per symbol/timeframe.  Older values
        are discarded (FIFO).  This bounds memory and keeps ``get_percentile``
        O(max_history) instead of O(total_klines_ever).
    """

    def __init__(
        self,
        min_samples: int = 200,
        max_history: int = DEFAULT_MAX_HISTORY,
    ):
        self.min_samples = min_samples
        self.max_history = max_history
        self._history: dict[str, deque[float]] = {}

    @staticmethod
    def _key(symbol: str, timeframe: str) -> str:
        return f"{symbol}_{timeframe}"

    @staticmethod
    def _is_valid(value: float) -> bool:
        """Check that a value is a finite positive number."""
        return isinstance(value, (int, float)) and math.isfinite(value) and value > 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, symbol: str, timeframe: str, atr_value: float) -> None:
        """Append a new ATR observation (must be a finite positive number)."""
        if not self._is_valid(atr_value):
            return  # silently skip NaN, inf, zero, negative
        key = self._key(symbol, timeframe)
        if key not in self._history:
            self._history[key] = deque(maxlen=self.max_history)
        self._history[key].append(atr_value)

    def get_percentile(
        self, symbol: str, timeframe: str, atr_value: float
    ) -> float | None:
        """Return the empirical CDF of *atr_value* within its history.

        Computes the fraction of historical values that are **<= atr_value**.
        For continuous data this is equivalent to ``pandas.rank(pct=True)``.

        Returns ``None`` when fewer than ``min_samples`` observations
        have been recorded (safe default: do not filter).
        """
        key = self._key(symbol, timeframe)
        buf = self._history.get(key)
        if buf is None or len(buf) < self.min_samples:
            return None

        arr = np.asarray(buf)
        return float((arr <= atr_value).sum() / len(arr))

    def get_count(self, symbol: str, timeframe: str) -> int:
        """Return the number of ATR observations stored."""
        key = self._key(symbol, timeframe)
        return len(self._history.get(key, deque()))

    def is_ready(self, symbol: str, timeframe: str) -> bool:
        """Return ``True`` if enough samples have been collected."""
        return self.get_count(symbol, timeframe) >= self.min_samples

    def bulk_load(
        self, symbol: str, timeframe: str, atr_values: list[float]
    ) -> None:
        """Load a batch of historical ATR values (for warmup at startup).

        Invalid values (NaN, negative, zero) are silently filtered out.
        If the total exceeds ``max_history``, only the most recent values
        are kept.
        """
        clean = [v for v in atr_values if self._is_valid(v)]
        key = self._key(symbol, timeframe)
        if key not in self._history:
            self._history[key] = deque(maxlen=self.max_history)
        self._history[key].extend(clean)
        logger.info(
            "ATR warmup: %s %s loaded %d values (filtered %d invalid, total %d)",
            symbol,
            timeframe,
            len(clean),
            len(atr_values) - len(clean),
            len(self._history[key]),
        )
