"""Trading configuration models."""

from __future__ import annotations

from decimal import Decimal
from pydantic import BaseModel


class StrategyConfig(BaseModel):
    """Strategy configuration parameters."""

    # Indicator periods
    ema_period: int = 50
    fib_period: int = 9
    atr_period: int = 9

    # Fibonacci levels for support/resistance
    fib_levels: list[Decimal] = [
        Decimal("0.382"),
        Decimal("0.5"),
        Decimal("0.618"),
    ]

    # TP/SL multipliers (based on ATR)
    tp_atr_mult: Decimal = Decimal("2.0")
    sl_atr_mult: Decimal = Decimal("8.84")  # 2 * 4.42

    # Risk management (reserved for future position sizing, not used by SignalGenerator)
    max_risk_percent: Decimal = Decimal("2.53")  # Max risk per trade as % of equity

    # Tolerance for price touching levels (as percentage)
    touch_tolerance: Decimal = Decimal("0.001")  # 0.1%


class SymbolConfig(BaseModel):
    """Per-symbol configuration."""

    symbol: str
    timeframe: str = "5m"
    enabled: bool = True
    strategy: StrategyConfig = StrategyConfig()

    # Price precision for the symbol
    price_precision: int = 2
    quantity_precision: int = 3


class SignalFilterConfig(BaseModel):
    """Per-strategy signal quality filter.

    Each entry defines a (symbol, timeframe) pair with its own
    streak range and ATR percentile threshold. Only signals that
    pass both filters are emitted.

    Derived from walk-forward validated backtest analysis on 499,671 signals.
    """

    symbol: str
    timeframe: str
    enabled: bool = True

    # Streak filter: accept signals where streak_at_signal in [lo, hi]
    streak_lo: int = -999
    streak_hi: int = 999

    # ATR percentile filter: accept signals where atr_pct > threshold
    # 0.0 = no filter, 0.6 = only top 40% volatility
    atr_pct_threshold: float = 0.0

    # Position sizing (in asset units, e.g. 50000 for XRP)
    position_qty: float = 0.0

    # Kill switch: stop strategy after N consecutive loss months
    max_consecutive_loss_months: int = 3

    @property
    def key(self) -> str:
        """Unique key for this filter: 'SYMBOL_TIMEFRAME'."""
        return f"{self.symbol}_{self.timeframe}"


# =============================================================================
# Portfolio B: 5 walk-forward validated strategies (recommended)
# Sharpe 1.43, Calmar 2.90, avg pairwise correlation 0.052
# =============================================================================
PORTFOLIO_B: list[SignalFilterConfig] = [
    SignalFilterConfig(
        symbol="XRPUSDT", timeframe="30m",
        streak_lo=0, streak_hi=3, atr_pct_threshold=0.60,
        position_qty=50000,
    ),
    SignalFilterConfig(
        symbol="XRPUSDT", timeframe="15m",
        streak_lo=0, streak_hi=4, atr_pct_threshold=0.80,
        position_qty=50000,
    ),
    SignalFilterConfig(
        symbol="SOLUSDT", timeframe="5m",
        streak_lo=0, streak_hi=3, atr_pct_threshold=0.80,
        position_qty=500,
    ),
    SignalFilterConfig(
        symbol="BTCUSDT", timeframe="15m",
        streak_lo=0, streak_hi=7, atr_pct_threshold=0.90,
        position_qty=1,
    ),
    SignalFilterConfig(
        symbol="BTCUSDT", timeframe="5m",
        streak_lo=0, streak_hi=3, atr_pct_threshold=0.90,
        position_qty=1,
    ),
]

# =============================================================================
# Portfolio A: 4 strategies, lowest drawdown (alternative)
# Calmar 3.63, Sortino 3.74, max DD $18K
# =============================================================================
PORTFOLIO_A: list[SignalFilterConfig] = [
    SignalFilterConfig(
        symbol="XRPUSDT", timeframe="30m",
        streak_lo=0, streak_hi=3, atr_pct_threshold=0.60,
        position_qty=50000,
    ),
    SignalFilterConfig(
        symbol="SOLUSDT", timeframe="5m",
        streak_lo=0, streak_hi=3, atr_pct_threshold=0.80,
        position_qty=500,
    ),
    SignalFilterConfig(
        symbol="BTCUSDT", timeframe="15m",
        streak_lo=0, streak_hi=7, atr_pct_threshold=0.90,
        position_qty=1,
    ),
    SignalFilterConfig(
        symbol="ETHUSDT", timeframe="30m",
        streak_lo=0, streak_hi=4, atr_pct_threshold=0.90,
        position_qty=10,
    ),
]
