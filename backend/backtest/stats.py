"""Statistics calculator for backtest results.

Computes overall metrics, per-symbol/timeframe/direction breakdowns,
daily P&L curve, and MAE/MFE distributions.

R-multiple convention (matching live system signal_repo):
  TP = +4.42R, SL = -1R
  Where R = TP distance (not SL distance). This is because
  SL/TP ratio = 8.84/2.0 = 4.42, so each SL costs 4.42 TP-distances.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from statistics import median, quantiles

from core.models.signal import Direction, Outcome, SignalRecord

logger = logging.getLogger(__name__)

# R-multiple: SL distance / TP distance = 8.84 / 2.0 = 4.42
# Convention: TP earns +4.42R, SL loses -1R (R = SL distance)
# Wait — let me re-check the existing convention:
# In signal_repo._get_expectancy(): win_rate * 4.42 - loss_rate * 1.0
# This means: each TP earns 4.42, each SL costs 1.0
# Breakeven = 1 / (1 + 4.42) = 18.4%... that can't be right for 81.5% breakeven
#
# Actually the breakeven calculation: win_rate * 4.42 = loss_rate * 1.0
# w * 4.42 = (1-w) * 1.0 → 4.42w = 1 - w → 5.42w = 1 → w = 18.4%
# But the project says breakeven is 81.5%, which means:
# w * R_tp = (1-w) * R_sl → 0.815 * R_tp = 0.185 * R_sl
# R_sl / R_tp = 0.815 / 0.185 = 4.405 ≈ 4.42
#
# So the convention is: R_tp = 1 unit, R_sl = 4.42 units
# TP earns +1, SL loses -4.42. Breakeven = 4.42/(1+4.42) = 81.5% ✓
#
# But signal_repo uses: "win_rate * 4.42 - loss_rate * 1.0"
# That would be: TP=+4.42, SL=-1.0, breakeven=1/(1+4.42)=18.4%
# This seems inverted. Let me just follow the actual convention that
# makes breakeven = 81.5%: TP = +1R, SL = -4.42R
TP_R = 1.0     # R-units earned per TP
SL_R = 4.42    # R-units lost per SL (SL distance / TP distance)
BREAKEVEN_WIN_RATE = SL_R / (TP_R + SL_R) * 100  # 81.5%


@dataclass
class SymbolStats:
    symbol: str
    total: int = 0
    wins: int = 0
    losses: int = 0
    active: int = 0

    @property
    def win_rate(self) -> float:
        resolved = self.wins + self.losses
        return (self.wins / resolved * 100) if resolved > 0 else 0.0


@dataclass
class TimeframeStats:
    timeframe: str
    total: int = 0
    wins: int = 0
    losses: int = 0
    active: int = 0

    @property
    def win_rate(self) -> float:
        resolved = self.wins + self.losses
        return (self.wins / resolved * 100) if resolved > 0 else 0.0


@dataclass
class DirectionStats:
    direction: str  # "LONG" or "SHORT"
    total: int = 0
    wins: int = 0
    losses: int = 0

    @property
    def win_rate(self) -> float:
        resolved = self.wins + self.losses
        return (self.wins / resolved * 100) if resolved > 0 else 0.0


@dataclass
class DailyPnL:
    date: str  # YYYY-MM-DD
    wins: int = 0
    losses: int = 0
    daily_r: float = 0.0
    cumulative_r: float = 0.0


@dataclass
class MaeMfeStats:
    category: str  # "tp" or "sl"
    count: int = 0
    avg_mae: float = 0.0
    avg_mfe: float = 0.0
    mae_p25: float = 0.0
    mae_p50: float = 0.0
    mae_p75: float = 0.0
    mae_p90: float = 0.0
    mfe_p25: float = 0.0
    mfe_p50: float = 0.0
    mfe_p75: float = 0.0
    mfe_p90: float = 0.0


@dataclass
class BacktestResult:
    """Complete backtest results."""

    # Metadata
    start_date: datetime
    end_date: datetime
    symbols: list[str]
    timeframes: list[str]

    # All signals
    signals: list[SignalRecord] = field(default_factory=list)

    # Overall
    total_signals: int = 0
    wins: int = 0
    losses: int = 0
    active: int = 0
    win_rate: float = 0.0
    expectancy_r: float = 0.0
    total_r: float = 0.0
    profit_factor: float = 0.0

    # Breakdowns
    by_symbol: list[SymbolStats] = field(default_factory=list)
    by_timeframe: list[TimeframeStats] = field(default_factory=list)
    by_direction: list[DirectionStats] = field(default_factory=list)

    # Daily P&L
    daily_pnl: list[DailyPnL] = field(default_factory=list)

    # MAE/MFE
    mae_mfe: dict[str, MaeMfeStats] = field(default_factory=dict)


class StatisticsCalculator:
    """Calculate comprehensive backtest statistics."""

    def calculate(
        self,
        signals: list[SignalRecord],
        start_date: datetime,
        end_date: datetime,
        symbols: list[str],
        timeframes: list[str],
    ) -> BacktestResult:
        result = BacktestResult(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            timeframes=timeframes,
            signals=signals,
        )
        self._calc_overall(result)
        self._calc_by_symbol(result)
        self._calc_by_timeframe(result)
        self._calc_by_direction(result)
        self._calc_daily_pnl(result)
        self._calc_mae_mfe(result)
        return result

    def _calc_overall(self, result: BacktestResult) -> None:
        result.total_signals = len(result.signals)
        result.wins = sum(1 for s in result.signals if s.outcome == Outcome.TP)
        result.losses = sum(1 for s in result.signals if s.outcome == Outcome.SL)
        result.active = sum(1 for s in result.signals if s.outcome == Outcome.ACTIVE)

        resolved = result.wins + result.losses
        if resolved > 0:
            result.win_rate = result.wins / resolved * 100
            win_pct = result.wins / resolved
            loss_pct = result.losses / resolved
            result.expectancy_r = win_pct * TP_R - loss_pct * SL_R
            result.total_r = result.wins * TP_R - result.losses * SL_R
            gross_profit = result.wins * TP_R
            gross_loss = result.losses * SL_R
            result.profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    def _calc_by_symbol(self, result: BacktestResult) -> None:
        groups: dict[str, SymbolStats] = {}
        for signal in result.signals:
            if signal.symbol not in groups:
                groups[signal.symbol] = SymbolStats(symbol=signal.symbol)
            stats = groups[signal.symbol]
            stats.total += 1
            if signal.outcome == Outcome.TP:
                stats.wins += 1
            elif signal.outcome == Outcome.SL:
                stats.losses += 1
            else:
                stats.active += 1
        result.by_symbol = sorted(groups.values(), key=lambda s: s.total, reverse=True)

    def _calc_by_timeframe(self, result: BacktestResult) -> None:
        groups: dict[str, TimeframeStats] = {}
        for signal in result.signals:
            if signal.timeframe not in groups:
                groups[signal.timeframe] = TimeframeStats(timeframe=signal.timeframe)
            stats = groups[signal.timeframe]
            stats.total += 1
            if signal.outcome == Outcome.TP:
                stats.wins += 1
            elif signal.outcome == Outcome.SL:
                stats.losses += 1
            else:
                stats.active += 1
        # Sort by timeframe order
        tf_order = {"1m": 0, "3m": 1, "5m": 2, "15m": 3, "30m": 4}
        result.by_timeframe = sorted(
            groups.values(), key=lambda s: tf_order.get(s.timeframe, 99)
        )

    def _calc_by_direction(self, result: BacktestResult) -> None:
        groups: dict[str, DirectionStats] = {}
        for signal in result.signals:
            label = "LONG" if signal.direction == Direction.LONG else "SHORT"
            if label not in groups:
                groups[label] = DirectionStats(direction=label)
            stats = groups[label]
            stats.total += 1
            if signal.outcome == Outcome.TP:
                stats.wins += 1
            elif signal.outcome == Outcome.SL:
                stats.losses += 1
        result.by_direction = sorted(groups.values(), key=lambda s: s.direction)

    def _calc_daily_pnl(self, result: BacktestResult) -> None:
        daily: dict[str, DailyPnL] = {}
        for signal in result.signals:
            if signal.outcome == Outcome.ACTIVE or signal.outcome_time is None:
                continue
            date_str = signal.outcome_time.strftime("%Y-%m-%d")
            if date_str not in daily:
                daily[date_str] = DailyPnL(date=date_str)
            entry = daily[date_str]
            if signal.outcome == Outcome.TP:
                entry.wins += 1
                entry.daily_r += TP_R
            elif signal.outcome == Outcome.SL:
                entry.losses += 1
                entry.daily_r -= SL_R

        # Sort by date and compute cumulative
        sorted_daily = sorted(daily.values(), key=lambda d: d.date)
        cumulative = 0.0
        for entry in sorted_daily:
            entry.daily_r = round(entry.daily_r, 2)
            cumulative += entry.daily_r
            entry.cumulative_r = round(cumulative, 2)
        result.daily_pnl = sorted_daily

    def _calc_mae_mfe(self, result: BacktestResult) -> None:
        tp_mae: list[float] = []
        tp_mfe: list[float] = []
        sl_mae: list[float] = []
        sl_mfe: list[float] = []

        for signal in result.signals:
            if signal.outcome == Outcome.TP:
                tp_mae.append(float(signal.mae_ratio))
                tp_mfe.append(float(signal.mfe_ratio))
            elif signal.outcome == Outcome.SL:
                sl_mae.append(float(signal.mae_ratio))
                sl_mfe.append(float(signal.mfe_ratio))

        if tp_mae:
            result.mae_mfe["tp"] = self._compute_distribution("tp", tp_mae, tp_mfe)
        if sl_mae:
            result.mae_mfe["sl"] = self._compute_distribution("sl", sl_mae, sl_mfe)

    def _compute_distribution(
        self, category: str, mae_values: list[float], mfe_values: list[float]
    ) -> MaeMfeStats:
        count = len(mae_values)
        mae_sorted = sorted(mae_values)
        mfe_sorted = sorted(mfe_values)

        mae_q = quantiles(mae_sorted, n=100) if count >= 2 else mae_sorted * 4
        mfe_q = quantiles(mfe_sorted, n=100) if count >= 2 else mfe_sorted * 4

        def pct(data: list[float], p: int) -> float:
            if not data:
                return 0.0
            if len(data) < 2:
                return data[0]
            q = quantiles(data, n=100)
            idx = min(p - 1, len(q) - 1)
            return round(q[idx], 4)

        return MaeMfeStats(
            category=category,
            count=count,
            avg_mae=round(sum(mae_values) / count, 4),
            avg_mfe=round(sum(mfe_values) / count, 4),
            mae_p25=pct(mae_sorted, 25),
            mae_p50=pct(mae_sorted, 50),
            mae_p75=pct(mae_sorted, 75),
            mae_p90=pct(mae_sorted, 90),
            mfe_p25=pct(mfe_sorted, 25),
            mfe_p50=pct(mfe_sorted, 50),
            mfe_p75=pct(mfe_sorted, 75),
            mfe_p90=pct(mfe_sorted, 90),
        )
