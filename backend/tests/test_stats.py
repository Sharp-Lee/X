"""Tests for StatisticsCalculator backtest statistics."""

import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from core.models.signal import Direction, Outcome, SignalRecord
from backtest.stats import (
    StatisticsCalculator,
    TP_R,
    SL_R,
    BREAKEVEN_WIN_RATE,
    SymbolStats,
    TimeframeStats,
    DirectionStats,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def make_signal(
    symbol: str = "BTCUSDT",
    timeframe: str = "5m",
    direction: Direction = Direction.LONG,
    outcome: Outcome = Outcome.ACTIVE,
    signal_time: datetime | None = None,
    outcome_time: datetime | None = None,
    entry_price: Decimal = Decimal("50000"),
    tp_price: Decimal = Decimal("51000"),
    sl_price: Decimal = Decimal("45580"),
    mae_ratio: Decimal = Decimal("0"),
    mfe_ratio: Decimal = Decimal("0"),
) -> SignalRecord:
    """Build a SignalRecord with sensible defaults for testing."""
    if signal_time is None:
        signal_time = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)
    return SignalRecord(
        symbol=symbol,
        timeframe=timeframe,
        signal_time=signal_time,
        direction=direction,
        entry_price=entry_price,
        tp_price=tp_price,
        sl_price=sl_price,
        outcome=outcome,
        outcome_time=outcome_time,
        mae_ratio=mae_ratio,
        mfe_ratio=mfe_ratio,
    )


# Default date range for calculator calls
START = datetime(2025, 1, 1, tzinfo=timezone.utc)
END = datetime(2025, 12, 31, tzinfo=timezone.utc)
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
TIMEFRAMES = ["5m", "15m"]


def _calc(signals: list[SignalRecord]) -> "BacktestResult":  # noqa: F821
    """Shortcut to run StatisticsCalculator.calculate."""
    return StatisticsCalculator().calculate(signals, START, END, SYMBOLS, TIMEFRAMES)


def _unique_time(base: datetime, offset_minutes: int) -> datetime:
    """Return base + offset_minutes as a datetime (safe for any count)."""
    return base + timedelta(minutes=offset_minutes)


# ---------------------------------------------------------------------------
# R-multiple constants
# ---------------------------------------------------------------------------

class TestRMultipleConstants:
    """Verify R-multiple constants match the strategy definition."""

    def test_tp_r(self):
        assert TP_R == 1.0

    def test_sl_r(self):
        assert SL_R == 4.42

    def test_breakeven_win_rate(self):
        # 4.42 / (1.0 + 4.42) * 100 = 81.549...%
        assert BREAKEVEN_WIN_RATE == pytest.approx(81.549, rel=1e-3)

    def test_constants_used_in_expectancy(self):
        """Ensure the calculator uses TP_R=1.0 and SL_R=4.42 in its formulas."""
        # 1 win, 1 loss => expectancy = 0.5*1.0 - 0.5*4.42 = -1.71
        signals = [
            make_signal(outcome=Outcome.TP, outcome_time=START),
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert result.expectancy_r == pytest.approx(0.5 * 1.0 - 0.5 * 4.42, abs=1e-9)


# ---------------------------------------------------------------------------
# Win rate
# ---------------------------------------------------------------------------

class TestWinRate:
    """Win rate = wins / (wins + losses) * 100.  Active signals excluded."""

    def test_all_losses(self):
        signals = [
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 1, 1, i, tzinfo=timezone.utc),
            )
            for i in range(10)
        ]
        result = _calc(signals)
        assert result.win_rate == pytest.approx(0.0)
        assert result.wins == 0
        assert result.losses == 10

    def test_all_wins(self):
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 1, 1, i, tzinfo=timezone.utc),
            )
            for i in range(10)
        ]
        result = _calc(signals)
        assert result.win_rate == pytest.approx(100.0)
        assert result.wins == 10
        assert result.losses == 0

    def test_mixed(self):
        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        wins = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=_unique_time(base, i),
            )
            for i in range(82)
        ]
        losses = [
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=_unique_time(base, 1000 + i),
            )
            for i in range(18)
        ]
        result = _calc(wins + losses)
        assert result.win_rate == pytest.approx(82.0)

    def test_active_excluded_from_denominator(self):
        """ACTIVE signals must NOT count toward win rate."""
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 1, 1, 0, tzinfo=timezone.utc),
            ),
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 1, 1, 1, tzinfo=timezone.utc),
            ),
            make_signal(
                outcome=Outcome.ACTIVE,
                signal_time=datetime(2025, 1, 1, 2, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        # denominator = 1 + 1 = 2 (not 3)
        assert result.win_rate == pytest.approx(50.0)
        assert result.active == 1


# ---------------------------------------------------------------------------
# Expectancy
# ---------------------------------------------------------------------------

class TestExpectancy:
    """expectancy_r = win% * TP_R - loss% * SL_R."""

    def _make_resolved(self, n_wins: int, n_losses: int) -> list[SignalRecord]:
        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        wins = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=_unique_time(base, i),
            )
            for i in range(n_wins)
        ]
        losses = [
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=_unique_time(base, 100_000 + i),
            )
            for i in range(n_losses)
        ]
        return wins + losses

    def test_breakeven(self):
        """~81.5% win rate should yield expectancy near zero."""
        # 815 wins, 185 losses -> 81.5%
        signals = self._make_resolved(815, 185)
        result = _calc(signals)
        # 0.815 * 1.0 - 0.185 * 4.42 = 0.815 - 0.8177 = -0.0027
        assert result.expectancy_r == pytest.approx(0.0, abs=0.01)

    def test_fifty_percent(self):
        """50% win rate -> expectancy = 0.5*1.0 - 0.5*4.42 = -1.71."""
        signals = self._make_resolved(50, 50)
        result = _calc(signals)
        assert result.expectancy_r == pytest.approx(-1.71, abs=0.001)

    def test_ninety_percent(self):
        """90% win rate -> expectancy = 0.9*1.0 - 0.1*4.42 = +0.458."""
        signals = self._make_resolved(90, 10)
        result = _calc(signals)
        assert result.expectancy_r == pytest.approx(0.458, abs=0.001)


# ---------------------------------------------------------------------------
# Profit factor
# ---------------------------------------------------------------------------

class TestProfitFactor:
    """profit_factor = (wins * TP_R) / (losses * SL_R)."""

    def test_all_wins_infinite(self):
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 1, 1, i, tzinfo=timezone.utc),
            )
            for i in range(5)
        ]
        result = _calc(signals)
        assert result.profit_factor == float("inf")

    def test_all_losses_zero(self):
        signals = [
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 1, 1, i, tzinfo=timezone.utc),
            )
            for i in range(5)
        ]
        result = _calc(signals)
        assert result.profit_factor == pytest.approx(0.0)

    def test_82_wins_18_losses(self):
        """82 * 1.0 / (18 * 4.42) = 82 / 79.56 ~ 1.0307."""
        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        wins = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=_unique_time(base, i),
            )
            for i in range(82)
        ]
        losses = [
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=_unique_time(base, 1000 + i),
            )
            for i in range(18)
        ]
        result = _calc(wins + losses)
        expected = 82.0 / (18 * 4.42)
        assert result.profit_factor == pytest.approx(expected, rel=1e-4)


# ---------------------------------------------------------------------------
# Total R
# ---------------------------------------------------------------------------

class TestTotalR:
    """total_r = wins * TP_R - losses * SL_R."""

    def test_82_wins_18_losses(self):
        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        wins = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=_unique_time(base, i),
            )
            for i in range(82)
        ]
        losses = [
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=_unique_time(base, 1000 + i),
            )
            for i in range(18)
        ]
        result = _calc(wins + losses)
        expected = 82 * 1.0 - 18 * 4.42  # 82 - 79.56 = 2.44
        assert result.total_r == pytest.approx(expected, abs=0.01)

    def test_all_wins(self):
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 1, 1, i, tzinfo=timezone.utc),
            )
            for i in range(10)
        ]
        result = _calc(signals)
        assert result.total_r == pytest.approx(10.0)

    def test_all_losses(self):
        signals = [
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 1, 1, i, tzinfo=timezone.utc),
            )
            for i in range(10)
        ]
        result = _calc(signals)
        assert result.total_r == pytest.approx(-44.2)


# ---------------------------------------------------------------------------
# Empty & all-active edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Empty list and all-ACTIVE signals must not crash."""

    def test_empty_signals(self):
        result = _calc([])
        assert result.total_signals == 0
        assert result.wins == 0
        assert result.losses == 0
        assert result.active == 0
        assert result.win_rate == 0.0
        assert result.expectancy_r == 0.0
        assert result.total_r == 0.0
        assert result.profit_factor == 0.0
        assert result.by_symbol == []
        assert result.by_timeframe == []
        assert result.by_direction == []
        assert result.daily_pnl == []
        assert result.mae_mfe == {}

    def test_all_active(self):
        signals = [
            make_signal(
                outcome=Outcome.ACTIVE,
                signal_time=datetime(2025, 1, 1, i, tzinfo=timezone.utc),
            )
            for i in range(5)
        ]
        result = _calc(signals)
        assert result.total_signals == 5
        assert result.wins == 0
        assert result.losses == 0
        assert result.active == 5
        assert result.win_rate == 0.0
        assert result.expectancy_r == 0.0
        assert result.total_r == 0.0
        assert result.profit_factor == 0.0
        # daily P&L should be empty (active excluded)
        assert result.daily_pnl == []


# ---------------------------------------------------------------------------
# By-symbol breakdown
# ---------------------------------------------------------------------------

class TestBySymbol:
    """Signals split correctly by symbol."""

    def test_single_symbol(self):
        signals = [
            make_signal(symbol="BTCUSDT", outcome=Outcome.TP, outcome_time=START),
            make_signal(
                symbol="BTCUSDT",
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert len(result.by_symbol) == 1
        s = result.by_symbol[0]
        assert s.symbol == "BTCUSDT"
        assert s.total == 2
        assert s.wins == 1
        assert s.losses == 1
        assert s.win_rate == pytest.approx(50.0)

    def test_multiple_symbols(self):
        signals = [
            make_signal(symbol="BTCUSDT", outcome=Outcome.TP, outcome_time=START),
            make_signal(
                symbol="BTCUSDT",
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
            ),
            make_signal(
                symbol="ETHUSDT",
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert len(result.by_symbol) == 2
        # Sorted by total descending: BTCUSDT(2) > ETHUSDT(1)
        assert result.by_symbol[0].symbol == "BTCUSDT"
        assert result.by_symbol[0].total == 2
        assert result.by_symbol[0].wins == 2
        assert result.by_symbol[1].symbol == "ETHUSDT"
        assert result.by_symbol[1].total == 1
        assert result.by_symbol[1].losses == 1

    def test_active_tracked_per_symbol(self):
        signals = [
            make_signal(symbol="BTCUSDT", outcome=Outcome.ACTIVE),
            make_signal(
                symbol="ETHUSDT",
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        btc = [s for s in result.by_symbol if s.symbol == "BTCUSDT"][0]
        eth = [s for s in result.by_symbol if s.symbol == "ETHUSDT"][0]
        assert btc.active == 1
        assert btc.wins == 0
        assert eth.active == 0
        assert eth.wins == 1


# ---------------------------------------------------------------------------
# By-timeframe breakdown
# ---------------------------------------------------------------------------

class TestByTimeframe:
    """Signals split correctly by timeframe."""

    def test_single_timeframe(self):
        signals = [
            make_signal(timeframe="5m", outcome=Outcome.TP, outcome_time=START),
            make_signal(
                timeframe="5m",
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert len(result.by_timeframe) == 1
        tf = result.by_timeframe[0]
        assert tf.timeframe == "5m"
        assert tf.total == 2
        assert tf.wins == 1
        assert tf.losses == 1

    def test_multiple_timeframes_sorted(self):
        """Timeframes should be sorted by canonical order (1m,3m,5m,15m,30m)."""
        signals = [
            make_signal(
                timeframe="15m",
                outcome=Outcome.TP,
                outcome_time=START,
            ),
            make_signal(
                timeframe="5m",
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
            ),
            make_signal(
                timeframe="3m",
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert len(result.by_timeframe) == 3
        assert [tf.timeframe for tf in result.by_timeframe] == ["3m", "5m", "15m"]

    def test_active_tracked_per_timeframe(self):
        signals = [
            make_signal(timeframe="5m", outcome=Outcome.ACTIVE),
            make_signal(
                timeframe="15m",
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        tf5 = [t for t in result.by_timeframe if t.timeframe == "5m"][0]
        tf15 = [t for t in result.by_timeframe if t.timeframe == "15m"][0]
        assert tf5.active == 1
        assert tf15.active == 0


# ---------------------------------------------------------------------------
# By-direction breakdown
# ---------------------------------------------------------------------------

class TestByDirection:
    """LONG vs SHORT split correctly."""

    def test_long_and_short(self):
        signals = [
            make_signal(direction=Direction.LONG, outcome=Outcome.TP, outcome_time=START),
            make_signal(
                direction=Direction.LONG,
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
            ),
            make_signal(
                direction=Direction.SHORT,
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc),
                tp_price=Decimal("49000"),
                sl_price=Decimal("54420"),
            ),
        ]
        result = _calc(signals)
        assert len(result.by_direction) == 2
        # Sorted alphabetically: LONG < SHORT
        assert result.by_direction[0].direction == "LONG"
        assert result.by_direction[1].direction == "SHORT"

        long_stats = result.by_direction[0]
        assert long_stats.total == 2
        assert long_stats.wins == 1
        assert long_stats.losses == 1
        assert long_stats.win_rate == pytest.approx(50.0)

        short_stats = result.by_direction[1]
        assert short_stats.total == 1
        assert short_stats.wins == 1
        assert short_stats.losses == 0
        assert short_stats.win_rate == pytest.approx(100.0)

    def test_only_long(self):
        signals = [
            make_signal(direction=Direction.LONG, outcome=Outcome.TP, outcome_time=START),
        ]
        result = _calc(signals)
        assert len(result.by_direction) == 1
        assert result.by_direction[0].direction == "LONG"

    def test_only_short(self):
        signals = [
            make_signal(
                direction=Direction.SHORT,
                outcome=Outcome.SL,
                outcome_time=START,
                tp_price=Decimal("49000"),
                sl_price=Decimal("54420"),
            ),
        ]
        result = _calc(signals)
        assert len(result.by_direction) == 1
        assert result.by_direction[0].direction == "SHORT"


# ---------------------------------------------------------------------------
# Daily P&L
# ---------------------------------------------------------------------------

class TestDailyPnL:
    """Daily P&L aggregation and cumulative R."""

    def test_same_day_aggregation(self):
        """Multiple signals on the same day should aggregate into one entry."""
        day = datetime(2025, 3, 15, tzinfo=timezone.utc)
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=datetime(2025, 3, 15, 10, 0, tzinfo=timezone.utc),
                signal_time=datetime(2025, 3, 15, 9, 0, tzinfo=timezone.utc),
            ),
            make_signal(
                outcome=Outcome.TP,
                outcome_time=datetime(2025, 3, 15, 14, 0, tzinfo=timezone.utc),
                signal_time=datetime(2025, 3, 15, 13, 0, tzinfo=timezone.utc),
            ),
            make_signal(
                outcome=Outcome.SL,
                outcome_time=datetime(2025, 3, 15, 18, 0, tzinfo=timezone.utc),
                signal_time=datetime(2025, 3, 15, 17, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert len(result.daily_pnl) == 1
        entry = result.daily_pnl[0]
        assert entry.date == "2025-03-15"
        assert entry.wins == 2
        assert entry.losses == 1
        # daily_r = 2 * 1.0 - 1 * 4.42 = -2.42
        assert entry.daily_r == pytest.approx(-2.42, abs=0.01)

    def test_cumulative_r(self):
        """Cumulative R should accumulate across days."""
        signals = [
            # Day 1: 1 TP -> +1.0R
            make_signal(
                outcome=Outcome.TP,
                outcome_time=datetime(2025, 3, 10, 12, 0, tzinfo=timezone.utc),
                signal_time=datetime(2025, 3, 10, 10, 0, tzinfo=timezone.utc),
            ),
            # Day 2: 1 SL -> -4.42R
            make_signal(
                outcome=Outcome.SL,
                outcome_time=datetime(2025, 3, 11, 12, 0, tzinfo=timezone.utc),
                signal_time=datetime(2025, 3, 11, 10, 0, tzinfo=timezone.utc),
            ),
            # Day 3: 1 TP -> +1.0R
            make_signal(
                outcome=Outcome.TP,
                outcome_time=datetime(2025, 3, 12, 12, 0, tzinfo=timezone.utc),
                signal_time=datetime(2025, 3, 12, 10, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert len(result.daily_pnl) == 3

        assert result.daily_pnl[0].date == "2025-03-10"
        assert result.daily_pnl[0].daily_r == pytest.approx(1.0)
        assert result.daily_pnl[0].cumulative_r == pytest.approx(1.0)

        assert result.daily_pnl[1].date == "2025-03-11"
        assert result.daily_pnl[1].daily_r == pytest.approx(-4.42)
        assert result.daily_pnl[1].cumulative_r == pytest.approx(1.0 - 4.42, abs=0.01)

        assert result.daily_pnl[2].date == "2025-03-12"
        assert result.daily_pnl[2].daily_r == pytest.approx(1.0)
        assert result.daily_pnl[2].cumulative_r == pytest.approx(1.0 - 4.42 + 1.0, abs=0.01)

    def test_active_excluded(self):
        """ACTIVE signals must not appear in daily P&L."""
        signals = [
            make_signal(
                outcome=Outcome.ACTIVE,
                signal_time=datetime(2025, 3, 15, 9, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert result.daily_pnl == []

    def test_sorted_by_date(self):
        """Daily entries should be sorted chronologically."""
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=datetime(2025, 5, 20, 12, 0, tzinfo=timezone.utc),
                signal_time=datetime(2025, 5, 20, 10, 0, tzinfo=timezone.utc),
            ),
            make_signal(
                outcome=Outcome.SL,
                outcome_time=datetime(2025, 1, 5, 12, 0, tzinfo=timezone.utc),
                signal_time=datetime(2025, 1, 5, 10, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        dates = [entry.date for entry in result.daily_pnl]
        assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# MAE / MFE distribution
# ---------------------------------------------------------------------------

class TestMaeMfe:
    """MAE/MFE distributions for TP and SL groups."""

    def test_no_resolved_signals(self):
        """Only active signals -> no MAE/MFE stats."""
        signals = [make_signal(outcome=Outcome.ACTIVE)]
        result = _calc(signals)
        assert result.mae_mfe == {}

    def test_tp_group_only(self):
        """Only TP signals -> only 'tp' key in mae_mfe."""
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                mae_ratio=Decimal("0.2"),
                mfe_ratio=Decimal("1.0"),
            ),
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
                mae_ratio=Decimal("0.4"),
                mfe_ratio=Decimal("1.2"),
            ),
        ]
        result = _calc(signals)
        assert "tp" in result.mae_mfe
        assert "sl" not in result.mae_mfe
        tp = result.mae_mfe["tp"]
        assert tp.count == 2
        assert tp.avg_mae == pytest.approx(0.3, abs=0.01)
        assert tp.avg_mfe == pytest.approx(1.1, abs=0.01)

    def test_sl_group_only(self):
        """Only SL signals -> only 'sl' key in mae_mfe."""
        signals = [
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                mae_ratio=Decimal("1.0"),
                mfe_ratio=Decimal("0.3"),
            ),
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
                mae_ratio=Decimal("1.2"),
                mfe_ratio=Decimal("0.5"),
            ),
        ]
        result = _calc(signals)
        assert "sl" in result.mae_mfe
        assert "tp" not in result.mae_mfe
        sl = result.mae_mfe["sl"]
        assert sl.count == 2
        assert sl.avg_mae == pytest.approx(1.1, abs=0.01)
        assert sl.avg_mfe == pytest.approx(0.4, abs=0.01)

    def test_both_groups(self):
        """TP and SL signals -> both keys present."""
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
                mae_ratio=Decimal("0.1"),
                mfe_ratio=Decimal("1.0"),
            ),
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
                mae_ratio=Decimal("1.5"),
                mfe_ratio=Decimal("0.2"),
            ),
        ]
        result = _calc(signals)
        assert "tp" in result.mae_mfe
        assert "sl" in result.mae_mfe


# ---------------------------------------------------------------------------
# BacktestResult metadata
# ---------------------------------------------------------------------------

class TestBacktestResultMetadata:
    """Verify metadata fields are set correctly."""

    def test_metadata(self):
        signals = [make_signal(outcome=Outcome.TP, outcome_time=START)]
        result = _calc(signals)
        assert result.start_date == START
        assert result.end_date == END
        assert result.symbols == SYMBOLS
        assert result.timeframes == TIMEFRAMES
        assert result.signals is signals

    def test_total_signals_count(self):
        signals = [
            make_signal(
                outcome=Outcome.TP,
                outcome_time=START,
            ),
            make_signal(
                outcome=Outcome.SL,
                outcome_time=START,
                signal_time=datetime(2025, 6, 1, 13, 0, tzinfo=timezone.utc),
            ),
            make_signal(
                outcome=Outcome.ACTIVE,
                signal_time=datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc),
            ),
        ]
        result = _calc(signals)
        assert result.total_signals == 3
        assert result.wins + result.losses + result.active == 3


# ---------------------------------------------------------------------------
# SymbolStats / TimeframeStats / DirectionStats win_rate property
# ---------------------------------------------------------------------------

class TestStatsWinRateProperty:
    """Unit-test the win_rate property on the dataclass helpers."""

    def test_symbol_stats_win_rate_no_resolved(self):
        s = SymbolStats(symbol="X", total=2, wins=0, losses=0, active=2)
        assert s.win_rate == 0.0

    def test_symbol_stats_win_rate(self):
        s = SymbolStats(symbol="X", total=4, wins=3, losses=1)
        assert s.win_rate == pytest.approx(75.0)

    def test_timeframe_stats_win_rate(self):
        t = TimeframeStats(timeframe="5m", total=10, wins=7, losses=3)
        assert t.win_rate == pytest.approx(70.0)

    def test_direction_stats_win_rate_all_wins(self):
        d = DirectionStats(direction="LONG", total=5, wins=5, losses=0)
        assert d.win_rate == pytest.approx(100.0)

    def test_direction_stats_win_rate_all_losses(self):
        d = DirectionStats(direction="SHORT", total=5, wins=0, losses=5)
        assert d.win_rate == pytest.approx(0.0)
