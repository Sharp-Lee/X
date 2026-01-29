"""Tests for signal detection and models."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal

from app.models import (
    Direction,
    Kline,
    KlineBuffer,
    Outcome,
    SignalRecord,
    StreakTracker,
)
from app.services.signal_generator import LevelManager


class TestSignalRecord:
    """Tests for SignalRecord model."""

    def test_signal_creation(self):
        """Test creating a signal record."""
        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49000"),
        )

        assert signal.id is not None
        assert signal.outcome == Outcome.ACTIVE
        assert signal.mae_ratio == Decimal("0")
        assert signal.mfe_ratio == Decimal("0")

    def test_risk_reward_calculation(self):
        """Test risk and reward amount calculation."""
        # LONG position
        long_signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),  # +500
            sl_price=Decimal("49000"),  # -1000
        )

        assert long_signal.risk_amount == Decimal("1000")
        assert long_signal.reward_amount == Decimal("500")

        # SHORT position
        short_signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.SHORT,
            entry_price=Decimal("50000"),
            tp_price=Decimal("49500"),  # +500
            sl_price=Decimal("51000"),  # -1000
        )

        assert short_signal.risk_amount == Decimal("1000")
        assert short_signal.reward_amount == Decimal("500")

    def test_mae_update_long(self):
        """Test MAE update for LONG position."""
        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("51000"),
            sl_price=Decimal("49000"),  # risk = 1000
        )

        # Price moves favorably
        signal.update_mae(Decimal("50500"))
        assert signal.mfe_ratio == Decimal("0.5")  # +500 / 1000

        # Price moves adversely
        signal.update_mae(Decimal("49500"))
        assert signal.mae_ratio == Decimal("0.5")  # -500 / 1000

    def test_check_outcome_tp(self):
        """Test TP hit detection."""
        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49000"),
        )

        now = datetime.now(timezone.utc)

        # Price below TP - no outcome
        assert not signal.check_outcome(Decimal("50400"), now)
        assert signal.outcome == Outcome.ACTIVE

        # Price hits TP
        assert signal.check_outcome(Decimal("50500"), now)
        assert signal.outcome == Outcome.TP
        assert signal.outcome_price == Decimal("50500")

    def test_check_outcome_sl(self):
        """Test SL hit detection."""
        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49000"),
        )

        now = datetime.now(timezone.utc)

        # Price hits SL
        assert signal.check_outcome(Decimal("49000"), now)
        assert signal.outcome == Outcome.SL


class TestStreakTracker:
    """Tests for StreakTracker."""

    def test_win_streak(self):
        """Test win streak tracking."""
        tracker = StreakTracker()

        tracker.record_outcome(Outcome.TP)
        assert tracker.current_streak == 1
        assert tracker.total_wins == 1

        tracker.record_outcome(Outcome.TP)
        assert tracker.current_streak == 2

        tracker.record_outcome(Outcome.TP)
        assert tracker.current_streak == 3

    def test_loss_streak(self):
        """Test loss streak tracking."""
        tracker = StreakTracker()

        tracker.record_outcome(Outcome.SL)
        assert tracker.current_streak == -1
        assert tracker.total_losses == 1

        tracker.record_outcome(Outcome.SL)
        assert tracker.current_streak == -2

    def test_streak_reset(self):
        """Test streak reset on direction change."""
        tracker = StreakTracker()

        # Build win streak
        tracker.record_outcome(Outcome.TP)
        tracker.record_outcome(Outcome.TP)
        assert tracker.current_streak == 2

        # Loss resets to -1
        tracker.record_outcome(Outcome.SL)
        assert tracker.current_streak == -1

        # Win resets to 1
        tracker.record_outcome(Outcome.TP)
        assert tracker.current_streak == 1

    def test_win_rate(self):
        """Test win rate calculation."""
        tracker = StreakTracker()

        tracker.record_outcome(Outcome.TP)
        tracker.record_outcome(Outcome.TP)
        tracker.record_outcome(Outcome.SL)

        assert tracker.win_rate == pytest.approx(0.6667, rel=0.01)


class TestKlineBuffer:
    """Tests for KlineBuffer."""

    def test_add_kline(self):
        """Test adding klines to buffer."""
        buffer = KlineBuffer(symbol="BTCUSDT", timeframe="5m", max_size=5)

        for i in range(3):
            kline = Kline(
                symbol="BTCUSDT",
                timeframe="5m",
                timestamp=datetime(2024, 1, 1, i, 0, tzinfo=timezone.utc),
                open=Decimal("50000"),
                high=Decimal("50100"),
                low=Decimal("49900"),
                close=Decimal("50050"),
                volume=Decimal("100"),
            )
            buffer.add(kline)

        assert len(buffer) == 3

    def test_buffer_max_size(self):
        """Test buffer respects max size."""
        buffer = KlineBuffer(symbol="BTCUSDT", timeframe="5m", max_size=3)

        for i in range(5):
            kline = Kline(
                symbol="BTCUSDT",
                timeframe="5m",
                timestamp=datetime(2024, 1, 1, i, 0, tzinfo=timezone.utc),
                open=Decimal("50000"),
                high=Decimal("50100"),
                low=Decimal("49900"),
                close=Decimal("50050"),
                volume=Decimal("100"),
            )
            buffer.add(kline)

        assert len(buffer) == 3

    def test_get_ohlcv_lists(self):
        """Test getting OHLCV lists from buffer."""
        buffer = KlineBuffer(symbol="BTCUSDT", timeframe="5m")

        kline = Kline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            open=Decimal("50000"),
            high=Decimal("50100"),
            low=Decimal("49900"),
            close=Decimal("50050"),
            volume=Decimal("100"),
        )
        buffer.add(kline)

        assert buffer.get_closes() == [Decimal("50050")]
        assert buffer.get_highs() == [Decimal("50100")]
        assert buffer.get_lows() == [Decimal("49900")]
        assert buffer.get_volumes() == [Decimal("100")]


class TestLevelManager:
    """Tests for LevelManager."""

    def test_get_levels(self):
        """Test support/resistance classification."""
        manager = LevelManager()

        close = Decimal("100")
        fib_382 = Decimal("105")  # Above close -> resistance
        fib_500 = Decimal("95")   # Below close -> support
        fib_618 = Decimal("102")  # Above close -> resistance
        vwap = Decimal("98")      # Below close -> support

        support, resistance = manager.get_levels(
            close, fib_382, fib_500, fib_618, vwap
        )

        assert fib_500 in support
        assert vwap in support
        assert fib_382 in resistance
        assert fib_618 in resistance

    def test_get_nearest_levels(self):
        """Test finding nearest support/resistance."""
        manager = LevelManager()

        close = Decimal("100")
        support = [Decimal("95"), Decimal("90"), Decimal("98")]
        resistance = [Decimal("102"), Decimal("110"), Decimal("105")]

        nearest_support, nearest_resistance = manager.get_nearest_levels(
            close, support, resistance
        )

        assert nearest_support == Decimal("98")  # Closest below
        assert nearest_resistance == Decimal("102")  # Closest above
