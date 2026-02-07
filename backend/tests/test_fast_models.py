"""Tests for hot path (fast) models."""

import pytest
from core.models.fast import (
    FastKline,
    FastTrade,
    FastSignal,
    FastKlineBuffer,
    DIRECTION_LONG,
    DIRECTION_SHORT,
    generate_signal_id,
)


class TestFastKline:
    """Tests for FastKline dataclass."""

    def test_kline_creation(self):
        """Test basic kline creation."""
        kline = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704067200.0,
            open=42000.0,
            high=42500.0,
            low=41800.0,
            close=42300.0,
            volume=1000.0,
        )
        assert kline.symbol == "BTCUSDT"
        assert kline.close == 42300.0
        assert kline.is_closed is True

    def test_kline_bullish(self):
        """Test bullish candle detection."""
        kline = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704067200.0,
            open=42000.0,
            high=42500.0,
            low=41800.0,
            close=42300.0,
            volume=1000.0,
        )
        assert kline.is_bullish is True
        assert kline.is_bearish is False

    def test_kline_bearish(self):
        """Test bearish candle detection."""
        kline = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704067200.0,
            open=42300.0,
            high=42500.0,
            low=41800.0,
            close=42000.0,
            volume=1000.0,
        )
        assert kline.is_bullish is False
        assert kline.is_bearish is True

    def test_kline_body_size(self):
        """Test candle body size calculation."""
        kline = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704067200.0,
            open=42000.0,
            high=42500.0,
            low=41800.0,
            close=42300.0,
            volume=1000.0,
        )
        assert kline.body_size == 300.0
        assert kline.range_size == 700.0


class TestFastTrade:
    """Tests for FastTrade dataclass."""

    def test_trade_creation(self):
        """Test basic trade creation."""
        trade = FastTrade(
            symbol="BTCUSDT",
            agg_trade_id=123456,
            price=42000.0,
            quantity=0.5,
            timestamp=1704067200.0,
            is_buyer_maker=False,
        )
        assert trade.symbol == "BTCUSDT"
        assert trade.price == 42000.0
        assert trade.is_buyer_maker is False


class TestFastSignal:
    """Tests for FastSignal dataclass."""

    def test_signal_creation(self):
        """Test basic signal creation."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_LONG,
            entry_price=42000.0,
            tp_price=42200.0,
            sl_price=41800.0,
        )
        assert signal.symbol == "BTCUSDT"
        assert signal.direction == 1
        assert signal.outcome == "active"
        assert signal.is_active is True

    def test_risk_reward_long(self):
        """Test risk/reward calculation for long position."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_LONG,
            entry_price=42000.0,
            tp_price=42400.0,  # +400
            sl_price=41800.0,  # -200
        )
        assert signal.risk_amount == 200.0
        assert signal.reward_amount == 400.0

    def test_risk_reward_short(self):
        """Test risk/reward calculation for short position."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_SHORT,
            entry_price=42000.0,
            tp_price=41600.0,  # -400
            sl_price=42200.0,  # +200
        )
        assert signal.risk_amount == 200.0
        assert signal.reward_amount == 400.0

    def test_mae_update_long(self):
        """Test MAE update for long position."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_LONG,
            entry_price=42000.0,
            tp_price=42400.0,
            sl_price=41800.0,  # risk = 200
        )

        # Price moves against (down)
        signal.update_mae(41900.0)  # adverse = 100, ratio = 0.5
        assert signal.mae_ratio == 0.5
        assert signal.mfe_ratio == 0.0

        # Price moves in favor (up)
        signal.update_mae(42200.0)  # favorable = 200, ratio = 1.0
        assert signal.mae_ratio == 0.5  # MAE stays the same
        assert signal.mfe_ratio == 1.0

    def test_mae_update_short(self):
        """Test MAE update for short position."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_SHORT,
            entry_price=42000.0,
            tp_price=41600.0,
            sl_price=42200.0,  # risk = 200
        )

        # Price moves against (up)
        signal.update_mae(42100.0)  # adverse = 100, ratio = 0.5
        assert signal.mae_ratio == 0.5
        assert signal.mfe_ratio == 0.0

        # Price moves in favor (down)
        signal.update_mae(41800.0)  # favorable = 200, ratio = 1.0
        assert signal.mae_ratio == 0.5
        assert signal.mfe_ratio == 1.0

    def test_check_outcome_tp_long(self):
        """Test TP hit for long position."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_LONG,
            entry_price=42000.0,
            tp_price=42400.0,
            sl_price=41800.0,
        )

        # Price hits TP
        changed = signal.check_outcome(42400.0, 1704070800.0)
        assert changed is True
        assert signal.outcome == "tp"
        assert signal.outcome_price == 42400.0
        assert signal.is_active is False

    def test_check_outcome_sl_long(self):
        """Test SL hit for long position."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_LONG,
            entry_price=42000.0,
            tp_price=42400.0,
            sl_price=41800.0,
        )

        # Price hits SL
        changed = signal.check_outcome(41800.0, 1704070800.0)
        assert changed is True
        assert signal.outcome == "sl"
        assert signal.outcome_price == 41800.0

    def test_check_outcome_tp_short(self):
        """Test TP hit for short position."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_SHORT,
            entry_price=42000.0,
            tp_price=41600.0,
            sl_price=42200.0,
        )

        # Price hits TP (goes down)
        changed = signal.check_outcome(41600.0, 1704070800.0)
        assert changed is True
        assert signal.outcome == "tp"

    def test_check_outcome_sl_short(self):
        """Test SL hit for short position."""
        signal = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704067200.0,
            direction=DIRECTION_SHORT,
            entry_price=42000.0,
            tp_price=41600.0,
            sl_price=42200.0,
        )

        # Price hits SL (goes up)
        changed = signal.check_outcome(42200.0, 1704070800.0)
        assert changed is True
        assert signal.outcome == "sl"


class TestFastKlineBuffer:
    """Tests for FastKlineBuffer."""

    def test_buffer_add(self):
        """Test adding klines to buffer."""
        buffer = FastKlineBuffer(symbol="BTCUSDT", timeframe="5m")

        kline1 = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704067200.0,
            open=42000.0,
            high=42100.0,
            low=41900.0,
            close=42050.0,
            volume=100.0,
        )
        kline2 = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704067500.0,
            open=42050.0,
            high=42200.0,
            low=42000.0,
            close=42150.0,
            volume=150.0,
        )

        buffer.add(kline1)
        buffer.add(kline2)

        assert len(buffer) == 2
        assert buffer[0].close == 42050.0
        assert buffer[1].close == 42150.0

    def test_buffer_update_same_timestamp(self):
        """Test updating kline with same timestamp."""
        buffer = FastKlineBuffer(symbol="BTCUSDT", timeframe="5m")

        kline1 = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704067200.0,
            open=42000.0,
            high=42100.0,
            low=41900.0,
            close=42050.0,
            volume=100.0,
        )
        buffer.add(kline1)

        # Update with same timestamp but different close
        kline2 = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704067200.0,
            open=42000.0,
            high=42200.0,
            low=41900.0,
            close=42150.0,
            volume=120.0,
        )
        buffer.add(kline2)

        assert len(buffer) == 1
        assert buffer[0].close == 42150.0

    def test_buffer_max_size(self):
        """Test buffer respects max size."""
        buffer = FastKlineBuffer(symbol="BTCUSDT", timeframe="5m", max_size=3)

        for i in range(5):
            kline = FastKline(
                symbol="BTCUSDT",
                timeframe="5m",
                timestamp=1704067200.0 + i * 300,
                open=42000.0,
                high=42100.0,
                low=41900.0,
                close=42000.0 + i * 10,
                volume=100.0,
            )
            buffer.add(kline)

        assert len(buffer) == 3
        # Should have the last 3 klines
        assert buffer[0].close == 42020.0
        assert buffer[1].close == 42030.0
        assert buffer[2].close == 42040.0

    def test_buffer_get_arrays(self):
        """Test getting price arrays from buffer."""
        buffer = FastKlineBuffer(symbol="BTCUSDT", timeframe="5m")

        for i in range(3):
            kline = FastKline(
                symbol="BTCUSDT",
                timeframe="5m",
                timestamp=1704067200.0 + i * 300,
                open=42000.0 + i,
                high=42100.0 + i,
                low=41900.0 + i,
                close=42050.0 + i,
                volume=100.0 + i,
            )
            buffer.add(kline)

        opens = buffer.get_opens()
        highs = buffer.get_highs()
        lows = buffer.get_lows()
        closes = buffer.get_closes()
        volumes = buffer.get_volumes()

        assert opens == [42000.0, 42001.0, 42002.0]
        assert highs == [42100.0, 42101.0, 42102.0]
        assert lows == [41900.0, 41901.0, 41902.0]
        assert closes == [42050.0, 42051.0, 42052.0]
        assert volumes == [100.0, 101.0, 102.0]


class TestGenerateSignalId:
    """Tests for signal ID generation."""

    def test_unique_ids(self):
        """Test that generated IDs are unique."""
        ids = [generate_signal_id() for _ in range(100)]
        assert len(set(ids)) == 100  # All unique
