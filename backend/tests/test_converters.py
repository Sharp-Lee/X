"""Tests for model converters."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.models import (
    Kline,
    KlineBuffer,
    AggTrade,
    SignalRecord,
    Direction,
    Outcome,
    FastKline,
    FastTrade,
    FastSignal,
    FastKlineBuffer,
    DIRECTION_LONG,
    DIRECTION_SHORT,
)
from app.models.converters import (
    kline_to_fast,
    fast_to_kline,
    kline_buffer_to_fast,
    fast_to_kline_buffer,
    aggtrade_to_fast,
    fast_to_aggtrade,
    signal_to_fast,
    fast_to_signal,
    datetime_to_timestamp,
    timestamp_to_datetime,
)


class TestTimestampConversion:
    """Tests for timestamp conversion helpers."""

    def test_datetime_to_timestamp(self):
        """Test datetime to timestamp conversion."""
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ts = datetime_to_timestamp(dt)
        assert ts == 1704110400.0

    def test_timestamp_to_datetime(self):
        """Test timestamp to datetime conversion."""
        ts = 1704110400.0
        dt = timestamp_to_datetime(ts)
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 1
        assert dt.hour == 12

    def test_roundtrip(self):
        """Test roundtrip conversion preserves data."""
        original = datetime(2024, 6, 15, 10, 30, 45, tzinfo=timezone.utc)
        ts = datetime_to_timestamp(original)
        recovered = timestamp_to_datetime(ts)
        assert original == recovered


class TestKlineConversion:
    """Tests for Kline conversion."""

    def test_kline_to_fast(self):
        """Test Pydantic Kline to FastKline conversion."""
        kline = Kline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            open=Decimal("42000.50"),
            high=Decimal("42500.00"),
            low=Decimal("41800.25"),
            close=Decimal("42300.75"),
            volume=Decimal("1000.5"),
            is_closed=True,
        )

        fast = kline_to_fast(kline)

        assert fast.symbol == "BTCUSDT"
        assert fast.timeframe == "5m"
        assert fast.timestamp == 1704110400.0
        assert fast.open == 42000.50
        assert fast.high == 42500.00
        assert fast.low == 41800.25
        assert fast.close == 42300.75
        assert fast.volume == 1000.5
        assert fast.is_closed is True

    def test_fast_to_kline(self):
        """Test FastKline to Pydantic Kline conversion."""
        fast = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=1704110400.0,
            open=42000.50,
            high=42500.00,
            low=41800.25,
            close=42300.75,
            volume=1000.5,
            is_closed=True,
        )

        kline = fast_to_kline(fast)

        assert kline.symbol == "BTCUSDT"
        assert kline.timeframe == "5m"
        assert kline.timestamp == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert kline.open == Decimal("42000.5")
        assert kline.close == Decimal("42300.75")
        assert kline.is_closed is True

    def test_kline_roundtrip(self):
        """Test roundtrip conversion preserves data."""
        original = Kline(
            symbol="ETHUSDT",
            timeframe="1h",
            timestamp=datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc),
            open=Decimal("3000.00"),
            high=Decimal("3050.00"),
            low=Decimal("2980.00"),
            close=Decimal("3025.00"),
            volume=Decimal("500.0"),
        )

        fast = kline_to_fast(original)
        recovered = fast_to_kline(fast)

        assert recovered.symbol == original.symbol
        assert recovered.timeframe == original.timeframe
        assert recovered.timestamp == original.timestamp
        # Note: float precision may cause minor differences
        assert abs(float(recovered.close) - float(original.close)) < 0.01


class TestKlineBufferConversion:
    """Tests for KlineBuffer conversion."""

    def test_buffer_roundtrip(self):
        """Test roundtrip conversion of buffer."""
        buffer = KlineBuffer(symbol="BTCUSDT", timeframe="5m", max_size=100)

        for i in range(3):
            kline = Kline(
                symbol="BTCUSDT",
                timeframe="5m",
                timestamp=datetime(2024, 1, 1, 12, i * 5, 0, tzinfo=timezone.utc),
                open=Decimal(str(42000 + i)),
                high=Decimal(str(42100 + i)),
                low=Decimal(str(41900 + i)),
                close=Decimal(str(42050 + i)),
                volume=Decimal("100.0"),
            )
            buffer.add(kline)

        fast_buffer = kline_buffer_to_fast(buffer)
        recovered = fast_to_kline_buffer(fast_buffer)

        assert len(recovered) == 3
        assert recovered.symbol == buffer.symbol
        assert recovered.timeframe == buffer.timeframe


class TestAggTradeConversion:
    """Tests for AggTrade conversion."""

    def test_aggtrade_to_fast(self):
        """Test Pydantic AggTrade to FastTrade conversion."""
        trade = AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=123456789,
            price=Decimal("42000.50"),
            quantity=Decimal("0.5"),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            is_buyer_maker=False,
        )

        fast = aggtrade_to_fast(trade)

        assert fast.symbol == "BTCUSDT"
        assert fast.agg_trade_id == 123456789
        assert fast.price == 42000.50
        assert fast.quantity == 0.5
        assert fast.timestamp == 1704110400.0
        assert fast.is_buyer_maker is False

    def test_fast_to_aggtrade(self):
        """Test FastTrade to Pydantic AggTrade conversion."""
        fast = FastTrade(
            symbol="BTCUSDT",
            agg_trade_id=123456789,
            price=42000.50,
            quantity=0.5,
            timestamp=1704110400.0,
            is_buyer_maker=True,
        )

        trade = fast_to_aggtrade(fast)

        assert trade.symbol == "BTCUSDT"
        assert trade.agg_trade_id == 123456789
        assert trade.price == Decimal("42000.5")
        assert trade.quantity == Decimal("0.5")
        assert trade.is_buyer_maker is True

    def test_aggtrade_roundtrip(self):
        """Test roundtrip conversion preserves data."""
        original = AggTrade(
            symbol="ETHUSDT",
            agg_trade_id=987654321,
            price=Decimal("3000.25"),
            quantity=Decimal("1.5"),
            timestamp=datetime(2024, 6, 15, 10, 30, 45, tzinfo=timezone.utc),
            is_buyer_maker=True,
        )

        fast = aggtrade_to_fast(original)
        recovered = fast_to_aggtrade(fast)

        assert recovered.symbol == original.symbol
        assert recovered.agg_trade_id == original.agg_trade_id
        assert recovered.timestamp == original.timestamp
        assert recovered.is_buyer_maker == original.is_buyer_maker


class TestSignalConversion:
    """Tests for SignalRecord conversion."""

    def test_signal_to_fast_long(self):
        """Test Pydantic SignalRecord to FastSignal conversion (LONG)."""
        signal = SignalRecord(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("42000.00"),
            tp_price=Decimal("42400.00"),
            sl_price=Decimal("41800.00"),
            streak_at_signal=3,
            mae_ratio=Decimal("0.5"),
            mfe_ratio=Decimal("1.0"),
        )

        fast = signal_to_fast(signal)

        assert fast.id == "test-123"
        assert fast.symbol == "BTCUSDT"
        assert fast.direction == DIRECTION_LONG
        assert fast.entry_price == 42000.0
        assert fast.tp_price == 42400.0
        assert fast.sl_price == 41800.0
        assert fast.streak_at_signal == 3
        assert fast.mae_ratio == 0.5
        assert fast.mfe_ratio == 1.0
        assert fast.outcome == "active"
        assert fast.outcome_time is None
        assert fast.outcome_price is None

    def test_signal_to_fast_short(self):
        """Test Pydantic SignalRecord to FastSignal conversion (SHORT)."""
        signal = SignalRecord(
            id="test-456",
            symbol="ETHUSDT",
            timeframe="1h",
            signal_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            direction=Direction.SHORT,
            entry_price=Decimal("3000.00"),
            tp_price=Decimal("2900.00"),
            sl_price=Decimal("3050.00"),
        )

        fast = signal_to_fast(signal)

        assert fast.direction == DIRECTION_SHORT

    def test_signal_to_fast_with_outcome(self):
        """Test conversion with outcome data."""
        signal = SignalRecord(
            id="test-789",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("42000.00"),
            tp_price=Decimal("42400.00"),
            sl_price=Decimal("41800.00"),
            outcome=Outcome.TP,
            outcome_time=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
            outcome_price=Decimal("42400.00"),
        )

        fast = signal_to_fast(signal)

        assert fast.outcome == "tp"
        assert fast.outcome_time == 1704114000.0
        assert fast.outcome_price == 42400.0

    def test_fast_to_signal_long(self):
        """Test FastSignal to Pydantic SignalRecord conversion (LONG)."""
        fast = FastSignal(
            id="test-123",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704110400.0,
            direction=DIRECTION_LONG,
            entry_price=42000.0,
            tp_price=42400.0,
            sl_price=41800.0,
            streak_at_signal=2,
            mae_ratio=0.25,
            mfe_ratio=0.75,
        )

        signal = fast_to_signal(fast)

        assert signal.id == "test-123"
        assert signal.direction == Direction.LONG
        assert signal.entry_price == Decimal("42000.0")
        assert signal.outcome == Outcome.ACTIVE

    def test_fast_to_signal_with_outcome(self):
        """Test conversion with outcome data."""
        fast = FastSignal(
            id="test-789",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=1704110400.0,
            direction=DIRECTION_LONG,
            entry_price=42000.0,
            tp_price=42400.0,
            sl_price=41800.0,
            outcome="sl",
            outcome_time=1704114000.0,
            outcome_price=41800.0,
        )

        signal = fast_to_signal(fast)

        assert signal.outcome == Outcome.SL
        assert signal.outcome_time == datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
        assert signal.outcome_price == Decimal("41800.0")

    def test_signal_roundtrip(self):
        """Test roundtrip conversion preserves data."""
        original = SignalRecord(
            id="roundtrip-test",
            symbol="SOLUSDT",
            timeframe="15m",
            signal_time=datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc),
            direction=Direction.SHORT,
            entry_price=Decimal("150.50"),
            tp_price=Decimal("145.00"),
            sl_price=Decimal("153.00"),
            streak_at_signal=-2,
            mae_ratio=Decimal("0.33"),
            mfe_ratio=Decimal("0.66"),
            outcome=Outcome.TP,
            outcome_time=datetime(2024, 6, 15, 11, 0, 0, tzinfo=timezone.utc),
            outcome_price=Decimal("145.00"),
        )

        fast = signal_to_fast(original)
        recovered = fast_to_signal(fast)

        assert recovered.id == original.id
        assert recovered.symbol == original.symbol
        assert recovered.direction == original.direction
        assert recovered.outcome == original.outcome
        assert recovered.signal_time == original.signal_time
        assert recovered.outcome_time == original.outcome_time


class TestPrecisionHandling:
    """Tests for precision handling in conversions."""

    def test_price_precision_preserved(self):
        """Test that price precision is reasonably preserved."""
        # Create a kline with specific precision
        kline = Kline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            open=Decimal("42000.12345678"),
            high=Decimal("42500.87654321"),
            low=Decimal("41800.11111111"),
            close=Decimal("42300.99999999"),
            volume=Decimal("1000.123"),
        )

        fast = kline_to_fast(kline)
        recovered = fast_to_kline(fast)

        # Float precision allows about 15-16 significant digits
        # For prices around 42000, we should preserve at least 8 decimal places
        assert abs(float(recovered.open) - 42000.12345678) < 1e-6
        assert abs(float(recovered.close) - 42300.99999999) < 1e-6

    def test_small_ratio_precision(self):
        """Test precision for small ratio values."""
        signal = SignalRecord(
            id="precision-test",
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("42000.00"),
            tp_price=Decimal("42400.00"),
            sl_price=Decimal("41800.00"),
            mae_ratio=Decimal("0.00123456"),
            mfe_ratio=Decimal("0.00654321"),
        )

        fast = signal_to_fast(signal)
        recovered = fast_to_signal(fast)

        # Small ratios should be preserved with reasonable precision
        assert abs(float(recovered.mae_ratio) - 0.00123456) < 1e-7
        assert abs(float(recovered.mfe_ratio) - 0.00654321) < 1e-7
