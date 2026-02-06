"""Tests for position tracking."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from app.models import AggTrade, Direction, Outcome, SignalRecord
from app.services.position_tracker import PositionTracker


class TestPositionTracker:
    """Tests for PositionTracker."""

    @pytest.fixture
    def mock_repo(self):
        """Create a mock signal repository."""
        repo = MagicMock()
        repo.update_outcome = AsyncMock()
        repo.get_active = AsyncMock(return_value=[])
        repo.save = AsyncMock()
        return repo

    @pytest.fixture
    def tracker(self, mock_repo):
        """Create a position tracker instance with mock repo."""
        return PositionTracker(update_interval=0, signal_repo=mock_repo)

    @pytest.fixture
    def long_signal(self):
        """Create a test LONG signal."""
        return SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49000"),
        )

    @pytest.fixture
    def short_signal(self):
        """Create a test SHORT signal."""
        return SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.SHORT,
            entry_price=Decimal("50000"),
            tp_price=Decimal("49500"),
            sl_price=Decimal("51000"),
        )

    @pytest.mark.asyncio
    async def test_add_signal(self, tracker, long_signal):
        """Test adding a signal to tracker."""
        await tracker.add_signal(long_signal)

        signals = await tracker.get_active_signals("BTCUSDT")
        assert len(signals) == 1
        assert signals[0].id == long_signal.id

    @pytest.mark.asyncio
    async def test_process_trade_mae_update(self, tracker, long_signal):
        """Test MAE update from trade."""
        await tracker.add_signal(long_signal)

        # Price moves adversely
        trade = AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=1,
            price=Decimal("49500"),  # -500 from entry
            quantity=Decimal("1"),
            timestamp=datetime.now(timezone.utc),
            is_buyer_maker=False,
        )

        await tracker.process_trade(trade)

        status = tracker.get_signal_status(long_signal.id)
        assert status is not None
        # MAE should be 500/1000 = 0.5 (50% of risk)
        assert status["mae_ratio"] == pytest.approx(0.5, rel=0.01)

    @pytest.mark.asyncio
    async def test_process_trade_tp_hit(self, tracker, long_signal):
        """Test TP hit detection."""
        await tracker.add_signal(long_signal)

        # Price hits TP
        trade = AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=1,
            price=Decimal("50500"),  # TP price
            quantity=Decimal("1"),
            timestamp=datetime.now(timezone.utc),
            is_buyer_maker=False,
        )

        await tracker.process_trade(trade)

        # Signal should be removed from active
        status = tracker.get_signal_status(long_signal.id)
        assert status is None

        signals = await tracker.get_active_signals("BTCUSDT")
        assert len(signals) == 0

    @pytest.mark.asyncio
    async def test_process_trade_sl_hit(self, tracker, long_signal):
        """Test SL hit detection."""
        await tracker.add_signal(long_signal)

        # Price hits SL
        trade = AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=1,
            price=Decimal("49000"),  # SL price
            quantity=Decimal("1"),
            timestamp=datetime.now(timezone.utc),
            is_buyer_maker=False,
        )

        await tracker.process_trade(trade)

        # Signal should be removed from active
        signals = await tracker.get_active_signals("BTCUSDT")
        assert len(signals) == 0

    @pytest.mark.asyncio
    async def test_multiple_symbols(self, tracker):
        """Test tracking signals for multiple symbols."""
        btc_signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49000"),
        )

        eth_signal = SignalRecord(
            symbol="ETHUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.SHORT,
            entry_price=Decimal("3000"),
            tp_price=Decimal("2950"),
            sl_price=Decimal("3100"),
        )

        await tracker.add_signal(btc_signal)
        await tracker.add_signal(eth_signal)

        assert len(await tracker.get_active_signals("BTCUSDT")) == 1
        assert len(await tracker.get_active_signals("ETHUSDT")) == 1
        assert len(await tracker.get_active_signals()) == 2

    @pytest.mark.asyncio
    async def test_short_position_tracking(self, tracker, short_signal):
        """Test SHORT position tracking."""
        await tracker.add_signal(short_signal)

        # Price moves adversely (up for SHORT)
        trade = AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=1,
            price=Decimal("50500"),  # +500 from entry (adverse for SHORT)
            quantity=Decimal("1"),
            timestamp=datetime.now(timezone.utc),
            is_buyer_maker=False,
        )

        await tracker.process_trade(trade)

        status = tracker.get_signal_status(short_signal.id)
        assert status is not None
        # MAE should be 500/1000 = 0.5
        assert status["mae_ratio"] == pytest.approx(0.5, rel=0.01)
