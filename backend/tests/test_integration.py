"""End-to-end integration tests.

Tests the complete data flow and system behavior:
1. Data flow: Trade data → Signal generation → Position tracking → Outcome
2. Graceful degradation: System works without Redis
3. Data consistency: Redis and DB stay in sync
"""

import asyncio
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from app.models import (
    AggTrade,
    Direction,
    Kline,
    KlineBuffer,
    Outcome,
    SignalRecord,
    StreakTracker,
    FastSignal,
    FastTrade,
)
from app.models import (
    signal_to_fast,
    fast_to_signal,
    aggtrade_to_fast,
    kline_to_fast,
)
from app.services.position_tracker import PositionTracker
from core.signal_generator import SignalGenerator


class TestDataFlowIntegration:
    """Test complete data flow from trade to outcome."""

    @pytest.fixture
    def sample_klines(self):
        """Create sample klines for indicator calculation."""
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        klines = []

        # Generate 60 klines with uptrend pattern
        price = Decimal("40000")
        for i in range(60):
            ts = base_time.replace(minute=i)
            # Slight uptrend
            open_price = price
            close_price = price + Decimal("10")
            high = close_price + Decimal("20")
            low = open_price - Decimal("20")

            klines.append(Kline(
                symbol="BTCUSDT",
                timeframe="1m",
                timestamp=ts,
                open=open_price,
                high=high,
                low=low,
                close=close_price,
                volume=Decimal("100"),
                is_closed=True,
            ))
            price = close_price

        return klines

    @pytest.fixture
    def kline_buffer(self, sample_klines):
        """Create a kline buffer with sample data."""
        buffer = KlineBuffer(symbol="BTCUSDT", timeframe="1m")
        for kline in sample_klines:
            buffer.add(kline)
        return buffer

    @pytest.mark.asyncio
    async def test_signal_to_position_flow(self):
        """Test flow: Signal created → Added to position tracker → MAE updates."""
        # Create a signal
        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49500"),
            streak_at_signal=0,
        )

        # Create position tracker with mocked repo
        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        # Mock cache operations
        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        # Add signal to tracker
                        await tracker.add_signal(signal)

                        assert tracker.active_count == 1

                        # Process a trade that moves price against position (MAE update)
                        trade1 = AggTrade(
                            symbol="BTCUSDT",
                            agg_trade_id=1,
                            price=Decimal("49800"),  # Price moved down
                            quantity=Decimal("1"),
                            timestamp=datetime.now(timezone.utc),
                            is_buyer_maker=False,
                        )

                        await tracker.process_trade(trade1)

                        # Check MAE was updated
                        status = tracker.get_signal_status(signal.id)
                        assert status is not None
                        assert status["mae_ratio"] > 0  # Adverse movement recorded

    @pytest.mark.asyncio
    async def test_signal_outcome_tp_hit(self):
        """Test flow: Signal → Trade hits TP → Outcome recorded."""
        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50100"),  # Close TP for quick test
            sl_price=Decimal("49500"),
            streak_at_signal=0,
        )

        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        outcome_received = []

        async def on_outcome(sig, outcome):
            outcome_received.append((sig, outcome))

        tracker.on_outcome(on_outcome)

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        await tracker.add_signal(signal)

                        # Trade that hits TP
                        trade_tp = AggTrade(
                            symbol="BTCUSDT",
                            agg_trade_id=2,
                            price=Decimal("50150"),  # Above TP
                            quantity=Decimal("1"),
                            timestamp=datetime.now(timezone.utc),
                            is_buyer_maker=False,
                        )

                        await tracker.process_trade(trade_tp)

                        # Signal should be removed
                        assert tracker.active_count == 0

                        # Outcome callback should have been called
                        assert len(outcome_received) == 1
                        assert outcome_received[0][1] == Outcome.TP

    @pytest.mark.asyncio
    async def test_signal_outcome_sl_hit(self):
        """Test flow: Signal → Trade hits SL → Outcome recorded."""
        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49900"),  # Close SL for quick test
            streak_at_signal=0,
        )

        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        outcome_received = []

        async def on_outcome(sig, outcome):
            outcome_received.append((sig, outcome))

        tracker.on_outcome(on_outcome)

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        await tracker.add_signal(signal)

                        # Trade that hits SL
                        trade_sl = AggTrade(
                            symbol="BTCUSDT",
                            agg_trade_id=3,
                            price=Decimal("49850"),  # Below SL
                            quantity=Decimal("1"),
                            timestamp=datetime.now(timezone.utc),
                            is_buyer_maker=False,
                        )

                        await tracker.process_trade(trade_sl)

                        # Signal should be removed
                        assert tracker.active_count == 0

                        # Outcome callback should have been called
                        assert len(outcome_received) == 1
                        assert outcome_received[0][1] == Outcome.SL


class TestGracefulDegradation:
    """Test system behavior when Redis is unavailable."""

    @pytest.mark.asyncio
    async def test_position_tracker_without_cache(self):
        """Test position tracker works without Redis cache."""
        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49500"),
            streak_at_signal=0,
        )

        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        # Simulate cache unavailable
        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=False):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=False):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=False):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        # Should still work
                        await tracker.add_signal(signal)
                        assert tracker.active_count == 1

                        # Process trade
                        trade = AggTrade(
                            symbol="BTCUSDT",
                            agg_trade_id=1,
                            price=Decimal("49800"),
                            quantity=Decimal("1"),
                            timestamp=datetime.now(timezone.utc),
                            is_buyer_maker=False,
                        )

                        await tracker.process_trade(trade)

                        # MAE should still update
                        status = tracker.get_signal_status(signal.id)
                        assert status is not None
                        assert status["mae_ratio"] > 0

    @pytest.mark.asyncio
    async def test_load_from_db_when_cache_empty(self):
        """Test loading signals from DB when cache is empty."""
        db_signals = [
            SignalRecord(
                id="db-signal-1",
                symbol="BTCUSDT",
                timeframe="1m",
                signal_time=datetime.now(timezone.utc),
                direction=Direction.LONG,
                entry_price=Decimal("50000"),
                tp_price=Decimal("50500"),
                sl_price=Decimal("49500"),
                streak_at_signal=0,
            )
        ]

        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=db_signals)

        # Cache returns empty, should fall back to DB
        with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
            with patch('app.storage.signal_cache.sync_from_db', new_callable=AsyncMock, return_value=1):
                await tracker.load_active_signals()

                assert tracker.active_count == 1
                assert tracker._cache_misses == 1  # Should record cache miss


class TestDataConsistency:
    """Test data consistency between hot path and cold path."""

    def test_signal_conversion_preserves_data(self):
        """Test that signal conversion preserves all data accurately."""
        original = SignalRecord(
            id="test-signal-123",
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000.50"),
            tp_price=Decimal("50500.75"),
            sl_price=Decimal("49500.25"),
            streak_at_signal=3,
            mae_ratio=Decimal("0.15"),
            mfe_ratio=Decimal("0.25"),
            outcome=Outcome.ACTIVE,
        )

        # Convert to fast and back
        fast = signal_to_fast(original)
        restored = fast_to_signal(fast)

        assert restored.id == original.id
        assert restored.symbol == original.symbol
        assert restored.timeframe == original.timeframe
        assert restored.direction == original.direction
        assert restored.streak_at_signal == original.streak_at_signal
        assert restored.outcome == original.outcome

        # Check numeric precision (allow small float errors)
        assert abs(float(restored.entry_price) - float(original.entry_price)) < 0.01
        assert abs(float(restored.tp_price) - float(original.tp_price)) < 0.01
        assert abs(float(restored.sl_price) - float(original.sl_price)) < 0.01

    def test_trade_conversion_preserves_data(self):
        """Test that trade conversion preserves all data accurately."""
        original = AggTrade(
            symbol="ETHUSDT",
            agg_trade_id=123456789,
            price=Decimal("3000.50"),
            quantity=Decimal("1.5"),
            timestamp=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            is_buyer_maker=True,
        )

        # Convert to fast
        fast = aggtrade_to_fast(original)

        assert fast.symbol == original.symbol
        assert fast.agg_trade_id == original.agg_trade_id
        assert fast.is_buyer_maker == original.is_buyer_maker
        assert abs(fast.price - float(original.price)) < 0.01
        assert abs(fast.quantity - float(original.quantity)) < 0.01

    def test_fast_signal_mae_mfe_tracking(self):
        """Test MAE/MFE tracking in FastSignal matches cold path logic."""
        # Create both versions
        cold_signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50500"),
            sl_price=Decimal("49500"),
            streak_at_signal=0,
        )

        fast_signal = signal_to_fast(cold_signal)

        # Apply same price movements
        test_prices = [49800, 49900, 50100, 50200, 49700]

        for price in test_prices:
            cold_signal.update_mae(Decimal(str(price)))
            fast_signal.update_mae(float(price))

        # Compare results (allow small float tolerance)
        assert abs(fast_signal.mae_ratio - float(cold_signal.mae_ratio)) < 0.001
        assert abs(fast_signal.mfe_ratio - float(cold_signal.mfe_ratio)) < 0.001


class TestMultiSymbolConcurrency:
    """Test handling multiple symbols concurrently."""

    @pytest.mark.asyncio
    async def test_multiple_symbols_tracking(self):
        """Test tracking signals for multiple symbols simultaneously."""
        signals = [
            SignalRecord(
                symbol="BTCUSDT",
                timeframe="1m",
                signal_time=datetime.now(timezone.utc),
                direction=Direction.LONG,
                entry_price=Decimal("50000"),
                tp_price=Decimal("50500"),
                sl_price=Decimal("49500"),
                streak_at_signal=0,
            ),
            SignalRecord(
                symbol="ETHUSDT",
                timeframe="1m",
                signal_time=datetime.now(timezone.utc),
                direction=Direction.SHORT,
                entry_price=Decimal("3000"),
                tp_price=Decimal("2950"),
                sl_price=Decimal("3050"),
                streak_at_signal=0,
            ),
            SignalRecord(
                symbol="SOLUSDT",
                timeframe="1m",
                signal_time=datetime.now(timezone.utc),
                direction=Direction.LONG,
                entry_price=Decimal("100"),
                tp_price=Decimal("105"),
                sl_price=Decimal("95"),
                streak_at_signal=0,
            ),
        ]

        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        # Add all signals
                        for sig in signals:
                            await tracker.add_signal(sig)

                        assert tracker.active_count == 3

                        # Process trades for different symbols
                        trades = [
                            AggTrade(symbol="BTCUSDT", agg_trade_id=1, price=Decimal("49900"),
                                    quantity=Decimal("1"), timestamp=datetime.now(timezone.utc),
                                    is_buyer_maker=False),
                            AggTrade(symbol="ETHUSDT", agg_trade_id=2, price=Decimal("3010"),
                                    quantity=Decimal("1"), timestamp=datetime.now(timezone.utc),
                                    is_buyer_maker=False),
                            AggTrade(symbol="SOLUSDT", agg_trade_id=3, price=Decimal("101"),
                                    quantity=Decimal("1"), timestamp=datetime.now(timezone.utc),
                                    is_buyer_maker=False),
                        ]

                        for trade in trades:
                            await tracker.process_trade(trade)

                        # All signals should still be active
                        assert tracker.active_count == 3

                        # Check each symbol's status
                        btc_signals = await tracker.get_active_signals("BTCUSDT")
                        eth_signals = await tracker.get_active_signals("ETHUSDT")
                        sol_signals = await tracker.get_active_signals("SOLUSDT")

                        assert len(btc_signals) == 1
                        assert len(eth_signals) == 1
                        assert len(sol_signals) == 1

    @pytest.mark.asyncio
    async def test_trades_only_affect_matching_symbol(self):
        """Test that trades only affect signals of the same symbol."""
        btc_signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="1m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=Decimal("50000"),
            tp_price=Decimal("50100"),
            sl_price=Decimal("49900"),
            streak_at_signal=0,
        )

        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        await tracker.add_signal(btc_signal)

                        # Send ETH trade that would hit TP if it was BTC
                        eth_trade = AggTrade(
                            symbol="ETHUSDT",
                            agg_trade_id=1,
                            price=Decimal("50200"),  # Would hit BTC TP
                            quantity=Decimal("1"),
                            timestamp=datetime.now(timezone.utc),
                            is_buyer_maker=False,
                        )

                        await tracker.process_trade(eth_trade)

                        # BTC signal should still be active
                        assert tracker.active_count == 1
                        status = tracker.get_signal_status(btc_signal.id)
                        assert status["outcome"] == "active"
