"""Stress tests for the trading system.

Light versions of stress tests suitable for CI/CD.
"""

import asyncio
import pytest
import time
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from app.models import (
    AggTrade,
    Direction,
    Outcome,
    SignalRecord,
)
from app.services.position_tracker import PositionTracker


class TestHighFrequencyTrades:
    """Test high-frequency trade processing."""

    @pytest.mark.asyncio
    async def test_process_1000_trades(self):
        """System should process 1000 trades without errors."""
        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        # Create a signal
                        signal = SignalRecord(
                            symbol="BTCUSDT",
                            timeframe="1m",
                            signal_time=datetime.now(timezone.utc),
                            direction=Direction.LONG,
                            entry_price=Decimal("50000"),
                            tp_price=Decimal("51000"),  # Wide enough to not hit
                            sl_price=Decimal("49000"),
                            streak_at_signal=0,
                        )
                        await tracker.add_signal(signal)

                        # Process 1000 trades
                        errors = []
                        start = time.perf_counter()

                        for i in range(1000):
                            price = Decimal(str(50000 + (i % 100) - 50))  # Oscillate around entry
                            trade = AggTrade(
                                symbol="BTCUSDT",
                                agg_trade_id=i,
                                price=price,
                                quantity=Decimal("1"),
                                timestamp=datetime.now(timezone.utc),
                                is_buyer_maker=False,
                            )
                            try:
                                await tracker.process_trade(trade)
                            except Exception as e:
                                errors.append(str(e))

                        duration = time.perf_counter() - start

                        assert len(errors) == 0, f"Errors occurred: {errors}"
                        # Should complete in reasonable time (< 1 second for 1000 trades)
                        assert duration < 1.0, f"Too slow: {duration:.2f}s for 1000 trades"

    @pytest.mark.asyncio
    async def test_throughput_above_minimum(self):
        """System should maintain minimum throughput."""
        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        signal = SignalRecord(
                            symbol="BTCUSDT",
                            timeframe="1m",
                            signal_time=datetime.now(timezone.utc),
                            direction=Direction.LONG,
                            entry_price=Decimal("50000"),
                            tp_price=Decimal("51000"),
                            sl_price=Decimal("49000"),
                            streak_at_signal=0,
                        )
                        await tracker.add_signal(signal)

                        num_trades = 500
                        start = time.perf_counter()

                        for i in range(num_trades):
                            trade = AggTrade(
                                symbol="BTCUSDT",
                                agg_trade_id=i,
                                price=Decimal("50000"),
                                quantity=Decimal("1"),
                                timestamp=datetime.now(timezone.utc),
                                is_buyer_maker=False,
                            )
                            await tracker.process_trade(trade)

                        duration = time.perf_counter() - start
                        throughput = num_trades / duration

                        # Should achieve at least 1000 trades/second
                        assert throughput >= 1000, f"Throughput too low: {throughput:.0f}/sec"


class TestConcurrentSignals:
    """Test handling many concurrent signals."""

    @pytest.mark.asyncio
    async def test_track_20_signals(self):
        """System should track 20 concurrent signals."""
        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
                        prices = {"BTCUSDT": 50000, "ETHUSDT": 3000, "SOLUSDT": 100, "BNBUSDT": 300}

                        # Create 20 signals (5 per symbol)
                        for i in range(20):
                            symbol = symbols[i % len(symbols)]
                            base_price = prices[symbol]
                            is_long = i % 2 == 0

                            # TP/SL direction depends on position direction
                            if is_long:
                                tp_price = Decimal(str(base_price * 1.05))  # Above for LONG
                                sl_price = Decimal(str(base_price * 0.95))  # Below for LONG
                            else:
                                tp_price = Decimal(str(base_price * 0.95))  # Below for SHORT
                                sl_price = Decimal(str(base_price * 1.05))  # Above for SHORT

                            signal = SignalRecord(
                                symbol=symbol,
                                timeframe="1m",
                                signal_time=datetime.now(timezone.utc),
                                direction=Direction.LONG if is_long else Direction.SHORT,
                                entry_price=Decimal(str(base_price)),
                                tp_price=tp_price,
                                sl_price=sl_price,
                                streak_at_signal=0,
                            )
                            await tracker.add_signal(signal)

                        assert tracker.active_count == 20

                        # Process trades for each symbol
                        for symbol in symbols:
                            for j in range(50):
                                base_price = prices[symbol]
                                trade = AggTrade(
                                    symbol=symbol,
                                    agg_trade_id=j,
                                    price=Decimal(str(base_price)),
                                    quantity=Decimal("1"),
                                    timestamp=datetime.now(timezone.utc),
                                    is_buyer_maker=False,
                                )
                                await tracker.process_trade(trade)

                        # All signals should still be active (prices didn't hit TP/SL)
                        assert tracker.active_count == 20


class TestRapidSignalTurnover:
    """Test rapid signal creation and closure."""

    @pytest.mark.asyncio
    async def test_create_and_close_30_signals(self):
        """System should handle rapid signal creation and closure."""
        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        outcomes = []

        async def on_outcome(signal, outcome):
            outcomes.append(outcome)

        tracker.on_outcome(on_outcome)

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        for i in range(30):
                            # Create signal with tight TP
                            signal = SignalRecord(
                                symbol="BTCUSDT",
                                timeframe="1m",
                                signal_time=datetime.now(timezone.utc),
                                direction=Direction.LONG,
                                entry_price=Decimal("50000"),
                                tp_price=Decimal("50001"),  # Very tight TP
                                sl_price=Decimal("49000"),
                                streak_at_signal=0,
                            )
                            await tracker.add_signal(signal)

                            # Send trade that hits TP
                            trade = AggTrade(
                                symbol="BTCUSDT",
                                agg_trade_id=i,
                                price=Decimal("50002"),
                                quantity=Decimal("1"),
                                timestamp=datetime.now(timezone.utc),
                                is_buyer_maker=False,
                            )
                            await tracker.process_trade(trade)

                        # All signals should have been closed
                        assert tracker.active_count == 0
                        assert len(outcomes) == 30
                        assert all(o == Outcome.TP for o in outcomes)


class TestMemoryStability:
    """Test memory stability under load."""

    @pytest.mark.asyncio
    async def test_no_memory_leak_basic(self):
        """Basic check that processing doesn't cause obvious memory issues."""
        import gc

        tracker = PositionTracker()
        tracker.signal_repo = MagicMock()
        tracker.signal_repo.get_active = AsyncMock(return_value=[])
        tracker.signal_repo.update_outcome = AsyncMock()

        with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                    with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                        # Force garbage collection
                        gc.collect()

                        # Create and close many signals
                        for i in range(100):
                            signal = SignalRecord(
                                symbol="BTCUSDT",
                                timeframe="1m",
                                signal_time=datetime.now(timezone.utc),
                                direction=Direction.LONG,
                                entry_price=Decimal("50000"),
                                tp_price=Decimal("50001"),
                                sl_price=Decimal("49000"),
                                streak_at_signal=0,
                            )
                            await tracker.add_signal(signal)

                            trade = AggTrade(
                                symbol="BTCUSDT",
                                agg_trade_id=i,
                                price=Decimal("50002"),
                                quantity=Decimal("1"),
                                timestamp=datetime.now(timezone.utc),
                                is_buyer_maker=False,
                            )
                            await tracker.process_trade(trade)

                        # Should have no active signals (all closed)
                        assert tracker.active_count == 0

                        # Internal tracking dicts should be cleaned up
                        # Note: _last_cache_update was removed as cache now syncs with DB
                        assert len(tracker._last_db_update) == 0
