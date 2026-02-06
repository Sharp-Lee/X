"""Tests for the K-line aggregator."""

import pytest
from app.models import FastKline
from app.services import KlineAggregator, AggregationBuffer, TIMEFRAME_MINUTES


def make_1m_kline(
    symbol: str = "BTCUSDT",
    timestamp: float = 0,
    open_price: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.5,
    volume: float = 10.0,
    is_closed: bool = True,
) -> FastKline:
    """Helper to create a 1m kline."""
    return FastKline(
        symbol=symbol,
        timeframe="1m",
        timestamp=timestamp,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        is_closed=is_closed,
    )


class TestAggregationBuffer:
    """Tests for AggregationBuffer."""

    def test_add_single_kline_no_aggregation(self):
        """Adding one kline to a 3m buffer should not trigger aggregation."""
        buffer = AggregationBuffer(
            symbol="BTCUSDT", timeframe="3m", period_minutes=3
        )

        result = buffer.add(make_1m_kline(timestamp=0))

        assert result is None
        assert len(buffer.klines_1m) == 1

    def test_aggregate_3m_from_3_klines(self):
        """Adding 3 klines should produce a 3m aggregated kline."""
        buffer = AggregationBuffer(
            symbol="BTCUSDT", timeframe="3m", period_minutes=3
        )

        # Add 3 klines with different OHLCV
        buffer.add(make_1m_kline(timestamp=0, open_price=100, high=102, low=99, close=101, volume=10))
        buffer.add(make_1m_kline(timestamp=60, open_price=101, high=105, low=100, close=103, volume=20))
        result = buffer.add(make_1m_kline(timestamp=120, open_price=103, high=104, low=98, close=99, volume=15))

        assert result is not None
        assert result.timeframe == "3m"
        assert result.symbol == "BTCUSDT"
        assert result.timestamp == 0  # First kline's timestamp
        assert result.open == 100  # First kline's open
        assert result.high == 105  # Highest high
        assert result.low == 98  # Lowest low
        assert result.close == 99  # Last kline's close
        assert result.volume == 45  # Sum of volumes
        assert result.is_closed is True

    def test_buffer_clears_after_aggregation(self):
        """Buffer should be cleared after successful aggregation."""
        buffer = AggregationBuffer(
            symbol="BTCUSDT", timeframe="3m", period_minutes=3
        )

        for i in range(3):
            buffer.add(make_1m_kline(timestamp=i * 60))

        assert len(buffer.klines_1m) == 0

    def test_reset(self):
        """Reset should clear the buffer."""
        buffer = AggregationBuffer(
            symbol="BTCUSDT", timeframe="3m", period_minutes=3
        )

        buffer.add(make_1m_kline(timestamp=0))
        buffer.add(make_1m_kline(timestamp=60))

        buffer.reset()

        assert len(buffer.klines_1m) == 0


class TestKlineAggregator:
    """Tests for KlineAggregator."""

    @pytest.mark.asyncio
    async def test_init_default_timeframes(self):
        """Default initialization should use standard timeframes."""
        aggregator = KlineAggregator()

        assert "3m" in aggregator.target_timeframes
        assert "5m" in aggregator.target_timeframes
        assert "15m" in aggregator.target_timeframes
        assert "30m" in aggregator.target_timeframes
        assert "1m" not in aggregator.target_timeframes  # 1m is not aggregated

    @pytest.mark.asyncio
    async def test_init_custom_timeframes(self):
        """Custom timeframes should be respected."""
        aggregator = KlineAggregator(target_timeframes=["3m", "5m"])

        assert "3m" in aggregator.target_timeframes
        assert "5m" in aggregator.target_timeframes
        assert "15m" not in aggregator.target_timeframes
        assert "30m" not in aggregator.target_timeframes

    @pytest.mark.asyncio
    async def test_ignore_non_1m_klines(self):
        """Non-1m klines should be ignored."""
        aggregator = KlineAggregator()

        kline = FastKline(
            symbol="BTCUSDT",
            timeframe="5m",  # Not 1m
            timestamp=0,
            open=100,
            high=101,
            low=99,
            close=100.5,
            volume=10,
            is_closed=True,
        )

        result = await aggregator.add_1m_kline(kline)

        assert result == []

    @pytest.mark.asyncio
    async def test_ignore_open_klines(self):
        """Open (not closed) klines should not trigger aggregation."""
        aggregator = KlineAggregator()

        result = await aggregator.add_1m_kline(
            make_1m_kline(timestamp=0, is_closed=False)
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_aggregate_3m_complete(self):
        """3 closed 1m klines should produce a 3m kline."""
        aggregator = KlineAggregator(target_timeframes=["3m"])
        results = []

        # Feed 3 klines
        for i in range(3):
            r = await aggregator.add_1m_kline(
                make_1m_kline(
                    timestamp=i * 60,
                    open_price=100 + i,
                    high=105 + i,
                    low=95 + i,
                    close=102 + i,
                    volume=10 + i,
                )
            )
            results.extend(r)

        assert len(results) == 1
        assert results[0].timeframe == "3m"
        assert results[0].open == 100  # First open
        assert results[0].close == 104  # Last close
        assert results[0].high == 107  # Max high
        assert results[0].low == 95  # Min low
        assert results[0].volume == 33  # Sum

    @pytest.mark.asyncio
    async def test_aggregate_multiple_timeframes(self):
        """Feed enough klines to complete multiple timeframe aggregations."""
        aggregator = KlineAggregator(target_timeframes=["3m", "5m"])
        collected: dict[str, list[FastKline]] = {"3m": [], "5m": []}

        # Feed 15 klines (enough for 5x 3m and 3x 5m)
        for i in range(15):
            results = await aggregator.add_1m_kline(
                make_1m_kline(timestamp=i * 60, volume=1)
            )
            for r in results:
                collected[r.timeframe].append(r)

        assert len(collected["3m"]) == 5  # 15 / 3 = 5
        assert len(collected["5m"]) == 3  # 15 / 5 = 3

    @pytest.mark.asyncio
    async def test_callback_invoked(self):
        """Callback should be invoked for each aggregated kline."""
        aggregator = KlineAggregator(target_timeframes=["3m"])
        callback_results = []

        async def callback(kline: FastKline):
            callback_results.append(kline)

        aggregator.on_aggregated_kline(callback)

        for i in range(3):
            await aggregator.add_1m_kline(make_1m_kline(timestamp=i * 60))

        assert len(callback_results) == 1
        assert callback_results[0].timeframe == "3m"

    @pytest.mark.asyncio
    async def test_multiple_symbols(self):
        """Aggregation should be independent per symbol."""
        aggregator = KlineAggregator(target_timeframes=["3m"])

        # Feed 3 klines for BTCUSDT
        for i in range(3):
            await aggregator.add_1m_kline(
                make_1m_kline(symbol="BTCUSDT", timestamp=i * 60)
            )

        # Feed 2 klines for ETHUSDT (not enough for aggregation)
        for i in range(2):
            await aggregator.add_1m_kline(
                make_1m_kline(symbol="ETHUSDT", timestamp=i * 60)
            )

        # BTCUSDT should have completed aggregation, ETHUSDT should not
        assert aggregator.get_partial_kline("BTCUSDT", "3m") is None or len(aggregator._buffers["BTCUSDT"]["3m"].klines_1m) == 0
        assert len(aggregator._buffers["ETHUSDT"]["3m"].klines_1m) == 2

    @pytest.mark.asyncio
    async def test_get_current_1m(self):
        """get_current_1m should return the latest 1m kline."""
        aggregator = KlineAggregator()

        await aggregator.add_1m_kline(
            make_1m_kline(symbol="BTCUSDT", timestamp=60, close=105)
        )

        current = aggregator.get_current_1m("BTCUSDT")
        assert current is not None
        assert current.timestamp == 60
        assert current.close == 105

    @pytest.mark.asyncio
    async def test_get_partial_kline(self):
        """get_partial_kline should return incomplete aggregated kline."""
        aggregator = KlineAggregator(target_timeframes=["5m"])

        # Feed 3 klines (not enough for 5m)
        for i in range(3):
            await aggregator.add_1m_kline(
                make_1m_kline(
                    timestamp=i * 60,
                    open_price=100 + i,
                    high=105 + i,
                    low=95 + i,
                    close=102 + i,
                    volume=10,
                )
            )

        partial = aggregator.get_partial_kline("BTCUSDT", "5m")

        assert partial is not None
        assert partial.timeframe == "5m"
        assert partial.is_closed is False
        assert partial.open == 100  # First kline's open
        assert partial.close == 104  # Last kline's close (102 + 2)
        assert partial.high == 107  # Max high
        assert partial.low == 95  # Min low
        assert partial.volume == 30  # Sum

    @pytest.mark.asyncio
    async def test_reset_single_symbol(self):
        """Reset should clear buffers for a specific symbol."""
        aggregator = KlineAggregator(target_timeframes=["3m"])

        await aggregator.add_1m_kline(make_1m_kline(symbol="BTCUSDT"))
        await aggregator.add_1m_kline(make_1m_kline(symbol="ETHUSDT"))

        aggregator.reset(symbol="BTCUSDT")

        assert len(aggregator._buffers["BTCUSDT"]["3m"].klines_1m) == 0
        assert len(aggregator._buffers["ETHUSDT"]["3m"].klines_1m) == 1

    @pytest.mark.asyncio
    async def test_reset_all_symbols(self):
        """Reset without symbol should clear all buffers."""
        aggregator = KlineAggregator(target_timeframes=["3m"])

        await aggregator.add_1m_kline(make_1m_kline(symbol="BTCUSDT"))
        await aggregator.add_1m_kline(make_1m_kline(symbol="ETHUSDT"))

        aggregator.reset()

        assert len(aggregator._buffers["BTCUSDT"]["3m"].klines_1m) == 0
        assert len(aggregator._buffers["ETHUSDT"]["3m"].klines_1m) == 0

    @pytest.mark.asyncio
    async def test_prefill_from_history(self):
        """Prefill should populate buffer with historical klines."""
        aggregator = KlineAggregator(target_timeframes=["5m"])

        # Create historical klines for an INCOMPLETE 5m period
        # 5m period starts at 300, we have klines at minute 0, 1, 2 of that period
        # Last kline ends at 480, which is NOT on a 5m boundary (480 % 300 != 0)
        history = [
            make_1m_kline(timestamp=300),  # Period start (minute 0)
            make_1m_kline(timestamp=360),  # Minute 1
            make_1m_kline(timestamp=420),  # Minute 2 (ends at 480, incomplete period)
        ]

        aggregator.prefill_from_history("BTCUSDT", history)

        # Buffer should have 3 klines (ready to complete on next minute 3, 4)
        assert len(aggregator._buffers["BTCUSDT"]["5m"].klines_1m) == 3

    @pytest.mark.asyncio
    async def test_continuous_aggregation(self):
        """Test continuous aggregation over multiple periods."""
        aggregator = KlineAggregator(target_timeframes=["3m"])
        all_results = []

        # Feed 9 klines (should produce 3 x 3m klines)
        for i in range(9):
            results = await aggregator.add_1m_kline(
                make_1m_kline(timestamp=i * 60, volume=1)
            )
            all_results.extend(results)

        assert len(all_results) == 3

        # Verify timestamps are sequential
        timestamps = [r.timestamp for r in all_results]
        assert timestamps == [0, 180, 360]

    @pytest.mark.asyncio
    async def test_30m_aggregation(self):
        """Test 30m aggregation (requires 30 x 1m klines)."""
        aggregator = KlineAggregator(target_timeframes=["30m"])
        results = []

        # Feed 30 klines
        for i in range(30):
            r = await aggregator.add_1m_kline(
                make_1m_kline(timestamp=i * 60, volume=2)
            )
            results.extend(r)

        assert len(results) == 1
        assert results[0].timeframe == "30m"
        assert results[0].volume == 60  # 30 * 2


class TestEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_callback_error_does_not_break_processing(self):
        """Callback errors should be logged but not break processing."""
        aggregator = KlineAggregator(target_timeframes=["3m"])

        async def bad_callback(kline: FastKline):
            raise ValueError("Test error")

        aggregator.on_aggregated_kline(bad_callback)

        # This should not raise even though callback throws
        for i in range(3):
            await aggregator.add_1m_kline(make_1m_kline(timestamp=i * 60))

    @pytest.mark.asyncio
    async def test_get_partial_kline_no_data(self):
        """get_partial_kline should return None if no data."""
        aggregator = KlineAggregator()

        result = aggregator.get_partial_kline("BTCUSDT", "5m")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_partial_kline_unknown_symbol(self):
        """get_partial_kline should return None for unknown symbol."""
        aggregator = KlineAggregator()

        await aggregator.add_1m_kline(make_1m_kline(symbol="BTCUSDT"))

        result = aggregator.get_partial_kline("UNKNOWN", "3m")
        assert result is None

    @pytest.mark.asyncio
    async def test_empty_history_prefill(self):
        """Prefill with empty history should not cause errors."""
        aggregator = KlineAggregator()

        aggregator.prefill_from_history("BTCUSDT", [])

        # Should have empty buffers
        assert len(aggregator._buffers["BTCUSDT"]["3m"].klines_1m) == 0
