#!/usr/bin/env python3
"""Stress test script for the trading system.

Simulates high-frequency trade processing and concurrent signals
to verify system stability under load.

Usage:
    python scripts/stress_test.py
"""

import asyncio
import gc
import os
import psutil
import random
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.models import (
    AggTrade,
    Direction,
    Outcome,
    SignalRecord,
    FastSignal,
    FastTrade,
)
from app.models.converters import signal_to_fast, aggtrade_to_fast
from app.services.position_tracker import PositionTracker


def get_memory_usage_mb():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def get_cpu_percent():
    """Get current CPU usage percentage."""
    return psutil.cpu_percent(interval=0.1)


class StressTestResults:
    """Container for stress test results."""

    def __init__(self):
        self.trades_processed = 0
        self.signals_tracked = 0
        self.outcomes_recorded = 0
        self.errors = []
        self.start_time = None
        self.end_time = None
        self.memory_samples = []
        self.latencies = []

    @property
    def duration_seconds(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0

    @property
    def trades_per_second(self):
        if self.duration_seconds > 0:
            return self.trades_processed / self.duration_seconds
        return 0

    @property
    def avg_latency_us(self):
        if self.latencies:
            return sum(self.latencies) / len(self.latencies)
        return 0

    @property
    def p99_latency_us(self):
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * 0.99)
            return sorted_latencies[idx]
        return 0

    def print_report(self):
        print("\n" + "=" * 60)
        print("STRESS TEST RESULTS")
        print("=" * 60)
        print(f"\nDuration: {self.duration_seconds:.2f} seconds")
        print(f"Trades processed: {self.trades_processed:,}")
        print(f"Trades/second: {self.trades_per_second:,.0f}")
        print(f"Signals tracked: {self.signals_tracked}")
        print(f"Outcomes recorded: {self.outcomes_recorded}")
        print(f"Errors: {len(self.errors)}")

        print(f"\nLatency:")
        print(f"  Average: {self.avg_latency_us:.2f} μs")
        print(f"  P99: {self.p99_latency_us:.2f} μs")

        if self.memory_samples:
            print(f"\nMemory:")
            print(f"  Start: {self.memory_samples[0]:.1f} MB")
            print(f"  End: {self.memory_samples[-1]:.1f} MB")
            print(f"  Peak: {max(self.memory_samples):.1f} MB")
            print(f"  Delta: {self.memory_samples[-1] - self.memory_samples[0]:.1f} MB")


async def stress_test_high_frequency_trades(
    trades_per_second: int = 1000,
    duration_seconds: int = 10,
    num_signals: int = 10,
) -> StressTestResults:
    """
    Stress test: High-frequency trade processing.

    Simulates a high volume of incoming trades being processed
    by the position tracker.

    Args:
        trades_per_second: Target trades per second
        duration_seconds: How long to run the test
        num_signals: Number of active signals to track
    """
    print(f"\n{'=' * 60}")
    print("STRESS TEST: High-Frequency Trades")
    print(f"{'=' * 60}")
    print(f"Target: {trades_per_second} trades/sec for {duration_seconds}s")
    print(f"Active signals: {num_signals}")

    results = StressTestResults()

    # Create position tracker with mocked repository
    tracker = PositionTracker()
    tracker.signal_repo = MagicMock()
    tracker.signal_repo.get_active = AsyncMock(return_value=[])
    tracker.signal_repo.update_outcome = AsyncMock()

    # Mock cache operations
    with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
        with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                    # Create signals for different symbols
                    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
                    base_prices = {"BTCUSDT": 50000, "ETHUSDT": 3000, "SOLUSDT": 100, "BNBUSDT": 300, "XRPUSDT": 0.5}

                    for i in range(num_signals):
                        symbol = symbols[i % len(symbols)]
                        base_price = base_prices[symbol]

                        signal = SignalRecord(
                            symbol=symbol,
                            timeframe="1m",
                            signal_time=datetime.now(timezone.utc),
                            direction=Direction.LONG if i % 2 == 0 else Direction.SHORT,
                            entry_price=Decimal(str(base_price)),
                            tp_price=Decimal(str(base_price * 1.01)),
                            sl_price=Decimal(str(base_price * 0.99)),
                            streak_at_signal=0,
                        )
                        await tracker.add_signal(signal)

                    results.signals_tracked = tracker.active_count

                    # Calculate interval between trades
                    interval = 1.0 / trades_per_second
                    total_trades = trades_per_second * duration_seconds

                    gc.collect()
                    results.memory_samples.append(get_memory_usage_mb())
                    results.start_time = time.perf_counter()

                    # Process trades
                    for i in range(total_trades):
                        # Generate random trade
                        symbol = random.choice(symbols)
                        base_price = base_prices[symbol]
                        # Random price within 0.5% of base
                        price = base_price * (1 + random.uniform(-0.005, 0.005))

                        trade = AggTrade(
                            symbol=symbol,
                            agg_trade_id=i,
                            price=Decimal(str(price)),
                            quantity=Decimal("1"),
                            timestamp=datetime.now(timezone.utc),
                            is_buyer_maker=random.choice([True, False]),
                        )

                        # Time the trade processing
                        start = time.perf_counter_ns()
                        try:
                            await tracker.process_trade(trade)
                            results.trades_processed += 1
                        except Exception as e:
                            results.errors.append(str(e))

                        latency_us = (time.perf_counter_ns() - start) / 1000
                        results.latencies.append(latency_us)

                        # Sample memory periodically
                        if i % 1000 == 0:
                            results.memory_samples.append(get_memory_usage_mb())

                        # Progress indicator
                        if i > 0 and i % (total_trades // 10) == 0:
                            progress = (i / total_trades) * 100
                            print(f"  Progress: {progress:.0f}%")

                    results.end_time = time.perf_counter()
                    results.memory_samples.append(get_memory_usage_mb())
                    results.outcomes_recorded = num_signals - tracker.active_count

    return results


async def stress_test_concurrent_signals(
    num_signals: int = 50,
    trades_per_signal: int = 100,
) -> StressTestResults:
    """
    Stress test: Many concurrent signals.

    Creates many signals and processes trades for each.

    Args:
        num_signals: Number of concurrent signals
        trades_per_signal: Number of trades to process per signal
    """
    print(f"\n{'=' * 60}")
    print("STRESS TEST: Concurrent Signals")
    print(f"{'=' * 60}")
    print(f"Signals: {num_signals}")
    print(f"Trades per signal: {trades_per_signal}")

    results = StressTestResults()

    tracker = PositionTracker()
    tracker.signal_repo = MagicMock()
    tracker.signal_repo.get_active = AsyncMock(return_value=[])
    tracker.signal_repo.update_outcome = AsyncMock()

    with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
        with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                    # Create many signals
                    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
                    base_prices = {"BTCUSDT": 50000, "ETHUSDT": 3000, "SOLUSDT": 100, "BNBUSDT": 300, "XRPUSDT": 0.5}

                    gc.collect()
                    results.memory_samples.append(get_memory_usage_mb())
                    results.start_time = time.perf_counter()

                    # Create signals
                    print("  Creating signals...")
                    for i in range(num_signals):
                        symbol = symbols[i % len(symbols)]
                        base_price = base_prices[symbol]

                        signal = SignalRecord(
                            symbol=symbol,
                            timeframe="1m",
                            signal_time=datetime.now(timezone.utc),
                            direction=Direction.LONG if i % 2 == 0 else Direction.SHORT,
                            entry_price=Decimal(str(base_price)),
                            tp_price=Decimal(str(base_price * 1.02)),  # 2% TP
                            sl_price=Decimal(str(base_price * 0.98)),  # 2% SL
                            streak_at_signal=0,
                        )
                        await tracker.add_signal(signal)

                    results.signals_tracked = tracker.active_count
                    results.memory_samples.append(get_memory_usage_mb())

                    print(f"  Created {results.signals_tracked} signals")
                    print("  Processing trades...")

                    # Process trades for each symbol
                    total_trades = len(symbols) * trades_per_signal
                    processed = 0

                    for _ in range(trades_per_signal):
                        for symbol in symbols:
                            base_price = base_prices[symbol]
                            # Small price movements
                            price = base_price * (1 + random.uniform(-0.01, 0.01))

                            trade = AggTrade(
                                symbol=symbol,
                                agg_trade_id=processed,
                                price=Decimal(str(price)),
                                quantity=Decimal("1"),
                                timestamp=datetime.now(timezone.utc),
                                is_buyer_maker=random.choice([True, False]),
                            )

                            start = time.perf_counter_ns()
                            try:
                                await tracker.process_trade(trade)
                                results.trades_processed += 1
                            except Exception as e:
                                results.errors.append(str(e))

                            latency_us = (time.perf_counter_ns() - start) / 1000
                            results.latencies.append(latency_us)
                            processed += 1

                        # Sample memory periodically
                        if processed % 500 == 0:
                            results.memory_samples.append(get_memory_usage_mb())

                    results.end_time = time.perf_counter()
                    results.memory_samples.append(get_memory_usage_mb())
                    results.outcomes_recorded = num_signals - tracker.active_count

    return results


async def stress_test_rapid_signal_turnover(
    signals_per_second: int = 10,
    duration_seconds: int = 5,
) -> StressTestResults:
    """
    Stress test: Rapid signal creation and closure.

    Creates and closes signals rapidly to test signal lifecycle handling.

    Args:
        signals_per_second: Rate of new signal creation
        duration_seconds: Test duration
    """
    print(f"\n{'=' * 60}")
    print("STRESS TEST: Rapid Signal Turnover")
    print(f"{'=' * 60}")
    print(f"Rate: {signals_per_second} signals/sec for {duration_seconds}s")

    results = StressTestResults()

    tracker = PositionTracker()
    tracker.signal_repo = MagicMock()
    tracker.signal_repo.get_active = AsyncMock(return_value=[])
    tracker.signal_repo.update_outcome = AsyncMock()

    outcomes_count = 0

    async def count_outcome(signal, outcome):
        nonlocal outcomes_count
        outcomes_count += 1

    tracker.on_outcome(count_outcome)

    with patch('app.storage.signal_cache.cache_signal', new_callable=AsyncMock, return_value=True):
        with patch('app.storage.signal_cache.update_signal', new_callable=AsyncMock, return_value=True):
            with patch('app.storage.signal_cache.remove_signal', new_callable=AsyncMock, return_value=True):
                with patch('app.storage.signal_cache.get_all_signals', new_callable=AsyncMock, return_value=[]):
                    gc.collect()
                    results.memory_samples.append(get_memory_usage_mb())
                    results.start_time = time.perf_counter()

                    total_signals = signals_per_second * duration_seconds
                    signal_id = 0

                    for i in range(total_signals):
                        # Create a signal with tight TP (will hit quickly)
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

                        start = time.perf_counter_ns()
                        await tracker.add_signal(signal)
                        results.signals_tracked += 1

                        # Immediately send a trade that hits TP
                        trade = AggTrade(
                            symbol="BTCUSDT",
                            agg_trade_id=i,
                            price=Decimal("50002"),  # Hits TP
                            quantity=Decimal("1"),
                            timestamp=datetime.now(timezone.utc),
                            is_buyer_maker=False,
                        )

                        await tracker.process_trade(trade)
                        results.trades_processed += 1

                        latency_us = (time.perf_counter_ns() - start) / 1000
                        results.latencies.append(latency_us)

                        if i % 10 == 0:
                            results.memory_samples.append(get_memory_usage_mb())

                    results.end_time = time.perf_counter()
                    results.memory_samples.append(get_memory_usage_mb())
                    results.outcomes_recorded = outcomes_count

    return results


async def main():
    print("\n" + "#" * 60)
    print("#" + " " * 20 + "STRESS TEST SUITE" + " " * 21 + "#")
    print("#" + " " * 15 + "MSR Retest Capture System" + " " * 18 + "#")
    print("#" * 60)

    all_passed = True

    # Test 1: High-frequency trades
    results1 = await stress_test_high_frequency_trades(
        trades_per_second=1000,
        duration_seconds=5,
        num_signals=10,
    )
    results1.print_report()

    # Check thresholds
    if results1.trades_per_second < 500:
        print("WARNING: Trade throughput below 500/sec")
        all_passed = False
    if results1.p99_latency_us > 1000:  # 1ms
        print("WARNING: P99 latency above 1ms")
        all_passed = False
    if len(results1.errors) > 0:
        print(f"WARNING: {len(results1.errors)} errors occurred")
        all_passed = False

    # Test 2: Concurrent signals
    results2 = await stress_test_concurrent_signals(
        num_signals=50,
        trades_per_signal=100,
    )
    results2.print_report()

    if len(results2.errors) > 0:
        print(f"WARNING: {len(results2.errors)} errors occurred")
        all_passed = False

    # Test 3: Rapid turnover
    results3 = await stress_test_rapid_signal_turnover(
        signals_per_second=20,
        duration_seconds=3,
    )
    results3.print_report()

    if len(results3.errors) > 0:
        print(f"WARNING: {len(results3.errors)} errors occurred")
        all_passed = False

    # Final summary
    print("\n" + "=" * 60)
    print("OVERALL RESULT")
    print("=" * 60)

    if all_passed:
        print("\n✅ All stress tests PASSED")
        print("\nSystem can handle:")
        print(f"  - {results1.trades_per_second:,.0f}+ trades/second")
        print(f"  - {results2.signals_tracked}+ concurrent signals")
        print(f"  - Rapid signal creation/closure")
    else:
        print("\n⚠️  Some thresholds not met - see warnings above")

    return 0 if all_passed else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
