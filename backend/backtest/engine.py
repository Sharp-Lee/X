"""Per-symbol backtest processing engine.

Ties together KlineAggregator, KlineBuffer, SignalGenerator, and
OutcomeTracker to process 1m klines for a single symbol across all
requested timeframes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from core.kline_aggregator import KlineAggregator
from core.models.config import StrategyConfig
from core.models.converters import fast_to_kline, kline_to_fast
from core.models.kline import Kline, KlineBuffer
from core.models.signal import Outcome, SignalRecord
from core.strategy import create_strategy

from backtest.outcome import OutcomeTracker

logger = logging.getLogger(__name__)


@dataclass
class SymbolResult:
    """Result of backtesting a single symbol."""

    symbol: str
    signals: list[SignalRecord] = field(default_factory=list)
    total_1m_klines: int = 0
    timeframes_processed: list[str] = field(default_factory=list)


class BacktestEngine:
    """Process 1m klines for a single symbol across all timeframes.

    Processing order for each 1m kline:
    1. Check active signals against this kline (outcome determination)
    2. Process 1m timeframe signal generation
    3. Feed through KlineAggregator to produce higher-timeframe klines
    4. Process each completed higher-timeframe kline for signal generation
    """

    def __init__(
        self,
        symbol: str,
        timeframes: list[str],
        strategy: StrategyConfig,
        signal_start_time: datetime | None = None,
        strategy_name: str = "msr_retest_capture",
    ):
        self.symbol = symbol
        self.timeframes = timeframes
        self._signal_start_time = signal_start_time
        self._total_1m = 0

        # KlineAggregator for higher timeframes (exclude 1m)
        aggregated_tfs = [tf for tf in timeframes if tf != "1m"]
        self._aggregator = KlineAggregator(target_timeframes=aggregated_tfs)

        # Per-timeframe: KlineBuffer + strategy instance
        self._buffers: dict[str, KlineBuffer] = {}
        self._generators: dict[str, object] = {}
        for tf in timeframes:
            self._buffers[tf] = KlineBuffer(symbol=symbol, timeframe=tf, max_size=200)
            # No persistence callbacks — backtesting is in-memory only
            self._generators[tf] = create_strategy(strategy_name, config=strategy)

        # Outcome tracker
        self._outcome_tracker = OutcomeTracker(
            on_outcome=self._handle_outcome,
        )

        # Collected signals
        self._signals: list[SignalRecord] = []

    async def init(self) -> None:
        """Initialize all signal generators."""
        for gen in self._generators.values():
            await gen.init()

    async def process_1m_kline(self, kline: Kline) -> None:
        """Process a single 1m kline.

        Args:
            kline: A closed 1m kline in chronological order
        """
        self._total_1m += 1

        # Step 1: Check active signals against this 1m kline
        await self._outcome_tracker.check_kline(kline)

        # Step 2: Process 1m timeframe
        if "1m" in self.timeframes:
            await self._process_kline_for_timeframe(kline, "1m")

        # Step 3: Feed through aggregator
        fast_kline = kline_to_fast(kline)
        aggregated = await self._aggregator.add_1m_kline(fast_kline)

        # Step 4: Process each completed higher-timeframe kline
        for fast_agg in aggregated:
            if fast_agg.timeframe in self.timeframes:
                agg_kline = fast_to_kline(fast_agg)
                await self._process_kline_for_timeframe(agg_kline, fast_agg.timeframe)

    async def _process_kline_for_timeframe(
        self, kline: Kline, timeframe: str
    ) -> None:
        """Process a kline through the buffer and signal generator."""
        buffer = self._buffers[timeframe]
        buffer.add(kline)

        gen = self._generators[timeframe]
        result = await gen.process_kline(kline, buffer)

        if result.signal:
            # Always track in OutcomeTracker (maintains position lock)
            self._outcome_tracker.add_signal(result.signal)
            # Only collect for output if after warmup period
            if (
                self._signal_start_time is None
                or result.signal.signal_time >= self._signal_start_time
            ):
                self._signals.append(result.signal)

        # Update max_atr for active signals
        if result.atr is not None:
            self._outcome_tracker.update_atr(self.symbol, timeframe, result.atr)

    async def _handle_outcome(
        self, signal: SignalRecord, outcome: Outcome
    ) -> None:
        """Called when OutcomeTracker resolves a signal."""
        tf = signal.timeframe
        if tf in self._generators:
            await self._generators[tf].record_outcome(
                outcome, signal.symbol, signal.timeframe
            )

    def finalize(self) -> None:
        """Finalize remaining active signals."""
        self._outcome_tracker.finalize()

    def get_result(self) -> SymbolResult:
        return SymbolResult(
            symbol=self.symbol,
            signals=self._signals,
            total_1m_klines=self._total_1m,
            timeframes_processed=self.timeframes,
        )
