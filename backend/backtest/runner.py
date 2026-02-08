"""BacktestRunner - orchestrates the full backtest pipeline.

Runs per-symbol engines sequentially, aggregates results, and
computes comprehensive statistics.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime

from core.models.config import StrategyConfig

from backtest.engine import BacktestEngine, SymbolResult
from backtest.stats import BacktestResult, StatisticsCalculator

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Configuration for a backtest run."""

    symbols: list[str]
    timeframes: list[str]
    start_date: datetime
    end_date: datetime
    strategy: StrategyConfig


class BacktestRunner:
    """Run backtests across multiple symbols sequentially."""

    def __init__(self, config: BacktestConfig):
        self.config = config

    async def run(self) -> BacktestResult:
        """Execute the full backtest pipeline."""
        start_time = time.time()

        logger.info(
            f"Starting backtest: {self.config.symbols} "
            f"{self.config.start_date:%Y-%m-%d} → {self.config.end_date:%Y-%m-%d} "
            f"timeframes={self.config.timeframes}"
        )

        # Run per-symbol engines sequentially (CPU-bound processing
        # doesn't benefit from asyncio.gather, and concurrent DB loads
        # can exhaust connections on large date ranges)
        all_signals = []
        for symbol in self.config.symbols:
            try:
                result = await self._run_symbol(symbol)
                all_signals.extend(result.signals)
                logger.info(
                    f"  {result.symbol}: {len(result.signals)} signals, "
                    f"{result.total_1m_klines:,} 1m klines"
                )
            except Exception:
                logger.error(f"Symbol backtest failed: {symbol}", exc_info=True)

        # Save signals to database
        if all_signals:
            await self._save_signals(all_signals)

        # Calculate statistics
        calculator = StatisticsCalculator()
        backtest_result = calculator.calculate(
            signals=all_signals,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            symbols=self.config.symbols,
            timeframes=self.config.timeframes,
        )

        elapsed = time.time() - start_time
        logger.info(f"Backtest completed in {elapsed:.1f}s: {len(all_signals)} signals")

        return backtest_result

    async def _run_symbol(self, symbol: str) -> SymbolResult:
        """Run backtest for a single symbol."""
        from app.storage.kline_repo import KlineRepository

        repo = KlineRepository()

        logger.info(
            f"[{symbol}] Loading 1m klines: "
            f"{self.config.start_date:%Y-%m-%d} → {self.config.end_date:%Y-%m-%d}"
        )

        # Fetch all 1m klines in range
        klines = await repo.get_range(
            symbol, "1m", self.config.start_date, self.config.end_date
        )

        if not klines:
            logger.warning(f"[{symbol}] No 1m klines found in range")
            return SymbolResult(symbol=symbol)

        logger.info(f"[{symbol}] Loaded {len(klines):,} 1m klines")

        # Create and init engine
        engine = BacktestEngine(
            symbol=symbol,
            timeframes=self.config.timeframes,
            strategy=self.config.strategy,
        )
        await engine.init()

        # Process all klines
        for i, kline in enumerate(klines):
            await engine.process_1m_kline(kline)
            if (i + 1) % 100000 == 0:
                logger.info(f"[{symbol}] Processed {i + 1:,}/{len(klines):,} klines")

        # Finalize
        engine.finalize()

        result = engine.get_result()
        logger.info(
            f"[{symbol}] Done: {len(result.signals)} signals "
            f"({result.total_1m_klines:,} klines)"
        )
        return result

    async def _save_signals(self, signals: list) -> None:
        """Save all backtest signals to the database."""
        from app.storage.signal_repo import SignalRepository

        repo = SignalRepository()
        for signal in signals:
            await repo.save(signal)
        logger.info(f"Saved {len(signals)} signals to database")

    async def download_data(self) -> dict[str, int]:
        """Download 1m klines for all symbols using KlineDownloader.

        Returns:
            Dict of {symbol: kline_count}
        """
        from app.services.kline_downloader import KlineDownloader

        results = {}
        for symbol in self.config.symbols:
            logger.info(f"Downloading 1m klines for {symbol}...")
            downloader = KlineDownloader()
            try:
                count = await downloader.sync_historical(
                    symbol=symbol,
                    timeframe="1m",
                    start_date=self.config.start_date,
                    end_date=self.config.end_date,
                )
                results[symbol] = count
                logger.info(f"  {symbol}: {count:,} klines downloaded")
            finally:
                await downloader.close()

        return results
