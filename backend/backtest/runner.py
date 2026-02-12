"""BacktestRunner — orchestrates the full backtest pipeline.

Completely independent of app/. Uses:
- backtest/storage for PostgreSQL access (shared asyncpg pool)
- backtest/downloader for downloading historical data
- core/ for pure business logic

Each run gets a unique run_id for tracking and comparison.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from core.models.config import StrategyConfig

from backtest.downloader import KlineDownloader
from backtest.engine import BacktestEngine, SymbolResult
from backtest.stats import BacktestResult, StatisticsCalculator
from backtest.storage.kline_source import KlineSource
from backtest.storage.signal_repo import BacktestSignalRepo

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Configuration for a backtest run."""

    symbols: list[str]
    timeframes: list[str]
    start_date: datetime
    end_date: datetime
    strategy: StrategyConfig
    strategy_name: str = "msr_retest_capture"


def generate_run_id(config: BacktestConfig) -> str:
    """Generate a unique run ID from config + timestamp."""
    key = (
        f"{config.start_date.isoformat()}"
        f":{config.end_date.isoformat()}"
        f":{','.join(sorted(config.symbols))}"
        f":{','.join(config.timeframes)}"
        f":{config.strategy.model_dump_json()}"
        f":{datetime.now(timezone.utc).isoformat()}"
    )
    return hashlib.sha256(key.encode()).hexdigest()[:16]


# 2 days of 1m warmup data = 2880 klines
# Ensures all timeframes (up to 30m × 50 periods = 1500 min) have
# enough history for indicator calculation from the very first signal.
WARMUP_DAYS = 2


class BacktestRunner:
    """Run backtests across multiple symbols sequentially."""

    def __init__(
        self,
        config: BacktestConfig,
        kline_source: KlineSource,
        signal_repo: BacktestSignalRepo,
    ):
        self.config = config
        self._kline_source = kline_source
        self._signal_repo = signal_repo

    async def run(self) -> BacktestResult:
        """Execute the full backtest pipeline."""
        start_time = time.time()
        run_id = generate_run_id(self.config)

        logger.info(
            f"Starting backtest run={run_id}: {self.config.symbols} "
            f"{self.config.start_date:%Y-%m-%d} → {self.config.end_date:%Y-%m-%d} "
            f"timeframes={self.config.timeframes}"
        )

        # Create run record
        await self._signal_repo.create_run(
            run_id=run_id,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            symbols=self.config.symbols,
            timeframes=self.config.timeframes,
            strategy=self.config.strategy,
        )

        try:
            # Run per-symbol engines sequentially
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
                    logger.error(
                        f"Symbol backtest failed: {symbol}", exc_info=True
                    )

            # Save signals to PostgreSQL
            if all_signals:
                count = await self._signal_repo.save_signals(
                    run_id, all_signals
                )
                logger.info(f"Saved {count} signals (run={run_id})")

            # Calculate statistics
            calculator = StatisticsCalculator()
            backtest_result = calculator.calculate(
                signals=all_signals,
                start_date=self.config.start_date,
                end_date=self.config.end_date,
                symbols=self.config.symbols,
                timeframes=self.config.timeframes,
            )

            # Update run with final stats
            await self._signal_repo.complete_run(run_id, backtest_result)

            elapsed = time.time() - start_time
            logger.info(
                f"Backtest run={run_id} completed in {elapsed:.1f}s: "
                f"{len(all_signals)} signals"
            )

            return backtest_result

        except Exception:
            await self._signal_repo.fail_run(run_id)
            raise

    async def _run_symbol(self, symbol: str) -> SymbolResult:
        """Run backtest for a single symbol.

        Loads extra warmup klines before start_date so indicators
        (EMA50 etc.) are fully primed from the very first signal.
        """
        warmup_start = self.config.start_date - timedelta(days=WARMUP_DAYS)

        logger.info(
            f"[{symbol}] Loading 1m klines: "
            f"{warmup_start:%Y-%m-%d} → {self.config.end_date:%Y-%m-%d} "
            f"(includes {WARMUP_DAYS}d warmup)"
        )

        klines = await self._kline_source.get_range(
            symbol, "1m", warmup_start, self.config.end_date
        )

        if not klines:
            logger.warning(f"[{symbol}] No 1m klines found in range")
            return SymbolResult(symbol=symbol)

        # Count warmup vs signal klines
        warmup_count = sum(
            1 for k in klines if k.timestamp < self.config.start_date
        )
        logger.info(
            f"[{symbol}] Loaded {len(klines):,} 1m klines "
            f"({warmup_count:,} warmup + {len(klines) - warmup_count:,} signal)"
        )

        engine = BacktestEngine(
            symbol=symbol,
            timeframes=self.config.timeframes,
            strategy=self.config.strategy,
            signal_start_time=self.config.start_date,
            strategy_name=self.config.strategy_name,
        )
        await engine.init()

        for i, kline in enumerate(klines):
            await engine.process_1m_kline(kline)
            if (i + 1) % 100_000 == 0:
                logger.info(
                    f"[{symbol}] Processed {i + 1:,}/{len(klines):,} klines"
                )

        engine.finalize()

        result = engine.get_result()
        logger.info(
            f"[{symbol}] Done: {len(result.signals)} signals "
            f"({result.total_1m_klines:,} klines)"
        )
        return result

    @staticmethod
    async def download_data(
        database_url: str,
        symbols: list[str],
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, int]:
        """Download 1m klines for all symbols.

        Returns dict of {symbol: kline_count}.
        """
        downloader = KlineDownloader(database_url=database_url)
        try:
            results = {}
            for symbol in symbols:
                logger.info(f"Downloading 1m klines for {symbol}...")
                count = await downloader.sync_historical(
                    symbol=symbol,
                    timeframe="1m",
                    start_date=start_date,
                    end_date=end_date,
                )
                results[symbol] = count
                logger.info(f"  {symbol}: {count:,} klines downloaded")
            return results
        finally:
            await downloader.close()
