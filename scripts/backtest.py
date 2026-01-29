#!/usr/bin/env python3
"""
Historical signal backtesting script.

Uses historical K-line data to generate signals, then uses aggTrade data
to precisely determine outcomes (TP/SL hit order) and calculate MAE.
"""

import asyncio
import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.config import get_settings
from app.core import IndicatorCalculator
from app.models import (
    AggTrade,
    Direction,
    Kline,
    KlineBuffer,
    Outcome,
    SignalRecord,
)
from app.services import SignalGenerator, BacktestTracker, AggTradeDownloader
from app.storage import KlineRepository, AggTradeRepository, SignalRepository, init_database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class Backtester:
    """Historical signal backtesting engine."""

    def __init__(self):
        self.kline_repo = KlineRepository()
        self.aggtrade_repo = AggTradeRepository()
        self.signal_repo = SignalRepository()
        self.signal_generator = SignalGenerator()
        self.backtest_tracker = BacktestTracker()
        self.indicator_calc = IndicatorCalculator()

    async def run_backtest(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict:
        """
        Run backtest for a symbol within a date range.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            start_date: Start of backtest period
            end_date: End of backtest period

        Returns:
            Dict with backtest results
        """
        logger.info(f"Running backtest for {symbol} from {start_date} to {end_date}")

        # Fetch historical klines
        klines = await self.kline_repo.get_range(symbol, timeframe, start_date, end_date)
        if not klines:
            logger.error(f"No klines found for {symbol}")
            return {"error": "No klines found"}

        logger.info(f"Loaded {len(klines)} klines")

        # Generate signals from klines
        signals = await self._generate_signals(klines, symbol, timeframe)
        logger.info(f"Generated {len(signals)} signals")

        if not signals:
            return {
                "symbol": symbol,
                "period": f"{start_date.date()} to {end_date.date()}",
                "total_signals": 0,
                "wins": 0,
                "losses": 0,
                "active": 0,
                "win_rate": 0,
            }

        # Backtest signals using aggTrade data
        results = await self._backtest_signals(signals, symbol)

        # Calculate statistics
        stats = self._calculate_stats(results)
        stats["symbol"] = symbol
        stats["period"] = f"{start_date.date()} to {end_date.date()}"

        return stats

    async def _generate_signals(
        self,
        klines: list[Kline],
        symbol: str,
        timeframe: str,
    ) -> list[SignalRecord]:
        """Generate signals from historical klines."""
        signals = []
        buffer = KlineBuffer(symbol=symbol, timeframe=timeframe, max_size=200)

        # Need at least 50 klines for indicators
        for i, kline in enumerate(klines):
            buffer.add(kline)

            if len(buffer) < 50:
                continue

            if not kline.is_closed:
                continue

            # Get OHLCV data
            opens = [k.open for k in buffer.klines]
            highs = [k.high for k in buffer.klines]
            lows = [k.low for k in buffer.klines]
            closes = [k.close for k in buffer.klines]
            volumes = [k.volume for k in buffer.klines]

            # Calculate indicators
            indicators = self.indicator_calc.calculate_latest(
                opens, highs, lows, closes, volumes
            )

            if indicators is None:
                continue

            # Get previous kline
            prev_kline = buffer.klines[-2] if len(buffer.klines) >= 2 else None

            # Detect signal
            signal = self.signal_generator.detect_signal(kline, prev_kline, indicators)

            if signal:
                signals.append(signal)

        return signals

    async def _backtest_signals(
        self,
        signals: list[SignalRecord],
        symbol: str,
    ) -> list[SignalRecord]:
        """Backtest signals using aggTrade data."""
        if not signals:
            return []

        # Group signals by date to fetch aggTrades efficiently
        results = []

        for signal in signals:
            # Fetch aggTrades from signal time to 24 hours later (or until outcome)
            start_time = signal.signal_time
            end_time = start_time + timedelta(hours=24)

            trades = await self.aggtrade_repo.get_range(symbol, start_time, end_time)

            if not trades:
                logger.warning(f"No aggTrades found for signal {signal.id}")
                results.append(signal)
                continue

            # Process trades to determine outcome
            for trade in trades:
                if trade.timestamp < signal.signal_time:
                    continue

                # Update MAE
                signal.update_mae(trade.price)

                # Check for outcome
                if signal.check_outcome(trade.price, trade.timestamp):
                    break

            results.append(signal)

            # Save to database
            await self.signal_repo.save(signal)

        return results

    def _calculate_stats(self, signals: list[SignalRecord]) -> dict:
        """Calculate backtest statistics."""
        total = len(signals)
        wins = sum(1 for s in signals if s.outcome == Outcome.TP)
        losses = sum(1 for s in signals if s.outcome == Outcome.SL)
        active = sum(1 for s in signals if s.outcome == Outcome.ACTIVE)

        win_rate = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

        # Calculate average MAE/MFE
        completed = [s for s in signals if s.outcome != Outcome.ACTIVE]
        avg_mae = (
            sum(float(s.mae_ratio) for s in completed) / len(completed)
            if completed
            else 0
        )
        avg_mfe = (
            sum(float(s.mfe_ratio) for s in completed) / len(completed)
            if completed
            else 0
        )

        # Calculate by direction
        longs = [s for s in signals if s.direction == Direction.LONG]
        shorts = [s for s in signals if s.direction == Direction.SHORT]
        long_wins = sum(1 for s in longs if s.outcome == Outcome.TP)
        short_wins = sum(1 for s in shorts if s.outcome == Outcome.TP)

        return {
            "total_signals": total,
            "wins": wins,
            "losses": losses,
            "active": active,
            "win_rate": round(win_rate, 2),
            "breakeven_win_rate": 81.5,
            "profitable": win_rate >= 81.5,
            "avg_mae": round(avg_mae * 100, 2),
            "avg_mfe": round(avg_mfe * 100, 2),
            "long_signals": len(longs),
            "long_wins": long_wins,
            "long_win_rate": round(long_wins / len(longs) * 100, 2) if longs else 0,
            "short_signals": len(shorts),
            "short_wins": short_wins,
            "short_win_rate": round(short_wins / len(shorts) * 100, 2) if shorts else 0,
        }


async def download_data_if_needed(
    symbol: str,
    start_date: datetime,
    end_date: datetime,
) -> None:
    """Download historical data if not available."""
    kline_repo = KlineRepository()
    aggtrade_repo = AggTradeRepository()

    # Check if we have klines
    last_kline = await kline_repo.get_last_timestamp(symbol, "5m")
    if not last_kline or last_kline < end_date:
        logger.info(f"Downloading klines for {symbol}...")
        from app.clients import BinanceRestClient

        client = BinanceRestClient()
        klines = await client.get_all_klines(symbol, "5m", start_date, end_date)
        await kline_repo.save_batch(klines)
        await client.close()
        logger.info(f"Downloaded {len(klines)} klines")

    # Check if we have aggTrades
    last_trade = await aggtrade_repo.get_last_timestamp(symbol)
    if not last_trade or last_trade < end_date:
        logger.info(f"Downloading aggTrades for {symbol}...")
        downloader = AggTradeDownloader()
        await downloader.sync_historical(symbol, start_date, end_date)
        await downloader.close()


async def main():
    parser = argparse.ArgumentParser(description="Backtest MSR Retest Capture strategy")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair")
    parser.add_argument("--days", type=int, default=7, help="Days to backtest")
    parser.add_argument("--download", action="store_true", help="Download data first")
    args = parser.parse_args()

    # Initialize database
    await init_database()

    # Calculate date range
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=args.days)

    # Download data if requested
    if args.download:
        await download_data_if_needed(args.symbol, start_date, end_date)

    # Run backtest
    backtester = Backtester()
    results = await backtester.run_backtest(
        symbol=args.symbol,
        timeframe="5m",
        start_date=start_date,
        end_date=end_date,
    )

    # Print results
    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    print(f"Symbol:           {results.get('symbol', 'N/A')}")
    print(f"Period:           {results.get('period', 'N/A')}")
    print(f"Total Signals:    {results.get('total_signals', 0)}")
    print(f"Wins:             {results.get('wins', 0)}")
    print(f"Losses:           {results.get('losses', 0)}")
    print(f"Active:           {results.get('active', 0)}")
    print(f"Win Rate:         {results.get('win_rate', 0)}%")
    print(f"Breakeven:        {results.get('breakeven_win_rate', 81.5)}%")
    print(f"Profitable:       {'YES' if results.get('profitable') else 'NO'}")
    print("-" * 60)
    print(f"Avg MAE:          {results.get('avg_mae', 0)}%")
    print(f"Avg MFE:          {results.get('avg_mfe', 0)}%")
    print("-" * 60)
    print(f"Long Signals:     {results.get('long_signals', 0)}")
    print(f"Long Win Rate:    {results.get('long_win_rate', 0)}%")
    print(f"Short Signals:    {results.get('short_signals', 0)}")
    print(f"Short Win Rate:   {results.get('short_win_rate', 0)}%")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
