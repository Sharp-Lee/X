"""CLI entry point for the backtesting system.

Completely independent of app/ — uses backtest's own storage layer
with a shared asyncpg pool for both klines and signals.

Usage:
    python -m backtest --start 2025-01-01 --end 2025-12-31
    python -m backtest --symbols BTCUSDT,ETHUSDT --start 2025-06-01 --end 2025-12-31
    python -m backtest --download --start 2025-01-01 --end 2025-12-31
    python -m backtest --list-runs
    python -m backtest --delete-run abc123
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

# Add backend to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models.config import StrategyConfig

from backtest.config import get_backtest_settings
from backtest.report import ReportFormatter
from backtest.runner import BacktestConfig, BacktestRunner
from backtest.storage.database import BacktestDatabase
from backtest.storage.kline_source import PostgresKlineSource
from backtest.storage.signal_repo import BacktestSignalRepo

DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT"
DEFAULT_TIMEFRAMES = "1m,3m,5m,15m,30m"


def parse_date(date_str: str) -> datetime:
    """Parse YYYY-MM-DD to timezone-aware datetime."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str} (expected YYYY-MM-DD)"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest MSR Retest Capture strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m backtest --start 2025-06-01 --end 2025-12-31
  python -m backtest --symbols BTCUSDT --start 2025-06-01 --end 2025-12-31 --timeframes 5m
  python -m backtest --download --start 2025-01-01 --end 2025-12-31
  python -m backtest --list-runs
  python -m backtest --delete-run abc123
        """,
    )

    # Management commands
    parser.add_argument(
        "--list-runs",
        action="store_true",
        help="List all previous backtest runs",
    )
    parser.add_argument(
        "--delete-run",
        type=str,
        default=None,
        help="Delete a backtest run by ID",
    )

    # Backtest parameters
    parser.add_argument(
        "--symbols",
        type=str,
        default=DEFAULT_SYMBOLS,
        help=f"Comma-separated symbols (default: {DEFAULT_SYMBOLS})",
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        default=None,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        default=None,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--timeframes",
        type=str,
        default=DEFAULT_TIMEFRAMES,
        help=f"Comma-separated timeframes (default: {DEFAULT_TIMEFRAMES})",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download 1m klines from data.binance.vision first",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file path for JSON results",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging",
    )
    return parser.parse_args()


async def cmd_list_runs(db: BacktestDatabase) -> None:
    """List all backtest runs."""
    repo = BacktestSignalRepo(db.pool)
    runs = await repo.list_runs()

    if not runs:
        print("No backtest runs found.")
        return

    print(
        f"\n{'ID':<18} {'Status':<10} {'Period':<25} "
        f"{'Signals':>8} {'Win%':>7} {'Total R':>9} {'PF':>6}"
    )
    print("-" * 90)
    for r in runs:
        start = r["start_date"].strftime("%Y-%m-%d")
        end = r["end_date"].strftime("%Y-%m-%d")
        period = f"{start} → {end}"
        print(
            f"{r['id']:<18} {r['status']:<10} {period:<25} "
            f"{r['total_signals']:>8} {r['win_rate']:>6.1f}% "
            f"{r['total_r']:>+8.1f}R {r['profit_factor']:>5.2f}"
        )
    print()


async def cmd_delete_run(db: BacktestDatabase, run_id: str) -> None:
    """Delete a backtest run."""
    repo = BacktestSignalRepo(db.pool)
    if await repo.delete_run(run_id):
        print(f"Deleted run {run_id}")
    else:
        print(f"Run {run_id} not found")


async def cmd_run_backtest(args: argparse.Namespace, db: BacktestDatabase) -> None:
    """Run a backtest."""
    if args.start is None or args.end is None:
        print("Error: --start and --end are required for backtest")
        sys.exit(1)

    settings = get_backtest_settings()
    symbols = [s.strip() for s in args.symbols.split(",")]
    timeframes = [t.strip() for t in args.timeframes.split(",")]

    # End date should include the full day
    end_date = args.end.replace(hour=23, minute=59, second=59)

    print(f"\nBacktest: {', '.join(symbols)}")
    print(f"Period: {args.start:%Y-%m-%d} → {end_date:%Y-%m-%d}")
    print(f"Timeframes: {', '.join(timeframes)}")

    config = BacktestConfig(
        symbols=symbols,
        timeframes=timeframes,
        start_date=args.start,
        end_date=end_date,
        strategy=StrategyConfig(),
    )

    # Optional: download data first
    if args.download:
        print("\nDownloading 1m klines from data.binance.vision...")
        counts = await BacktestRunner.download_data(
            database_url=settings.database_url,
            symbols=symbols,
            start_date=args.start,
            end_date=end_date,
        )
        for symbol, count in counts.items():
            print(f"  {symbol}: {count:,} klines")

    # Create runner with shared pool
    kline_source = PostgresKlineSource(db.pool)
    signal_repo = BacktestSignalRepo(db.pool)

    runner = BacktestRunner(
        config=config,
        kline_source=kline_source,
        signal_repo=signal_repo,
    )

    # Run backtest
    print("\nRunning backtest...")
    result = await runner.run()

    # Print console report
    ReportFormatter.print_console(result)

    # Optional: save JSON
    if args.output:
        ReportFormatter.save_json(result, args.output)


async def main() -> None:
    args = parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    settings = get_backtest_settings()

    # Initialize shared database pool
    db = BacktestDatabase(settings.database_url)
    await db.init()

    try:
        if args.list_runs:
            await cmd_list_runs(db)
        elif args.delete_run:
            await cmd_delete_run(db, args.delete_run)
        else:
            await cmd_run_backtest(args, db)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
