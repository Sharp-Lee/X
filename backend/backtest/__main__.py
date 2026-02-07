"""CLI entry point for the backtesting system.

Usage:
    python -m backtest --start 2025-01-01 --end 2025-12-31
    python -m backtest --symbols BTCUSDT,ETHUSDT --start 2025-06-01 --end 2025-12-31
    python -m backtest --download --start 2025-01-01 --end 2025-12-31
    python -m backtest --start 2025-01-01 --end 2025-12-31 -o results.json
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

from backtest.report import ReportFormatter
from backtest.runner import BacktestConfig, BacktestRunner

DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT"
DEFAULT_TIMEFRAMES = "1m,3m,5m,15m,30m"


def parse_date(date_str: str) -> datetime:
    """Parse YYYY-MM-DD to timezone-aware datetime."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str} (expected YYYY-MM-DD)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest MSR Retest Capture strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m backtest --start 2025-06-01 --end 2025-12-31
  python -m backtest --symbols BTCUSDT --start 2025-06-01 --end 2025-12-31 --timeframes 5m
  python -m backtest --download --start 2025-01-01 --end 2025-12-31
  python -m backtest --start 2025-01-01 --end 2025-12-31 -o results.json
        """,
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=DEFAULT_SYMBOLS,
        help=f"Comma-separated symbols (default: {DEFAULT_SYMBOLS})",
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        required=True,
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
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output file path for JSON results",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Reduce noise from libraries
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    symbols = [s.strip() for s in args.symbols.split(",")]
    timeframes = [t.strip() for t in args.timeframes.split(",")]

    print(f"\nBacktest: {', '.join(symbols)}")
    print(f"Period: {args.start:%Y-%m-%d} → {args.end:%Y-%m-%d}")
    print(f"Timeframes: {', '.join(timeframes)}")

    # Initialize database
    from app.storage.database import init_database

    await init_database()

    # Create config
    config = BacktestConfig(
        symbols=symbols,
        timeframes=timeframes,
        start_date=args.start,
        end_date=args.end,
        strategy=StrategyConfig(),
    )

    runner = BacktestRunner(config)

    # Optional: download data first
    if args.download:
        print("\nDownloading 1m klines from data.binance.vision...")
        counts = await runner.download_data()
        for symbol, count in counts.items():
            print(f"  {symbol}: {count:,} klines")

    # Run backtest
    print("\nRunning backtest...")
    result = await runner.run()

    # Print console report
    ReportFormatter.print_console(result)

    # Optional: save JSON
    if args.output:
        ReportFormatter.save_json(result, args.output)


if __name__ == "__main__":
    asyncio.run(main())
