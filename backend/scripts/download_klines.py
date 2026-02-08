#!/usr/bin/env python3
"""
历史 K 线数据下载脚本
====================

从 Binance Data Vision 下载历史 K 线数据并存入数据库。
使用 backtest/downloader.py（独立于 app/）。

使用方式:
    # 下载最近 7 天 1m K 线 (所有交易对)
    python scripts/download_klines.py --days 7

    # 下载指定交易对和周期
    python scripts/download_klines.py --symbol BTCUSDT --timeframe 1m --days 30

    # 下载多个周期
    python scripts/download_klines.py --days 7 --timeframes 1m,5m,15m

    # 下载历史数据
    python scripts/download_klines.py --start 2025-01-01 --end 2025-12-31
"""

import argparse
import asyncio
import logging
import sys
import os
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.config import get_backtest_settings
from backtest.downloader import KlineDownloader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]


def parse_date(date_str: str) -> datetime:
    """解析日期字符串"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"无效日期格式: {date_str}，请使用 YYYY-MM-DD")


async def main():
    parser = argparse.ArgumentParser(
        description="下载历史 K 线数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python scripts/download_klines.py --days 7
  python scripts/download_klines.py --symbol BTCUSDT --timeframe 5m --days 30
  python scripts/download_klines.py --days 7 --timeframes 1m,5m,15m
        """
    )

    symbol_group = parser.add_mutually_exclusive_group()
    symbol_group.add_argument("--symbol", "-s", type=str, help="指定交易对")
    symbol_group.add_argument("--all", "-a", action="store_true", help="所有默认交易对")

    parser.add_argument("--timeframe", "-t", type=str, default="1m", help="K线周期 (默认: 1m)")
    parser.add_argument("--timeframes", type=str, help="多个周期,逗号分隔 (如: 1m,5m,15m)")

    time_group = parser.add_mutually_exclusive_group(required=True)
    time_group.add_argument("--days", "-d", type=int, help="下载最近 N 天")
    time_group.add_argument("--start", type=parse_date, help="开始日期 (YYYY-MM-DD)")

    parser.add_argument("--end", type=parse_date, help="结束日期")
    parser.add_argument("--parallel", "-p", type=int, default=10, help="并行下载数 (默认: 10)")

    args = parser.parse_args()

    symbols = [args.symbol.upper()] if args.symbol else DEFAULT_SYMBOLS

    # Parse timeframes
    if args.timeframes:
        timeframes = [t.strip() for t in args.timeframes.split(",")]
    else:
        timeframes = [args.timeframe]

    # Calculate date range
    if args.days:
        end_date = datetime.now(timezone.utc) - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
    else:
        start_date = args.start
        end_date = args.end or (datetime.now(timezone.utc) - timedelta(days=1))

    print()
    print("=" * 60)
    print("   K 线历史数据下载")
    print("=" * 60)
    print()
    print(f"交易对: {', '.join(symbols)}")
    print(f"周期: {', '.join(timeframes)}")
    print(f"时间范围: {start_date.date()} 到 {end_date.date()}")
    print(f"并行度: {args.parallel}")
    print()
    print("-" * 60)

    settings = get_backtest_settings()
    start_time = time.time()
    results: dict[str, dict[str, int]] = {}

    downloader = KlineDownloader(
        database_url=settings.database_url,
        max_concurrent_downloads=args.parallel,
    )

    try:
        for symbol in symbols:
            results[symbol] = {}
            for timeframe in timeframes:
                count = await downloader.sync_historical(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                )
                results[symbol][timeframe] = count
    finally:
        await downloader.close()

    elapsed = time.time() - start_time

    # Calculate totals
    total_count = sum(
        count for tf_results in results.values() for count in tf_results.values()
    )

    print()
    print("-" * 60)
    print("下载完成!")
    print("-" * 60)
    print()

    for symbol, tf_results in sorted(results.items()):
        print(f"  {symbol}:")
        for timeframe, count in sorted(tf_results.items()):
            print(f"    {timeframe}: {count:,} 条")

    print()
    print(f"  总计: {total_count:,} 条")
    print(f"  耗时: {elapsed:.1f} 秒")
    if elapsed > 0:
        print(f"  速度: {total_count / elapsed:,.0f} 条/秒")
    print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n下载已取消。")
        sys.exit(0)
