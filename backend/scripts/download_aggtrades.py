#!/usr/bin/env python3
"""
历史 AggTrade 数据下载脚本 (并行优化版)
========================================

从 Binance Data Vision 下载历史聚合交易数据并存入数据库。
支持并行下载多天/多交易对数据。

使用方式:
    # 下载最近 7 天数据 (所有交易对并行)
    python scripts/download_aggtrades.py --days 7

    # 下载指定交易对
    python scripts/download_aggtrades.py --symbol BTCUSDT --days 30

    # 下载所有配置的交易对 (并行)
    python scripts/download_aggtrades.py --all --days 7

    # 调整并行度
    python scripts/download_aggtrades.py --days 7 --parallel 10 --symbols-parallel 3

优化特性:
    - 多天并行下载 (单个 symbol 内)
    - 多 symbol 并行处理
    - 连接池复用
    - COPY 命令批量导入
"""

import argparse
import asyncio
import logging
import sys
import os
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import get_settings
from app.services.aggtrade_downloader import AggTradeDownloader, download_all_symbols_parallel
from app.storage import init_database
from app.storage.fast_import import close_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> datetime:
    """解析日期字符串"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"无效日期格式: {date_str}，请使用 YYYY-MM-DD")


async def main():
    parser = argparse.ArgumentParser(
        description="下载历史 AggTrade 数据 (并行优化版)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python scripts/download_aggtrades.py --days 7
  python scripts/download_aggtrades.py --symbol BTCUSDT --days 30
  python scripts/download_aggtrades.py --all --days 7 --symbols-parallel 5
        """
    )

    symbol_group = parser.add_mutually_exclusive_group()
    symbol_group.add_argument("--symbol", "-s", type=str, help="指定交易对")
    symbol_group.add_argument("--all", "-a", action="store_true", help="所有配置的交易对")

    time_group = parser.add_mutually_exclusive_group(required=True)
    time_group.add_argument("--days", "-d", type=int, help="下载最近 N 天")
    time_group.add_argument("--start", type=parse_date, help="开始日期 (YYYY-MM-DD)")

    parser.add_argument("--end", type=parse_date, help="结束日期")
    parser.add_argument("--parallel", "-p", type=int, default=10, help="每个symbol并行下载数 (默认: 10)")
    parser.add_argument("--symbols-parallel", type=int, default=3, help="并行处理的symbol数 (默认: 3)")
    parser.add_argument("--batch-size", type=int, default=50000, help="批量写入大小 (默认: 50000)")

    args = parser.parse_args()

    settings = get_settings()
    symbols = [args.symbol.upper()] if args.symbol else settings.symbols

    print()
    print("=" * 60)
    print("   AggTrade 历史数据下载 (并行优化版)")
    print("=" * 60)
    print()
    print(f"交易对: {', '.join(symbols)}")
    if args.days:
        print(f"时间范围: 最近 {args.days} 天")
    else:
        end = args.end or (datetime.now(timezone.utc) - timedelta(days=1))
        print(f"时间范围: {args.start.date()} 到 {end.date()}")
    print(f"并行度: {args.symbols_parallel} symbols × {args.parallel} downloads")
    print()
    print("-" * 60)

    await init_database()
    start_time = time.time()

    try:
        if len(symbols) > 1 and args.days:
            # 多交易对并行下载
            results = await download_all_symbols_parallel(
                symbols=symbols,
                days=args.days,
                max_concurrent_symbols=args.symbols_parallel,
            )
        else:
            # 单交易对下载
            downloader = AggTradeDownloader(
                batch_size=args.batch_size,
                max_concurrent_downloads=args.parallel,
            )
            results = {}
            try:
                for symbol in symbols:
                    if args.days:
                        count = await downloader.sync_recent(symbol, args.days)
                    else:
                        count = await downloader.sync_historical(symbol, args.start, args.end)
                    results[symbol] = count
            finally:
                await downloader.close()

        elapsed = time.time() - start_time
        total_count = sum(results.values())

        print()
        print("-" * 60)
        print("下载完成!")
        print("-" * 60)
        print()

        for symbol, count in sorted(results.items()):
            print(f"  {symbol}: {count:,} 条")

        print()
        print(f"  总计: {total_count:,} 条")
        print(f"  耗时: {elapsed:.1f} 秒")
        if elapsed > 0:
            print(f"  速度: {total_count / elapsed:,.0f} 条/秒")
        print()

    finally:
        # 关闭连接池
        await close_pool()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n下载已取消。")
        sys.exit(0)
