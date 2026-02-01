#!/usr/bin/env python3
"""
AggTrades 全量历史数据下载脚本
==============================

从 Binance Data Vision 下载完整历史 aggTrades 数据。

特性:
- 支持断点续传（检查已有数据，跳过已下载的月份）
- 实时进度显示
- 下载速度和预估时间
- 支持单独下载指定币对

使用方式:
    # 检查当前状态
    python scripts/download_aggtrades_full.py --status

    # 下载所有币对全部历史
    python scripts/download_aggtrades_full.py --download

    # 只下载指定币对
    python scripts/download_aggtrades_full.py --download --symbol BTCUSDT

    # 从指定日期开始下载
    python scripts/download_aggtrades_full.py --download --start 2023-01-01

    # 跳过已有月份（断点续传）
    python scripts/download_aggtrades_full.py --download --resume

数据来源:
    https://data.binance.vision/data/futures/um/monthly/aggTrades/
    https://data.binance.vision/data/futures/um/daily/aggTrades/
"""

import asyncio
import argparse
import csv
import io
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
from app.config import get_settings

# Constants
MONTHLY_URL = "https://data.binance.vision/data/futures/um/monthly/aggTrades"
DAILY_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
BATCH_SIZE = 50000


def format_size(size_bytes: int) -> str:
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}TB"


def format_time(seconds: float) -> str:
    """格式化时间"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}min"
    else:
        return f"{seconds/3600:.1f}h"


async def get_existing_months(conn, symbol: str) -> set:
    """获取数据库中已有数据的月份"""
    rows = await conn.fetch("""
        SELECT DISTINCT DATE_TRUNC('month', timestamp)::date as month
        FROM aggtrades
        WHERE symbol = $1
    """, symbol)
    return {row['month'].strftime('%Y-%m') for row in rows}


async def check_status():
    """检查当前数据状态"""
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url)

    print()
    print("=" * 70)
    print("   AggTrades 数据状态")
    print("=" * 70)

    stats = await conn.fetch("""
        SELECT
            symbol,
            COUNT(*) as cnt,
            MIN(timestamp)::date as min_date,
            MAX(timestamp)::date as max_date
        FROM aggtrades
        GROUP BY symbol
        ORDER BY symbol
    """)

    if not stats:
        print()
        print("  数据库中无 aggtrades 数据")
    else:
        print()
        total = 0
        for row in stats:
            cnt = row['cnt']
            total += cnt
            print(f"  {row['symbol']}: {cnt:>15,} 条  {row['min_date']} ~ {row['max_date']}")
        print("-" * 70)
        print(f"  总计: {total:,} 条")

    # 估算完整数据量
    print()
    print("完整历史数据估算 (2020-01 至今):")
    print("-" * 70)
    months = (datetime.now().year - 2020) * 12 + datetime.now().month
    est_per_symbol = 7_000_000 * months  # 约 7M/月/币对
    est_total = est_per_symbol * len(SYMBOLS)
    print(f"  预计每币对: ~{est_per_symbol/1e6:.0f}M 条 ({months} 个月)")
    print(f"  预计总量: ~{est_total/1e6:.0f}M 条")
    print(f"  预计存储: ~{est_total * 50 / 1e9:.0f}GB")  # 约 50 bytes/条
    print()

    await conn.close()


async def download_file(client: httpx.AsyncClient, url: str) -> bytes | None:
    """下载单个文件"""
    try:
        response = await client.get(url)
        if response.status_code == 200:
            return response.content
        elif response.status_code == 404:
            return None
        else:
            return None
    except Exception as e:
        print(f"\n  下载错误: {e}")
        return None


async def save_batch(conn, symbol: str, records: list) -> int:
    """批量保存数据"""
    if not records:
        return 0

    try:
        # 使用 COPY 命令快速导入
        result = await conn.copy_records_to_table(
            'aggtrades',
            records=records,
            columns=['symbol', 'timestamp', 'agg_trade_id', 'price', 'quantity', 'is_buyer_maker']
        )
        return int(result.split()[1])
    except asyncpg.UniqueViolationError:
        # 有重复数据，使用 INSERT ON CONFLICT
        await conn.executemany("""
            INSERT INTO aggtrades (symbol, timestamp, agg_trade_id, price, quantity, is_buyer_maker)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (symbol, timestamp, agg_trade_id) DO NOTHING
        """, records)
        return 0  # 无法准确计数


def parse_csv(content: bytes, symbol: str):
    """解析 CSV 内容，返回记录列表"""
    records = []
    text = content.decode('utf-8')
    reader = csv.reader(io.StringIO(text))

    for row in reader:
        if not row or row[0] == 'agg_trade_id' or not row[0].isdigit():
            continue
        try:
            records.append((
                symbol,
                datetime.fromtimestamp(int(row[5]) / 1000, tz=timezone.utc),
                int(row[0]),
                float(row[1]),
                float(row[2]),
                row[6].lower() == 'true'
            ))
        except:
            continue

    return records


async def download_symbol(
    symbol: str,
    start_date: datetime,
    end_date: datetime,
    resume: bool = False
):
    """下载单个币对的全部历史数据"""
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url)

    # 获取已有月份
    existing_months = set()
    if resume:
        existing_months = await get_existing_months(conn, symbol)
        if existing_months:
            print(f"  已有 {len(existing_months)} 个月份数据，将跳过")

    # 创建 HTTP 客户端
    client = httpx.AsyncClient(timeout=600.0)  # 10分钟超时

    total_records = 0
    total_bytes = 0
    start_time = time.time()

    current = start_date.replace(day=1)

    try:
        while current < end_date:
            year, month = current.year, current.month
            month_str = f"{year}-{month:02d}"
            month_end = (current + timedelta(days=32)).replace(day=1) - timedelta(days=1)

            # 检查是否跳过
            if month_str in existing_months:
                print(f"  {month_str} 已存在，跳过")
                current = (current + timedelta(days=32)).replace(day=1)
                continue

            if month_end < end_date:
                # 完整月份 - 下载月度文件
                url = f"{MONTHLY_URL}/{symbol}/{symbol}-aggTrades-{year}-{month:02d}.zip"
                print(f"  {month_str}...", end=" ", flush=True)

                dl_start = time.time()
                content = await download_file(client, url)
                dl_time = time.time() - dl_start

                if content:
                    size = len(content)
                    total_bytes += size
                    speed = size / dl_time if dl_time > 0 else 0

                    # 解压并解析
                    print(f"下载 {format_size(size)} ({format_size(speed)}/s)", end=" ", flush=True)

                    try:
                        with zipfile.ZipFile(io.BytesIO(content)) as zf:
                            for name in zf.namelist():
                                if name.endswith('.csv'):
                                    csv_content = zf.read(name)
                                    records = parse_csv(csv_content, symbol)

                                    # 分批保存
                                    month_count = 0
                                    for i in range(0, len(records), BATCH_SIZE):
                                        batch = records[i:i+BATCH_SIZE]
                                        count = await save_batch(conn, symbol, batch)
                                        month_count += count

                                    total_records += month_count
                                    print(f"-> {month_count:,} 条")
                                    break
                    except Exception as e:
                        print(f"解析错误: {e}")
                else:
                    print("无数据")
            else:
                # 部分月份 - 使用日度文件
                dates = []
                day = current
                while day <= end_date and day.month == month:
                    dates.append(day)
                    day += timedelta(days=1)

                print(f"  {month_str} (日度x{len(dates)})...", end=" ", flush=True)

                month_count = 0
                for d in dates:
                    ds = d.strftime('%Y-%m-%d')
                    url = f"{DAILY_URL}/{symbol}/{symbol}-aggTrades-{ds}.zip"

                    content = await download_file(client, url)
                    if content:
                        total_bytes += len(content)
                        try:
                            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                                for name in zf.namelist():
                                    if name.endswith('.csv'):
                                        csv_content = zf.read(name)
                                        records = parse_csv(csv_content, symbol)

                                        for i in range(0, len(records), BATCH_SIZE):
                                            batch = records[i:i+BATCH_SIZE]
                                            count = await save_batch(conn, symbol, batch)
                                            month_count += count
                                        break
                        except:
                            pass

                total_records += month_count
                print(f"-> {month_count:,} 条")

            current = (current + timedelta(days=32)).replace(day=1)

        elapsed = time.time() - start_time
        print(f"  完成: {total_records:,} 条, {format_size(total_bytes)}, {format_time(elapsed)}")

    finally:
        await client.aclose()
        await conn.close()

    return total_records


async def download_all(
    symbols: list[str],
    start_date: datetime,
    end_date: datetime,
    resume: bool = False
):
    """下载所有币对"""
    print()
    print("=" * 70)
    print("   AggTrades 全量历史数据下载")
    print("=" * 70)
    print()
    print(f"币对: {', '.join(symbols)}")
    print(f"时间: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print(f"续传: {'是' if resume else '否'}")
    print()

    results = {}
    total_start = time.time()

    for symbol in symbols:
        print(f"[{symbol}]")
        count = await download_symbol(symbol, start_date, end_date, resume)
        results[symbol] = count
        print()

    total_elapsed = time.time() - total_start
    total_count = sum(results.values())

    print("=" * 70)
    print("   下载完成")
    print("=" * 70)
    for symbol, count in sorted(results.items()):
        print(f"  {symbol}: {count:,} 条")
    print("-" * 70)
    print(f"  总计: {total_count:,} 条")
    print(f"  耗时: {format_time(total_elapsed)}")
    if total_elapsed > 0:
        print(f"  速度: {total_count/total_elapsed:,.0f} 条/秒")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="AggTrades 全量历史数据下载",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--status", action="store_true", help="查看当前数据状态")
    parser.add_argument("--download", action="store_true", help="开始下载")
    parser.add_argument("--symbol", type=str, help="只下载指定币对")
    parser.add_argument("--start", type=str, default="2020-01-01", help="开始日期 (默认: 2020-01-01)")
    parser.add_argument("--end", type=str, help="结束日期 (默认: 昨天)")
    parser.add_argument("--resume", action="store_true", help="断点续传 (跳过已有月份)")

    args = parser.parse_args()

    if args.status:
        asyncio.run(check_status())
    elif args.download:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if args.end:
            end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)

        symbols = [args.symbol.upper()] if args.symbol else SYMBOLS

        asyncio.run(download_all(symbols, start_date, end_date, args.resume))
    else:
        # 默认显示状态
        asyncio.run(check_status())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n下载已取消。使用 --resume 可断点续传。")
        sys.exit(0)
