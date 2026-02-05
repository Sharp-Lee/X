#!/usr/bin/env python3
"""
从本地 zip 文件导入 aggTrades 数据到数据库
"""

import asyncio
import csv
import io
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
from app.config import get_settings

# 配置
DATA_DIR = Path(__file__).parent.parent.parent.parent / "binance-data"
SYMBOL_DIRS = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "bnb": "BNBUSDT",
    "xrp": "XRPUSDT",
}
BATCH_SIZE = 100000


def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}TB"


def format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}min"
    else:
        return f"{seconds/3600:.1f}h"


def parse_csv(content: bytes, symbol: str):
    """解析 CSV 内容"""
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
        except Exception:
            continue

    return records


async def import_zip(conn, zip_path: Path, symbol: str) -> int:
    """导入单个 zip 文件"""
    total = 0

    try:
        with zipfile.ZipFile(zip_path) as zf:
            for name in zf.namelist():
                if name.endswith('.csv'):
                    csv_content = zf.read(name)
                    records = parse_csv(csv_content, symbol)

                    # 分批导入
                    for i in range(0, len(records), BATCH_SIZE):
                        batch = records[i:i+BATCH_SIZE]
                        try:
                            result = await conn.copy_records_to_table(
                                'aggtrades',
                                records=batch,
                                columns=['symbol', 'timestamp', 'agg_trade_id', 'price', 'quantity', 'is_buyer_maker']
                            )
                            total += int(result.split()[1])
                        except asyncpg.UniqueViolationError:
                            # 有重复，逐条插入
                            await conn.executemany("""
                                INSERT INTO aggtrades (symbol, timestamp, agg_trade_id, price, quantity, is_buyer_maker)
                                VALUES ($1, $2, $3, $4, $5, $6)
                                ON CONFLICT (symbol, timestamp, agg_trade_id) DO NOTHING
                            """, batch)
                    break
    except Exception as e:
        print(f" 错误: {e}")
        return 0

    return total


async def check_files():
    """检查文件完整性"""
    print()
    print("=" * 70)
    print("   数据文件检查")
    print("=" * 70)
    print()

    all_valid = True
    total_size = 0
    total_files = 0

    for dir_name, symbol in SYMBOL_DIRS.items():
        dir_path = DATA_DIR / dir_name
        if not dir_path.exists():
            print(f"  {symbol}: 目录不存在 ✗")
            all_valid = False
            continue

        zip_files = sorted(dir_path.glob(f"{symbol}-aggTrades-*.zip"))
        if not zip_files:
            print(f"  {symbol}: 无 zip 文件 ✗")
            all_valid = False
            continue

        # 检查文件
        size = sum(f.stat().st_size for f in zip_files)
        total_size += size
        total_files += len(zip_files)

        # 获取时间范围
        dates = []
        for f in zip_files:
            parts = f.stem.split('-')
            if len(parts) >= 4:
                dates.append(f"{parts[2]}-{parts[3]}")

        first = min(dates) if dates else "?"
        last = max(dates) if dates else "?"

        # 验证几个文件
        valid = True
        for zf in [zip_files[0], zip_files[-1]]:
            try:
                with zipfile.ZipFile(zf) as z:
                    z.testzip()
            except Exception:
                valid = False
                break

        status = "✓" if valid else "✗"
        print(f"  {symbol}: {len(zip_files)} 文件, {format_size(size)}, {first} ~ {last} {status}")

        if not valid:
            all_valid = False

    print()
    print(f"  总计: {total_files} 文件, {format_size(total_size)}")
    print()

    return all_valid


async def import_symbol(symbol: str):
    """导入单个币对数据"""
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url)

    # 找到对应目录
    dir_name = None
    for dn, sym in SYMBOL_DIRS.items():
        if sym == symbol:
            dir_name = dn
            break

    if not dir_name:
        print(f"未知币对: {symbol}")
        return

    dir_path = DATA_DIR / dir_name
    zip_files = sorted(dir_path.glob(f"{symbol}-aggTrades-*.zip"))

    print(f"[{symbol}] 开始导入 {len(zip_files)} 个文件")
    print()

    symbol_start = time.time()
    symbol_count = 0

    for i, zf in enumerate(zip_files, 1):
        parts = zf.stem.split('-')
        month = f"{parts[2]}-{parts[3]}" if len(parts) >= 4 else zf.name

        size = zf.stat().st_size
        print(f"  {month} ({format_size(size)})...", end=" ", flush=True)

        start = time.time()
        count = await import_zip(conn, zf, symbol)
        elapsed = time.time() - start

        speed = count / elapsed if elapsed > 0 else 0
        print(f"{count:>12,} 条 ({speed:,.0f}/s)")

        symbol_count += count

    symbol_elapsed = time.time() - symbol_start
    print()
    print(f"[{symbol}] 完成: {symbol_count:,} 条, {format_time(symbol_elapsed)}")

    await conn.close()
    return symbol_count


async def import_all():
    """导入所有数据"""
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url)

    print()
    print("=" * 70)
    print("   开始导入 aggTrades 数据")
    print("=" * 70)
    print()

    total_records = 0
    total_start = time.time()

    for dir_name, symbol in SYMBOL_DIRS.items():
        dir_path = DATA_DIR / dir_name
        zip_files = sorted(dir_path.glob(f"{symbol}-aggTrades-*.zip"))

        print(f"[{symbol}] {len(zip_files)} 个文件")

        symbol_start = time.time()
        symbol_count = 0

        for i, zf in enumerate(zip_files, 1):
            # 提取月份
            parts = zf.stem.split('-')
            month = f"{parts[2]}-{parts[3]}" if len(parts) >= 4 else zf.name

            size = zf.stat().st_size
            print(f"  {month} ({format_size(size)})...", end=" ", flush=True)

            start = time.time()
            count = await import_zip(conn, zf, symbol)
            elapsed = time.time() - start

            speed = count / elapsed if elapsed > 0 else 0
            print(f"{count:>12,} 条 ({speed:,.0f}/s)")

            symbol_count += count

        symbol_elapsed = time.time() - symbol_start
        print(f"  完成: {symbol_count:,} 条, {format_time(symbol_elapsed)}")
        print()

        total_records += symbol_count

    total_elapsed = time.time() - total_start

    print("=" * 70)
    print("   导入完成")
    print("=" * 70)
    print(f"  总记录: {total_records:,} 条")
    print(f"  总耗时: {format_time(total_elapsed)}")
    print(f"  平均速度: {total_records/total_elapsed:,.0f} 条/秒")
    print()

    # 验证数据
    print("数据验证:")
    stats = await conn.fetch("""
        SELECT symbol, COUNT(*) as cnt,
               MIN(timestamp)::date as min_date,
               MAX(timestamp)::date as max_date
        FROM aggtrades
        GROUP BY symbol
        ORDER BY symbol
    """)
    for row in stats:
        print(f"  {row['symbol']}: {row['cnt']:,} 条, {row['min_date']} ~ {row['max_date']}")

    await conn.close()


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="导入本地 aggTrades zip 文件")
    parser.add_argument("--check", action="store_true", help="只检查文件，不导入")
    parser.add_argument("--import", dest="do_import", action="store_true", help="执行导入")
    parser.add_argument("--symbol", type=str, help="只导入指定币对 (如 BTCUSDT)")

    args = parser.parse_args()

    if args.check or not args.do_import:
        valid = await check_files()
        if not args.do_import:
            print("使用 --import 开始导入数据")
            return

    if args.do_import:
        if args.symbol:
            await import_symbol(args.symbol.upper())
        else:
            await import_all()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n已取消")
        sys.exit(1)
