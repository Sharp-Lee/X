#!/usr/bin/env python3
"""
AggTrades 终极导入脚本
======================

特性:
- 内存 COPY (跳过磁盘 I/O)
- 合并多月 CSV 批量导入
- UNLOGGED 表 (无 WAL)
- 删除索引后导入
- 5 币对全并行
- 自动验证

预计性能: 104GB 数据 5-10 分钟
"""

import asyncio
import io
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg

# 配置
DATA_DIR = Path(__file__).parent.parent.parent.parent / "binance-data"
SYMBOL_DIRS = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "bnb": "BNBUSDT",
    "xrp": "XRPUSDT",
}
DB_URL = "postgresql://localhost/crypto_data"

# 内存控制参数
MONTHS_PER_BATCH = 1   # 每次只处理1个月 (控制单进程内存)
MAX_PARALLEL = 1       # 最多1个并行导入 (减少资源占用)


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


def combine_csvs(csv_contents: list[bytes]) -> bytes:
    """合并多个 CSV，只保留第一个 header"""
    if not csv_contents:
        return b''

    result = csv_contents[0]
    for csv in csv_contents[1:]:
        # 跳过第一行 (header)
        newline_idx = csv.find(b'\n')
        if newline_idx != -1 and newline_idx < len(csv) - 1:
            result += csv[newline_idx + 1:]

    return result


async def prepare_database(conn):
    """准备数据库：删除索引、设置 UNLOGGED"""
    print("准备数据库...")

    # 获取现有索引
    indexes = await conn.fetch("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'aggtrades' AND indexname != 'aggtrades_pkey'
    """)

    # 删除索引
    for idx in indexes:
        await conn.execute(f"DROP INDEX IF EXISTS {idx['indexname']}")
        print(f"  删除索引: {idx['indexname']}")

    # 设置 UNLOGGED
    try:
        await conn.execute("ALTER TABLE aggtrades SET UNLOGGED")
        print("  设置 UNLOGGED 模式")
    except Exception as e:
        print(f"  UNLOGGED 设置跳过: {e}")

    # staging 表由各 symbol 导入时独立创建
    print("  staging 表将由各币对独立创建")
    print()


async def restore_database(conn):
    """恢复数据库：重建索引、设置 LOGGED"""
    print()
    print("恢复数据库...")

    # 设置 LOGGED
    try:
        await conn.execute("ALTER TABLE aggtrades SET LOGGED")
        print("  恢复 LOGGED 模式")
    except Exception as e:
        print(f"  LOGGED 设置跳过: {e}")

    # 重建索引
    print("  重建索引 (可能需要几分钟)...")

    start = time.time()
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_aggtrades_symbol_tradeid
        ON aggtrades (symbol, agg_trade_id)
    """)
    print(f"    idx_aggtrades_symbol_tradeid: {time.time()-start:.1f}s")

    start = time.time()
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS aggtrades_timestamp_idx
        ON aggtrades (timestamp)
    """)
    print(f"    aggtrades_timestamp_idx: {time.time()-start:.1f}s")

    # staging 表已由各 symbol 自行清理
    print("  staging 表已清理")

    # VACUUM ANALYZE
    print("  运行 VACUUM ANALYZE...")
    await conn.execute("VACUUM ANALYZE aggtrades")
    print()


async def import_symbol(symbol: str, dir_name: str, results: dict, semaphore: asyncio.Semaphore):
    """导入单个币对的所有数据"""
    async with semaphore:  # 限制并行数
        await _do_import_symbol(symbol, dir_name, results)


async def _do_import_symbol(symbol: str, dir_name: str, results: dict):
    """实际执行导入"""
    conn = await asyncpg.connect(DB_URL)

    # 每个币对使用独立的 staging 表
    staging_table = f"staging_{symbol.lower()}"

    # 创建独立 staging 表
    await conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
    await conn.execute(f"""
        CREATE UNLOGGED TABLE {staging_table} (
            agg_trade_id BIGINT,
            price TEXT,
            quantity TEXT,
            first_trade_id BIGINT,
            last_trade_id BIGINT,
            transact_time BIGINT,
            is_buyer_maker TEXT
        )
    """)

    dir_path = DATA_DIR / dir_name
    zip_files = sorted(dir_path.glob(f"{symbol}-aggTrades-*.zip"))

    print(f"[{symbol}] 开始导入 {len(zip_files)} 个文件")

    symbol_start = time.time()
    total_records = 0
    total_bytes = 0

    # 分批处理
    for batch_start in range(0, len(zip_files), MONTHS_PER_BATCH):
        batch_files = zip_files[batch_start:batch_start + MONTHS_PER_BATCH]
        batch_num = batch_start // MONTHS_PER_BATCH + 1
        total_batches = (len(zip_files) + MONTHS_PER_BATCH - 1) // MONTHS_PER_BATCH

        # 解压并合并 CSV
        csv_contents = []
        batch_size = 0

        for zf in batch_files:
            try:
                with zipfile.ZipFile(zf) as z:
                    for name in z.namelist():
                        if name.endswith('.csv'):
                            content = z.read(name)
                            csv_contents.append(content)
                            batch_size += len(content)
                            break
            except Exception as e:
                print(f"  [{symbol}] 错误读取 {zf.name}: {e}")
                continue

        if not csv_contents:
            continue

        # 合并 CSV
        combined_csv = combine_csvs(csv_contents)
        total_bytes += batch_size

        # 获取月份范围
        first_month = batch_files[0].stem.split('-')[2:4]
        last_month = batch_files[-1].stem.split('-')[2:4]
        month_range = f"{'-'.join(first_month)}~{'-'.join(last_month)}"

        print(f"  [{symbol}] 批次 {batch_num}/{total_batches} ({month_range}, {format_size(batch_size)})...", end=" ", flush=True)

        batch_start_time = time.time()

        try:
            # COPY 到独立 staging 表 (内存)
            await conn.execute(f"TRUNCATE {staging_table}")

            await conn.copy_to_table(
                staging_table,
                source=io.BytesIO(combined_csv),
                format='csv',
                header=True
            )

            # 获取 staging 表记录数
            staging_count = await conn.fetchval(f"SELECT COUNT(*) FROM {staging_table}")

            # 转换并插入目标表 (ON CONFLICT 处理重复)
            await conn.execute(f"""
                INSERT INTO aggtrades (symbol, timestamp, agg_trade_id, price, quantity, is_buyer_maker)
                SELECT
                    '{symbol}',
                    to_timestamp(transact_time / 1000.0) AT TIME ZONE 'UTC',
                    agg_trade_id,
                    price::numeric,
                    quantity::numeric,
                    is_buyer_maker::boolean
                FROM {staging_table}
                ON CONFLICT (symbol, timestamp, agg_trade_id) DO NOTHING
            """)

            total_records += staging_count

            elapsed = time.time() - batch_start_time
            speed = staging_count / elapsed if elapsed > 0 else 0
            print(f"{staging_count:,} 条 ({speed:,.0f}/s)")

        except Exception as e:
            print(f"错误: {e}")
            import traceback
            traceback.print_exc()

    # 清理 staging 表
    await conn.execute(f"DROP TABLE IF EXISTS {staging_table}")

    # 完成
    symbol_elapsed = time.time() - symbol_start
    results[symbol] = {
        'records': total_records,
        'bytes': total_bytes,
        'time': symbol_elapsed
    }

    print(f"  [{symbol}] 完成: {total_records:,} 条, {format_size(total_bytes)}, {format_time(symbol_elapsed)}")

    await conn.close()


async def verify_data(conn):
    """验证导入数据"""
    print("数据验证:")
    print("-" * 70)

    stats = await conn.fetch("""
        SELECT
            symbol,
            COUNT(*) as cnt,
            MIN(timestamp)::date as min_date,
            MAX(timestamp)::date as max_date,
            MIN(agg_trade_id) as min_id,
            MAX(agg_trade_id) as max_id
        FROM aggtrades
        GROUP BY symbol
        ORDER BY symbol
    """)

    total = 0
    all_valid = True

    for row in stats:
        cnt = row['cnt']
        total += cnt

        # 基本检查
        valid = True
        issues = []

        if cnt == 0:
            valid = False
            issues.append("无数据")

        if row['min_date'] is None:
            valid = False
            issues.append("时间戳异常")

        status = "✓" if valid else f"✗ {', '.join(issues)}"
        if not valid:
            all_valid = False

        print(f"  {row['symbol']}: {cnt:>15,} 条  {row['min_date']} ~ {row['max_date']}  {status}")

    print("-" * 70)
    print(f"  总计: {total:,} 条")
    print()

    # 抽样检查数据质量
    print("数据质量抽样检查:")

    # 检查是否有异常值
    checks = await conn.fetch("""
        SELECT
            COUNT(*) FILTER (WHERE price <= 0) as negative_price,
            COUNT(*) FILTER (WHERE quantity <= 0) as negative_qty,
            COUNT(*) FILTER (WHERE timestamp < '2019-01-01') as old_timestamp,
            COUNT(*) FILTER (WHERE timestamp > NOW()) as future_timestamp
        FROM aggtrades
    """)

    check = checks[0]
    issues = []
    if check['negative_price'] > 0:
        issues.append(f"负价格: {check['negative_price']}")
    if check['negative_qty'] > 0:
        issues.append(f"负数量: {check['negative_qty']}")
    if check['old_timestamp'] > 0:
        issues.append(f"过早时间戳: {check['old_timestamp']}")
    if check['future_timestamp'] > 0:
        issues.append(f"未来时间戳: {check['future_timestamp']}")

    if issues:
        print(f"  警告: {', '.join(issues)}")
        all_valid = False
    else:
        print("  ✓ 所有检查通过")

    print()
    return all_valid


async def main():
    print()
    print("=" * 70)
    print("   AggTrades 终极导入")
    print("=" * 70)
    print()
    print(f"数据目录: {DATA_DIR}")
    print(f"数据库: {DB_URL}")
    print(f"批次大小: {MONTHS_PER_BATCH} 个月/批")
    print()

    # 主连接用于准备和恢复
    conn = await asyncpg.connect(DB_URL)

    # 准备数据库
    await prepare_database(conn)

    # 并行导入所有币对 (限制并发数)
    print(f"开始导入 (最多 {MAX_PARALLEL} 并行):")
    print("-" * 70)

    total_start = time.time()
    results = {}
    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    tasks = [
        import_symbol(symbol, dir_name, results, semaphore)
        for dir_name, symbol in SYMBOL_DIRS.items()
    ]

    await asyncio.gather(*tasks)

    total_elapsed = time.time() - total_start

    # 汇总
    print()
    print("=" * 70)
    print("   导入完成")
    print("=" * 70)

    total_records = sum(r['records'] for r in results.values())
    total_bytes = sum(r['bytes'] for r in results.values())

    print(f"  总记录: {total_records:,} 条")
    print(f"  总数据: {format_size(total_bytes)}")
    print(f"  总耗时: {format_time(total_elapsed)}")
    print(f"  平均速度: {total_records/total_elapsed:,.0f} 条/秒")
    print()

    # 恢复数据库
    await restore_database(conn)

    # 验证数据
    await verify_data(conn)

    await conn.close()

    print("导入完成!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n已取消")
        sys.exit(1)
