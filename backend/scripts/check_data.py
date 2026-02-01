#!/usr/bin/env python3
"""
数据库数据质量检查脚本
======================

检查内容:
1. 数据量统计
2. 重复数据检测
3. 数据连续性 (agg_trade_id 缺口)
4. 时间范围覆盖
5. 数据一致性

使用方式:
    python scripts/check_data.py
    python scripts/check_data.py --fix-duplicates  # 删除重复数据
"""

import argparse
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
from app.config import get_settings


async def check_data(fix_duplicates: bool = False):
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url)

    print()
    print("=" * 60)
    print("   数据库数据质量检查")
    print("=" * 60)

    # 1. 数据量统计
    print()
    print("[1] 数据量统计")
    print("-" * 50)

    tables = ["aggtrades", "klines", "signals"]
    for table in tables:
        try:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table}: {count:,} 条")
        except Exception as e:
            print(f"  {table}: 错误 - {e}")

    # 2. AggTrades 按 symbol 分布
    print()
    print("[2] AggTrades 按交易对分布")
    print("-" * 50)

    rows = await conn.fetch("""
        SELECT symbol, COUNT(*) as cnt,
               MIN(timestamp) as min_ts,
               MAX(timestamp) as max_ts,
               MIN(agg_trade_id) as min_id,
               MAX(agg_trade_id) as max_id
        FROM aggtrades
        GROUP BY symbol
        ORDER BY cnt DESC
    """)

    for row in rows:
        print(f"  {row['symbol']}: {row['cnt']:,} 条")
        print(f"    时间: {row['min_ts'].strftime('%Y-%m-%d %H:%M')} ~ {row['max_ts'].strftime('%Y-%m-%d %H:%M')}")
        print(f"    ID范围: {row['min_id']:,} ~ {row['max_id']:,}")

    # 3. 重复数据检查
    print()
    print("[3] 重复数据检查")
    print("-" * 50)

    # 主键重复 (应该为0，因为有主键约束)
    dup_pk = await conn.fetchval("""
        SELECT COUNT(*) FROM (
            SELECT symbol, timestamp, agg_trade_id, COUNT(*) as cnt
            FROM aggtrades
            GROUP BY symbol, timestamp, agg_trade_id
            HAVING COUNT(*) > 1
        ) t
    """)
    print(f"  主键 (symbol, timestamp, agg_trade_id) 重复: {dup_pk}")

    # 同一 symbol 内 agg_trade_id 重复 (不同时间戳)
    dup_id = await conn.fetchval("""
        SELECT COUNT(*) FROM (
            SELECT symbol, agg_trade_id, COUNT(*) as cnt
            FROM aggtrades
            GROUP BY symbol, agg_trade_id
            HAVING COUNT(*) > 1
        ) t
    """)
    print(f"  同 symbol 内 agg_trade_id 重复: {dup_id}")

    if dup_id > 0:
        # 显示重复详情
        dup_details = await conn.fetch("""
            SELECT symbol, agg_trade_id, COUNT(*) as cnt
            FROM aggtrades
            GROUP BY symbol, agg_trade_id
            HAVING COUNT(*) > 1
            LIMIT 5
        """)
        print("  重复详情 (前5条):")
        for d in dup_details:
            print(f"    {d['symbol']} ID={d['agg_trade_id']}: {d['cnt']} 条")

        if fix_duplicates:
            print()
            print("  [修复] 删除重复数据...")
            # 保留每个 (symbol, agg_trade_id) 的第一条记录
            deleted = await conn.execute("""
                DELETE FROM aggtrades a
                USING (
                    SELECT symbol, agg_trade_id, MIN(ctid) as keep_ctid
                    FROM aggtrades
                    GROUP BY symbol, agg_trade_id
                    HAVING COUNT(*) > 1
                ) b
                WHERE a.symbol = b.symbol
                  AND a.agg_trade_id = b.agg_trade_id
                  AND a.ctid != b.keep_ctid
            """)
            print(f"  [修复] 删除完成: {deleted}")

    # 4. 数据连续性检查
    print()
    print("[4] 数据连续性检查 (agg_trade_id 缺口)")
    print("-" * 50)

    for row in rows[:5]:  # 检查前5个 symbol
        symbol = row["symbol"]

        # 计算缺口数量
        gap_count = await conn.fetchval("""
            WITH ordered AS (
                SELECT
                    agg_trade_id,
                    LAG(agg_trade_id) OVER (ORDER BY agg_trade_id) as prev_id
                FROM aggtrades
                WHERE symbol = $1
            )
            SELECT COUNT(*)
            FROM ordered
            WHERE prev_id IS NOT NULL AND agg_trade_id - prev_id > 1
        """, symbol)

        if gap_count == 0:
            print(f"  {symbol}: 无缺口 ✓")
        else:
            # 获取最大的几个缺口
            gaps = await conn.fetch("""
                WITH ordered AS (
                    SELECT
                        agg_trade_id,
                        LAG(agg_trade_id) OVER (ORDER BY agg_trade_id) as prev_id
                    FROM aggtrades
                    WHERE symbol = $1
                )
                SELECT prev_id, agg_trade_id as curr_id, (agg_trade_id - prev_id - 1) as gap_size
                FROM ordered
                WHERE prev_id IS NOT NULL AND agg_trade_id - prev_id > 1
                ORDER BY gap_size DESC
                LIMIT 3
            """, symbol)

            print(f"  {symbol}: {gap_count} 个缺口")
            for g in gaps:
                print(f"    ID {g['prev_id']:,} → {g['curr_id']:,} (缺少 {g['gap_size']:,} 条)")

    # 5. Klines 数据检查
    print()
    print("[5] Klines 数据分布")
    print("-" * 50)

    kline_stats = await conn.fetch("""
        SELECT symbol, timeframe, COUNT(*) as cnt,
               MIN(timestamp) as min_ts,
               MAX(timestamp) as max_ts
        FROM klines
        GROUP BY symbol, timeframe
        ORDER BY symbol, timeframe
    """)

    current_symbol = ""
    for k in kline_stats:
        if k["symbol"] != current_symbol:
            current_symbol = k["symbol"]
            print(f"  {current_symbol}:")
        print(f"    {k['timeframe']}: {k['cnt']:,} 条 ({k['min_ts'].strftime('%m-%d')} ~ {k['max_ts'].strftime('%m-%d')})")

    # 6. Signals 数据检查
    print()
    print("[6] Signals 统计")
    print("-" * 50)

    signal_stats = await conn.fetch("""
        SELECT
            outcome,
            COUNT(*) as cnt
        FROM signals
        GROUP BY outcome
    """)

    for s in signal_stats:
        print(f"  {s['outcome']}: {s['cnt']} 条")

    # 计算胜率
    tp_count = sum(s["cnt"] for s in signal_stats if s["outcome"] == "tp")
    sl_count = sum(s["cnt"] for s in signal_stats if s["outcome"] == "sl")
    total = tp_count + sl_count
    if total > 0:
        win_rate = tp_count / total * 100
        print(f"  胜率: {win_rate:.1f}% ({tp_count}/{total})")

    await conn.close()

    print()
    print("=" * 60)
    print("   检查完成")
    print("=" * 60)
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="数据库数据质量检查")
    parser.add_argument(
        "--fix-duplicates",
        action="store_true",
        help="删除重复数据",
    )
    args = parser.parse_args()

    asyncio.run(check_data(fix_duplicates=args.fix_duplicates))
