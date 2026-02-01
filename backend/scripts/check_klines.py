#!/usr/bin/env python3
"""
K 线数据质量检查脚本
====================

检查内容:
1. 重复数据
2. 数据缺口
3. 覆盖率
4. OHLC 逻辑
5. 周期比例一致性

使用方式:
    python scripts/check_klines.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
from app.config import get_settings


async def check_klines():
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url)

    print()
    print("=" * 70)
    print("   K 线数据质量检查")
    print("=" * 70)

    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    timeframes = ["1m", "3m", "5m", "15m", "30m"]
    tf_minutes = {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30}

    # ========== 1. 重复数据检查 ==========
    print()
    print("[1] 重复数据检查")
    print("-" * 70)

    dup_count = await conn.fetchval("""
        SELECT COUNT(*) FROM (
            SELECT symbol, timeframe, timestamp, COUNT(*) as cnt
            FROM klines
            GROUP BY symbol, timeframe, timestamp
            HAVING COUNT(*) > 1
        ) t
    """)

    if dup_count == 0:
        print("  ✓ 无重复数据")
    else:
        print(f"  ✗ 发现 {dup_count} 组重复数据")

    # ========== 2. 数据缺口检查 ==========
    print()
    print("[2] 数据缺口检查 (1m K线)")
    print("-" * 70)

    total_gaps = 0
    for symbol in symbols:
        gaps = await conn.fetch("""
            WITH ordered AS (
                SELECT
                    timestamp,
                    LAG(timestamp) OVER (ORDER BY timestamp) as prev_ts
                FROM klines
                WHERE symbol = $1 AND timeframe = '1m'
            )
            SELECT
                prev_ts,
                timestamp,
                EXTRACT(EPOCH FROM (timestamp - prev_ts)) / 3600 as gap_hours
            FROM ordered
            WHERE prev_ts IS NOT NULL
              AND EXTRACT(EPOCH FROM (timestamp - prev_ts)) / 60 > 1
            ORDER BY gap_hours DESC
            LIMIT 5
        """, symbol)

        gap_count = await conn.fetchval("""
            WITH ordered AS (
                SELECT
                    timestamp,
                    LAG(timestamp) OVER (ORDER BY timestamp) as prev_ts
                FROM klines
                WHERE symbol = $1 AND timeframe = '1m'
            )
            SELECT COUNT(*)
            FROM ordered
            WHERE prev_ts IS NOT NULL
              AND EXTRACT(EPOCH FROM (timestamp - prev_ts)) / 60 > 1
        """, symbol)

        total_gaps += gap_count

        if gap_count == 0:
            print(f"  {symbol}: ✓ 无缺口")
        else:
            print(f"  {symbol}: {gap_count} 个缺口")
            for g in gaps[:3]:
                print(f"    {g['prev_ts'].strftime('%Y-%m-%d')} → {g['timestamp'].strftime('%Y-%m-%d')} ({g['gap_hours']:.0f}h)")

    print(f"  总缺口: {total_gaps}")

    # ========== 3. 数据覆盖率 ==========
    print()
    print("[3] 数据覆盖率")
    print("-" * 70)

    for symbol in symbols:
        print(f"  {symbol}:")
        for tf in timeframes:
            stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as cnt,
                    MIN(timestamp) as min_ts,
                    MAX(timestamp) as max_ts
                FROM klines
                WHERE symbol = $1 AND timeframe = $2
            """, symbol, tf)

            if stats["cnt"] > 0:
                minutes = tf_minutes[tf]
                time_span = stats["max_ts"] - stats["min_ts"]
                expected = int(time_span.total_seconds() / 60 / minutes) + 1
                coverage = stats["cnt"] / expected * 100

                status = "✓" if coverage >= 99.5 else "⚠" if coverage >= 95 else "✗"
                print(f"    {tf}: {stats['cnt']:>10,} 条 | 覆盖: {coverage:>6.2f}% {status}")

    # ========== 4. OHLC 逻辑检查 ==========
    print()
    print("[4] OHLC 数据逻辑检查")
    print("-" * 70)

    invalid_ohlc = await conn.fetchval("""
        SELECT COUNT(*) FROM klines
        WHERE high < low
           OR high < open
           OR high < close
           OR low > open
           OR low > close
    """)

    if invalid_ohlc == 0:
        print("  ✓ OHLC 逻辑正确")
    else:
        print(f"  ✗ 发现 {invalid_ohlc} 条 OHLC 逻辑错误")

    negative = await conn.fetchval("""
        SELECT COUNT(*) FROM klines
        WHERE open < 0 OR high < 0 OR low < 0 OR close < 0 OR volume < 0
    """)

    if negative == 0:
        print("  ✓ 无负值数据")
    else:
        print(f"  ✗ 发现 {negative} 条负值数据")

    zero_price = await conn.fetchval("""
        SELECT COUNT(*) FROM klines
        WHERE open = 0 OR high = 0 OR low = 0 OR close = 0
    """)

    if zero_price == 0:
        print("  ✓ 无零价格数据")
    else:
        print(f"  ⚠ 发现 {zero_price} 条零价格数据")

    # ========== 5. 周期比例一致性 ==========
    print()
    print("[5] 周期数据量比例检查")
    print("-" * 70)

    for symbol in symbols:
        counts = {}
        for tf in timeframes:
            cnt = await conn.fetchval("""
                SELECT COUNT(*) FROM klines
                WHERE symbol = $1 AND timeframe = $2
            """, symbol, tf)
            counts[tf] = cnt

        r3 = counts["1m"] / counts["3m"] if counts["3m"] > 0 else 0
        r5 = counts["1m"] / counts["5m"] if counts["5m"] > 0 else 0
        r15 = counts["1m"] / counts["15m"] if counts["15m"] > 0 else 0
        r30 = counts["1m"] / counts["30m"] if counts["30m"] > 0 else 0

        ok = all([
            2.9 <= r3 <= 3.1,
            4.9 <= r5 <= 5.1,
            14.9 <= r15 <= 15.1,
            29.9 <= r30 <= 30.1,
        ])
        status = "✓" if ok else "⚠"
        print(f"  {symbol}: 1m/3m={r3:.2f} 1m/5m={r5:.2f} 1m/15m={r15:.2f} 1m/30m={r30:.2f} {status}")

    # ========== 总结 ==========
    total = await conn.fetchval("SELECT COUNT(*) FROM klines")

    print()
    print("=" * 70)
    print(f"   总计: {total:,} 条 K 线数据")
    print("=" * 70)
    print()

    await conn.close()


if __name__ == "__main__":
    asyncio.run(check_klines())
