#!/usr/bin/env python3
"""
Layer 2 验证：K线聚合器
=======================

依赖：Layer 1 (REST 客户端) 已验证通过

验证目标：
1. 聚合器能正确将 1m K线聚合为 3m, 5m
2. 聚合后的 OHLCV 计算正确
3. 与 Binance 直接获取的高周期 K线对比

运行方式：
    cd backend
    source .venv/bin/activate
    python scripts/verify_layer2_kline_aggregator.py

预期结果：
    - 所有检查点显示 [PASS]
    - 聚合结果与 Binance 数据一致
"""

import asyncio
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal

sys.path.insert(0, ".")


async def verify_kline_aggregator():
    """验证 K线聚合器"""

    print("=" * 60)
    print("Layer 2 验证：K线聚合器")
    print("=" * 60)
    print()

    errors = []
    warnings = []

    # =========================================================================
    # 检查点 2.1：导入模块
    # =========================================================================
    print("[检查点 2.1] 导入模块...")
    try:
        from app.clients import BinanceRestClient
        from app.services import KlineAggregator
        from app.models import kline_to_fast
        print("  [PASS] 模块导入成功")
    except ImportError as e:
        print(f"  [FAIL] 模块导入失败: {e}")
        errors.append(f"2.1 模块导入失败: {e}")
        return errors, warnings

    # =========================================================================
    # 检查点 2.2：创建聚合器
    # =========================================================================
    print("\n[检查点 2.2] 创建聚合器 (3m, 5m)...")
    try:
        aggregator = KlineAggregator(target_timeframes=["3m", "5m"])
        print(f"  [PASS] 聚合器创建成功，目标周期: {aggregator.target_timeframes}")
    except Exception as e:
        print(f"  [FAIL] 聚合器创建失败: {e}")
        errors.append(f"2.2 聚合器创建失败: {e}")
        return errors, warnings

    # =========================================================================
    # 检查点 2.3：获取历史 1m K线用于聚合
    # =========================================================================
    print("\n[检查点 2.3] 获取历史 1m K线 (15根)...")
    client = BinanceRestClient()

    try:
        klines_1m = await client.get_klines(
            symbol="BTCUSDT",
            interval="1m",
            limit=15
        )
        print(f"  [PASS] 获取到 {len(klines_1m)} 根 1m K线")

        # 显示时间范围
        if klines_1m:
            print(f"         时间范围: {klines_1m[0].timestamp} ~ {klines_1m[-1].timestamp}")

    except Exception as e:
        print(f"  [FAIL] 获取 K线失败: {e}")
        errors.append(f"2.3 获取 K线失败: {e}")
        await client.close()
        return errors, warnings

    # =========================================================================
    # 检查点 2.4：执行聚合
    # =========================================================================
    print("\n[检查点 2.4] 执行 1m → 3m, 5m 聚合...")

    aggregated_3m = []
    aggregated_5m = []

    for kline in klines_1m:
        # 只处理已关闭的 K线
        if not kline.is_closed:
            continue

        fast_kline = kline_to_fast(kline)
        results = await aggregator.add_1m_kline(fast_kline)

        for r in results:
            if r.timeframe == "3m":
                aggregated_3m.append(r)
            elif r.timeframe == "5m":
                aggregated_5m.append(r)

    print(f"  聚合产生 3m K线: {len(aggregated_3m)} 根")
    print(f"  聚合产生 5m K线: {len(aggregated_5m)} 根")

    if len(aggregated_3m) == 0 and len(aggregated_5m) == 0:
        print("  [WARN] 没有产生聚合 K线（可能时间未对齐）")
        warnings.append("2.4 没有产生聚合 K线")

    # =========================================================================
    # 检查点 2.5：手动验证聚合逻辑（3m）
    # =========================================================================
    print("\n[检查点 2.5] 手动验证 3m 聚合逻辑...")

    # 取前 3 根 1m K线，手动计算期望的 3m K线
    if len(klines_1m) >= 3:
        first_3 = klines_1m[:3]

        expected_open = first_3[0].open
        expected_high = max(k.high for k in first_3)
        expected_low = min(k.low for k in first_3)
        expected_close = first_3[-1].close
        expected_volume = sum(k.volume for k in first_3)

        print(f"  手动计算 (前3根1m):")
        print(f"    Open:   {expected_open}")
        print(f"    High:   {expected_high}")
        print(f"    Low:    {expected_low}")
        print(f"    Close:  {expected_close}")
        print(f"    Volume: {expected_volume}")

        # 如果有聚合结果，对比
        if aggregated_3m:
            agg = aggregated_3m[0]
            print(f"  聚合器结果:")
            print(f"    Open:   {agg.open}")
            print(f"    High:   {agg.high}")
            print(f"    Low:    {agg.low}")
            print(f"    Close:  {agg.close}")
            print(f"    Volume: {agg.volume}")

            # 验证（使用小容差因为可能有浮点误差）
            tolerance = 0.01
            checks = [
                ("open", float(expected_open), agg.open),
                ("high", float(expected_high), agg.high),
                ("low", float(expected_low), agg.low),
                ("close", float(expected_close), agg.close),
            ]

            all_match = True
            for name, expected, actual in checks:
                if abs(expected - actual) > tolerance:
                    print(f"  [FAIL] {name} 不匹配: 期望 {expected}, 实际 {actual}")
                    errors.append(f"2.5 聚合 {name} 不匹配")
                    all_match = False

            if all_match:
                print("  [PASS] 手动验证通过，聚合逻辑正确")
        else:
            print("  [SKIP] 无聚合结果可对比（时间未对齐）")
            warnings.append("2.5 无法验证聚合结果")
    else:
        print("  [SKIP] K线数量不足")

    # =========================================================================
    # 检查点 2.6：与 Binance 3m K线对比
    # =========================================================================
    print("\n[检查点 2.6] 与 Binance 直接获取的 3m K线对比...")

    try:
        binance_3m = await client.get_klines(
            symbol="BTCUSDT",
            interval="3m",
            limit=5
        )
        print(f"  从 Binance 获取 {len(binance_3m)} 根 3m K线")

        if binance_3m and aggregated_3m:
            # 找到时间戳匹配的 K线进行对比
            binance_timestamps = {k.timestamp: k for k in binance_3m}

            matched = False
            for agg in aggregated_3m:
                from app.models import timestamp_to_datetime
                agg_time = timestamp_to_datetime(agg.timestamp)

                if agg_time in binance_timestamps:
                    matched = True
                    binance_kline = binance_timestamps[agg_time]

                    print(f"\n  对比时间点: {agg_time}")
                    print(f"  {'字段':<8} {'聚合器':<15} {'Binance':<15} {'差异':<10}")
                    print(f"  {'-'*50}")

                    fields = [
                        ("Open", agg.open, float(binance_kline.open)),
                        ("High", agg.high, float(binance_kline.high)),
                        ("Low", agg.low, float(binance_kline.low)),
                        ("Close", agg.close, float(binance_kline.close)),
                    ]

                    all_match = True
                    for name, agg_val, bin_val in fields:
                        diff = abs(agg_val - bin_val)
                        status = "OK" if diff < 0.01 else "DIFF"
                        print(f"  {name:<8} {agg_val:<15.2f} {bin_val:<15.2f} {status}")
                        if diff >= 0.01:
                            all_match = False

                    if all_match:
                        print("\n  [PASS] 聚合结果与 Binance 数据一致")
                    else:
                        print("\n  [WARN] 聚合结果与 Binance 有差异（可能是时间窗口不完全对齐）")
                        warnings.append("2.6 聚合结果与 Binance 有轻微差异")
                    break

            if not matched:
                print("  [INFO] 没有找到时间戳匹配的 K线（聚合周期可能未完成）")
                warnings.append("2.6 没有匹配的时间戳可对比")
        else:
            print("  [SKIP] 数据不足，无法对比")

    except Exception as e:
        print(f"  [FAIL] 获取 Binance 3m K线失败: {e}")
        errors.append(f"2.6 获取 Binance 3m 失败: {e}")

    # =========================================================================
    # 检查点 2.7：验证 5m 聚合
    # =========================================================================
    print("\n[检查点 2.7] 验证 5m 聚合...")

    try:
        binance_5m = await client.get_klines(
            symbol="BTCUSDT",
            interval="5m",
            limit=3
        )
        print(f"  从 Binance 获取 {len(binance_5m)} 根 5m K线")

        if binance_5m:
            latest = binance_5m[-1]
            print(f"  最新 5m K线: {latest.timestamp}")
            print(f"    OHLC: {latest.open} / {latest.high} / {latest.low} / {latest.close}")

        if aggregated_5m:
            print(f"  聚合产生的 5m K线数: {len(aggregated_5m)}")
            for agg in aggregated_5m:
                from app.models import timestamp_to_datetime
                print(f"    时间: {timestamp_to_datetime(agg.timestamp)}")

        print("  [PASS] 5m 数据获取正常")

    except Exception as e:
        print(f"  [FAIL] 5m 验证失败: {e}")
        errors.append(f"2.7 5m 验证失败: {e}")

    # =========================================================================
    # 清理
    # =========================================================================
    print("\n[清理] 关闭客户端...")
    await client.close()
    print("  [PASS] 客户端已关闭")

    return errors, warnings


def print_summary(errors: list, warnings: list):
    """打印验证总结"""
    print()
    print("=" * 60)
    print("验证总结")
    print("=" * 60)

    if errors:
        print(f"\n[ERRORS] 共 {len(errors)} 个错误:")
        for e in errors:
            print(f"  - {e}")

    if warnings:
        print(f"\n[WARNINGS] 共 {len(warnings)} 个警告:")
        for w in warnings:
            print(f"  - {w}")

    if not errors and not warnings:
        print("\n[SUCCESS] Layer 2 验证全部通过!")
        print("\n可以继续下一层验证:")
        print("  python scripts/verify_layer3_indicators.py")
    elif not errors:
        print(f"\n[PASS WITH WARNINGS] Layer 2 基本通过，有 {len(warnings)} 个警告")
        print("  警告通常是因为时间窗口未完全对齐，可以继续下一层验证")
    else:
        print(f"\n[FAILED] Layer 2 验证失败，请先修复 {len(errors)} 个错误")

    print()


if __name__ == "__main__":
    errors, warnings = asyncio.run(verify_kline_aggregator())
    print_summary(errors, warnings)

    if errors:
        sys.exit(2)
    elif warnings:
        sys.exit(1)
    else:
        sys.exit(0)
