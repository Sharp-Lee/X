#!/usr/bin/env python3
"""
Layer 1 验证：Binance REST 客户端
=================================

验证目标：
1. REST 客户端能正确连接 Binance
2. 能获取 K线数据
3. 数据格式正确（OHLCV 都是有效数值）

运行方式：
    cd backend
    source .venv/bin/activate
    python scripts/verify_layer1_rest_client.py

预期结果：
    - 所有检查点显示 [PASS]
    - 无 [FAIL] 或 [WARN]
"""

import asyncio
import sys
from datetime import datetime, timezone
from decimal import Decimal

# 添加项目路径
sys.path.insert(0, ".")


async def verify_rest_client():
    """验证 REST 客户端基础功能"""

    print("=" * 60)
    print("Layer 1 验证：Binance REST 客户端")
    print("=" * 60)
    print()

    errors = []
    warnings = []

    # =========================================================================
    # 检查点 1.1：导入模块
    # =========================================================================
    print("[检查点 1.1] 导入 BinanceRestClient...")
    try:
        from app.clients import BinanceRestClient
        print("  [PASS] 模块导入成功")
    except ImportError as e:
        print(f"  [FAIL] 模块导入失败: {e}")
        errors.append(f"1.1 模块导入失败: {e}")
        return errors, warnings

    # =========================================================================
    # 检查点 1.2：创建客户端实例
    # =========================================================================
    print("\n[检查点 1.2] 创建客户端实例...")
    try:
        client = BinanceRestClient()
        print("  [PASS] 客户端创建成功")
    except Exception as e:
        print(f"  [FAIL] 客户端创建失败: {e}")
        errors.append(f"1.2 客户端创建失败: {e}")
        return errors, warnings

    # =========================================================================
    # 检查点 1.3：获取单根 K线
    # =========================================================================
    print("\n[检查点 1.3] 获取 BTCUSDT 1m K线 (limit=5)...")
    try:
        klines = await client.get_klines(
            symbol="BTCUSDT",
            interval="1m",
            limit=5
        )

        if not klines:
            print("  [FAIL] 返回空数据")
            errors.append("1.3 K线数据为空")
        elif len(klines) != 5:
            print(f"  [WARN] 请求 5 根，返回 {len(klines)} 根")
            warnings.append(f"1.3 请求 5 根 K线，返回 {len(klines)} 根")
        else:
            print(f"  [PASS] 获取到 {len(klines)} 根 K线")

    except Exception as e:
        print(f"  [FAIL] 获取 K线失败: {e}")
        errors.append(f"1.3 获取 K线失败: {e}")
        await client.close()
        return errors, warnings

    # =========================================================================
    # 检查点 1.4：验证 K线数据结构
    # =========================================================================
    print("\n[检查点 1.4] 验证 K线数据结构...")

    required_fields = ["symbol", "timeframe", "timestamp", "open", "high", "low", "close", "volume"]

    for i, kline in enumerate(klines):
        # 检查必要字段
        for field in required_fields:
            if not hasattr(kline, field):
                print(f"  [FAIL] K线 {i} 缺少字段: {field}")
                errors.append(f"1.4 K线缺少字段: {field}")
                continue

        # 检查数值有效性
        if kline.high < kline.low:
            print(f"  [FAIL] K线 {i}: high({kline.high}) < low({kline.low})")
            errors.append(f"1.4 K线数据异常: high < low")

        if kline.close > kline.high or kline.close < kline.low:
            print(f"  [FAIL] K线 {i}: close 超出 high-low 范围")
            errors.append(f"1.4 K线数据异常: close 超出范围")

        if kline.open > kline.high or kline.open < kline.low:
            print(f"  [FAIL] K线 {i}: open 超出 high-low 范围")
            errors.append(f"1.4 K线数据异常: open 超出范围")

        if kline.volume < 0:
            print(f"  [FAIL] K线 {i}: volume({kline.volume}) 为负数")
            errors.append(f"1.4 K线数据异常: volume 为负")

    if not any("1.4" in e for e in errors):
        print("  [PASS] 所有 K线数据结构正确")

    # =========================================================================
    # 检查点 1.5：验证时间戳顺序
    # =========================================================================
    print("\n[检查点 1.5] 验证时间戳顺序...")

    timestamps = [k.timestamp for k in klines]
    is_sorted = all(timestamps[i] < timestamps[i+1] for i in range(len(timestamps)-1))

    if is_sorted:
        print("  [PASS] 时间戳按升序排列")
    else:
        print("  [FAIL] 时间戳顺序异常")
        errors.append("1.5 时间戳顺序异常")

    # =========================================================================
    # 检查点 1.6：打印样本数据
    # =========================================================================
    print("\n[检查点 1.6] 样本数据展示...")
    print("-" * 60)

    latest = klines[-1]
    print(f"  Symbol:    {latest.symbol}")
    print(f"  Timeframe: {latest.timeframe}")
    print(f"  Timestamp: {latest.timestamp}")
    print(f"  Open:      {latest.open}")
    print(f"  High:      {latest.high}")
    print(f"  Low:       {latest.low}")
    print(f"  Close:     {latest.close}")
    print(f"  Volume:    {latest.volume}")
    print(f"  IsClosed:  {latest.is_closed}")
    print("-" * 60)

    # =========================================================================
    # 检查点 1.7：验证多个交易对
    # =========================================================================
    print("\n[检查点 1.7] 验证多个交易对...")

    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for symbol in symbols:
        try:
            test_klines = await client.get_klines(symbol=symbol, interval="1m", limit=1)
            if test_klines:
                print(f"  [PASS] {symbol}: {test_klines[0].close}")
            else:
                print(f"  [WARN] {symbol}: 无数据")
                warnings.append(f"1.7 {symbol} 无数据")
        except Exception as e:
            print(f"  [FAIL] {symbol}: {e}")
            errors.append(f"1.7 {symbol} 获取失败: {e}")

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
        print("\n[SUCCESS] Layer 1 验证全部通过!")
        print("\n可以继续下一层验证:")
        print("  python scripts/verify_layer2_kline_aggregator.py")
    elif not errors:
        print(f"\n[PASS WITH WARNINGS] Layer 1 基本通过，但有 {len(warnings)} 个警告需要关注")
    else:
        print(f"\n[FAILED] Layer 1 验证失败，请先修复 {len(errors)} 个错误")

    print()


if __name__ == "__main__":
    errors, warnings = asyncio.run(verify_rest_client())
    print_summary(errors, warnings)

    # 返回码：0=成功, 1=有警告, 2=有错误
    if errors:
        sys.exit(2)
    elif warnings:
        sys.exit(1)
    else:
        sys.exit(0)
