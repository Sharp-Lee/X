#!/usr/bin/env python3
"""
Layer 3 验证：技术指标计算
=========================

依赖：Layer 1, 2 已验证通过

验证目标：
1. EMA(50) 计算正确性
2. ATR(9) 计算正确性
3. Fibonacci 支撑压力位计算
4. 与 TradingView 数值对比（手动验证）

运行方式：
    cd backend
    source .venv/bin/activate
    python scripts/verify_layer3_indicators.py

预期结果：
    - 打印当前指标值，供与 TradingView 手动对比
    - 验证指标计算逻辑的数学正确性
"""

import asyncio
import sys
from decimal import Decimal

sys.path.insert(0, ".")


async def verify_indicators():
    """验证技术指标计算"""

    print("=" * 60)
    print("Layer 3 验证：技术指标计算")
    print("=" * 60)
    print()

    errors = []
    warnings = []

    # =========================================================================
    # 检查点 3.1：导入模块
    # =========================================================================
    print("[检查点 3.1] 导入模块...")
    try:
        from app.clients import BinanceRestClient
        from app.core import IndicatorCalculator, is_talib_available
        from app.config import get_settings
        print("  [PASS] 模块导入成功")
        print(f"  指标库: {'TA-Lib' if is_talib_available() else 'NumPy 手写实现'}")
    except ImportError as e:
        print(f"  [FAIL] 模块导入失败: {e}")
        errors.append(f"3.1 模块导入失败: {e}")
        return errors, warnings

    # =========================================================================
    # 检查点 3.2：获取足够的历史 K线
    # =========================================================================
    print("\n[检查点 3.2] 获取历史 K线 (100根 5m)...")
    client = BinanceRestClient()
    settings = get_settings()

    try:
        klines = await client.get_klines(
            symbol="BTCUSDT",
            interval="5m",
            limit=100
        )
        print(f"  [PASS] 获取到 {len(klines)} 根 K线")
        print(f"  时间范围: {klines[0].timestamp} ~ {klines[-1].timestamp}")

    except Exception as e:
        print(f"  [FAIL] 获取 K线失败: {e}")
        errors.append(f"3.2 获取 K线失败: {e}")
        await client.close()
        return errors, warnings

    # =========================================================================
    # 检查点 3.3：创建指标计算器
    # =========================================================================
    print("\n[检查点 3.3] 创建指标计算器...")
    try:
        calculator = IndicatorCalculator(
            ema_period=settings.ema_period,  # 50
            fib_period=settings.fib_period,  # 9
            atr_period=settings.atr_period,  # 9
        )
        print(f"  [PASS] 指标计算器创建成功")
        print(f"  EMA周期: {settings.ema_period}")
        print(f"  Fib周期: {settings.fib_period}")
        print(f"  ATR周期: {settings.atr_period}")
    except Exception as e:
        print(f"  [FAIL] 创建失败: {e}")
        errors.append(f"3.3 创建指标计算器失败: {e}")
        await client.close()
        return errors, warnings

    # =========================================================================
    # 检查点 3.4：计算指标
    # =========================================================================
    print("\n[检查点 3.4] 计算指标...")

    # 提取 OHLCV
    opens = [k.open for k in klines]
    highs = [k.high for k in klines]
    lows = [k.low for k in klines]
    closes = [k.close for k in klines]
    volumes = [k.volume for k in klines]

    try:
        indicators = calculator.calculate_latest(opens, highs, lows, closes, volumes)

        if indicators is None:
            print("  [FAIL] 指标计算返回 None")
            errors.append("3.4 指标计算返回 None")
            await client.close()
            return errors, warnings

        print("  [PASS] 指标计算成功")

    except Exception as e:
        print(f"  [FAIL] 指标计算失败: {e}")
        errors.append(f"3.4 指标计算失败: {e}")
        await client.close()
        return errors, warnings

    # =========================================================================
    # 检查点 3.5：显示指标值（供 TradingView 对比）
    # =========================================================================
    print("\n[检查点 3.5] 当前指标值（请与 TradingView 对比）...")
    print("-" * 60)
    print(f"  交易对: BTCUSDT")
    print(f"  周期:   5m")
    print(f"  最新K线时间: {klines[-1].timestamp}")
    print(f"  当前价格:    {klines[-1].close}")
    print("-" * 60)
    print(f"  EMA(50):     {indicators['ema50']:.2f}")
    print(f"  ATR(9):      {indicators['atr']:.2f}")
    print(f"  VWAP:        {indicators['vwap']:.2f}")
    print("-" * 60)
    print(f"  Fib 0.382:   {indicators['fib_382']:.2f}")
    print(f"  Fib 0.500:   {indicators['fib_500']:.2f}")
    print(f"  Fib 0.618:   {indicators['fib_618']:.2f}")
    print("-" * 60)

    # =========================================================================
    # 检查点 3.6：验证 EMA 计算逻辑
    # =========================================================================
    print("\n[检查点 3.6] 验证 EMA 计算逻辑...")

    # EMA 应该在价格附近
    current_price = float(closes[-1])
    ema_value = float(indicators['ema50'])

    deviation = abs(ema_value - current_price) / current_price * 100
    print(f"  当前价格: {current_price:.2f}")
    print(f"  EMA(50):  {ema_value:.2f}")
    print(f"  偏离度:   {deviation:.2f}%")

    if deviation > 20:
        print("  [WARN] EMA 偏离价格超过 20%，可能异常")
        warnings.append("3.6 EMA 偏离度过大")
    else:
        print("  [PASS] EMA 偏离度正常")

    # =========================================================================
    # 检查点 3.7：验证 ATR 合理性
    # =========================================================================
    print("\n[检查点 3.7] 验证 ATR 合理性...")

    atr_value = float(indicators['atr'])
    atr_percent = atr_value / current_price * 100

    print(f"  ATR(9):    {atr_value:.2f}")
    print(f"  ATR占比:   {atr_percent:.3f}%")

    # 对于 5m K线，ATR 通常在 0.01% ~ 1% 之间
    if atr_percent < 0.001:
        print("  [WARN] ATR 过小，可能计算有误")
        warnings.append("3.7 ATR 过小")
    elif atr_percent > 5:
        print("  [WARN] ATR 过大，可能计算有误")
        warnings.append("3.7 ATR 过大")
    else:
        print("  [PASS] ATR 在合理范围内")

    # =========================================================================
    # 检查点 3.8：验证 Fibonacci 层级
    # =========================================================================
    print("\n[检查点 3.8] 验证 Fibonacci 层级...")

    fib_382 = float(indicators['fib_382'])
    fib_500 = float(indicators['fib_500'])
    fib_618 = float(indicators['fib_618'])

    # Fib 层级应该满足 382 < 500 < 618 或 382 > 500 > 618
    if fib_382 < fib_500 < fib_618:
        print("  [PASS] Fib 层级顺序正确 (上升趋势)")
        print(f"    0.382: {fib_382:.2f}")
        print(f"    0.500: {fib_500:.2f}")
        print(f"    0.618: {fib_618:.2f}")
    elif fib_382 > fib_500 > fib_618:
        print("  [PASS] Fib 层级顺序正确 (下降趋势)")
        print(f"    0.382: {fib_382:.2f}")
        print(f"    0.500: {fib_500:.2f}")
        print(f"    0.618: {fib_618:.2f}")
    else:
        print(f"  [WARN] Fib 层级顺序异常")
        print(f"    0.382: {fib_382:.2f}")
        print(f"    0.500: {fib_500:.2f}")
        print(f"    0.618: {fib_618:.2f}")
        warnings.append("3.8 Fib 层级顺序异常")

    # =========================================================================
    # 检查点 3.9：验证 VWAP
    # =========================================================================
    print("\n[检查点 3.9] 验证 VWAP...")

    vwap_value = float(indicators['vwap'])
    vwap_deviation = abs(vwap_value - current_price) / current_price * 100

    print(f"  VWAP:    {vwap_value:.2f}")
    print(f"  偏离度:  {vwap_deviation:.2f}%")

    if vwap_deviation > 10:
        print("  [WARN] VWAP 偏离价格超过 10%")
        warnings.append("3.9 VWAP 偏离度较大")
    else:
        print("  [PASS] VWAP 在合理范围内")

    # =========================================================================
    # 检查点 3.10：计算 TP/SL 示例
    # =========================================================================
    print("\n[检查点 3.10] TP/SL 计算示例...")

    tp_mult = float(settings.tp_atr_mult)  # 2.0
    sl_mult = float(settings.sl_atr_mult)  # 8.84

    tp_distance = atr_value * tp_mult
    sl_distance = atr_value * sl_mult

    print(f"  ATR:         {atr_value:.2f}")
    print(f"  TP倍数:      {tp_mult}x")
    print(f"  SL倍数:      {sl_mult}x")
    print(f"  TP距离:      {tp_distance:.2f} ({tp_distance/current_price*100:.3f}%)")
    print(f"  SL距离:      {sl_distance:.2f} ({sl_distance/current_price*100:.3f}%)")
    print(f"  盈亏比:      1:{sl_mult/tp_mult:.2f}")

    # LONG 示例
    print(f"\n  LONG 信号示例 (入场价 {current_price:.2f}):")
    print(f"    TP: {current_price + tp_distance:.2f}")
    print(f"    SL: {current_price - sl_distance:.2f}")

    # SHORT 示例
    print(f"\n  SHORT 信号示例 (入场价 {current_price:.2f}):")
    print(f"    TP: {current_price - tp_distance:.2f}")
    print(f"    SL: {current_price + sl_distance:.2f}")

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

    print("\n[提示] 请手动与 TradingView 对比以下指标:")
    print("  1. 打开 TradingView，选择 BTCUSDT 5m")
    print("  2. 添加 EMA(50) 指标，对比数值")
    print("  3. 添加 ATR(9) 指标，对比数值")
    print("  4. 添加 VWAP 指标，对比数值")

    if not errors and not warnings:
        print("\n[SUCCESS] Layer 3 自动验证全部通过!")
    elif not errors:
        print(f"\n[PASS WITH WARNINGS] Layer 3 基本通过，有 {len(warnings)} 个警告需要关注")

    print("\n可以继续下一层验证:")
    print("  python scripts/verify_layer4_signal_logic.py")
    print()


if __name__ == "__main__":
    errors, warnings = asyncio.run(verify_indicators())
    print_summary(errors, warnings)

    if errors:
        sys.exit(2)
    elif warnings:
        sys.exit(1)
    else:
        sys.exit(0)
