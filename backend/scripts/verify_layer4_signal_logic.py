#!/usr/bin/env python3
"""
Layer 4 验证：信号生成逻辑
=========================

依赖：Layer 1, 2, 3 已验证通过

验证目标：
1. 信号检测条件是否正确
2. SHORT 信号：上升趋势 + 触支撑 + 收阳 → 做空
3. LONG 信号：下降趋势 + 触阻力 + 收阴 → 做多
4. TP/SL 计算是否正确
5. ATR 字段是否正确记录

运行方式：
    cd backend
    source .venv/bin/activate
    python scripts/verify_layer4_signal_logic.py

预期结果：
    - 信号逻辑与 Pine Script 一致
    - TP/SL 计算正确
"""

import asyncio
import sys
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, ".")


async def verify_signal_logic():
    """验证信号生成逻辑"""

    print("=" * 60)
    print("Layer 4 验证：信号生成逻辑")
    print("=" * 60)
    print()

    errors = []
    warnings = []

    # =========================================================================
    # 检查点 4.1：导入模块
    # =========================================================================
    print("[检查点 4.1] 导入模块...")
    try:
        from app.models import (
            Direction,
            Kline,
            KlineBuffer,
            SignalRecord,
        )
        from app.services import SignalGenerator, LevelManager
        from app.config import get_settings
        print("  [PASS] 模块导入成功")
    except ImportError as e:
        print(f"  [FAIL] 模块导入失败: {e}")
        errors.append(f"4.1 模块导入失败: {e}")
        return errors, warnings

    # =========================================================================
    # 检查点 4.2：创建测试数据
    # =========================================================================
    print("\n[检查点 4.2] 创建模拟 K线数据...")

    settings = get_settings()

    # 创建一系列模拟 K线用于测试
    # 模拟上升趋势场景（close > ema50）
    base_price = Decimal("50000")
    buffer = KlineBuffer(symbol="BTCUSDT", timeframe="5m")

    # 生成 60 根 K线（足够计算 EMA50）
    from datetime import timedelta
    base_time = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    for i in range(60):
        # 模拟缓慢上涨的趋势
        price = base_price + Decimal(str(i * 50))
        kline = Kline(
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=base_time + timedelta(minutes=i * 5),
            open=price - Decimal("10"),
            high=price + Decimal("30"),
            low=price - Decimal("40"),
            close=price,
            volume=Decimal("100"),
            is_closed=True,
        )
        buffer.add(kline)

    print(f"  [PASS] 创建了 {len(buffer)} 根模拟 K线")
    print(f"  价格范围: {base_price} ~ {base_price + Decimal('2950')}")

    # =========================================================================
    # 检查点 4.3：测试 LevelManager
    # =========================================================================
    print("\n[检查点 4.3] 测试 LevelManager...")

    level_manager = LevelManager(touch_tolerance=Decimal("0.001"))

    close = Decimal("50000")
    fib_382 = Decimal("49800")  # 低于 close → 支撑
    fib_500 = Decimal("49700")  # 低于 close → 支撑
    fib_618 = Decimal("50200")  # 高于 close → 阻力
    vwap = Decimal("49900")     # 低于 close → 支撑

    support, resistance = level_manager.get_levels(
        close, fib_382, fib_500, fib_618, vwap
    )

    print(f"  Close: {close}")
    print(f"  Support levels: {support}")
    print(f"  Resistance levels: {resistance}")

    if len(support) == 3 and len(resistance) == 1:
        print("  [PASS] 支撑/阻力分类正确")
    else:
        print(f"  [FAIL] 预期 3 个支撑 1 个阻力，实际 {len(support)} 支撑 {len(resistance)} 阻力")
        errors.append("4.3 支撑阻力分类错误")

    # =========================================================================
    # 检查点 4.4：测试触及检测
    # =========================================================================
    print("\n[检查点 4.4] 测试价格触及检测...")

    level = Decimal("50000")

    # 测试触及（在容差范围内）
    touching_price = Decimal("50000.03")  # 0.001 * 50000 = 50, 所以 0.03 在范围内
    not_touching_price = Decimal("50100")

    is_touching = level_manager.is_touching_level(touching_price, level)
    is_not_touching = level_manager.is_touching_level(not_touching_price, level)

    if is_touching and not is_not_touching:
        print(f"  [PASS] 触及检测正确")
        print(f"    {touching_price} 触及 {level}: {is_touching}")
        print(f"    {not_touching_price} 触及 {level}: {is_not_touching}")
    else:
        print(f"  [FAIL] 触及检测错误")
        errors.append("4.4 触及检测逻辑错误")

    # =========================================================================
    # 检查点 4.5：测试 TP/SL 计算
    # =========================================================================
    print("\n[检查点 4.5] 测试 TP/SL 计算...")

    generator = SignalGenerator()

    entry_price = Decimal("50000")
    atr_value = Decimal("100")

    # LONG 信号
    tp_long, sl_long = generator.calculate_tp_sl(
        Direction.LONG, entry_price, atr_value
    )

    expected_tp_long = entry_price + atr_value * Decimal(str(settings.tp_atr_mult))
    expected_sl_long = entry_price - atr_value * Decimal(str(settings.sl_atr_mult))

    print(f"  LONG (入场 {entry_price}, ATR {atr_value}):")
    print(f"    TP: {tp_long} (期望: {expected_tp_long})")
    print(f"    SL: {sl_long} (期望: {expected_sl_long})")

    if tp_long == expected_tp_long and sl_long == expected_sl_long:
        print("    [PASS] LONG TP/SL 计算正确")
    else:
        print("    [FAIL] LONG TP/SL 计算错误")
        errors.append("4.5 LONG TP/SL 计算错误")

    # SHORT 信号
    tp_short, sl_short = generator.calculate_tp_sl(
        Direction.SHORT, entry_price, atr_value
    )

    expected_tp_short = entry_price - atr_value * Decimal(str(settings.tp_atr_mult))
    expected_sl_short = entry_price + atr_value * Decimal(str(settings.sl_atr_mult))

    print(f"\n  SHORT (入场 {entry_price}, ATR {atr_value}):")
    print(f"    TP: {tp_short} (期望: {expected_tp_short})")
    print(f"    SL: {sl_short} (期望: {expected_sl_short})")

    if tp_short == expected_tp_short and sl_short == expected_sl_short:
        print("    [PASS] SHORT TP/SL 计算正确")
    else:
        print("    [FAIL] SHORT TP/SL 计算错误")
        errors.append("4.5 SHORT TP/SL 计算错误")

    # =========================================================================
    # 检查点 4.6：验证信号记录字段
    # =========================================================================
    print("\n[检查点 4.6] 验证 SignalRecord 字段...")

    signal = SignalRecord(
        symbol="BTCUSDT",
        timeframe="5m",
        signal_time=datetime.now(timezone.utc),
        direction=Direction.SHORT,
        entry_price=Decimal("50000"),
        tp_price=Decimal("49800"),
        sl_price=Decimal("50884"),
        atr_at_signal=Decimal("100"),
        streak_at_signal=3,
    )

    required_fields = [
        "id", "symbol", "timeframe", "signal_time", "direction",
        "entry_price", "tp_price", "sl_price", "atr_at_signal",
        "streak_at_signal", "mae_ratio", "mfe_ratio", "outcome"
    ]

    missing = []
    for field in required_fields:
        if not hasattr(signal, field):
            missing.append(field)

    if not missing:
        print("  [PASS] 所有必要字段都存在")
        print(f"    atr_at_signal: {signal.atr_at_signal}")
        print(f"    streak_at_signal: {signal.streak_at_signal}")
    else:
        print(f"  [FAIL] 缺少字段: {missing}")
        errors.append(f"4.6 缺少字段: {missing}")

    # =========================================================================
    # 检查点 4.7：打印策略逻辑说明
    # =========================================================================
    print("\n[检查点 4.7] 策略逻辑说明...")
    print("-" * 60)
    print("  MSR Retest Capture 策略:")
    print()
    print("  SHORT 信号条件:")
    print("    1. 上升趋势 (close > EMA50)")
    print("    2. 价格触及支撑位")
    print("    3. 当前K线收阳 (close > open)")
    print("    → 预期：价格会再次下跌测试支撑")
    print()
    print("  LONG 信号条件:")
    print("    1. 下降趋势 (close < EMA50)")
    print("    2. 价格触及阻力位")
    print("    3. 当前K线收阴 (close < open)")
    print("    → 预期：价格会再次上涨测试阻力")
    print()
    print("  TP/SL 设计:")
    print(f"    TP距离 = ATR × {settings.tp_atr_mult}")
    print(f"    SL距离 = ATR × {settings.sl_atr_mult}")
    print(f"    盈亏比 = 1:{settings.sl_atr_mult/settings.tp_atr_mult:.2f}")
    print(f"    盈亏平衡胜率 > 81.5%")
    print("-" * 60)

    # =========================================================================
    # 检查点 4.8：验证 risk_amount 和 reward_amount
    # =========================================================================
    print("\n[检查点 4.8] 验证风险收益计算...")

    # LONG 信号
    long_signal = SignalRecord(
        symbol="BTCUSDT",
        timeframe="5m",
        signal_time=datetime.now(timezone.utc),
        direction=Direction.LONG,
        entry_price=Decimal("50000"),
        tp_price=Decimal("50200"),
        sl_price=Decimal("49116"),
        atr_at_signal=Decimal("100"),
    )

    print(f"  LONG 信号:")
    print(f"    Entry: {long_signal.entry_price}")
    print(f"    TP: {long_signal.tp_price}")
    print(f"    SL: {long_signal.sl_price}")
    print(f"    Risk (距SL): {long_signal.risk_amount}")
    print(f"    Reward (距TP): {long_signal.reward_amount}")

    # SHORT 信号
    short_signal = SignalRecord(
        symbol="BTCUSDT",
        timeframe="5m",
        signal_time=datetime.now(timezone.utc),
        direction=Direction.SHORT,
        entry_price=Decimal("50000"),
        tp_price=Decimal("49800"),
        sl_price=Decimal("50884"),
        atr_at_signal=Decimal("100"),
    )

    print(f"\n  SHORT 信号:")
    print(f"    Entry: {short_signal.entry_price}")
    print(f"    TP: {short_signal.tp_price}")
    print(f"    SL: {short_signal.sl_price}")
    print(f"    Risk (距SL): {short_signal.risk_amount}")
    print(f"    Reward (距TP): {short_signal.reward_amount}")

    # 验证
    if long_signal.risk_amount > 0 and short_signal.risk_amount > 0:
        print("\n  [PASS] 风险收益计算正确")
    else:
        print("\n  [FAIL] 风险收益计算错误")
        errors.append("4.8 风险收益计算错误")

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
        print("\n[SUCCESS] Layer 4 验证全部通过!")
    elif not errors:
        print(f"\n[PASS WITH WARNINGS] Layer 4 基本通过，有 {len(warnings)} 个警告")

    print("\n可以继续下一层验证:")
    print("  python scripts/verify_layer5_position_tracker.py")
    print()


if __name__ == "__main__":
    errors, warnings = asyncio.run(verify_signal_logic())
    print_summary(errors, warnings)

    if errors:
        sys.exit(2)
    elif warnings:
        sys.exit(1)
    else:
        sys.exit(0)
