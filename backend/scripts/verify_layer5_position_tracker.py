#!/usr/bin/env python3
"""
Layer 5 验证：持仓追踪器
=======================

依赖：Layer 1, 2, 3, 4 已验证通过

验证目标：
1. MAE/MFE 计算正确性
2. TP/SL 触发判断
3. 多信号并发追踪
4. 信号状态转换

运行方式：
    cd backend
    source .venv/bin/activate
    python scripts/verify_layer5_position_tracker.py

预期结果：
    - MAE/MFE 计算符合定义
    - TP/SL 在正确价格触发
"""

import asyncio
import sys
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, ".")


async def verify_position_tracker():
    """验证持仓追踪器"""

    print("=" * 60)
    print("Layer 5 验证：持仓追踪器")
    print("=" * 60)
    print()

    errors = []
    warnings = []

    # =========================================================================
    # 检查点 5.1：导入模块
    # =========================================================================
    print("[检查点 5.1] 导入模块...")
    try:
        from app.models import (
            Direction,
            Outcome,
            SignalRecord,
            AggTrade,
        )
        from app.services import PositionTracker
        print("  [PASS] 模块导入成功")
    except ImportError as e:
        print(f"  [FAIL] 模块导入失败: {e}")
        errors.append(f"5.1 模块导入失败: {e}")
        return errors, warnings

    # =========================================================================
    # 检查点 5.2：创建测试信号
    # =========================================================================
    print("\n[检查点 5.2] 创建测试信号...")

    # LONG 信号
    long_signal = SignalRecord(
        symbol="BTCUSDT",
        timeframe="5m",
        signal_time=datetime.now(timezone.utc),
        direction=Direction.LONG,
        entry_price=Decimal("50000"),
        tp_price=Decimal("50200"),     # +200
        sl_price=Decimal("49116"),     # -884
        atr_at_signal=Decimal("100"),
        streak_at_signal=0,
    )

    # SHORT 信号
    short_signal = SignalRecord(
        symbol="ETHUSDT",
        timeframe="5m",
        signal_time=datetime.now(timezone.utc),
        direction=Direction.SHORT,
        entry_price=Decimal("3000"),
        tp_price=Decimal("2988"),      # -12
        sl_price=Decimal("3053"),      # +53
        atr_at_signal=Decimal("6"),
        streak_at_signal=0,
    )

    print(f"  LONG 信号: {long_signal.symbol}")
    print(f"    Entry: {long_signal.entry_price}, TP: {long_signal.tp_price}, SL: {long_signal.sl_price}")
    print(f"  SHORT 信号: {short_signal.symbol}")
    print(f"    Entry: {short_signal.entry_price}, TP: {short_signal.tp_price}, SL: {short_signal.sl_price}")
    print("  [PASS] 测试信号创建成功")

    # =========================================================================
    # 检查点 5.3：测试 MAE 更新 (LONG)
    # =========================================================================
    print("\n[检查点 5.3] 测试 MAE 更新 (LONG)...")

    # 创建副本用于测试
    test_signal = SignalRecord(
        symbol=long_signal.symbol,
        timeframe=long_signal.timeframe,
        signal_time=long_signal.signal_time,
        direction=long_signal.direction,
        entry_price=long_signal.entry_price,
        tp_price=long_signal.tp_price,
        sl_price=long_signal.sl_price,
        atr_at_signal=long_signal.atr_at_signal,
    )

    # Risk = entry - sl = 50000 - 49116 = 884
    risk = test_signal.risk_amount
    print(f"  Risk amount: {risk}")

    # 价格下跌到 49500（不利移动 500）
    test_signal.update_mae(Decimal("49500"))
    expected_mae = Decimal("500") / risk
    print(f"  价格 49500 → MAE: {test_signal.mae_ratio:.4f} (期望: {expected_mae:.4f})")

    if abs(test_signal.mae_ratio - expected_mae) < Decimal("0.0001"):
        print("  [PASS] MAE 计算正确")
    else:
        print("  [FAIL] MAE 计算错误")
        errors.append("5.3 MAE 计算错误")

    # 价格上涨到 50100（有利移动 100）
    test_signal.update_mae(Decimal("50100"))
    expected_mfe = Decimal("100") / risk
    print(f"  价格 50100 → MFE: {test_signal.mfe_ratio:.4f} (期望: {expected_mfe:.4f})")

    if abs(test_signal.mfe_ratio - expected_mfe) < Decimal("0.0001"):
        print("  [PASS] MFE 计算正确")
    else:
        print("  [FAIL] MFE 计算错误")
        errors.append("5.3 MFE 计算错误")

    # =========================================================================
    # 检查点 5.4：测试 MAE 更新 (SHORT)
    # =========================================================================
    print("\n[检查点 5.4] 测试 MAE 更新 (SHORT)...")

    test_short = SignalRecord(
        symbol=short_signal.symbol,
        timeframe=short_signal.timeframe,
        signal_time=short_signal.signal_time,
        direction=short_signal.direction,
        entry_price=short_signal.entry_price,
        tp_price=short_signal.tp_price,
        sl_price=short_signal.sl_price,
        atr_at_signal=short_signal.atr_at_signal,
    )

    # Risk = sl - entry = 3053 - 3000 = 53
    risk_short = test_short.risk_amount
    print(f"  Risk amount: {risk_short}")

    # 价格上涨到 3020（对 SHORT 不利，移动 20）
    test_short.update_mae(Decimal("3020"))
    expected_mae_short = Decimal("20") / risk_short
    print(f"  价格 3020 → MAE: {test_short.mae_ratio:.4f} (期望: {expected_mae_short:.4f})")

    if abs(test_short.mae_ratio - expected_mae_short) < Decimal("0.0001"):
        print("  [PASS] SHORT MAE 计算正确")
    else:
        print("  [FAIL] SHORT MAE 计算错误")
        errors.append("5.4 SHORT MAE 计算错误")

    # 价格下跌到 2995（对 SHORT 有利，移动 5）
    test_short.update_mae(Decimal("2995"))
    expected_mfe_short = Decimal("5") / risk_short
    print(f"  价格 2995 → MFE: {test_short.mfe_ratio:.4f} (期望: {expected_mfe_short:.4f})")

    if abs(test_short.mfe_ratio - expected_mfe_short) < Decimal("0.0001"):
        print("  [PASS] SHORT MFE 计算正确")
    else:
        print("  [FAIL] SHORT MFE 计算错误")
        errors.append("5.4 SHORT MFE 计算错误")

    # =========================================================================
    # 检查点 5.5：测试 TP 触发 (LONG)
    # =========================================================================
    print("\n[检查点 5.5] 测试 TP 触发 (LONG)...")

    tp_test = SignalRecord(
        symbol="BTCUSDT",
        timeframe="5m",
        signal_time=datetime.now(timezone.utc),
        direction=Direction.LONG,
        entry_price=Decimal("50000"),
        tp_price=Decimal("50200"),
        sl_price=Decimal("49116"),
        atr_at_signal=Decimal("100"),
    )

    now = datetime.now(timezone.utc)

    # 价格未达到 TP
    result1 = tp_test.check_outcome(Decimal("50100"), now)
    print(f"  价格 50100 (< TP 50200): outcome={tp_test.outcome.value}, changed={result1}")

    if not result1 and tp_test.outcome == Outcome.ACTIVE:
        print("  [PASS] 未触发 TP")
    else:
        print("  [FAIL] 不应触发 TP")
        errors.append("5.5 TP 误触发")

    # 价格达到 TP
    result2 = tp_test.check_outcome(Decimal("50200"), now)
    print(f"  价格 50200 (= TP): outcome={tp_test.outcome.value}, changed={result2}")

    if result2 and tp_test.outcome == Outcome.TP:
        print("  [PASS] 正确触发 TP")
    else:
        print("  [FAIL] 应触发 TP")
        errors.append("5.5 TP 未触发")

    # =========================================================================
    # 检查点 5.6：测试 SL 触发 (SHORT)
    # =========================================================================
    print("\n[检查点 5.6] 测试 SL 触发 (SHORT)...")

    sl_test = SignalRecord(
        symbol="ETHUSDT",
        timeframe="5m",
        signal_time=datetime.now(timezone.utc),
        direction=Direction.SHORT,
        entry_price=Decimal("3000"),
        tp_price=Decimal("2988"),
        sl_price=Decimal("3053"),
        atr_at_signal=Decimal("6"),
    )

    # 价格未达到 SL
    result3 = sl_test.check_outcome(Decimal("3040"), now)
    print(f"  价格 3040 (< SL 3053): outcome={sl_test.outcome.value}, changed={result3}")

    if not result3 and sl_test.outcome == Outcome.ACTIVE:
        print("  [PASS] 未触发 SL")
    else:
        print("  [FAIL] 不应触发 SL")
        errors.append("5.6 SL 误触发")

    # 价格达到 SL
    result4 = sl_test.check_outcome(Decimal("3053"), now)
    print(f"  价格 3053 (= SL): outcome={sl_test.outcome.value}, changed={result4}")

    if result4 and sl_test.outcome == Outcome.SL:
        print("  [PASS] 正确触发 SL")
    else:
        print("  [FAIL] 应触发 SL")
        errors.append("5.6 SL 未触发")

    # =========================================================================
    # 检查点 5.7：测试 PositionTracker
    # =========================================================================
    print("\n[检查点 5.7] 测试 PositionTracker...")

    tracker = PositionTracker()

    # 添加信号
    await tracker.add_signal(long_signal)
    await tracker.add_signal(short_signal)

    active_count = len(tracker._active_signals)
    print(f"  添加 2 个信号，当前活跃: {active_count}")

    if active_count == 2:
        print("  [PASS] 信号添加成功")
    else:
        print("  [FAIL] 信号添加失败")
        errors.append("5.7 信号添加失败")

    # =========================================================================
    # 检查点 5.8：测试交易处理
    # =========================================================================
    print("\n[检查点 5.8] 测试交易处理...")

    # 创建一笔交易
    trade = AggTrade(
        symbol="BTCUSDT",
        agg_trade_id=1,
        price=Decimal("49800"),  # 价格下跌
        quantity=Decimal("1"),
        timestamp=datetime.now(timezone.utc),
        is_buyer_maker=True,
    )

    await tracker.process_trade(trade)

    # 检查 MAE 是否更新
    # _active_signals 是 dict[str, list[FastSignal]]，按 symbol 分组
    btc_signals = tracker._active_signals.get("BTCUSDT", [])
    if btc_signals:
        btc_signal = btc_signals[0]
        print(f"  处理交易后 BTCUSDT MAE: {btc_signal.mae_ratio:.4f}")
        if btc_signal.mae_ratio > 0:
            print("  [PASS] MAE 已更新")
        else:
            print("  [WARN] MAE 未更新")
            warnings.append("5.8 MAE 未更新")
    else:
        print("  [INFO] BTCUSDT 信号不在活跃列表（可能已触发 SL）")

    # =========================================================================
    # 检查点 5.9：状态总结
    # =========================================================================
    print("\n[检查点 5.9] 状态总结...")
    print("-" * 60)
    print("  MAE (Maximum Adverse Excursion):")
    print("    定义: 持仓期间最大不利偏移 / 风险金额")
    print("    LONG: (entry - min_price) / (entry - sl)")
    print("    SHORT: (max_price - entry) / (sl - entry)")
    print()
    print("  MFE (Maximum Favorable Excursion):")
    print("    定义: 持仓期间最大有利偏移 / 风险金额")
    print("    LONG: (max_price - entry) / (entry - sl)")
    print("    SHORT: (entry - min_price) / (sl - entry)")
    print()
    print("  MAE = 1.0 表示价格触及止损位")
    print("  MFE = R:R 表示价格触及止盈位")
    print("-" * 60)

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
        print("\n[SUCCESS] Layer 5 验证全部通过!")
        print("\n所有分层验证完成! 可以进行端到端测试:")
        print("  python scripts/verify_e2e.py")
    elif not errors:
        print(f"\n[PASS WITH WARNINGS] Layer 5 基本通过，有 {len(warnings)} 个警告")

    print()


if __name__ == "__main__":
    errors, warnings = asyncio.run(verify_position_tracker())
    print_summary(errors, warnings)

    if errors:
        sys.exit(2)
    elif warnings:
        sys.exit(1)
    else:
        sys.exit(0)
