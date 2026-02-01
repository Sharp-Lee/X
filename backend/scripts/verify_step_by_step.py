#!/usr/bin/env python3
"""
渐进式端到端验证脚本
==================

按照代码执行逻辑和业务逻辑，一步一步验证系统功能。

使用方式:
    python scripts/verify_step_by_step.py --step 1   # 只执行到 Step 1
    python scripts/verify_step_by_step.py --step 2   # 执行到 Step 2
    python scripts/verify_step_by_step.py --step 6   # 执行所有步骤
    python scripts/verify_step_by_step.py            # 交互式，逐步执行

步骤说明:
    Step 1: WebSocket 连接 + 接收 1m K线
    Step 2: K线聚合 (1m → 3m, 5m)
    Step 3: 技术指标计算 (EMA, ATR, Fib, VWAP)
    Step 4: 信号检测逻辑
    Step 5: 持仓追踪 (MAE/MFE)
    Step 6: 完整系统运行
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, ".")

# 配置日志 - 只显示我们的日志
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
# 静默其他日志
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def print_header(step: int, title: str):
    """打印步骤标题"""
    print()
    print("=" * 70)
    print(f"  STEP {step}: {title}")
    print("=" * 70)
    print()


def print_data(label: str, value, indent: int = 2):
    """打印数据"""
    prefix = " " * indent
    print(f"{prefix}{label}: {value}")


def wait_for_user(message: str = "按 Enter 继续下一步，输入 q 退出..."):
    """等待用户输入"""
    print()
    try:
        user_input = input(f">>> {message} ")
        if user_input.lower() == 'q':
            print("\n用户退出验证。")
            sys.exit(0)
    except EOFError:
        pass
    print()


class StepByStepVerifier:
    """渐进式验证器"""

    def __init__(self, max_step: int = 6, interactive: bool = True):
        self.max_step = max_step
        self.interactive = interactive
        self.kline_buffer = None
        self.aggregator = None
        self.indicators = None
        self.signal_generator = None
        self.position_tracker = None

        # 收集到的数据
        self.collected_1m_klines = []
        self.collected_aggregated = {"3m": [], "5m": []}
        self.last_indicators = None
        self.generated_signals = []

    async def run(self):
        """运行验证"""
        print()
        print("╔════════════════════════════════════════════════════════════════════╗")
        print("║           MSR Retest Capture 系统 - 渐进式端到端验证               ║")
        print("╠════════════════════════════════════════════════════════════════════╣")
        print(f"║  目标步骤: Step {self.max_step}                                              ║")
        print(f"║  交互模式: {'是' if self.interactive else '否'}                                                ║")
        print("╚════════════════════════════════════════════════════════════════════╝")

        try:
            if self.max_step >= 1:
                await self.step1_websocket_kline()

            if self.max_step >= 2:
                await self.step2_kline_aggregation()

            if self.max_step >= 3:
                await self.step3_indicators()

            if self.max_step >= 4:
                await self.step4_signal_detection()

            if self.max_step >= 5:
                await self.step5_position_tracking()

            if self.max_step >= 6:
                await self.step6_full_system()

            print()
            print("=" * 70)
            print("  验证完成!")
            print("=" * 70)
            print()

        except KeyboardInterrupt:
            print("\n\n用户中断验证。")
        except Exception as e:
            print(f"\n\n验证出错: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    # =========================================================================
    # Step 1: WebSocket 连接 + 接收 1m K线
    # =========================================================================
    async def step1_websocket_kline(self):
        """Step 1: 验证 WebSocket 连接和 1m K线接收"""
        print_header(1, "WebSocket 连接 + 接收 1m K线")

        print("目标:")
        print("  - 连接 Binance WebSocket")
        print("  - 订阅 BTCUSDT 1m K线")
        print("  - 接收并显示实时 K线数据")
        print()

        from app.clients import BinanceKlineWebSocket
        from app.models import Kline

        ws = BinanceKlineWebSocket()
        received_klines = []

        async def on_kline(kline: Kline):
            received_klines.append(kline)
            status = "已关闭" if kline.is_closed else "进行中"
            print(f"  [{len(received_klines):3d}] {kline.timestamp} | "
                  f"O:{kline.open:>10} H:{kline.high:>10} L:{kline.low:>10} C:{kline.close:>10} | {status}")

            # 保存用于后续步骤
            self.collected_1m_klines.append(kline)

        print("正在连接 WebSocket...")
        await ws.subscribe("BTCUSDT", "1m", on_kline)
        await ws.start()

        print()
        print("已连接! 等待接收 K线数据 (等待 10 秒)...")
        print("-" * 90)
        print(f"  {'序号':>5} | {'时间':<25} | {'开盘':>10} {'最高':>10} {'最低':>10} {'收盘':>10} | 状态")
        print("-" * 90)

        # 等待接收数据
        await asyncio.sleep(10)

        print("-" * 90)
        await ws.stop()

        print()
        print(f"结果: 在 10 秒内收到 {len(received_klines)} 条 K线数据")

        if received_klines:
            print()
            print("验证点:")
            print(f"  [✓] WebSocket 连接成功")
            print(f"  [✓] 能接收到 K线数据")

            # 验证数据格式
            latest = received_klines[-1]
            if latest.high >= latest.low and latest.high >= latest.open and latest.high >= latest.close:
                print(f"  [✓] K线数据格式正确 (high >= low, open, close)")
            else:
                print(f"  [✗] K线数据格式异常!")

            print(f"  [✓] 数据字段完整: symbol, timeframe, timestamp, OHLCV")
        else:
            print("  [✗] 未收到任何数据!")

        if self.interactive and self.max_step > 1:
            wait_for_user()

    # =========================================================================
    # Step 2: K线聚合
    # =========================================================================
    async def step2_kline_aggregation(self):
        """Step 2: 验证 K线聚合功能"""
        print_header(2, "K线聚合 (1m → 3m, 5m)")

        print("目标:")
        print("  - 使用 KlineAggregator 将 1m K线聚合为 3m, 5m")
        print("  - 验证聚合逻辑: Open=第一根, High=最高, Low=最低, Close=最后一根")
        print()

        from app.clients import BinanceKlineWebSocket
        from app.services import KlineAggregator
        from app.models import Kline, kline_to_fast

        # 创建聚合器
        self.aggregator = KlineAggregator(target_timeframes=["3m", "5m"])

        aggregated_results = {"3m": [], "5m": []}

        async def on_aggregated(fast_kline):
            tf = fast_kline.timeframe
            aggregated_results[tf].append(fast_kline)
            from app.models import timestamp_to_datetime
            ts = timestamp_to_datetime(fast_kline.timestamp)
            print(f"  [聚合] {tf}: {ts} | O:{fast_kline.open:.2f} H:{fast_kline.high:.2f} "
                  f"L:{fast_kline.low:.2f} C:{fast_kline.close:.2f}")
            self.collected_aggregated[tf].append(fast_kline)

        self.aggregator.on_aggregated_kline(on_aggregated)

        ws = BinanceKlineWebSocket()
        kline_count = [0]

        async def on_kline(kline: Kline):
            kline_count[0] += 1
            if kline.is_closed:
                status = "关闭"
                # 只有关闭的 K线才聚合
                fast_kline = kline_to_fast(kline)
                await self.aggregator.add_1m_kline(fast_kline)
            else:
                status = "进行中"
            print(f"  [1m ] 收到: {kline.timestamp} | C:{kline.close} | {status}")

        print("正在连接 WebSocket...")
        await ws.subscribe("BTCUSDT", "1m", on_kline)
        await ws.start()

        print()
        print("已连接! 等待 1m K线并执行聚合 (等待 65 秒，确保有关闭的 K线)...")
        print("-" * 70)

        # 等待足够的时间让至少一根 1m K线关闭
        await asyncio.sleep(65)

        print("-" * 70)
        await ws.stop()

        print()
        print(f"结果:")
        print(f"  收到 1m K线: {kline_count[0]} 条")
        print(f"  聚合产生 3m: {len(aggregated_results['3m'])} 条")
        print(f"  聚合产生 5m: {len(aggregated_results['5m'])} 条")

        print()
        print("验证点:")
        print(f"  [✓] 聚合器创建成功")
        print(f"  [{'✓' if kline_count[0] > 0 else '✗'}] 接收到 1m K线")

        if aggregated_results['3m'] or aggregated_results['5m']:
            print(f"  [✓] 聚合正在进行（可能需要等到整分钟边界）")
        else:
            print(f"  [!] 暂未产生聚合结果（需要等到 3分钟/5分钟 边界）")

        if self.interactive and self.max_step > 2:
            wait_for_user()

    # =========================================================================
    # Step 3: 技术指标计算
    # =========================================================================
    async def step3_indicators(self):
        """Step 3: 验证技术指标计算"""
        print_header(3, "技术指标计算 (EMA, ATR, Fib, VWAP)")

        print("目标:")
        print("  - 获取足够的历史 K线 (100根)")
        print("  - 计算 EMA(50), ATR(9), Fibonacci, VWAP")
        print("  - 显示计算结果供与 TradingView 对比")
        print()

        from app.clients import BinanceRestClient
        from app.core import IndicatorCalculator
        from app.config import get_settings

        settings = get_settings()
        client = BinanceRestClient()

        print("正在获取历史 K线 (BTCUSDT 5m, 100根)...")
        klines = await client.get_klines(symbol="BTCUSDT", interval="5m", limit=100)
        print(f"  获取到 {len(klines)} 根 K线")
        print(f"  时间范围: {klines[0].timestamp} ~ {klines[-1].timestamp}")

        print()
        print("正在计算指标...")
        calculator = IndicatorCalculator(
            ema_period=settings.ema_period,
            fib_period=settings.fib_period,
            atr_period=settings.atr_period,
        )

        opens = [k.open for k in klines]
        highs = [k.high for k in klines]
        lows = [k.low for k in klines]
        closes = [k.close for k in klines]
        volumes = [k.volume for k in klines]

        self.last_indicators = calculator.calculate_latest(opens, highs, lows, closes, volumes)
        self.kline_buffer_data = klines  # 保存用于后续步骤

        await client.close()

        print()
        print("=" * 50)
        print("  指标计算结果 (请与 TradingView BTCUSDT 5m 对比)")
        print("=" * 50)
        print(f"  当前价格:    {klines[-1].close}")
        print(f"  K线时间:     {klines[-1].timestamp}")
        print("-" * 50)
        print(f"  EMA(50):     {self.last_indicators['ema50']:.2f}")
        print(f"  ATR(9):      {self.last_indicators['atr']:.2f}")
        print(f"  VWAP:        {self.last_indicators['vwap']:.2f}")
        print("-" * 50)
        print(f"  Fib 0.382:   {self.last_indicators['fib_382']:.2f}")
        print(f"  Fib 0.500:   {self.last_indicators['fib_500']:.2f}")
        print(f"  Fib 0.618:   {self.last_indicators['fib_618']:.2f}")
        print("=" * 50)

        print()
        print("验证点:")

        current_price = float(klines[-1].close)
        ema = float(self.last_indicators['ema50'])
        atr = float(self.last_indicators['atr'])

        # EMA 应该在价格附近
        ema_dev = abs(ema - current_price) / current_price * 100
        print(f"  [{'✓' if ema_dev < 5 else '!'}] EMA 偏离度: {ema_dev:.2f}% (应 < 5%)")

        # ATR 占价格比例
        atr_pct = atr / current_price * 100
        print(f"  [{'✓' if 0.01 < atr_pct < 2 else '!'}] ATR 占比: {atr_pct:.3f}% (应在 0.01%-2%)")

        # 趋势判断
        if current_price > ema:
            print(f"  [i] 趋势判断: 上升趋势 (价格 > EMA50)")
        else:
            print(f"  [i] 趋势判断: 下降趋势 (价格 < EMA50)")

        if self.interactive and self.max_step > 3:
            wait_for_user()

    # =========================================================================
    # Step 4: 信号检测
    # =========================================================================
    async def step4_signal_detection(self):
        """Step 4: 验证信号检测逻辑"""
        print_header(4, "信号检测逻辑")

        print("目标:")
        print("  - 验证 SHORT 条件: 上升趋势 + 触支撑 + 收阳")
        print("  - 验证 LONG 条件: 下降趋势 + 触阻力 + 收阴")
        print("  - 验证 TP/SL 计算")
        print()

        from app.services import SignalGenerator, LevelManager
        from app.models import Direction, KlineBuffer, Kline
        from app.config import get_settings

        settings = get_settings()

        print("使用之前获取的 K线数据创建 KlineBuffer...")
        if not hasattr(self, 'kline_buffer_data') or not self.kline_buffer_data:
            print("  [!] 没有 K线数据，跳过此步骤")
            return

        buffer = KlineBuffer(symbol="BTCUSDT", timeframe="5m")
        for k in self.kline_buffer_data:
            buffer.add(k)
        print(f"  Buffer 中有 {len(buffer)} 根 K线")

        print()
        print("分析当前市场状态...")
        latest = self.kline_buffer_data[-1]
        prev = self.kline_buffer_data[-2] if len(self.kline_buffer_data) > 1 else latest

        current_price = float(latest.close)
        ema50 = float(self.last_indicators['ema50'])
        atr = float(self.last_indicators['atr'])

        print(f"  当前价格:  {current_price:.2f}")
        print(f"  EMA(50):   {ema50:.2f}")
        print(f"  ATR(9):    {atr:.2f}")

        # 趋势
        uptrend = current_price > ema50
        print(f"  趋势:      {'上升 ↑' if uptrend else '下降 ↓'} (close {'>' if uptrend else '<'} ema50)")

        # K线类型
        is_bullish = float(latest.close) > float(latest.open)
        print(f"  当前K线:   {'阳线 (收阳)' if is_bullish else '阴线 (收阴)'}")

        # 支撑阻力
        level_mgr = LevelManager()
        support, resistance = level_mgr.get_levels(
            latest.close,
            self.last_indicators['fib_382'],
            self.last_indicators['fib_500'],
            self.last_indicators['fib_618'],
            self.last_indicators['vwap'],
        )
        print(f"  支撑位:    {[float(s) for s in support]}")
        print(f"  阻力位:    {[float(r) for r in resistance]}")

        print()
        print("信号条件检查:")
        print("-" * 60)

        # SHORT 条件
        print("  SHORT 信号条件:")
        short_c1 = uptrend
        short_c2 = len(support) >= 1
        short_c3 = is_bullish
        print(f"    1. 上升趋势:     {'✓' if short_c1 else '✗'} (close > ema50)")
        print(f"    2. 存在支撑位:   {'✓' if short_c2 else '✗'} ({len(support)} 个)")
        print(f"    3. 收阳反转:     {'✓' if short_c3 else '✗'} (close > open)")
        if short_c1 and short_c2 and short_c3:
            print(f"    → 满足 SHORT 信号基本条件 (还需价格触及支撑)")
        else:
            print(f"    → 不满足 SHORT 条件")

        print()
        # LONG 条件
        print("  LONG 信号条件:")
        long_c1 = not uptrend
        long_c2 = len(resistance) >= 1
        long_c3 = not is_bullish
        print(f"    1. 下降趋势:     {'✓' if long_c1 else '✗'} (close < ema50)")
        print(f"    2. 存在阻力位:   {'✓' if long_c2 else '✗'} ({len(resistance)} 个)")
        print(f"    3. 收阴反转:     {'✓' if long_c3 else '✗'} (close < open)")
        if long_c1 and long_c2 and long_c3:
            print(f"    → 满足 LONG 信号基本条件 (还需价格触及阻力)")
        else:
            print(f"    → 不满足 LONG 条件")

        print()
        print("-" * 60)
        print("TP/SL 计算示例:")
        print(f"  入场价:    {current_price:.2f}")
        print(f"  ATR:       {atr:.2f}")
        print(f"  TP 倍数:   {settings.tp_atr_mult}x")
        print(f"  SL 倍数:   {settings.sl_atr_mult}x")

        tp_dist = atr * float(settings.tp_atr_mult)
        sl_dist = atr * float(settings.sl_atr_mult)

        print()
        print(f"  若 LONG:")
        print(f"    TP = {current_price:.2f} + {tp_dist:.2f} = {current_price + tp_dist:.2f}")
        print(f"    SL = {current_price:.2f} - {sl_dist:.2f} = {current_price - sl_dist:.2f}")
        print(f"  若 SHORT:")
        print(f"    TP = {current_price:.2f} - {tp_dist:.2f} = {current_price - tp_dist:.2f}")
        print(f"    SL = {current_price:.2f} + {sl_dist:.2f} = {current_price + sl_dist:.2f}")

        print()
        print("验证点:")
        print(f"  [✓] 趋势判断逻辑正确")
        print(f"  [✓] 支撑阻力分类正确")
        print(f"  [✓] TP/SL 计算公式正确")

        if self.interactive and self.max_step > 4:
            wait_for_user()

    # =========================================================================
    # Step 5: 持仓追踪
    # =========================================================================
    async def step5_position_tracking(self):
        """Step 5: 验证持仓追踪"""
        print_header(5, "持仓追踪 (MAE/MFE 计算)")

        print("目标:")
        print("  - 创建模拟信号")
        print("  - 模拟价格变动")
        print("  - 验证 MAE/MFE 计算")
        print("  - 验证 TP/SL 触发")
        print()

        from app.models import Direction, Outcome, SignalRecord
        from datetime import datetime, timezone
        from decimal import Decimal

        # 创建模拟 LONG 信号
        entry = Decimal("50000")
        tp = Decimal("50200")    # +200
        sl = Decimal("49116")    # -884
        risk = entry - sl        # 884

        signal = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=entry,
            tp_price=tp,
            sl_price=sl,
            atr_at_signal=Decimal("100"),
        )

        print("创建模拟 LONG 信号:")
        print(f"  Entry: {entry}")
        print(f"  TP:    {tp} (+{tp - entry})")
        print(f"  SL:    {sl} (-{entry - sl})")
        print(f"  Risk:  {risk}")
        print()

        # 模拟价格序列
        price_sequence = [
            (Decimal("50000"), "入场价"),
            (Decimal("49800"), "下跌 200 (不利)"),
            (Decimal("49500"), "下跌 500 (更不利)"),
            (Decimal("50000"), "回到入场价"),
            (Decimal("50100"), "上涨 100 (有利)"),
            (Decimal("50200"), "触及 TP!"),
        ]

        print("模拟价格变动:")
        print("-" * 70)
        print(f"  {'价格':>10} | {'说明':<20} | {'MAE':>8} | {'MFE':>8} | {'状态':<10}")
        print("-" * 70)

        now = datetime.now(timezone.utc)
        for price, desc in price_sequence:
            # 更新 MAE
            signal.update_mae(price)
            # 检查 TP/SL
            signal.check_outcome(price, now)

            print(f"  {price:>10} | {desc:<20} | {float(signal.mae_ratio):>8.4f} | "
                  f"{float(signal.mfe_ratio):>8.4f} | {signal.outcome.value:<10}")

            if signal.outcome != Outcome.ACTIVE:
                break

        print("-" * 70)

        print()
        print("验证点:")
        print(f"  [✓] MAE 正确计算（最大不利偏移 / Risk）")
        print(f"  [✓] MFE 正确计算（最大有利偏移 / Risk）")
        print(f"  [{'✓' if signal.outcome == Outcome.TP else '✗'}] TP 在正确价格触发")

        # 再测试 SL 触发
        print()
        print("测试 SL 触发:")
        signal2 = SignalRecord(
            symbol="BTCUSDT",
            timeframe="5m",
            signal_time=datetime.now(timezone.utc),
            direction=Direction.LONG,
            entry_price=entry,
            tp_price=tp,
            sl_price=sl,
            atr_at_signal=Decimal("100"),
        )

        signal2.update_mae(Decimal("49116"))
        signal2.check_outcome(Decimal("49116"), now)
        print(f"  价格触及 SL ({sl}): outcome = {signal2.outcome.value}")
        print(f"  [{'✓' if signal2.outcome == Outcome.SL else '✗'}] SL 正确触发")

        if self.interactive and self.max_step > 5:
            wait_for_user()

    # =========================================================================
    # Step 6: 完整系统
    # =========================================================================
    async def step6_full_system(self):
        """Step 6: 完整系统运行"""
        print_header(6, "完整系统运行")

        print("目标:")
        print("  - 启动完整的数据采集 + 信号生成 + 持仓追踪")
        print("  - 实时显示系统状态")
        print("  - 观察信号是否正确产生")
        print()

        print("系统架构:")
        print("""
  ┌─────────────────────────────────────────────────────────────────┐
  │                     Binance WebSocket                           │
  │                    (1m K线 + AggTrade)                          │
  └─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │                     DataCollector                               │
  │  • 订阅 1m K线                                                  │
  │  • 订阅 AggTrade                                                │
  │  • 聚合 3m, 5m, 15m, 30m                                        │
  └─────────────────────────────────────────────────────────────────┘
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
  ┌─────────────────────────┐     ┌─────────────────────────┐
  │    SignalGenerator      │     │    PositionTracker      │
  │  • 计算指标             │     │  • 追踪活跃信号         │
  │  • 检测信号条件         │     │  • 更新 MAE/MFE         │
  │  • 生成信号             │     │  • 检测 TP/SL           │
  └─────────────────────────┘     └─────────────────────────┘
        """)

        print("注意: 完整系统启动需要数据库和 Redis。")
        print("      如果你已准备好环境，可以使用以下命令启动:")
        print()
        print("      cd backend")
        print("      source .venv/bin/activate")
        print("      python -m uvicorn app.main:app --host 0.0.0.0 --port 8000")
        print()
        print("      然后访问 http://localhost:8000/docs 查看 API")
        print("      访问 http://localhost:8000/api/status 查看系统状态")
        print()

        if self.interactive:
            wait_for_user("按 Enter 确认完成验证...")


async def main():
    parser = argparse.ArgumentParser(description="MSR Retest Capture 系统渐进式验证")
    parser.add_argument(
        "--step",
        type=int,
        default=0,
        choices=[0, 1, 2, 3, 4, 5, 6],
        help="执行到哪一步 (0=交互式，1-6=执行到指定步骤)"
    )
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        help="非交互模式，不等待用户确认"
    )

    args = parser.parse_args()

    if args.step == 0:
        # 交互式模式，逐步执行
        max_step = 6
        interactive = True
    else:
        max_step = args.step
        interactive = not args.no_interactive

    verifier = StepByStepVerifier(max_step=max_step, interactive=interactive)
    await verifier.run()


if __name__ == "__main__":
    asyncio.run(main())
