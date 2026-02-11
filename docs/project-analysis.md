# MSR Retest Capture - 项目完整分析报告

> 生成时间: 2026-02-11
> 分析方法: 5 个专家团队并行 review（架构、策略、回测、实盘、前端）
> 代码统计: ~21,000 行（Python ~18K + TypeScript ~3K）
> 测试: 253 个测试全部通过

---

## 目录

1. [项目概述](#1-项目概述)
2. [系统架构](#2-系统架构)
3. [策略原理](#3-策略原理详解)
4. [指标体系](#4-指标体系)
5. [信号检测逻辑](#5-信号检测逻辑)
6. [TP/SL 计算](#6-tpsl-计算)
7. [信号过滤系统](#7-信号过滤系统)
8. [仓位锁与连胜追踪](#8-仓位锁与连胜追踪)
9. [回测系统](#9-回测系统)
10. [实盘系统](#10-实盘系统)
11. [前端仪表盘](#11-前端仪表盘)
12. [数据流全链路](#12-数据流全链路)
13. [容错与恢复机制](#13-容错与恢复机制)
14. [Portfolio 配置与业绩](#14-portfolio-配置与业绩)
15. [已知风险与局限性](#15-已知风险与局限性)
16. [代码清单](#16-代码清单)

---

## 1. 项目概述

### 一句话描述

基于 Fibonacci 回撤 + VWAP + EMA 趋势的**反趋势均值回归**策略，用于 Binance USDT-M 永续合约，通过严格的信号质量过滤（连胜范围 + ATR 百分位）实现净盈利。

### 技术栈

| 层 | 技术 | 说明 |
|---|------|------|
| 策略核心 | Python (pure) | 零 I/O 依赖，可复用于回测与实盘 |
| 实盘后端 | FastAPI + uvloop | WebSocket 实时推送 |
| 数据库 | TimescaleDB (PostgreSQL) | K 线 + 信号 + 逐笔成交 |
| 缓存 | Redis | 活跃信号、连胜状态、价格缓存 |
| 前端 | React 19 + Vite + Tailwind | 监控仪表盘 |
| 数据源 | Binance WebSocket + REST | 1m K 线 + aggTrades |

### 核心数据

- 回测信号: **499,671 笔**（2020-01 ~ 2025-12）
- 原始胜率: **82.44%**（盈亏比 1:4.42，盈亏平衡点 81.5%）
- 原始边际: **-0.11%**（手续费后净亏损）
- 过滤后胜率: **85.9%**（Portfolio B, 1,941 笔交易）
- 过滤后净利: **$369,250**（扣除 $165,816 手续费）
- 2025 年样本外: **$96,280**（725 笔交易）

---

## 2. 系统架构

### Monorepo 结构

```
backend/
├── core/           ← 纯业务逻辑（零 I/O，回测与实盘共用）
│   ├── signal_generator.py    (667 行) - 策略核心
│   ├── kline_aggregator.py    (416 行) - 1m → 3m/5m/15m/30m
│   ├── atr_tracker.py         (128 行) - ATR 百分位追踪
│   ├── models/                         - 数据模型
│   └── indicators/                     - TA-Lib 指标
├── app/            ← 实盘系统（FastAPI + WebSocket + Redis）
│   ├── main.py                (429 行) - 启动/关闭生命周期
│   ├── services/                       - 数据采集/仓位追踪/重放
│   ├── storage/                        - 数据库/Redis 仓库
│   ├── api/                            - REST + WebSocket
│   └── clients/                        - Binance REST/WS 客户端
├── backtest/       ← 回测系统（独立于 app/）
│   ├── engine.py              (152 行) - 逐 K 线回测引擎
│   ├── outcome.py             (142 行) - 基于 1m K 线的 TP/SL 判定
│   ├── runner.py              (223 行) - 多 symbol 编排
│   └── storage/                        - 独立的 DB 层
└── tests/          ← 253 个测试
    └── 14 个测试文件           (5,379 行)

frontend/
└── src/            ← React 监控仪表盘
    ├── hooks/                          - WebSocket/信号/分析 hooks
    ├── features/                       - 分析页面
    └── components/                     - UI 组件
```

### 关键设计模式

**1. 回调注入（Callback Injection）**

`SignalGenerator` 不直接依赖数据库或 Redis。所有 I/O 通过构造函数注入的回调完成：

```python
# core/signal_generator.py
class SignalGenerator:
    def __init__(self,
        config: StrategyConfig,
        save_signal: Callable | None = None,      # 持久化信号
        save_streak: Callable | None = None,       # 保存连胜状态
        load_streaks: Callable | None = None,      # 加载连胜状态
        load_active_signals: Callable | None = None,  # 加载活跃信号
        filters: dict | None = None,               # 信号过滤配置
        atr_tracker: AtrPercentileTracker | None = None,  # ATR 追踪器
    ):
```

- **实盘**: 注入 PostgreSQL + Redis 回调
- **回测**: 回调设为 `None`，信号在内存中收集

**2. K 线聚合**

只订阅 1m WebSocket（5 个流），用 `KlineAggregator` 本地聚合高时间框架：

```
1m kline → KlineAggregator → 3m/5m/15m/30m klines
```

聚合规则：`open=first`, `high=max`, `low=min`, `close=last`, `volume=sum`。周期边界对齐到 Unix 纪元（如 5m 对齐到被 300 整除的时间戳）。

**3. 数据完整性三层保障**

```
Phase 1: 缓冲模式 → WebSocket 数据入队，不处理
Phase 3: 间隙检测 → 检查 last_processed_time 到当前的 K 线连续性
Phase 4: 缓冲区恢复 → 从 DB 加载 200 根 K 线还原缓冲区状态
Phase 5: 重放 → 从检查点逐根重放未处理的 K 线
Phase 6: 切换到实时 → 二阶段刷新，零数据丢失
```

---

## 3. 策略原理详解

### 核心假设

> 当价格处于上升趋势并触及支撑位时，随后的看涨反弹是**陷阱**——做空，预期支撑被突破。
> 反之，当价格处于下降趋势并触及阻力位时，看跌拒绝是**陷阱**——做多，预期阻力被突破。

这是一个**反直觉的逆势策略**：

| 条件 | 信号 | 赌注 |
|------|------|------|
| 上涨趋势 + 触及支撑 + 看涨 K 线 | **做空** | 反弹是假的，支撑会被击穿 |
| 下跌趋势 + 触及阻力 + 看跌 K 线 | **做多** | 拒绝是假的，阻力会被突破 |

策略名称中的 "Retest" 指的是：价格会回来**重新测试**该水平并突破。

### 信号名称解析

**MSR = Mean (均值) + Support/Resistance (支撑/阻力)**

这不是传统的均值回归（回到均值），而是利用 S/R 水平上的**假反转**。当所有人认为支撑已经守住时（看涨 K 线），策略反向操作。

### 为什么需要过滤

原始策略（499,671 笔交易）在扣除手续费后**净亏损**：

```
原始胜率: 82.44%
盈亏平衡胜率: 81.5% (= 4.42 / (4.42 + 1))
原始边际: 82.44% - 81.5% = +0.94% (看似有边际)

但实际每笔手续费 ≈ $42.10，总手续费 $21M
原始边际被手续费完全吞噬 → 净边际 -0.11%
```

唯一的盈利路径：**通过质量过滤减少交易频率**，只保留高胜率信号。

---

## 4. 指标体系

所有指标在每根 closed kline 上计算（`core/indicators/`）：

| 指标 | 周期 | 公式 | 用途 |
|------|------|------|------|
| **EMA(50)** | 50 | `talib.EMA(closes, 50)` | 趋势方向过滤 |
| **Fib 38.2%** | 9 | `HH(9) - range × 0.382` | 支撑/阻力水平 |
| **Fib 50.0%** | 9 | `HH(9) - range × 0.500` | 支撑/阻力水平 |
| **Fib 61.8%** | 9 | `HH(9) - range × 0.618` | 支撑/阻力水平 |
| **VWAP** | 累计 | `Σ(TP × vol) / Σ(vol)`，`TP=(H+L+C)/3` | 支撑/阻力水平 |
| **ATR(9)** | 9 | Wilder 平滑 (RMA) | TP/SL 定价 + 信号过滤 |

其中 `range = HH(9) - LL(9)`，`HH = highest high`，`LL = lowest low`。

Fibonacci 水平是基于滚动 9 根 K 线的窗口动态计算的，而非固定价格。

---

## 5. 信号检测逻辑

> 源码: `core/signal_generator.py`, `detect_signal()` 方法 (327-466 行)

### 水平分类

四个水平（fib_382, fib_500, fib_618, VWAP）根据当前收盘价分类：

```python
# 收盘价在水平之上 → 该水平是支撑
# 收盘价在水平之下 → 该水平是阻力
for level in [fib_382, fib_500, fib_618, vwap]:
    if close < level:
        resistance_levels.append(level)
    else:
        support_levels.append(level)
```

### 水平评分

每个水平的距离越近，评分越高：

```
score += 1 / (1 + distance_pct)
其中 distance_pct = abs(price - level) / price × 100
```

要求总评分 ≥ 1.0 且至少有 1 个水平。

### 做空信号条件（全部满足）

```
1. close > EMA50                        ← 上升趋势
2. support_count >= 1                    ← 至少 1 个支撑位
3. support_score >= 1.0                  ← 支撑位足够近
4. nearest_support is not None           ← 最近支撑存在
5. low ≤ nearest_support                 ← 当根或前一根触及支撑
   OR prev_low ≤ nearest_support
6. close > open                          ← 当根是看涨 K 线（反弹）
7. _active_positions[key] == False       ← 没有持仓锁
```

→ 入场价 = close，方向 = SHORT

### 做多信号条件（全部满足）

```
1. close < EMA50                        ← 下降趋势
2. resistance_count >= 1                 ← 至少 1 个阻力位
3. resistance_score >= 1.0               ← 阻力位足够近
4. nearest_resistance is not None        ← 最近阻力存在
5. high ≥ nearest_resistance             ← 当根或前一根触及阻力
   OR prev_high ≥ nearest_resistance
6. close < open                          ← 当根是看跌 K 线（拒绝）
7. _active_positions[key] == False       ← 没有持仓锁
```

→ 入场价 = close，方向 = LONG

### 条件可视化

```
  ┌─── 上升趋势（close > EMA50）
  │    支撑位存在且足够近
  │    价格触及支撑位
  │    但形成了看涨 K 线（假反弹）
  │    ────── 做空 ──────
  │
  │         ┌──── 阻力水平 (fib/VWAP)
  │    ↗    │
  │   /  ↑  │  close > open = 看涨 K 线
  │  /   │  │
  └──── 支撑水平 ← low 触及此处
       ──────── EMA50 ────────

  ┌─── 下降趋势（close < EMA50）
  │    阻力位存在且足够近
  │    价格触及阻力位
  │    但形成了看跌 K 线（假拒绝）
  │    ────── 做多 ──────
  │
       ──────── EMA50 ────────
  └──── 阻力水平 ← high 触及此处
  │  \   │  │
  │   \  ↓  │  close < open = 看跌 K 线
  │    ↘    │
  │         └──── 支撑水平
```

---

## 6. TP/SL 计算

> 源码: `core/signal_generator.py`, `calculate_tp_sl()` 方法 (287-325 行)

### 参数

```python
tp_atr_mult = 2.0    # TP = 2.0 × ATR
sl_atr_mult = 8.84   # SL = 8.84 × ATR = 4.42 × TP
```

### R-多倍数

```
盈亏比 = SL / TP = 8.84 / 2.0 = 4.42
即：冒 4.42 单位风险，赚 1 单位利润
盈亏平衡胜率 = 4.42 / (4.42 + 1) = 81.5%
```

### 做多 TP/SL

```python
tp_raw  = entry + ATR × 2.0
tp_cap  = high + ATR            # TP 不超过最高价 + ATR
tp      = min(tp_raw, tp_cap)
sl      = entry - ATR × 8.84
```

### 做空 TP/SL

```python
tp_raw  = entry - ATR × 2.0
tp_cap  = low - ATR             # TP 不低于最低价 - ATR
tp      = max(tp_raw, tp_cap)
sl      = entry + ATR × 8.84
```

TP cap 防止 TP 设置在不合理的价格位。

### 为什么 SL 这么宽？

经过对 13 个 SL 水平 × 63 个 SL+TP 组合的穷举测试：

- 收紧 SL（例如 2.0 ATR）→ 胜率下降，更多止损 → 净亏损更大
- SL = 8.84 ATR 在 4/5 策略中最优
- 宽 SL + 窄 TP 的策略本质：**高频小赢，低频大亏**

---

## 7. 信号过滤系统

> 源码: `core/signal_generator.py`, `_passes_filter()` (468-535 行)
> 源码: `core/atr_tracker.py` (ATR 百分位计算)
> 源码: `core/models/config.py` (Portfolio 配置)

### 过滤流程

```
信号生成 → _passes_filter() → [通过] → 保存 + 回调
                                [拒绝] → 丢弃（不保存、不锁仓、不回调）
```

### 第一关：Portfolio 白名单

```python
if self._filters is None:
    return True   # 无过滤配置 → 全部通过（向后兼容）

key = f"{signal.symbol}_{signal.timeframe}"
fc = self._filters.get(key)
if fc is None or not fc.enabled:
    return False  # 不在 Portfolio 中 → 拒绝
```

当配置了过滤器时，任何不在 Portfolio 中的 symbol/timeframe 自动被拒绝。

### 第二关：连胜范围过滤

```python
if not (fc.streak_lo <= signal.streak_at_signal <= fc.streak_hi):
    return False
```

`streak_at_signal` 是信号触发时的连胜/连亏计数（正数 = 连胜，负数 = 连亏）。

所有 Portfolio 策略的 `streak_lo = 0`，这意味着：
- **永远拒绝负连胜**（连亏后不交易）
- 只接受刚从亏损恢复（streak=0）或正在连胜中的信号
- `streak_hi` 限制连胜长度（防止过度暴露）

### 第三关：ATR 百分位过滤

```python
pct = atr_tracker.get_percentile(symbol, timeframe, atr_value)
if pct is None:       # 数据不足（< 200 样本）
    return False      # 安全拒绝
if pct <= threshold:  # 波动率不够高
    return False      # 拒绝
```

ATR 百分位 = 当前 ATR 在历史 ATR 中的经验 CDF（<= 当前值的比例）。

- `threshold = 0.60` → 只在 top 40% 波动率时交易
- `threshold = 0.90` → 只在 top 10% 波动率时交易
- `threshold = 0.00` → 不过滤

### AtrPercentileTracker 实现细节

```python
class AtrPercentileTracker:
    _history: dict[str, deque[float]]  # 每个 sym_tf 一个 deque
    max_history = 10,000               # 滚动窗口上限
    min_samples = 200                  # 最少样本数

    def get_percentile(self, symbol, tf, value):
        arr = np.asarray(buf)
        return float((arr <= value).sum() / len(arr))  # 经验 CDF
```

- 内存上限: 10,000 × 8 bytes × 25 pairs = ~2 MB
- 每根 closed kline 都更新（不仅仅是信号 K 线），确保分布无偏
- 启动时从 DB 预加载 1,500 根 K 线的 ATR 历史（O(n) 算法）
- 输入验证: 拒绝 NaN、Inf、零、负数

### SignalFilterConfig 定义

```python
class SignalFilterConfig(BaseModel):
    symbol: str                      # e.g. "BTCUSDT"
    timeframe: str                   # e.g. "15m"
    enabled: bool = True
    streak_lo: int = -999            # 连胜下限
    streak_hi: int = 999             # 连胜上限
    atr_pct_threshold: float = 0.0   # ATR 百分位阈值
    position_qty: float = 0.0        # 仓位大小
    max_consecutive_loss_months: int = 3  # 熔断开关（预留）

    @property
    def key(self) -> str:
        return f"{self.symbol}_{self.timeframe}"
```

---

## 8. 仓位锁与连胜追踪

### 仓位锁（Position Lock）

> 对应 Pine Script: `strategy.position_size == 0`

每个 `symbol_timeframe` 只允许一个活跃仓位：

```
信号保存成功 → _active_positions[key] = True   # 锁定
outcome 触发  → del _active_positions[key]      # 释放

detect_signal() 中检查:
if _active_positions.get(key, False):
    return None  # 有活跃仓位，跳过信号生成
```

锁的粒度：`BTCUSDT_5m` 和 `BTCUSDT_15m` 是**独立**的。启动时从 DB 恢复活跃信号的锁。

### 连胜追踪（StreakTracker）

> 源码: `core/models/signal.py`, `StreakTracker` 类

```python
class StreakTracker:
    current_streak: int = 0   # 正数=连胜, 负数=连亏
    total_wins: int = 0
    total_losses: int = 0

def record_outcome(self, outcome):
    if outcome == TP:
        total_wins += 1
        current_streak = current_streak + 1 if current_streak >= 0 else 1
    elif outcome == SL:
        total_losses += 1
        current_streak = current_streak - 1 if current_streak <= 0 else -1
```

示例:
```
Win, Win, Win   → streak = +3
Win, Win, Loss  → streak = -1
Loss, Loss, Win → streak = +1
```

每个 `(symbol, timeframe)` 有独立的追踪器。连胜状态持久化到 Redis，启动时批量加载。

### 关键时序：过滤发生在保存之前

```
detect_signal()           → 生成信号（含 streak_at_signal）
  ↓
_passes_filter()          → 检查连胜范围 + ATR 百分位
  ↓ [通过]                ↓ [拒绝]
save_signal()             return None  ← 不保存、不锁仓、不回调
  ↓
_active_positions = True
  ↓
notify callbacks
```

被过滤的信号**不影响**连胜追踪器（连胜只在 `record_outcome` 时更新）。

---

## 9. 回测系统

> 源码: `backend/backtest/`

### 工作流程

```
CLI 启动 → 下载 1m K 线 → 逐根处理 → 计算统计 → 输出报告
```

### 数据获取

从 `data.binance.vision` 下载官方 1m K 线数据：

- 完整月份: 月度 ZIP 文件（单次下载）
- 尾部月份: 逐日 ZIP 文件
- 并发下载: 信号量限制 10 个并发 HTTP 请求
- 幂等: `ON CONFLICT DO UPDATE`（重复下载安全）

### 逐 K 线处理（核心循环）

```
对每根 1m kline:
  Step 1: outcome_tracker.check_kline()      ← 检查活跃信号是否触及 TP/SL
  Step 2: process_kline("1m")                 ← 在 1m 上生成信号
  Step 3: aggregator.add_1m_kline()           ← 聚合到高时间框架
  Step 4: 对每个聚合出的 kline:
            process_kline(timeframe)           ← 在高时间框架上生成信号
```

**顺序至关重要**: outcome 检查在信号生成之前，防止同一根 K 线既生成又结算。

### Warmup 机制

```python
WARMUP_DAYS = 2  # 2880 根额外的 1m kline（≈ 48 小时）
```

- 30m × 50 根 = 1500 分钟 = 25 小时 → 2 天足够
- Warmup 期间的信号**仍然**被 OutcomeTracker 追踪（维持正确的仓位锁和连胜计数）
- 但**不计入**最终结果集（`signal_time >= start_date` 过滤）

### TP/SL 判定

```python
# LONG: TP hit when kline.high >= tp_price
#        SL hit when kline.low  <= sl_price
# SHORT: TP hit when kline.low  <= tp_price
#         SL hit when kline.high >= sl_price

# 悲观规则: 如果同一根 1m K 线同时触及 TP 和 SL → 判定为 SL
# 概率 << 0.01%（需要 >10.84 ATR 的单分钟范围）
```

### MAE/MFE 追踪

```python
# Maximum Adverse Excursion（最大不利偏移）
# Maximum Favorable Excursion（最大有利偏移）
# 均以风险金额的比例表示:
adverse_ratio = adverse_move / risk_amount
favorable_ratio = favorable_move / risk_amount
```

### 统计指标

| 指标 | 公式 |
|------|------|
| 胜率 | wins / (wins + losses) × 100 |
| 期望 R | (win% × 1.0) - (loss% × 4.42) |
| 总 R | (wins × 1.0) - (losses × 4.42) |
| 盈亏比 | (wins × 1.0) / (losses × 4.42) |
| MAE/MFE | P25, P50, P75, P90 分位数 |

### CLI 用法

```bash
# 运行回测
python -m backtest --start 2025-01-01 --end 2025-12-31

# 指定 symbol 和 timeframe
python -m backtest --start 2024-01-01 --end 2024-12-31 \
  --symbols BTCUSDT,XRPUSDT --timeframes 5m,15m,30m

# 先下载数据再回测
python -m backtest --start 2024-01-01 --end 2024-12-31 --download

# 查看历史运行
python -m backtest --list-runs

# 删除运行
python -m backtest --delete-run <run_id>
```

### 回测局限性

| 局限 | 影响 |
|------|------|
| 基于 K 线而非逐笔成交 | 同时触及 TP+SL 使用悲观规则（概率极低） |
| 无滑点模拟 | 入场价 = K 线收盘价（无滑点） |
| 无手续费模拟 | 手续费在回测后的分析阶段加入 |
| 无资金费率 | 永续合约 8 小时费率未计入 |
| 无仓位管理 | R-多倍数，不模拟实际权益 |
| 过滤器未在回测中运行 | 生成全量信号，在分析阶段应用过滤 |
| 顺序处理 | 不模拟跨 symbol 资金竞争 |

---

## 10. 实盘系统

> 源码: `backend/app/`

### 启动顺序（6 个阶段）

```
┌─────────────────────────────────────────────────┐
│ Phase 0: 基础设施初始化                           │
│  ├─ init_database()        [30s 超时]             │
│  ├─ recover_pending_states()                      │
│  └─ init_cache()           [10s 超时, 优雅降级]   │
├─────────────────────────────────────────────────┤
│ Phase 0.5: 服务构建                               │
│  ├─ 构建 StrategyConfig                           │
│  ├─ 构建 SignalFilterConfig (PORTFOLIO_B)         │
│  ├─ 创建 AtrPercentileTracker(min_samples=200)    │
│  ├─ warmup_atr_tracker() [O(n), 仅 Portfolio 对]  │
│  ├─ 实例化 DataCollector, SignalGenerator,        │
│  │   PositionTracker                              │
│  ├─ signal_generator.init() [加载连胜+活跃信号]   │
│  ├─ 注册 4 个回调                                  │
│  └─ position_tracker.load_active_signals()        │
├─────────────────────────────────────────────────┤
│ Phase 1: WebSocket 连接                           │
│  ├─ 设置缓冲模式 = True                           │
│  ├─ 订阅 5 个 symbol 的 1m kline stream            │
│  └─ 订阅 5 个 symbol 的 aggTrade stream            │
├─────────────────────────────────────────────────┤
│ Phase 2: 处理状态检查                             │
│  ├─ 首次: 同步 48 小时历史 K 线                   │
│  └─ 恢复: 记录上次处理时间                        │
├─────────────────────────────────────────────────┤
│ Phase 3: 间隙检测 & 回填                          │
│  ├─ 检测 last_processed_time 到现在的间隙         │
│  └─ 从 Binance REST API 回填缺失 K 线             │
├─────────────────────────────────────────────────┤
│ Phase 4: 缓冲区恢复                               │
│  └─ 从 DB 加载 200 根 K 线还原每个 buffer          │
├─────────────────────────────────────────────────┤
│ Phase 5: 重放                                     │
│  ├─ 标记 processing_state = "pending"             │
│  ├─ 逐根重放 checkpoint 之后的 1m kline            │
│  ├─ 每 100 根检查点                                │
│  └─ 最终 checkpoint = "confirmed"                 │
├─────────────────────────────────────────────────┤
│ Phase 6: 切换到实时                               │
│  ├─ 第一次快照: 获取锁 → 快照缓冲 → 释放锁 → 处理 │
│  ├─ 第二次快照: 获取锁 → 快照剩余 → 设置 live → 处理│
│  └─ 零数据丢失保证                                │
└─────────────────────────────────────────────────┘
```

### REST API 端点

| 端点 | 方法 | 用途 |
|------|------|------|
| `/api/status` | GET | 系统状态 |
| `/api/signals` | GET | 最近信号（可按 symbol/outcome 过滤） |
| `/api/signals/active` | GET | 活跃信号 |
| `/api/signals/{id}` | GET | 单个信号详情 |
| `/api/stats` | GET | 胜率统计 |
| `/api/analytics/summary` | GET | 完整分析仪表盘数据 |
| `/api/order` | POST | 手动下单（测试网） |
| `/api/execute-signal` | POST | 执行信号（市价 + SL/TP） |
| `/api/position/{symbol}` | GET | 当前交易所仓位 |
| `/api/balance` | GET | 账户余额 |
| `/api/close-position/{symbol}` | POST | 平仓 |
| `/api/set-leverage/{symbol}` | POST | 设置杠杆 |
| `/health` | GET | 健康检查 |
| `/ws` | WebSocket | 实时推送 |

### WebSocket 消息协议

**服务端 → 客户端:**

| 类型 | 时机 | 数据 |
|------|------|------|
| `connected` | 连接建立 | `{message}` |
| `signal` | 新信号生成 | `{id, symbol, timeframe, direction, entry, tp, sl, streak}` |
| `mae_update` | MAE/MFE 更新 | `{signal_id, mae_ratio, mfe_ratio}` |
| `outcome` | TP/SL 触发 | `{signal_id, outcome, exit_price}` |
| `ping` | 60 秒无活动 | `{}` (keepalive) |

**客户端 → 服务端:**

| 类型 | 用途 |
|------|------|
| `ping` | 心跳 |
| `subscribe` | 订阅特定 symbol |

### 数据库表

| 表名 | 类型 | 用途 |
|------|------|------|
| `klines` | TimescaleDB 超表 | 所有时间框架的 K 线数据 |
| `aggtrades` | TimescaleDB 超表 | 历史逐笔成交（8.32B 行，67GB） |
| `signals` | 普通表 | 信号全生命周期 |
| `processing_state` | 普通表 | 每个 sym/tf 的处理检查点 |

### Redis 缓存

| Key 模式 | 用途 | TTL |
|----------|------|-----|
| `signal:{id}` | 活跃信号数据 (orjson) | 24h |
| `signals:{symbol}` | 某 symbol 的活跃信号 ID 集合 | - |
| `signals:all` | 全局活跃信号 ID 集合 | - |
| `price:{symbol}` | 最新价格 | 60s |
| `streak:{sym}_{tf}` | 连胜状态 | 永久 |

价格缓存使用**批量管线更新**: aggTrades 以 1000+/秒到达，先在内存中累积，每 1-2 秒用 Redis pipeline 批量刷新。

### 优雅关闭

```
1. 取消价格缓存刷新任务（最后一次 flush）
2. 停止 DataCollector（关闭 WebSocket）
3. 关闭 Redis 连接池
4. 关闭 PostgreSQL 连接池
```

---

## 11. 前端仪表盘

> 源码: `frontend/src/`（~40 个文件，~2,900 行 TypeScript）

### 技术栈

React 19 + Vite 7 + TypeScript + Tailwind CSS v4 + shadcn/ui + lightweight-charts (TradingView)

**无路由**（单页 Tab 切换），**无外部状态管理**（纯 React hooks），**无订单执行**（仅监控）。

### 功能布局

**Dashboard 标签页:**

```
┌─────────────────────────────────────────────────────────┐
│  Header: Symbol 选择器 | 连接状态 | 主题切换              │
├──────────┬──────────────────────────────────────────────┤
│ 左侧栏    │ 右侧内容                                      │
│ (350px)  │                                               │
│          │ TradingChart (K 线图 + Entry/TP/SL 水平线)    │
│ 活跃仓位  │                                               │
│ ├ symbol │ Timeframe 网格 (1m|3m|5m|15m|30m)             │
│ ├ 方向    │ ├ 每个时间框架显示活跃/TP/SL 计数             │
│ ├ 入场价  │                                               │
│ ├ TP/SL  │ 信号表格（按时间框架 Tab 过滤）                │
│ ├ MAE 进度│                                               │
│ └ MFE 进度│ 最近已平仓信号表格                             │
│          │                                               │
│ 统计网格  │                                               │
│ ├ 总信号  │                                               │
│ ├ 胜/亏   │                                               │
│ └ 胜率条  │ ← 81.5% 盈亏平衡标记                         │
├──────────┴──────────────────────────────────────────────┤
│  Footer: R:R = 1:4.42 | 盈亏平衡胜率 = 81.5%            │
└─────────────────────────────────────────────────────────┘
```

**Analytics 标签页:**

```
┌─────────────────────────────────────────────────────────┐
│ 时间范围选择: 7d | 30d | 90d | All time                  │
├─────────────────────────────────────────────────────────┤
│ 概览指标: 总平仓 | 胜率 | 期望R | 盈亏比 | 累计R          │
├─────────────────────────────────────────────────────────┤
│ LONG vs SHORT 对比卡片（含胜率条和盈亏平衡标记）          │
├─────────────────────────────────────────────────────────┤
│ 表现分解（按 Symbol / 按 Timeframe Tab 切换）            │
├─────────────────────────────────────────────────────────┤
│ 每日 P&L 图表（堆叠柱状图: 绿=盈, 红=亏）               │
├─────────────────────────────────────────────────────────┤
│ MAE/MFE 分布图（TP 组 vs SL 组的 P25/P50/P75/P90）      │
└─────────────────────────────────────────────────────────┘
```

### WebSocket 集成

```
useWebSocket (底层)
  ├─ 自动重连（3 秒延迟）
  ├─ useRef 稳定回调（避免重连循环）
  └─ React StrictMode 兼容

useSignals (业务层)
  ├─ 启动: 并行获取 getSignals + getActiveSignals
  ├─ onSignal: 新信号 → 添加到状态（去重）
  ├─ onMaeUpdate: 更新 MAE/MFE 比率
  └─ onOutcome: 从活跃列表移除 + 更新 outcome

K 线数据直接从 Binance 公共 API 获取，不经过后端代理。
```

---

## 12. 数据流全链路

### 实盘完整数据流

```
Binance WebSocket
    │
    ├── 1m kline stream ──────────────────────────────────────┐
    │                                                          │
    │   DataCollector._handle_kline()                          │
    │     ├─ [缓冲模式] → 入队 _ws_buffer                     │
    │     └─ [实时模式] → _process_kline_live()                │
    │           ├─ 更新 1m KlineBuffer                         │
    │           ├─ 保存到 DB (upsert)                          │
    │           ├─ 更新 processing_state checkpoint             │
    │           ├─ KlineAggregator.add_1m_kline()              │
    │           │     └─ 产出 3m/5m/15m/30m → 保存 + 回调      │
    │           └─ 通知 kline 回调                              │
    │                 │                                        │
    │                 ▼                                        │
    │           on_kline_update() [main.py]                    │
    │             ├─ [跳过 if replaying/buffering]             │
    │             ├─ signal_generator.process_kline()          │
    │             │     ├─ IndicatorCalculator.calculate_latest│
    │             │     ├─ atr_tracker.update() [每根 K 线]    │
    │             │     ├─ detect_signal() → SignalRecord?     │
    │             │     ├─ _passes_filter() [连胜+ATR]         │
    │             │     ├─ save_signal() → DB                  │
    │             │     ├─ _active_positions[key] = True       │
    │             │     └─ notify callbacks → on_new_signal()  │
    │             │           ├─ position_tracker.add_signal() │
    │             │           └─ WebSocket broadcast("signal") │
    │             └─ position_tracker.update_max_atr()         │
    │                                                          │
    ├── aggTrade stream ──────────────────────────────────────┐
    │                                                          │
    │   DataCollector._handle_aggtrade()                       │
    │     ├─ price_cache.update_price() [内存批量]             │
    │     └─ 通知回调 → on_aggtrade_update()                   │
    │           │                                              │
    │           ▼                                              │
    │     position_tracker.process_trade()                     │
    │       ├─ 对每个活跃信号:                                 │
    │       │   ├─ check_outcome(price) → TP/SL?              │
    │       │   └─ update_mae(price) → MAE/MFE               │
    │       ├─ [outcome 触发]:                                 │
    │       │   ├─ 更新 DB (outcome, outcome_time, price)     │
    │       │   ├─ 从 Redis 缓存移除                          │
    │       │   └─ 通知回调 → on_outcome()                    │
    │       │         ├─ signal_generator.record_outcome()    │
    │       │         │   ├─ streak_tracker.record_outcome()  │
    │       │         │   ├─ save_streak() → Redis            │
    │       │         │   └─ del _active_positions[key]       │
    │       │         └─ WebSocket broadcast("outcome")       │
    │       └─ [MAE 更新]: → DB + Redis (1 秒节流)           │
    │                                                          │
    └──────────────────────────────────────────────────────────┘
```

### 回测数据流

```
data.binance.vision → download 1m ZIP → PostgreSQL
    │
    ▼
PostgresKlineSource.get_range(start, end)
    │
    ▼
BacktestEngine.process_1m_kline()
    ├─ outcome_tracker.check_kline()     [先检查 outcome]
    ├─ process_kline("1m")               [1m 信号生成]
    ├─ aggregator.add_1m_kline()         [聚合到高时间框架]
    └─ process_kline(higher_tf)          [高时间框架信号生成]
         └─ signal → outcome_tracker.add_signal()
              └─ 追踪直到 TP/SL 触发或回测结束
```

---

## 13. 容错与恢复机制

### 启动失败恢复

```python
# main.py lifespan()
# 跟踪初始化状态：db_initialized, cache_initialized, services_started
# 任何异常 → 按初始化反序清理 → 重新抛出（阻止启动）
```

### Redis 优雅降级

所有 Redis 操作都包装在 `if not cache.is_cache_available(): return <default>`。系统可以在没有 Redis 的情况下完全运行（回退到数据库查询）。

### 数据库连接池

```python
pool_size = 20      # 基础连接
max_overflow = 30   # 溢出连接
pool_pre_ping = True       # 使用前验证连接
pool_recycle = 3600        # 防止连接过期
connect_timeout = 10       # 连接超时
command_timeout = 60       # 查询超时
```

### 崩溃恢复

```
崩溃时 processing_state 停留在 "pending" 状态
  ↓
下次启动: recover_pending_states()
  ↓
标记为 "confirmed" → 间隙检测重新发现缺失 → 重放修复
```

### 回调错误隔离

每个回调调用都包装在 `try/except` 中。一个回调的失败不会中断数据管线或其他回调。

### 价格缓存内存控制

- 每 60 次刷新执行 `_cleanup_stale_prices()`
- 删除超过 5 分钟的条目
- 硬性上限: 100 个 symbol

---

## 14. Portfolio 配置与业绩

### Portfolio B（推荐）

**Sharpe 1.43 | Calmar 2.90 | 平均相关性 0.052**

| # | Symbol | TF | 连胜范围 | ATR 过滤 | 仓位 | 全期交易 | 胜率 | 净利 |
|---|--------|-----|---------|----------|------|---------|------|------|
| 1 | XRPUSDT | 30m | 0~3 | >P60 | 50,000 XRP | 256 | 86.3% | $106,875 |
| 2 | XRPUSDT | 15m | 0~4 | >P80 | 50,000 XRP | 299 | 83.6% | $72,606 |
| 3 | SOLUSDT | 5m | 0~3 | >P80 | 500 SOL | 791 | 85.7% | $104,076 |
| 4 | BTCUSDT | 15m | 0~7 | >P90 | 1 BTC | 188 | 87.7% | $50,429 |
| 5 | BTCUSDT | 5m | 0~3 | >P90 | 1 BTC | 407 | 85.5% | $33,488 |
| | **合计** | | | | | **1,941** | **85.9%** | **$369,250** |

### 2025 年样本外表现

| 策略 | 交易 | 胜率 | 净利 |
|------|------|------|------|
| XRP 30m | 114 | 85.1% | $50,784 |
| XRP 15m | 173 | 82.7% | $21,559 |
| SOL 5m | 215 | 86.5% | $28,969 |
| BTC 15m | 74 | 82.4% | -$6,502 |
| BTC 5m | 149 | 85.2% | $1,470 |
| **合计** | **725** | **84.7%** | **$96,280** |

### Walk-Forward 验证

扩展窗口: 在测试年之前的所有年份训练，在目标年测试。

| 策略 | 2022 | 2023 | 2024 | 2025 | 通过率 |
|------|------|------|------|------|--------|
| XRP 30m | FAIL | PASS | PASS | PASS | 3/4 |
| XRP 15m | PASS | FAIL | PASS | PASS | 3/4 |
| SOL 5m | PASS | FAIL | PASS | PASS | 3/4 |
| BTC 15m | PASS | PASS | PASS | PASS | 4/4 |
| BTC 5m | PASS | FAIL | PASS | PASS | 3/4 |

总通过率: **16/20 (80%)**。Portfolio 级别每年均为正（除 2023 年 -$4,580，边际亏损）。

### 相关性矩阵

```
                XRP 30m   XRP 15m   SOL 5m   BTC 15m   BTC 5m
XRP 30m          1.000     0.261     0.071    -0.123     0.061
XRP 15m          0.261     1.000    -0.104    -0.015     0.026
SOL 5m           0.071    -0.104     1.000     0.301     0.011
BTC 15m         -0.123    -0.015     0.301     1.000     0.030
BTC 5m           0.061     0.026     0.011     0.030     1.000
```

平均配对相关性: **0.052**（接近零，极佳分散化）。

### 手续费模型

```
Entry:  taker 0.04% (市价单)
TP Exit: maker 0.02% (限价单)
SL Exit: taker 0.04% (止损市价单)
滑点:   每边 2bp (保守估计)

Win:  (0.04% + 0.02% + 0.04%) × 名义价值 = 0.10%
Loss: (0.04% + 0.04% + 0.04%) × 名义价值 = 0.12%
```

手续费占毛利比: **31%**（$165,816 / $535,066）。

---

## 15. 已知风险与局限性

### 策略风险

| 风险 | 严重度 | 说明 |
|------|--------|------|
| BTC 15m 2025 年亏损 $6.5K | 高 | 需持续监控，可能需要剔除 |
| 2023 年弱势表现 | 中 | 低波动率减少信号质量 |
| 手续费敏感 | 高 | 边际仅 3-5% 高于盈亏平衡点 |
| 市场体制变化 | 高 | 策略依赖均值回归；趋势市场会侵蚀边际 |
| 相关性飙升 | 中 | 市场崩盘时所有加密货币相关性趋近 1.0 |

### 回测局限

| 局限 | 说明 |
|------|------|
| K 线级别判定 | 非逐笔成交，同时触及 TP+SL 使用悲观规则 |
| 无滑点 | 入场价 = K 线收盘价 |
| 无资金费率 | 永续合约 8 小时费率未计入 |
| 过滤器离线 | 回测生成全量信号，过滤在分析阶段应用 |
| 固定仓位 | 不模拟权益变化和复利 |
| 无市场冲击 | 假设仓位不影响价格 |

### 实盘特有风险

| 风险 | 说明 |
|------|------|
| ATR 冷启动 | 数据不足时（<200 样本）拒绝所有信号，5m 需 ~17 小时 |
| 重放期间信号 | 重放生成的信号不检查历史 outcome（可能标记为 "active"） |
| 缓冲切换间隙 | 从缓冲到实时切换期间有几秒的信号间隙 |
| 订单执行 | 当前为手动执行（API 已实现但前端无下单 UI） |

### 实现状态（检查清单）

| 功能 | 状态 | 说明 |
|------|------|------|
| 信号生成 | ✅ 完成 | MSR Retest Capture 全逻辑 |
| K 线聚合 | ✅ 完成 | 1m → 3m/5m/15m/30m |
| 间隙检测 + 回填 | ✅ 完成 | 自动从 Binance REST 补缺 |
| 重放恢复 | ✅ 完成 | 崩溃后自动恢复 |
| 连胜过滤 | ✅ 完成 | streak_at_signal in [lo, hi] |
| ATR 百分位过滤 | ✅ 完成 | 滚动窗口 10,000 + 启动预热 |
| Portfolio 配置 | ✅ 完成 | PORTFOLIO_A / PORTFOLIO_B |
| WebSocket 推送 | ✅ 完成 | 实时信号/outcome/MAE 更新 |
| 前端仪表盘 | ✅ 完成 | 监控 + 分析（无下单） |
| 回测系统 | ✅ 完成 | CLI + 独立 DB + 统计报告 |
| 测试套件 | ✅ 完成 | 253 测试通过 |
| 自动下单 | ⬜ 未完成 | API 已有，前端 UI 未实现 |
| 仓位管理 | ⬜ 未完成 | 固定仓位，无权益比例调整 |
| 熔断开关 | ⬜ 未完成 | max_consecutive_loss_months 已定义但未使用 |
| 资金费率计算 | ⬜ 未完成 | 回测和实盘均未纳入 |

---

## 16. 代码清单

### 核心文件（按重要性排序）

| 文件 | 行数 | 职责 |
|------|------|------|
| `core/signal_generator.py` | 667 | 策略核心: 信号检测 + 过滤 + 仓位锁 |
| `core/kline_aggregator.py` | 416 | 1m → 高时间框架聚合 |
| `core/atr_tracker.py` | 128 | ATR 百分位追踪（bounded deque + 输入验证） |
| `core/models/config.py` | 139 | StrategyConfig + SignalFilterConfig + Portfolio |
| `core/models/signal.py` | 190 | SignalRecord + StreakTracker + Outcome |
| `core/models/kline.py` | 81 | Kline + KlineBuffer |
| `core/models/fast.py` | 268 | 热路径 dataclass（FastKline/FastSignal） |
| `core/models/converters.py` | 298 | Pydantic ↔ dataclass 转换 |
| `core/indicators/talib_indicators.py` | 299 | EMA/ATR/Fib/VWAP 计算 |
| `core/indicators/indicators.py` | 494 | IndicatorCalculator 接口 |

### 实盘系统

| 文件 | 行数 | 职责 |
|------|------|------|
| `app/main.py` | 429 | 生命周期管理 + ATR 预热 + 回调注入 |
| `app/services/data_collector.py` | 687 | WebSocket 数据采集 + 间隙检测 + 重放 |
| `app/services/position_tracker.py` | 364 | 仓位追踪 + MAE/MFE + outcome 检测 |
| `app/services/kline_replay.py` | 277 | 启动时 K 线重放 |
| `app/services/order_service.py` | 461 | 订单执行 |
| `app/storage/signal_repo.py` | 513 | 信号 CRUD |
| `app/storage/kline_repo.py` | 417 | K 线 CRUD |
| `app/storage/database.py` | 215 | TimescaleDB + 连接池 |
| `app/api/routes.py` | 514 | REST API |
| `app/api/websocket.py` | 223 | WebSocket 管理 |

### 回测系统

| 文件 | 行数 | 职责 |
|------|------|------|
| `backtest/__main__.py` | 243 | CLI 入口 |
| `backtest/runner.py` | 223 | 多 symbol 编排 |
| `backtest/engine.py` | 152 | 逐 K 线引擎 |
| `backtest/outcome.py` | 142 | TP/SL 判定 |
| `backtest/downloader.py` | 255 | data.binance.vision 下载 |
| `backtest/stats.py` | 299 | 统计计算 |

### 测试

| 文件 | 行数 | 覆盖 |
|------|------|------|
| `tests/test_signal_filter.py` | 683 | 信号过滤（48 测试，含异步集成） |
| `tests/test_outcome.py` | 843 | TP/SL 判定 |
| `tests/test_stats.py` | 838 | 统计计算 |
| `tests/test_integration.py` | 513 | 端到端信号流 |
| `tests/test_kline_aggregator.py` | 400 | K 线聚合 |
| `tests/test_converters.py` | 420 | 模型转换 |
| `tests/test_fast_models.py` | 396 | Fast dataclass |
| `tests/test_signal.py` | 284 | 信号生成 |
| `tests/test_stress.py` | 286 | 压力测试 |
| `tests/test_benchmark.py` | 213 | 性能基准 |
| `tests/test_indicators.py` | 183 | 指标计算 |
| `tests/test_position_tracker.py` | 181 | 仓位追踪 |
| `tests/test_streak_cache.py` | 139 | 连胜缓存 |

---

*本文档由 5 个专家团队并行分析生成，经交叉审核确认。*
*所有代码引用基于 2026-02-11 的 main 分支 (commit 7d67a71)。*
