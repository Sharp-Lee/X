# Trading 配置系统使用指南

## 目录

1. [概述](#1-概述)
2. [快速开始](#2-快速开始)
3. [配置文件详解](#3-配置文件详解)
4. [Portfolio 选择](#4-portfolio-选择)
5. [自定义策略](#5-自定义策略)
6. [多账户自动下单](#6-多账户自动下单)
7. [信号执行流程](#7-信号执行流程)
8. [安全机制](#8-安全机制)
9. [常见场景](#9-常见场景)
10. [故障排查](#10-故障排查)

---

## 1. 概述

系统通过 `backend/trading.yaml` 配置文件管理两个核心功能：

- **Portfolio 选择** — 切换预定义策略组合（A/B）或自定义策略，无需改代码
- **多账户自动下单** — 配置多个 Binance API 账户，每个账户可运行不同策略子集

**向后兼容**：不创建 `trading.yaml` 文件，系统行为与之前完全一致（使用 Portfolio B，仅生成信号，不自动下单）。

### 文件位置

```
backend/
├── trading.yaml           # 你的配置（.gitignore 已排除，不会提交）
├── trading.yaml.example   # 示例模板（提交到 git）
├── .env                   # API 密钥存放处
└── app/
    ├── trading_config.py      # 配置加载逻辑
    └── services/
        └── account_manager.py # 多账户管理
```

---

## 2. 快速开始

### 2.1 仅切换 Portfolio（不自动下单）

```bash
# 复制示例文件
cp backend/trading.yaml.example backend/trading.yaml

# 编辑，只需改一行
```

```yaml
# backend/trading.yaml
portfolio: "A"
```

启动系统即可。日志会显示：

```
Loaded trading config: portfolio=A, 4 strategies, 0 accounts (0 auto-trade)
```

### 2.2 启用自动下单

1. 在 `.env` 中添加 API 密钥：

```bash
# backend/.env
BINANCE_API_KEY_1=your_real_api_key
BINANCE_API_SECRET_1=your_real_api_secret
```

2. 在 `trading.yaml` 中配置账户：

```yaml
# backend/trading.yaml
portfolio: "B"

accounts:
  - name: "my-account"
    api_key_env: "BINANCE_API_KEY_1"
    api_secret_env: "BINANCE_API_SECRET_1"
    testnet: false
    enabled: true
    auto_trade: true
    leverage: 5
```

启动后日志：

```
Loaded trading config: portfolio=B, 5 strategies, 1 accounts (1 auto-trade)
Account 'my-account' connected (testnet=False, strategies=ALL)
Account manager started: 1 account(s) active
```

---

## 3. 配置文件详解

### 3.1 完整配置结构

```yaml
# Portfolio 选择（必填）
portfolio: "B"          # "A" | "B" | "custom"

# 自定义策略列表（仅 portfolio="custom" 时使用）
strategies:
  - symbol: XRPUSDT
    timeframe: 30m
    enabled: true
    streak_lo: 0
    streak_hi: 3
    atr_pct_threshold: 0.60
    position_qty: 50000
    max_consecutive_loss_months: 3

# 交易账户列表（可选）
accounts:
  - name: "account-name"
    api_key_env: "ENV_VAR_NAME"
    api_secret_env: "ENV_VAR_NAME"
    testnet: true
    enabled: true
    auto_trade: false
    leverage: 5
    strategies: []
```

### 3.2 字段默认值

如果不写某个字段，系统使用以下默认值：

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `portfolio` | `"B"` | 使用 Portfolio B |
| `strategies` | `[]` | 空列表 |
| `accounts` | `[]` | 无交易账户 |

---

## 4. Portfolio 选择

系统内置两个经过回测验证的策略组合：

### 4.1 Portfolio A — 低回撤组合

```yaml
portfolio: "A"
```

| 策略 | 币种 | 周期 | Streak 范围 | ATR 阈值 | 仓位 |
|------|------|------|-------------|----------|------|
| 1 | XRPUSDT | 30m | [0, 3] | 0.60 | 50,000 XRP |
| 2 | SOLUSDT | 5m | [0, 3] | 0.80 | 500 SOL |
| 3 | BTCUSDT | 15m | [0, 7] | 0.90 | 1 BTC |
| 4 | ETHUSDT | 30m | [0, 4] | 0.90 | 10 ETH |

**特点**：Calmar 3.63, Sortino 3.74, 最大回撤 $18K

### 4.2 Portfolio B — 推荐组合

```yaml
portfolio: "B"
```

| 策略 | 币种 | 周期 | Streak 范围 | ATR 阈值 | 仓位 |
|------|------|------|-------------|----------|------|
| 1 | XRPUSDT | 30m | [0, 3] | 0.60 | 50,000 XRP |
| 2 | XRPUSDT | 15m | [0, 4] | 0.80 | 50,000 XRP |
| 3 | SOLUSDT | 5m | [0, 3] | 0.80 | 500 SOL |
| 4 | BTCUSDT | 15m | [0, 7] | 0.90 | 1 BTC |
| 5 | BTCUSDT | 5m | [0, 3] | 0.90 | 1 BTC |

**特点**：Sharpe 1.43, Calmar 2.90, 策略间平均相关性 0.052

### 4.3 选择建议

- **保守型** → Portfolio A（4 策略，更低回撤）
- **均衡型** → Portfolio B（5 策略，更高 Sharpe，推荐）
- **定制型** → `custom`（自行定义策略参数）

---

## 5. 自定义策略

当 `portfolio: "custom"` 时，必须提供 `strategies` 列表：

```yaml
portfolio: "custom"

strategies:
  - symbol: XRPUSDT
    timeframe: 30m
    enabled: true             # false 可临时禁用该策略
    streak_lo: 0              # 接受信号的最小连胜/连败值
    streak_hi: 3              # 接受信号的最大连胜/连败值
    atr_pct_threshold: 0.60   # ATR 百分位阈值（0.60 = 仅 top 40% 波动率）
    position_qty: 50000       # 每次下单数量（资产单位，如 50000 XRP）
    max_consecutive_loss_months: 3  # 连续亏损月数达到此值后停止策略

  - symbol: BTCUSDT
    timeframe: 15m
    streak_lo: 0
    streak_hi: 7
    atr_pct_threshold: 0.90
    position_qty: 0.5         # 0.5 BTC per trade
```

### 5.1 策略参数说明

#### `symbol`（必填）

Binance 合约交易对名称，如 `BTCUSDT`、`ETHUSDT`、`SOLUSDT`、`XRPUSDT`、`BNBUSDT`。

必须是系统 `.env` 中 `SYMBOLS` 列表里的币种，否则无法收到行情数据。

#### `timeframe`（必填）

K 线周期。支持的值：`3m`、`5m`、`15m`、`30m`。

系统只订阅 1m WebSocket，然后本地聚合生成更高周期。1m 本身不产生 MSR 信号。

#### `enabled`（默认 `true`）

设为 `false` 可临时禁用某策略，无需删除整段配置。

#### `streak_lo` / `streak_hi`（默认 -999 / 999）

连胜/连败过滤器。仅当信号产生时的 `streak_at_signal` 值在 `[streak_lo, streak_hi]` 范围内时，信号才会被接受。

- 正值 = 连胜，负值 = 连败，0 = 无连续记录
- 默认 [-999, 999] 表示不过滤
- 典型设置 [0, 3] 表示只接受刚开始连胜的信号

#### `atr_pct_threshold`（默认 `0.0`）

ATR 百分位阈值。仅当当前 ATR 在历史分布中的百分位 > 此阈值时，信号才会被接受。

- `0.0` = 不过滤（接受所有波动率条件）
- `0.60` = 仅接受 top 40% 波动率时段的信号
- `0.90` = 仅接受 top 10% 波动率时段的信号

需要至少 200 个 ATR 历史样本才能进行百分位计算，不足时信号会被拒绝。

#### `position_qty`（默认 `0.0`）

每次交易的下单数量，以资产单位计：

- XRP: `50000`（50,000 XRP）
- SOL: `500`（500 SOL）
- BTC: `1`（1 BTC）
- ETH: `10`（10 ETH）

设为 `0` 时，即使账户匹配也不会执行自动下单。

#### `max_consecutive_loss_months`（默认 `3`）

连续亏损月数阈值。达到后策略自动暂停（kill switch）。

---

## 6. 多账户自动下单

### 6.1 架构设计

```
信号生成管线（共享）
       │
       ▼
  SignalGenerator
       │ on_new_signal()
       ▼
  AccountManager
       │ execute_signal()
       ├──→ Account 1 (OrderService) ──→ Binance API
       ├──→ Account 2 (OrderService) ──→ Binance API
       └──→ Account N (OrderService) ──→ Binance API
```

- 信号生成只有一条管线（一个 WebSocket 连接，一个 SignalGenerator）
- 生成的信号通过 AccountManager 分发到匹配的账户
- 每个账户有独立的 OrderService 实例和 API 连接

### 6.2 账户配置

```yaml
accounts:
  - name: "my-account"             # 账户标识（日志中显示）
    api_key_env: "BINANCE_API_KEY_1"    # .env 中的变量名
    api_secret_env: "BINANCE_API_SECRET_1"
    testnet: true                  # true=测试网, false=正式网
    enabled: true                  # false 可临时禁用
    auto_trade: true               # false=不自动下单
    leverage: 5                    # 杠杆倍数
    strategies: []                 # 空=全部策略
```

### 6.3 账户字段说明

#### `name`（必填）

账户名称，用于日志标识。建议使用有意义的名字如 `"xrp-main"`, `"btc-hedge"`。

#### `api_key_env` / `api_secret_env`

指向 `.env` 文件中环境变量的**名称**（不是密钥本身）。

```
# .env
BINANCE_API_KEY_1=abc123...
BINANCE_API_SECRET_1=xyz789...
```

```yaml
# trading.yaml
api_key_env: "BINANCE_API_KEY_1"      # 引用 .env 中的变量名
api_secret_env: "BINANCE_API_SECRET_1"
```

这样设计是为了安全：`trading.yaml` 不直接包含密钥，即使误提交也不会泄露。

如果指定的环境变量不存在或为空，该账户会被跳过并记录警告日志。

#### `testnet`（默认 `true`）

- `true` — 连接 Binance 测试网（testnet.binancefuture.com），用于测试
- `false` — 连接 Binance 正式网，真实交易

**强烈建议**：首次使用先设为 `true`，确认信号匹配和下单逻辑正确后再切换。

#### `enabled`（默认 `true`）

设为 `false` 可快速停用账户，保留配置不删除。

#### `auto_trade`（默认 `false`）

核心安全开关：

- `false` — 账户不参与自动下单（即使 enabled=true）
- `true` — 收到匹配信号时自动执行交易

账户必须同时满足 `enabled: true` 且 `auto_trade: true` 才会被激活。

#### `leverage`（默认 `5`）

每次下单前自动设置的杠杆倍数。不同账户可以使用不同杠杆。

#### `strategies`（默认 `[]`）

该账户运行的策略子集，格式为 `"SYMBOL_TIMEFRAME"` 列表：

```yaml
# 只交易 XRP 30m 和 XRP 15m
strategies: ["XRPUSDT_30m", "XRPUSDT_15m"]

# 空列表 = 交易 Portfolio 中的所有策略
strategies: []
```

这使得一个账户可以只运行特定的策略组合，实现风险隔离。

---

## 7. 信号执行流程

当一个新信号产生时，系统按以下顺序执行：

```
1. SignalGenerator 检测到信号
   ↓
2. 信号过滤（streak + ATR percentile）
   ↓ 通过
3. 保存信号到数据库
   ↓
4. on_new_signal() 回调触发
   ├── 4a. 添加到 PositionTracker（监控 TP/SL）
   ├── 4b. WebSocket 广播到前端
   └── 4c. AccountManager.execute_signal()
        ├── 查找 position_qty（来自 filter config）
        ├── 匹配账户（按 strategies 列表）
        └── 对每个匹配账户：
            ├── set_leverage()
            └── execute_signal()（市价开仓 + 止损/止盈挂单）
```

### 下单内容

`OrderService.execute_signal()` 执行以下操作：

1. 市价开仓（MARKET order）
2. 止损挂单（STOP_MARKET order）
3. 止盈挂单（TAKE_PROFIT_MARKET order）

方向、价格、止损/止盈全部来自信号本身，数量来自配置的 `position_qty`。

---

## 8. 安全机制

### 8.1 密钥安全

- API 密钥只存在 `.env` 文件中
- `trading.yaml` 只存储环境变量**名称**，不存储密钥
- `.env` 和 `trading.yaml` 都在 `.gitignore` 中，不会被提交

### 8.2 多重安全开关

启用自动下单需要同时满足：

1. `trading.yaml` 文件存在
2. `accounts` 段已配置
3. 账户 `enabled: true`
4. 账户 `auto_trade: true`
5. 环境变量中的 API 密钥有效
6. `position_qty > 0`

任何一个条件不满足，该账户都不会执行交易。

### 8.3 Testnet 先行

所有账户默认 `testnet: true`，必须显式设为 `false` 才会使用正式网。

### 8.4 异常隔离

- 单个账户下单失败不影响其他账户
- 下单异常不影响信号生成和 WebSocket 广播
- 启动时某账户连接失败，其余账户仍正常工作

---

## 9. 常见场景

### 场景 1：纯信号模式（不下单）

```yaml
portfolio: "B"
# 不配置 accounts 段
```

或者完全不创建 `trading.yaml`。系统仅生成信号并通过 WebSocket 推送。

### 场景 2：单账户跑全部策略

```yaml
portfolio: "B"

accounts:
  - name: "main"
    api_key_env: "BINANCE_API_KEY_1"
    api_secret_env: "BINANCE_API_SECRET_1"
    testnet: false
    auto_trade: true
    leverage: 5
    strategies: []  # 空 = 全部
```

### 场景 3：一个币种一个账户

```yaml
portfolio: "B"

accounts:
  - name: "xrp-account"
    api_key_env: "BINANCE_API_KEY_XRP"
    api_secret_env: "BINANCE_API_SECRET_XRP"
    testnet: false
    auto_trade: true
    leverage: 5
    strategies: ["XRPUSDT_30m", "XRPUSDT_15m"]

  - name: "sol-account"
    api_key_env: "BINANCE_API_KEY_SOL"
    api_secret_env: "BINANCE_API_SECRET_SOL"
    testnet: false
    auto_trade: true
    leverage: 5
    strategies: ["SOLUSDT_5m"]

  - name: "btc-account"
    api_key_env: "BINANCE_API_KEY_BTC"
    api_secret_env: "BINANCE_API_SECRET_BTC"
    testnet: false
    auto_trade: true
    leverage: 10
    strategies: ["BTCUSDT_15m", "BTCUSDT_5m"]
```

对应 `.env`：

```bash
BINANCE_API_KEY_XRP=...
BINANCE_API_SECRET_XRP=...
BINANCE_API_KEY_SOL=...
BINANCE_API_SECRET_SOL=...
BINANCE_API_KEY_BTC=...
BINANCE_API_SECRET_BTC=...
```

### 场景 4：Testnet 测试 + 正式网交易并行

```yaml
portfolio: "B"

accounts:
  # 测试账户：全部策略，testnet
  - name: "test"
    api_key_env: "BINANCE_TESTNET_KEY"
    api_secret_env: "BINANCE_TESTNET_SECRET"
    testnet: true
    auto_trade: true
    leverage: 5
    strategies: []

  # 正式账户：仅 XRP 30m
  - name: "live-xrp"
    api_key_env: "BINANCE_LIVE_KEY"
    api_secret_env: "BINANCE_LIVE_SECRET"
    testnet: false
    auto_trade: true
    leverage: 5
    strategies: ["XRPUSDT_30m"]
```

### 场景 5：自定义策略 + 修改参数

```yaml
portfolio: "custom"

strategies:
  # 只跑一个策略，放宽过滤条件
  - symbol: XRPUSDT
    timeframe: 30m
    streak_lo: -2       # 接受小幅连败
    streak_hi: 5        # 接受更长连胜
    atr_pct_threshold: 0.40  # 降低波动率门槛
    position_qty: 100000     # 加大仓位

accounts:
  - name: "aggressive"
    api_key_env: "BINANCE_API_KEY_1"
    api_secret_env: "BINANCE_API_SECRET_1"
    testnet: false
    auto_trade: true
    leverage: 10
```

### 场景 6：临时停用某账户

```yaml
accounts:
  - name: "active-account"
    # ...
    enabled: true
    auto_trade: true

  - name: "paused-account"
    # ...
    enabled: true
    auto_trade: false    # 改为 false 即可停止下单
```

---

## 10. 故障排查

### 10.1 日志确认

启动时检查日志中的关键信息：

```
# 配置加载成功
Loaded trading config: portfolio=B, 5 strategies, 2 accounts (1 auto-trade)

# 账户连接成功
Account 'my-account' connected (testnet=False, strategies=ALL)

# AccountManager 启动
Account manager started: 1 account(s) active
```

### 10.2 常见问题

#### 启动后显示 "No trading.yaml found, using defaults"

正常行为。如果你希望使用配置文件，确保 `trading.yaml` 位于 `backend/` 目录下。

#### 账户被跳过："env vars not set"

```
Account 'my-account': env vars not set (BINANCE_API_KEY_1, BINANCE_API_SECRET_1), skipping
```

检查 `.env` 文件中是否定义了对应的环境变量，且变量名与 `trading.yaml` 中的 `api_key_env`/`api_secret_env` 一致。

#### 信号产生但不下单："No position_qty for XXX"

```
No position_qty for XRPUSDT_30m, skipping auto-trade
```

对应策略的 `position_qty` 为 0。在 Portfolio A/B 中已内置仓位大小，但 custom portfolio 需要手动指定。

#### 账户连接失败

```
Account 'my-account' failed to connect: ...
```

检查 API 密钥是否正确、是否启用了合约交易权限、网络是否可达。其他账户不受影响。

#### YAML 语法错误

系统启动会报错并终止。使用 YAML 校验工具检查语法。常见错误：

- 缩进不一致（必须用空格，不能用 Tab）
- 字符串值含特殊字符时未加引号
- 列表项缺少 `-` 前缀

#### 无效的 portfolio 值

```
ValueError: portfolio must be one of ('A', 'B', 'custom'), got 'C'
```

`portfolio` 只接受 `"A"`、`"B"` 或 `"custom"`。

#### custom 模式没有 strategies

```
ValueError: portfolio='custom' requires at least one entry in 'strategies'
```

使用 `portfolio: "custom"` 时必须提供至少一个策略。

### 10.3 验证配置文件

可以用 Python 脚本快速验证配置是否正确：

```bash
cd backend
python -c "
from app.trading_config import load_trading_config
config = load_trading_config()
print(f'Portfolio: {config.portfolio}')
print(f'Strategies: {len(config.get_signal_filters())}')
for f in config.get_signal_filters():
    print(f'  {f.key}: streak=[{f.streak_lo},{f.streak_hi}] atr>{f.atr_pct_threshold} qty={f.position_qty}')
print(f'Accounts: {len(config.accounts)} ({len(config.get_enabled_accounts())} auto-trade)')
for a in config.accounts:
    key_ok = '***' if a.api_key else 'MISSING'
    print(f'  {a.name}: enabled={a.enabled} auto_trade={a.auto_trade} testnet={a.testnet} key={key_ok}')
"
```
