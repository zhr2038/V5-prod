# V5 量化交易系统

> 一个面向 OKX 现货市场的实盘量化交易系统。V5 以“信号质量优先、风险控制优先、可观测性优先”为设计原则，围绕 Alpha 因子、多策略融合、市场状态识别、自动风控、实盘执行、订单审计和诊断打包构建。

---

## 目录

- [项目定位](#项目定位)
- [核心特性](#核心特性)
- [系统架构](#系统架构)
- [策略与信号](#策略与信号)
- [风险控制](#风险控制)
- [执行与订单管理](#执行与订单管理)
- [诊断与可观测性](#诊断与可观测性)
- [Web Dashboard](#web-dashboard)
- [目录结构](#目录结构)
- [环境准备](#环境准备)
- [配置说明](#配置说明)
- [运行方式](#运行方式)
- [测试与验证](#测试与验证)
- [常见问题](#常见问题)
- [安全说明](#安全说明)
- [免责声明](#免责声明)

---

## 项目定位

V5 是一个以数字资产现货交易为目标的量化交易系统，当前主要用于白名单主流币种的实盘策略验证和自动化执行。

当前生产配置采用小范围白名单 universe，聚焦：

```text
BTC/USDT
ETH/USDT
SOL/USDT
BNB/USDT
```

系统设计重点不是高频交易，也不是无限扩展币池，而是在实盘环境中解决以下问题：

- 信号是否有足够 alpha；
- 成本、滑点和手续费是否吃掉收益；
- 弱信号是否被风控及时拦截；
- 已有浮盈是否能被保护；
- 低质量追涨是否能被避免；
- 每一次“为什么买 / 为什么不买 / 为什么卖 / 为什么没卖”是否可复盘；
- 实盘状态、订单、成交、账务和诊断数据是否能离线打包分析。

---

## 核心特性

### 1. 白名单实盘 universe

生产环境使用显式白名单，避免动态 universe 把低流动性、小币种或高噪声标的带入实盘路径。

```yaml
symbols:
  - BTC/USDT
  - ETH/USDT
  - SOL/USDT
  - BNB/USDT

universe:
  enabled: false
  use_universe_symbols: false
```

这意味着生产策略默认只在明确配置的主流交易对上工作。

---

### 2. 多策略信号融合

系统支持多类信号：

- Alpha6 多因子信号；
- TrendFollowing 趋势跟踪信号；
- MeanReversion 均值回归信号；
- 可选 ML overlay；
- 可选 Alpha158 风格 overlay；
- 市场脉冲 probe；
- BTC leadership breakout probe；
- ALT impulse shadow evaluator。

当前生产侧更重视 Alpha6 的确认作用。TrendFollowing 可以参与排序和观察，但在 PROTECT 风险档位下，通常不能单独触发实盘买入。

---

### 3. PROTECT 风险档位下的强 gate

当账户处于较大回撤或保护状态时，系统会进入 `PROTECT` 档位。

在 PROTECT 下，普通多头开仓需要满足更严格条件：

- 需要 Alpha6Factor 同向 buy；
- 不允许 TrendFollowing-only 直接买入；
- Alpha6 score 需要达到阈值；
- RSI 趋势确认需要达标；
- 成交量确认需要达标；
- 信号需要经过多轮确认或强单轮信号确认；
- 成本感知 gate 需要确认收益空间足够覆盖交易成本。

该机制用于防止系统在弱趋势、震荡或假突破中频繁追高。

---

### 4. 成本感知开仓

系统不会只看分数高低，还会估算 round-trip 交易成本。

典型生产参数：

```yaml
execution:
  fee_bps: 10
  slippage_bps: 5
  cost_aware_entry_enabled: true
  cost_aware_roundtrip_cost_bps: 30
```

如果信号分数不足以覆盖真实双边成本，系统会跳过开仓。

---

### 5. Negative Expectancy 自动冷却

系统持续统计各 symbol 的闭环交易表现。如果某个 symbol 在近期真实成交中呈现负期望，会触发：

- score penalty；
- open block；
- fast-fail open block；
- cooldown；
- market impulse 条件下的有限 softening。

该机制用于防止系统在同一标的上连续试错。

---

### 6. Probe 小仓试探机制

V5 不会轻易放松普通交易 gate。对于特殊行情，使用小仓 probe 机制做受控试探。

当前主要 probe 类型：

```text
market_impulse_probe
btc_leadership_probe
```

#### market_impulse_probe

用于识别 BTC 带动、多个白名单币同时出现趋势买入的 broad market impulse。

特点：

- 仅小仓；
- 支持动态 sizing，确保不低于交易所最小成交额；
- 可绕过单次 fast-fail，但不能绕过 active cooldown；
- 使用专用 probe exit policy；
- 不应被普通 zero-target rebalance 过早平掉。

#### btc_leadership_probe

用于识别 BTC 率先突破时的 BTC-only 小仓试探。

特点：

- 只针对 BTC；
- 需要突破 rolling high；
- 需要 Alpha6 / f4 / f5 基本确认；
- 受 PROTECT、Risk-Off、cooldown 和 same-symbol re-entry guard 约束。

---

### 7. Profit Lock 浮盈保护

系统在 PROTECT 下支持普通仓位的浮盈保护：

- 达到一定净浮盈后，把 stop 抬到保本上方；
- 达到更高浮盈后启动 trailing；
- 在阻力区或行情回撤时尽量锁住已经获得的收益。

该机制用于解决“入场是对的，但盈利回吐”的问题。

---

### 8. Same-symbol Re-entry Guard

系统会记录同一 symbol 最近一次退出原因和价格，避免出现：

```text
刚刚 profit-lock 卖出 BTC
几小时内又在同一区间买回 BTC
随后止损
```

冷却期内允许突破例外，但必须满足明显突破上一次高点或 exit price 的条件。

---

### 9. Dust-aware 状态清理

小账户实盘常见问题是平仓后留下极小残仓 dust。V5 对 dust 做了专门处理：

- dust 不再触发 anti-chase add-size；
- dust 不再被当成有效 open position；
- dust-only 不反复生成无意义 close order；
- 平仓后清理 stale profit / stop / highest / probe state；
- 打包诊断中区分真实持仓和 dust residual。

---

## 系统架构

整体流程可以概括为：

```text
Market Data
   │
   ▼
Alpha Engine
   ├─ Alpha6 factors
   ├─ TrendFollowing
   ├─ MeanReversion
   ├─ ML overlay, optional
   └─ Alpha158 overlay, optional
   │
   ▼
Regime Engine
   ├─ Trending
   ├─ Sideways
   └─ Risk-Off
   │
   ▼
Portfolio Engine
   ├─ ranking
   ├─ target weights
   ├─ optimizer
   └─ TopK / dropout control
   │
   ▼
Risk & Gate Layer
   ├─ PROTECT entry gate
   ├─ cost-aware gate
   ├─ negative expectancy gate
   ├─ same-symbol re-entry guard
   ├─ whitelist guard
   └─ dust-aware state hygiene
   │
   ▼
Execution Router
   ├─ OPEN_LONG
   ├─ CLOSE_LONG
   ├─ rebalance
   ├─ probe order
   └─ exit order
   │
   ▼
Live Execution Engine
   ├─ OKX spot API
   ├─ order store
   ├─ fill store
   ├─ bills / ledger
   └─ reconcile / kill switch
   │
   ▼
Reports & Diagnostics
   ├─ decision_audit.json
   ├─ trades.csv
   ├─ equity.jsonl
   ├─ skipped candidate labels
   ├─ probe lifecycle audit
   ├─ high-score blocked targets
   └─ follow-up bundle
```

---

## 策略与信号

### Alpha6 因子

当前核心 Alpha6 因子包括：

```text
f1_mom_5d
f2_mom_20d
f3_vol_adj_ret
f4_volume_expansion
f5_rsi_trend_confirm
```

生产配置中，系统支持：

- 静态权重；
- regime 权重覆盖；
- dynamic IC weighting；
- regime + dynamic IC 组合；
- factor contribution audit。

### TrendFollowing

趋势跟踪策略使用 MA 和 ADX 逻辑，并对趋势做二次确认。它可以发现市场趋势，但在 PROTECT 下不能单独触发普通开仓。

### MeanReversion

均值回归策略用于识别 RSI 超买超卖、布林带偏离和成交量萎缩等反转场景。它在不同 regime 下可以有不同 allocation multiplier。

### ML Factor

系统支持 ML overlay，但生产配置要求 promotion 通过，且会根据在线表现自动 downweight 或 shadow。没有通过 promotion 的模型不应直接主导实盘。

### Alpha158 Overlay

系统保留 Alpha158 风格 overlay 配置，但生产中默认关闭：

```yaml
alpha158_overlay:
  enabled: false
```

---

## 风险控制

V5 的风险控制分多层。

### 1. AutoRisk

根据 drawdown 自动调整仓位数量和仓位大小。

典型规则：

```yaml
dd_0_5:    max_positions: 8, position_size_pct: 1.0
dd_5_10:   max_positions: 5, position_size_pct: 0.7
dd_10_15:  max_positions: 3, position_size_pct: 0.5
dd_15_plus: max_positions: 1, position_size_pct: 0.15
```

当系统处于较大回撤时，会自动进入更防守的交易状态。

### 2. Regime Risk-Off

当 regime 判定为 Risk-Off，生产配置中可以直接将目标仓位归零：

```yaml
pos_mult_risk_off: 0.0
```

### 3. Cost-aware Entry

如果预期 edge 不足以覆盖 round-trip cost，开仓会被跳过。

### 4. Negative Expectancy

对近期闭环交易持续亏损的标的自动降权、阻断或冷却。

### 5. Profit Lock

普通非 probe 仓位达到一定净浮盈后，自动提高止损或启动 trailing。

### 6. Probe Exit Policy

probe 仓位由专用出场策略管理：

```text
probe_take_profit
probe_stop_loss
probe_trailing_stop
probe_time_stop
```

### 7. Kill Switch / Reconcile / Ledger

实盘交易前需要通过：

- kill switch 检查；
- reconcile 检查；
- ledger 检查；
- live arm 检查；
- 账户配置检查；
- 借币 / 负债保护。

---

## 执行与订单管理

### 订单路径

V5 会把策略目标转化为订单意图：

```text
OPEN_LONG
CLOSE_LONG
REBALANCE
```

然后经由 live execution engine 提交到 OKX。

### 订单和成交存储

常见运行文件包括：

```text
reports/orders.sqlite
reports/fills.sqlite
reports/bills.sqlite
reports/positions.sqlite
reports/ledger_state.json
reports/reconcile_status.json
reports/kill_switch.json
```

### Dust 处理

对于低于最小成交额或系统定义 dust threshold 的极小残仓：

- 不视为有效持仓；
- 不触发 anti-chase；
- 不触发无意义 close；
- 在诊断中作为 dust residual 独立记录。

---

## 诊断与可观测性

V5 的一个重点是“每次交易和每次未交易都要能解释”。

### 1. Decision Audit

每个 run 会生成 `decision_audit.json`，包含：

- top scores；
- targets pre/post risk；
- router decisions；
- rejects / counts；
- PROTECT gate 状态；
- strategy signals；
- negative expectancy state；
- target execution explanation；
- market impulse / BTC leadership probe 信息。

### 2. 高分但未成交解释

系统会记录：

```text
target_execution_explain
high_score_blocked_targets.csv
```

用于回答：

```text
为什么 ETH 分数高但没买？
为什么 target 里有这个币，但 router 没下单？
```

典型原因：

```text
trend_only
no_alpha6_confirmation
alpha6_sell
alpha6_score_too_low
volume_confirm_negative
rsi_confirm_too_weak
cost_aware
negative_expectancy
risk_off
```

### 3. Skipped Candidate Labeler

被 gate 拦住的候选会记录下来，并在 4h / 8h / 12h / 24h 后补标签：

```text
如果当时买入，扣除 round-trip cost 后是否赚钱？
```

这让系统可以量化：

- gate 是拦对了；
- 还是错过了机会。

### 4. ALT Impulse Shadow

ETH/SOL/BNB 高分但因 PROTECT gate 被挡时，系统可以只做 shadow，不交易。

输出包括：

```text
alt_impulse_shadow_labels.jsonl
alt_impulse_shadow_outcomes.csv
alt_impulse_shadow_outcomes_by_symbol.csv
```

等 shadow 数据证明有效后，才考虑未来的 live probe。

### 5. Probe Lifecycle Audit

用于检查 probe：

- 是否触发；
- 是否成交；
- gross/net bps；
- 出场 reason；
- 是否使用 probe exit policy；
- 平仓后 state 是否清理；
- 是否有 dust residual。

### 6. Negative Expectancy Consistency

系统会检查：

```text
negative_expectancy_cooldown.json
vs
trades_roundtrips.csv
```

防止出现真实闭环赚钱，但 negative expectancy 错误判定为负的情况。

### 7. Config Runtime Consumption Audit

用于识别：

```text
配置里写了，但运行时代码没消费
```

例如 `split_orders` 这类潜在死配置。

---

## Web Dashboard

V5 内置 Web Dashboard，用于把实盘运行状态、账户、成交、评分、K 线、系统健康和诊断信息集中展示。它不是独立交易系统，而是运行在 V5 工作区之上的可视化与观测入口。

### 功能概览

Dashboard 的主要目标是让运维和策略分析人员快速回答：

```text
系统现在是否健康？
账户和 ledger 是否正常？
当前是否有持仓？
最近是否有成交？
哪些币分数最高？
哪些候选被 gate 拦住？
当前 regime 是什么？
风控是否处于 PROTECT / Risk-Off？
probe 是否触发？
ML / shadow / skipped label 是否有异常？
```

当前 Dashboard 覆盖：

- 账户总览；
- 交易历史；
- 币种评分；
- K 线图表；
- 系统状态；
- regime / vote history；
- HMM 概率和衍生投票；
- ML signal / shadow ML 面板；
- 持仓聚焦 K 线；
- Prometheus metrics；
- health / ready / liveness 检查；
- React SPA 或 legacy Jinja template 渲染模式。

### 工作区与渲染模式

Dashboard 会自动识别工作区：

```text
V5_WORKSPACE
脚本所在仓库目录
当前工作目录
```

它优先查找：

```text
web/templates/monitor_v2.html
```

并将工作区内的 `reports/`、`data/cache/`、`web/` 作为数据和前端资源来源。

Dashboard 支持两种渲染模式：

```text
template / jinja / legacy
react / spa / dist
```

可以通过环境变量切换：

```bash
export V5_DASHBOARD_RENDERER=template
# 或
export V5_DASHBOARD_RENDERER=react
```

React build 路径也可以通过环境变量指定：

```bash
export V5_DASHBOARD_DIST=/path/to/web/dist
```

如果没有指定，系统会尝试：

```text
web/dist
dist
frontend/dist
```

### Flask 后端

Dashboard 使用 Flask 提供 HTTP 服务。后端会注册模板目录和静态文件目录：

```text
web/templates
web/static
```

同时会给响应增加 no-cache header，避免移动端或浏览器缓存旧前端脚本，导致样式和图表不同步。

### API 与页面能力

Dashboard 页面和前端 JS 会读取多个 API 数据源，典型能力包括：

- `GET /`：主监控页面；
- `GET /metrics`：Prometheus 格式指标；
- `/health`：健康检查；
- `/ready`：ready check；
- `/liveness`：liveness check；
- `/api/...`：账户、持仓、评分、成交、K 线、ML、shadow、regime 等 JSON 数据接口；
- 静态资源：`/static/...`；
- React SPA fallback：非 API 路径可回退到 `index.html`。

Dashboard 的测试覆盖了主页面关键 DOM 元素，例如：

```text
update-time
health-content
vote-history
history-tooltip
position-kline-chart
position-kline-symbols
position-kline-timeframes
ml-impact-headline
ml-impact-content
```

这说明 Dashboard 不只是简单静态页面，而是围绕实盘监控场景构建的交互式监控前端。

### Prometheus 指标

Dashboard 暴露：

```text
/metrics
```

用于输出 Prometheus 文本格式指标。若 metrics exporter 正常，返回类似：

```text
v5_metrics_exporter_up 1
```

如果 exporter 导入或执行失败，会返回：

```text
v5_metrics_exporter_up 0
```

这可以被 Prometheus、Grafana 或其他监控系统采集。

### 健康检查

Dashboard 会尝试注册健康检查蓝图：

```text
/health
/ready
/liveness
```

这些接口用于区分：

- 服务是否存活；
- 是否可以对外提供服务；
- 是否满足部署或探针条件。

### 缓存策略

Dashboard 后端有两层缓存：

1. **请求内缓存**  
   同一个请求上下文中，多次调用同一个 API 函数会复用结果。

2. **路由级短 TTL 缓存**  
   例如 Dashboard 视图会根据 `view` 参数使用不同 TTL：

   ```text
   primary: 8 秒
   deferred: 20 秒
   full: 10 秒
   ```

这能减少 Dashboard 刷新时对 sqlite、reports、OKX public ticker 和系统状态文件的重复读取。

### 安全设计

Dashboard 有几项安全/稳健性设计：

- 错误响应会隐藏 traceback；
- 错误响应会避免泄露本地路径；
- API 未知路径不会被 SPA fallback 吞掉；
- 静态文件路径会防止 path traversal；
- 响应中启用 no-cache，减少旧资源缓存造成的误判；
- 敏感环境变量不应通过 Dashboard 暴露；
- Dashboard 只应部署在可信网络、VPN、内网或受控反向代理后。

### 建议部署方式

开发或本地查看可以直接运行：

```bash
python scripts/web_dashboard.py
```

生产建议使用 systemd、反向代理或内网访问控制。例如：

```text
v5-dashboard.service
nginx / caddy reverse proxy
VPN / Tailscale / WireGuard
basic auth 或内网白名单
```

建议不要将 Dashboard 裸露在公网，尤其是在系统连接真实交易所账户时。

### 运维检查清单

打开 Dashboard 后，建议优先检查：

```text
1. health / ready / liveness 是否正常；
2. kill_switch 是否 false；
3. reconcile_status 是否 ok；
4. ledger_status 是否 ok；
5. 当前是否有非 dust 持仓；
6. 最近 24h 是否有成交；
7. 最新 selected / router decision 是什么；
8. 当前是否处于 PROTECT / Risk-Off；
9. 当前 high-score blocked target 是否集中在某个 symbol；
10. probe 是否触发、是否成交、是否被 stop-loss / time-stop；
11. skipped label 是否正常补 4h / 8h / 12h / 24h；
12. negative expectancy 是否与 roundtrip summary 一致；
13. config runtime consumption audit 是否出现 configured_not_consumed。
```

### Dashboard 与 follow-up bundle 的关系

Dashboard 适合实时查看；follow-up bundle 适合离线分析。

两者的关系可以理解为：

```text
Dashboard = 当前状态和近期运行的在线视图
follow-up bundle = 可审计、可复盘、可交给专家分析的离线证据包
```

如果 Dashboard 和 follow-up bundle 结论不一致，应优先检查：

- 采样窗口是否一致；
- Dashboard 缓存是否过期；
- runtime config path 是否一致；
- reports path 是否一致；
- service 是否读取同一个工作区；
- 是否存在 stale state 或旧 run 混入。

---

## 目录结构

典型结构如下：

```text
.
├── main.py
├── configs/
│   ├── live_prod.yaml
│   ├── schema.py
│   └── ...
├── src/
│   ├── alpha/
│   │   └── alpha_engine.py
│   ├── core/
│   │   └── pipeline.py
│   ├── execution/
│   │   ├── live_execution_engine.py
│   │   ├── order_store.py
│   │   ├── fill_store.py
│   │   └── same_symbol_reentry_guard.py
│   ├── portfolio/
│   ├── regime/
│   ├── reporting/
│   │   ├── decision_audit.py
│   │   ├── skipped_candidate_tracker.py
│   │   └── alt_impulse_shadow.py
│   ├── research/
│   └── risk/
├── scripts/
│   ├── web_dashboard.py
│   ├── generate_v5_bundle_remote.sh
│   ├── health_check.py
│   ├── reconcile_with_retry.py
│   ├── fill_sync.py
│   └── ...
├── web/
│   ├── templates/
│   ├── static/
│   └── dist/        # 可选 React build 输出
├── tests/
│   ├── test_probe_exit_policy.py
│   ├── test_market_impulse_probe.py
│   ├── test_negative_expectancy_market_aware.py
│   ├── test_alt_impulse_shadow.py
│   └── ...
└── reports/
    ├── runs/
    ├── orders.sqlite
    ├── fills.sqlite
    ├── ledger_state.json
    ├── decision artifacts
    └── summaries
```

---

## 环境准备

### Python 依赖

项目使用 Python 生态，主要依赖包括：

```text
pydantic
python-dotenv
numpy
pandas
requests
httpx
pyyaml
scipy
ccxt
flask
waitress
scikit-learn
xgboost
paramiko
```

安装示例：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 配置说明

### 生产配置

当前生产配置文件：

```text
configs/live_prod.yaml
```

关键配置包括：

- 交易白名单；
- OKX exchange；
- Alpha 权重；
- regime；
- AutoRisk；
- PROTECT entry gate；
- market impulse probe；
- BTC leadership probe；
- probe exit policy；
- same-symbol re-entry guard；
- negative expectancy；
- diagnostics；
- backtest cost。

### 环境变量

敏感信息不应写入 GitHub。建议通过 `.env` 或系统环境变量注入：

```bash
EXCHANGE_API_KEY=...
EXCHANGE_API_SECRET=...
EXCHANGE_PASSPHRASE=...
V5_LIVE_ARM=YES
```

`.env` 必须加入 `.gitignore`，不得提交到仓库。

---

## 运行方式

### 本地检查

建议先运行测试：

```bash
pytest
```

或者只运行关键测试：

```bash
pytest tests/test_probe_exit_policy.py
pytest tests/test_market_impulse_probe.py
pytest tests/test_negative_expectancy_market_aware.py
pytest tests/test_alt_impulse_shadow.py
pytest tests/test_live_contract_guards.py
```

### 生产运行

生产运行应使用明确的配置文件和受控服务。典型生产配置为：

```text
configs/live_prod.yaml
```

实盘前必须确认：

```text
kill_switch = false
reconcile_status.ok = true
ledger_status.ok = true
V5_LIVE_ARM = YES
.env 未提交
账户无借币 / 负债风险
```

### Web Dashboard

可以通过 Flask Dashboard 查看账户、评分、K 线、系统状态和诊断信息：

```bash
python scripts/web_dashboard.py
```

常见环境变量：

```bash
export V5_WORKSPACE=/home/ubuntu/clawd/v5-prod
export V5_DASHBOARD_RENDERER=template   # 或 react
export V5_DASHBOARD_DIST=/home/ubuntu/clawd/v5-prod/web/dist
```

Prometheus 指标入口：

```text
/metrics
```

健康检查入口：

```text
/health
/ready
/liveness
```

生产环境建议放在内网、VPN 或反向代理鉴权之后，不建议直接暴露公网。

### 诊断打包

项目提供 follow-up bundle 打包脚本，用于把最近运行证据打包成可离线分析的压缩包：

```bash
bash scripts/generate_v5_bundle_remote.sh
```

输出通常包含：

```text
raw/state/
raw/recent_runs/
raw/logs/
raw/reports/
summaries/
README.md
manifest.json
commands.log
```

该包用于分析：

- 今天是否该交易；
- 是否有真实成交；
- 成交 gross/net bps；
- gate 是否拦对；
- probe 是否触发；
- dust 是否污染状态；
- negative expectancy 是否与 roundtrip 一致；
- 高分目标为什么没成交；
- shadow 样本是否支持未来优化。

---

## 测试与验证

建议在每次改动后至少跑：

```bash
pytest tests/test_probe_exit_policy.py
pytest tests/test_market_impulse_probe.py
pytest tests/test_protect_entry_gate.py
pytest tests/test_skipped_candidate_tracker.py
pytest tests/test_alt_impulse_shadow.py
pytest tests/test_backtest_cost_alignment.py
pytest tests/test_web_dashboard.py
```

如果改动 live execution 或风控逻辑，还应补充：

```bash
pytest tests/test_live_contract_guards.py
pytest tests/test_dust_aware_router.py
pytest tests/test_negative_expectancy_market_aware.py
```

---

## 常见问题

### Q1：为什么 ETH 分数很高，但系统没有买？

因为 `final_score` 高不等于最终可执行。  
在 PROTECT 下，普通买入需要 Alpha6 同向确认。如果 ETH 高分主要来自 TrendFollowing，而 Alpha6 没有 buy，或者 Alpha6 是 sell，系统会跳过。

常见原因：

```text
protect_entry_trend_only
protect_entry_no_alpha6_confirmation
protect_entry_alpha6_score_too_low
protect_entry_volume_confirm_negative
protect_entry_rsi_confirm_too_weak
```

这些都会写入 `target_execution_explain` 和 high-score blocked 诊断文件。

### Q2：为什么 BTC 分数不最高，系统却买了 BTC？

BTC 可能通过 `market_impulse_probe` 或 `btc_leadership_probe` 专用通道小仓买入。  
probe 是特殊行情下的小仓试探，不等同于普通 top-score 选币。

### Q3：为什么系统经常不交易？

在 PROTECT 下，系统宁可少交易，也不允许低质量信号反复试错。  
不交易可能是合理防守，也可能是 gate 过严。需要看：

```text
skipped_candidate_outcomes
high_score_blocked_outcomes
alt_impulse_shadow_outcomes
```

### Q4：系统是否支持动态 universe？

支持，但生产配置默认关闭动态 universe，并使用显式白名单。  
这是为了降低噪声、滑点、min notional、dust 和小币种不稳定性。

### Q5：ML 会直接参与实盘吗？

只有在 promotion 通过、模型不过期、在线控制允许的情况下，ML overlay 才能参与。否则应该 shadow 或 downweight。

### Q6：如何判断 gate 是否太严格？

看 skipped candidate label：

```text
4h / 8h / 12h / 24h net bps
win rate
by reason
by symbol
```

如果某类被挡样本长期净正，才考虑放松或新增 probe。  
如果长期净负，继续拦截是正确的。

---

## 安全说明

请不要在仓库中提交：

```text
.env
API key
API secret
passphrase
token
cookie
私钥
账户截图
未脱敏交易所响应
```

生产环境建议：

- 使用最小权限 API key；
- 关闭不必要权限；
- 禁止借币；
- 保留 kill switch；
- 每次 live 前检查 reconcile / ledger；
- 保持日志脱敏；
- 定期备份 reports / sqlite / state；
- 只通过受控脚本生成诊断包。

---

## 当前设计原则

V5 当前遵循以下原则：

```text
1. 不为交易而交易。
2. 先保护本金，再追求收益。
3. 普通仓要有 Alpha6 确认。
4. Trend-only 先 shadow，不直接 live。
5. Probe 只能小仓、可止损、可追踪、可复盘。
6. 有浮盈要保护。
7. 每次不交易也要能解释。
8. 配置必须被运行时代码消费，否则标记为 inactive。
9. 实盘成本必须优先于回测假设。
10. 所有策略改动必须由诊断数据支持。
```

---

## 免责声明

本项目仅用于量化交易系统研究、工程验证和个人实盘实验，不构成任何投资建议。

数字资产价格波动剧烈，自动化交易可能产生亏损。使用者应自行承担所有交易风险，包括但不限于：

- 市场风险；
- 策略失效；
- 滑点和手续费；
- API 故障；
- 网络延迟；
- 交易所异常；
- 配置错误；
- 状态文件污染；
- 程序 bug；
- 账户安全风险。

在任何实盘部署前，请务必使用小资金、dry-run、shadow、回测和严格风控逐步验证。

---

## License

请根据实际情况补充许可证。例如：

```text
MIT License
Apache-2.0
Private / All rights reserved
```

如果该仓库用于个人研究或生产系统，建议明确写明使用范围和责任边界。
