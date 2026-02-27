# v5-trading-bot

V5 横截面趋势轮动系统（OKX 现货），**先 dry-run**。

---

## 📋 最近更新 (2026-02-27)

### Web监控面板修复
- ✅ **持仓盈亏显示**：改用最新买入价格计算成本，与交易所APP一致
- ✅ **实时价格**：优先OKX API，缓存仅作为fallback（15分钟内有效）
- ✅ **持仓同步**：OKX API成功但返回空持仓时，不再回退到缓存数据
- ✅ **策略信号时间**：修复时间戳显示错误，使用文件修改时间

### 数据库修复
- ✅ **订单数量字段**：修复`sz`字段为空的问题，已校准123笔历史订单
- ✅ **成本计算**：新增FIFO成本计算方法（后续启用）

### 生产环境修复 (2026-02-26)
- ✅ **粉尘持仓过滤**：小于$1或0.01个的持仓自动过滤
- ✅ **退出加速**：收紧close-only死区，加快清理移除的持仓
- ✅ **小账户去杠杆**：20U模式放宽回撤限制
- ✅ **分阶段止盈**：新增`profit_taking.py`，支持保本/部分止盈/追踪保护/排名退出
- ✅ **重启连续性**：启动时自动注册现有持仓到止损/止盈管理器

---

本仓库包含：
- **信号流水线**（Alpha → Regime → Portfolio → Risk → Orders）
- **多策略并行系统**（趋势跟踪 + 均值回归 + 信号融合）
- **执行层**：dry-run（模拟成交）/ live（OKX 私有接口：下单/查单/撤单）
- **反思Agent**：自动交易后分析与优化建议
- SQLite 落盘：Positions/Account/Orders/Fills/Bills（幂等可追溯）
- 回测 + walk-forward 框架
- 成本校准与回灌（F2）
- 日级预算监控 + 预算驱动的换手抑制（F3）
- 市场微观结构快照：bid/ask/mid/spread（F1.2）

---

## 快速开始

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# dry-run（默认使用 MockProvider）
python3 main.py

# 运行测试
pytest -q
```

---

## 多策略并行系统（Multi-Strategy）

V5 支持同时运行多个策略，通过信号融合生成最终交易决策。

### 架构

```
┌─────────────────────────────────────────────────────────┐
│                   StrategyOrchestrator                   │
│  ┌──────────────────┐  ┌──────────────────┐            │
│  │ TrendFollowing   │  │ MeanReversion    │            │
│  │ (趋势跟踪)        │  │ (均值回归)        │            │
│  └────────┬─────────┘  └────────┬─────────┘            │
│           │                     │                       │
│           ▼                     ▼                       │
│      ┌──────────────────────────────────┐              │
│      │      Signal Fusion (信号融合)     │              │
│      │  - 同向信号加权                   │              │
│      │  - 反向信号冲突解决               │              │
│      └──────────────────────────────────┘              │
└─────────────────────────────────────────────────────────┘
```

### 内置策略

| 策略 | 类型 | 核心逻辑 |
|------|------|----------|
| TrendFollowing | 趋势跟踪 | 双均线交叉 + ADX确认 |
| MeanReversion | 均值回归 | RSI超买超卖 + 布林带 |

### 配置

```yaml
# configs/multi_strategy.yaml
strategy_allocations:
  TrendFollowing: 0.5    # 50% 资金
  MeanReversion: 0.3     # 30% 资金
  Momentum: 0.2          # 20% 资金 (预留)
```

### 演示

```bash
python3 scripts/multi_strategy_demo.py
```

---

## 反思Agent（Reflection Agent）

自动分析交易记录，识别问题并生成优化建议。

### 功能

- **数据加载**：自动从 SQLite 读取最近7天交易记录
- **绩效计算**：整体/币种/策略三级绩效指标
- **洞察识别**：6类交易洞察自动检测
- **建议生成**：按优先级排序的可执行建议

### 洞察类型

| 类型 | 说明 |
|------|------|
| STRONG_PERFORMER | 表现优秀 |
| UNDER_PERFORMER | 表现不佳 |
| FACTOR_DECAY | 因子失效 |
| RISK_CONCENTRATION | 风险集中 |
| OPPORTUNITY | 潜在机会 |
| ANOMALY | 异常检测 |

### 定时任务

```bash
# 启用（每天21:00自动运行）
systemctl --user enable v5-reflection-agent.timer
systemctl --user start v5-reflection-agent.timer

# 手动运行
python3 src/execution/reflection_agent.py
```

### 演示

```bash
python3 scripts/reflection_demo.py
```

---

## 执行模式（dry-run / live）

执行层通过 `cfg.execution.mode` 分流：
- `dry_run`：使用 `ExecutionEngine`（默认，安全，不会触发实盘下单）
- `live`：使用 `LiveExecutionEngine`（OKX 现货私有接口下单/查单/撤单）

### Live 最后一道保险（ARM）
即使配置写了 `mode: live`，也必须显式 arm 才会真的运行：

```bash
export V5_LIVE_ARM=YES
python3 main.py
```

如果未设置 ARM 环境变量，`main.py` 会直接拒绝启动 live（避免 timer/误配置触发实盘）。

### Live Preflight（上线建议：常开）
Live 每次执行前会先做一轮 **preflight catch-up**（不改变策略逻辑，只做运行自洽/可运维）：

1) `bills_sync`：追平最近 7 天账单流水（事实源）
2) `ledger_once`：用 bills 聚合推导 expected balance，并与 OKX balance 做闭环校验
3) `reconcile_guard`：交易所余额/仓位 vs 本地 Store 对账 + kill-switch guard
4) 输出明确结论：`ALLOW / SELL_ONLY / ABORT`

> 目的：避免“timer 还没刷新状态文件”导致 live 用旧的 ok=true 误放行。

#### 可选：Preflight Bootstrap Patch（受控状态对齐，不是财务真相）
成交后，交易所余额会先变化，本地 `AccountStore/PositionStore` 可能在下一轮 preflight 前尚未反映，导致 `reconcile_guard` 短暂变红（`base_mismatch/usdt_mismatch`）。

开启 `preflight_bootstrap_patch` 后：当 **ledger 已 ok** 且 reconcile 的失败原因属于 mismatch 时，preflight 会对本地做一次受控 patch，并再次 reconcile，做到“自动回绿”，减少人工 bootstrap。

关键原则：
- **账务真相以 `fills → bills → ledger` 为主**，bootstrap patch 仅用于状态对齐
- patch **只覆盖**：`cash(USDT)` + `positions.qty`
- patch **不覆盖**：`avg_px / pnl / strategy_state`
- 带安全阈值与最小间隔，避免抖动/限频

推荐上线默认值（仅 live 启用）：
```yaml
execution:
  # preflight
  preflight_enabled: true
  preflight_max_pages: 5
  max_status_age_sec: 180
  preflight_fail_action: sell_only   # sell_only|abort

  # controlled bootstrap patch (exchange -> local)
  preflight_bootstrap_patch_enabled: true
  preflight_bootstrap_patch_min_interval_sec: 300   # 5min, avoid thrash
  preflight_bootstrap_patch_max_total_usdt: 50.0    # refuse patch if estimated drift > 50U
```

运维手工入口（查看 preflight 细节 JSON）：
```bash
python3 scripts/live_preflight_once.py --max-pages 5 --max-status-age-sec 180
```

### OKX 私有接口自检（balance）
在写好 `.env`（api_key/api_secret/passphrase）后可运行：

```bash
python3 scripts/okx_private_selfcheck.py
```

### Fill 同步与 slippage（G0.3）
Live 模式下，执行引擎会在 `poll_open()` 里 best-effort 做 fills → orders 的状态推进，并把 fills 导出为 `trades.csv` / `cost_events`。

slippage 计算：
- 优先从 `reports/spread_snapshots/YYYYMMDD.jsonl` 找到该 symbol 在 fill 时间点之前最近一条 snapshot（mid/bid/ask）
- 找不到 snapshot 时：
  - `trades.csv` 的 `slippage_usdt` 写空值（表示 N/A）
  - `cost_events` 的 mid/bid/ask/slippage 字段保持 null

你也可以手动同步 fills 到本地 SQLite：

```bash
python3 scripts/fill_sync.py --db reports/fills.sqlite
```

## Bills 同步（G0.4）

Bills 是账本闭环的事实源（覆盖所有导致余额变化的事件，包含但不限于成交）。

手动同步 bills：
```bash
python3 scripts/bills_sync.py --db reports/bills.sqlite
```

## 运维：reconcile timer（G1.1）

仓库提供 systemd timer `v5-reconcile.timer`，用于定期刷新 `reports/reconcile_status.json`（默认每 5 分钟）。

安装（system-wide，需要 sudo）：
```bash
bash deploy/install_systemd.sh
```

安装（user-level，不需要 sudo）：
```bash
bash deploy/install_systemd.sh --user
```

注意：如果使用 **user-level timer** 且希望“用户不登录也运行”，需要开启 lingering：
```bash
sudo loginctl enable-linger admin
```

巡检：
```bash
systemctl list-timers --all | grep v5-reconcile
journalctl -u v5-reconcile.service -n 50 --no-pager

# 文件侧闭环（确认是否持续刷新）
ls -l --time-style=long-iso reports/reconcile_status.json
cat reports/reconcile_status.json
```

落盘文件：
- fills：`reports/fills.sqlite`
- orders：`reports/orders.sqlite`
- bills：`reports/bills.sqlite`

FillStore 去重规则：同一 `instId` 下同一 `tradeId` 只处理一次（主键 `(inst_id, trade_id)`）。

### OKX expTime
OKX 支持在交易接口请求头传 `expTime`（epoch 毫秒）。本项目配置项 `execution.okx_exp_time_ms` 若小于 1e12，会被当作“从现在起的 delta 毫秒”自动换算成 epoch 毫秒。

### 使用 OKX 公共行情数据（可选）

```bash
export V5_DATA_PROVIDER=okx
python3 main.py
```

## 运行输出（reports/）

执行 `python3 main.py` 后，会生成最新一轮输出以及按 run_id 分目录的产物。

## Alpha 研究工具（IC / forward returns / regime-aware weights）

### 1) IC 诊断（含按 Regime 分层）
输出：`reports/ic_diagnostics_30d_*.json`

```bash
python3 scripts/ic_diagnostics.py --lookback-days 30 --universe 20u
```

### 2) 修复/回填 alpha_history.db 的 fwd_ret_*（从 market_data_1h 重算）
当发现 `fwd_ret_col_distinct=1`（forward return 列被 0/占位污染）时，使用该脚本回填：

```bash
python3 scripts/update_forward_returns_from_market_data.py --lookback-days 30
```

### 3) 计算按 Regime 的动态权重
输出：`reports/alpha_dynamic_weights_by_regime.json`

```bash
python3 scripts/compute_dynamic_alpha_weights_by_regime.py --lookback-days 30 --horizon 1h
```

### 4) Shadow 模式（dry-run）验证 Regime-aware 权重（不影响实盘）
仓库提供 shadow config + user-level systemd timer 示例：
- `configs/live_20u_shadow_regime.yaml`
- `deploy/systemd/v5-shadow-regime.user.{service,timer}`

注意：shadow 会设置 `execution: dry_run`，并通过 `V5_DISABLE_TOPLEVEL_ARTIFACTS=1` 避免覆盖顶层快照文件。

### 顶层产物（概览）
- `reports/alpha_snapshot.json`
- `reports/regime.json`
- `reports/portfolio.json`
- `reports/execution_report.json`
- `reports/slippage.sqlite`（dry-run 的占位记录）

### 按次运行产物（建议重点看）
- `reports/runs/<run_id>/decision_audit.json`：解释“为什么 0 单 / 为什么被拒绝”
- `reports/runs/<run_id>/summary.json`：本次窗口指标汇总（并包含 budget 打标）
- `reports/runs/<run_id>/trades.csv`：逐笔成交（live 时来自真实 fills；slippage 若无 snapshot 会写空值）
- `reports/runs/<run_id>/equity.jsonl`：净值曲线点
- `reports/runs/<run_id>/spread_snapshot.json`：当小时 bid/ask/mid/spread_bps 快照（即使 0 单也会写）

## F2：回测成本模型校准/回灌

### 成本事件（cost_events）与日统计（cost_stats）
- 原始事件：`reports/cost_events/YYYYMMDD.jsonl`（NDJSON）
- 日统计：`reports/cost_stats_real/daily_cost_stats_YYYYMMDD.json`（定时任务自动生成）
- Web显示：`cost_stats_real` 实时数据，自动过滤异常值

Live fills 会导出为 `cost_events`，并尽量附带 micro-structure 与运行上下文：
- bid/ask/mid + spread_bps（优先 submit meta，其次 spread snapshot）
- slippage_bps / fee_bps / cost_bps_total
- **regime / deadband_pct / drift**（从 `reports/runs/<run_id>/decision_audit.json` 回填，用于分桶）

生成/重跑某天统计：
```bash
python3 scripts/rollup_costs.py --day YYYYMMDD --source okx_fill --check_anomaly --lookback_days 7
```

**异常值过滤**：Web面板自动跳过成本 > 1000 bps 或 < 0 的异常数据（避免PEPE等极端数据污染统计）

回测支持 **calibrated** 成本模型（来自日级统计）：
- 统计文件：`reports/cost_stats_real/daily_cost_stats_YYYYMMDD.json`
- 回退可追踪：每笔 fill 会记录 fallback level；回测结果会汇总 `fallback_level_counts`

关键输出：
- `reports/walk_forward.json`（schema_version=2）
  - 顶层：`cost_assumption_meta`、`cost_assumption_aggregate.fallback_level_counts`
  - 每个 fold：`cost_assumption` + `result.cost_assumption`

运行 walk-forward：
```bash
python3 scripts/run_walk_forward.py
# 输出：reports/walk_forward.json
```

## F3：日级预算监控 + 预算动作（控换手/控成本）

V5 会维护 UTC 日切的预算状态，并把预算信息写回每次运行的报告：
- 日级状态：`reports/budget_state/YYYYMMDD.json`
- summary 打标：`reports/runs/<run_id>/summary.json` → `budget{...}`
- audit 打标：`reports/runs/<run_id>/decision_audit.json` → `budget{...}` + `budget_action{...}`

当 `budget.exceeded == true` 时（且 `cfg.budget.action_enabled == true`），会触发预算动作：
- Stage-1（F3.1）：扩大 deadband（no-trade region），优先用“策略一致”的方式降低无效再平衡
- Stage-2（F3.2）：当成交样本足够且小额噪声单占比高时，提高 `min_trade_notional`，过滤极小额噪声交易

所有触发条件、有效阈值与抑制计数，都会写入 `decision_audit.json` 的 `budget_action` 字段，保证可追责。

## F1.2：Spread 快照（不依赖成交样本）

为了解决“0 单导致 fills 样本增长慢”的问题，V5 会在每次 hourly run 记录市场微观结构快照（top-of-book）：
- 日级 NDJSON：`reports/spread_snapshots/YYYYMMDD.jsonl`
- 每次运行副本：`reports/runs/<run_id>/spread_snapshot.json`

并提供日级 rollup（分位数统计）：
- `reports/spread_stats/daily_spread_stats_YYYYMMDD.json`

手动 rollup：
```bash
python3 scripts/rollup_spreads.py --day YYYYMMDD
```

（可选）systemd timer：`v5-spread-rollup.timer` 会在 **00:20 UTC** 自动 rollup 昨天数据。

## v4 vs v5 对比（compare）

小时级对比输出：`reports/compare/hourly/compare_YYYYMMDD_HH.md`。

对比文档顶部会包含 **deadband + budget 控制状态**，确保打开 md 第一屏就能判断：
- 今天是否超预算
- 是否因预算扩大 deadband
- 是否触发二段动作抬最小下单额

手动运行 compare：
```bash
python3 scripts/compare_runs.py \
  --v4_reports_dir /home/admin/clawd/v4-trading-bot/reports \
  --v5_summary reports/runs/<run_id>/summary.json \
  --out /tmp/compare.md
```

---

## 智能告警系统 (Smart Alert)

**只报异常，不报平安** - 异常时主动推送，无异常时静默。

### 告警规则

| 告警类型 | 触发条件 | 优先级 | 冷却期 |
|---------|---------|--------|--------|
| 有信号无成交 | 连续2轮 `selected>0 && rebalance=0` | 高 | 2小时 |
| 行情好却不买 | Sideways/Trending 6小时无买入 | 中 | 6小时 |
| 回撤超限 | 回撤 > 10% | 高 | 3小时 |
| IC因子失效 | IC < 0 持续12小时 | 中 | 12小时 |
| Kill Switch触发 | 任意时刻 | 紧急 | 30分钟 |

### 启用告警

```bash
# 启用（每30分钟检测一次）
systemctl --user enable v5-smart-alert.timer
systemctl --user start v5-smart-alert.timer

# 手动检查
python3 scripts/smart_alert_check.py
```

### 告警推送

告警通过 Telegram 推送（需配置 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID`）：
- 高优先级：⚠️ 有信号无成交、🔴 回撤超限
- 中优先级：📉 行情正常但无买入、📊 IC因子失效
- 紧急：🚨 Kill Switch触发

---

## ML 模型训练自动化

V5 内置 LightGBM 机器学习因子模型，支持自动训练与评估。

### 特征工程

- **20+特征**：动量、波动率、成交量、技术指标
- **自动特征重要性分析**
- **IC评估**：模型预测与实际收益的相关系数

### 定时训练

```bash
# 启用（每天00:30自动训练）
systemctl --user enable v5-daily-ml-training.timer
systemctl --user start v5-daily-ml-training.timer

# 手动训练
python3 scripts/daily_ml_training.py
```

### 训练流程

1. 导出最近交易数据（需100+条记录）
2. 训练 LightGBM 模型
3. 计算 IC（信息系数）
4. IC > 0.02 时保存模型
5. 生成特征重要性报告

---

## 回测系统

支持多版本回测策略验证。

### 快速回测

```bash
python3 scripts/quick_backtest.py
```

### 策略版本

| 版本 | 特点 | 适用场景 |
|------|------|----------|
| v1 | 基础回测 | 快速验证 |
| v2 | Risk-Off + 做空 | 完整测试 |
| v3 | 保守策略（仅做多，4h频率） | 实盘参考 |

### Walk-Forward 分析

```bash
python3 scripts/run_walk_forward.py
# 输出：reports/walk_forward.json
```

---

## Web 监控面板

实时交易监控与数据分析。

### 启动面板

```bash
# Flask 后端（端口5000）
python3 scripts/web_dashboard.py

# React 前端（开发模式，端口3000）
cd /home/admin/v5-trading-dashboard && npm run dev
```

### API 端点

- `/api/account` - 账户信息
- `/api/trades` - 交易记录
- `/api/scores` - 信号评分
- `/api/dashboard` - 综合数据
- `/api/decision_chain` - 决策归因分析
- `/api/shadow_test` - 参数A/B影子测试
- `/api/smart_alerts` - 智能告警状态
- `/api/ic_diagnostics` - IC因子诊断
- `/api/cost_calibration` - F2成本校准

### 决策归因面板

解答"为什么没买"的透明化分析工具：
- **策略层**：展示各策略信号强度与选中币种
- **风控层**：Regime状态与回撤降杠杆比例
- **执行层**：选中vs成交对比，拦截原因分析
- **阻塞归因**：deadband拦截、漂移值等详细数据

### 参数A/B影子测试

历史参数效果对比，避免盲目调参：
- 对比当前vs建议参数的历史表现
- 分析最近50轮（约7天）数据
- 统计选中率、再平衡率、成交率
- 智能建议："谨慎尝试"或"保持现状"

---

## 约束 / 备注

- v5 phase-1：不做做空
- 不加杠杆
- 实盘（live）需要：
  - `execution.mode: live`
  - 环境变量 ARM：`V5_LIVE_ARM=YES`
  - OKX API key 完整（key/secret/passphrase）
- 对账门控（G1）尚在推进中：当前 live 侧会读取 `reports/kill_switch.json` / `reports/reconcile_status.json` 来决定是否进入 SELL_ONLY。

---

## 多Agent架构

V5 采用行业标准的6-Agent量化架构：

| Agent | 模块 | 职责 |
|-------|------|------|
| 数据 | `DataFetcher` | 行情数据获取与缓存 |
| 策略 | `StrategyOrchestrator` | 多策略并行与信号融合 |
| 风控 | `RiskEngine` | Risk-Off检测与仓位控制 |
| 执行 | `ExecutionEngine` + `PositionBuilder` | 订单执行与分批建仓 |
| 学习 | `MLFactorModel` | LightGBM因子模型训练 |
| 监控 | `AuditEngine` + Dashboard | 实时交易监控 |
| 反思 | `ReflectionAgent` | 交易后分析与优化建议 |

### 通信机制

- **SQLite**：持久化存储（positions/orders/fills）
- **文件系统**：状态快照与报告
- **JSON配置**：策略参数与运行配置

---

## 系统定时任务

| Timer | 频率 | 功能 |
|-------|------|------|
| `v5-live-20u.user.timer` | 每小时 | 实盘交易执行 |
| `v5-reconcile.timer` | 每5分钟 | 对账状态刷新 |
| `v5-daily-ml-training.timer` | 每天00:30 | ML模型自动训练 |
| `v5-reflection-agent.timer` | 每天21:00 | 交易后分析 |
| `v5-smart-alert.timer` | 每30分钟 | 智能异常检测 |
| `v5-cost-rollup-real.timer` | 每天08:20 | 成本统计汇总 |

---

*V5 Trading Bot - 专业级量化交易系统*
