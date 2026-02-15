# v5-trading-bot

V5 横截面趋势轮动系统（OKX 现货），**先 dry-run**。

本仓库包含：
- 信号流水线（Alpha → Regime → Portfolio → Risk → Orders）
- 执行层：dry-run（模拟成交）/ live（OKX 私有接口：下单/查单/撤单）
- SQLite 落盘：Positions/Account/Orders/Fills（幂等可追溯）
- 回测 + walk-forward 框架
- 成本校准与回灌（F2）
- 日级预算监控 + 预算驱动的换手抑制（F3）
- 市场微观结构快照：bid/ask/mid/spread（F1.2）

## 快速开始

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# dry-run（默认使用 MockProvider）
python3 main.py

# 运行测试
pytest -q
```

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

回测支持 **calibrated** 成本模型（来自日级统计）：
- 统计文件：`reports/cost_stats/daily_cost_stats_YYYYMMDD.json`
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

## 约束 / 备注

- v5 phase-1：不做做空
- 不加杠杆
- 实盘（live）需要：
  - `execution.mode: live`
  - 环境变量 ARM：`V5_LIVE_ARM=YES`
  - OKX API key 完整（key/secret/passphrase）
- 对账门控（G1）尚在推进中：当前 live 侧会读取 `reports/kill_switch.json` / `reports/reconcile_status.json` 来决定是否进入 SELL_ONLY。
