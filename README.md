# v5-trading-bot

V5 横截面趋势轮动系统（OKX 现货），**先 dry-run**。

本仓库包含：
- 信号流水线（Alpha → Regime → Portfolio → Risk → Orders）
- Dry-run 执行引擎 + 持久化存储（仓位/账户/成交日志）
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
- `reports/runs/<run_id>/trades.csv`：逐笔成交（dry-run fill）
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
- 执行引擎目前以 dry-run 为主；实盘执行与对账门控计划在后续阶段引入
