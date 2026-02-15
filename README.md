# v5-trading-bot

中文说明见：**README.zh-CN.md**

V5 cross-sectional trend rotation system (OKX spot), **dry-run first**.

This repo includes:
- Signal pipeline (Alpha → Regime → Portfolio → Risk → Orders)
- Dry-run execution + persistent stores
- Backtest + walk-forward harness
- Cost calibration plumbing (F2)
- Daily budget monitoring + budget-driven turnover suppression (F3)

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# dry-run (default uses MockProvider)
python3 main.py

# run tests
pytest -q
```

### Use OKX public market data (optional)

```bash
export V5_DATA_PROVIDER=okx
python3 main.py
```

## Runtime outputs (reports/)

After `python3 main.py` (latest run + per-run artifacts):
- `reports/alpha_snapshot.json`
- `reports/regime.json`
- `reports/portfolio.json`
- `reports/execution_report.json`
- `reports/slippage.sqlite` (dry-run placeholder records)

Per-run directory (recommended for inspection):
- `reports/runs/<run_id>/decision_audit.json` (why 0 orders / why rejected)
- `reports/runs/<run_id>/summary.json` (metrics + budget tag)
- `reports/runs/<run_id>/trades.csv`
- `reports/runs/<run_id>/equity.jsonl`

## F2: Backtest cost model calibration

Backtest supports a **calibrated** cost model sourced from daily stats:
- stats: `reports/cost_stats/daily_cost_stats_YYYYMMDD.json`
- fallback tracking: each fill records a fallback level; backtest result aggregates `fallback_level_counts`

Key outputs:
- `reports/walk_forward.json` (schema_version=2)
  - top-level: `cost_assumption_meta`, `cost_assumption_aggregate.fallback_level_counts`
  - per-fold: `cost_assumption` + `result.cost_assumption`

Run walk-forward:
```bash
python3 scripts/run_walk_forward.py
# writes: reports/walk_forward.json
```

## F3: Daily budget monitoring + actions

V5 maintains a UTC daily budget state and tags each run:
- state: `reports/budget_state/YYYYMMDD.json`
- tag: `reports/runs/<run_id>/summary.json` → `budget{...}`
- tag: `reports/runs/<run_id>/decision_audit.json` → `budget{...}` + `budget_action{...}`

Actions (only when `budget.exceeded==true`):
- Stage-1 (F3.1): widen deadband (no-trade region)
- Stage-2 (F3.2): if fills are sufficient and trades are dominated by small-notional noise, raise `min_trade_notional`

All triggers + effective thresholds are written into `decision_audit.json` under `budget_action`.

## Compare v4 vs v5

Hourly compare is written to `reports/compare/hourly/compare_YYYYMMDD_HH.md`.
The header includes **deadband + budget control-state** so you can tell (on the first screen) whether budget tightening suppressed orders.

Run compare manually:
```bash
python3 scripts/compare_runs.py \
  --v4_reports_dir /home/admin/clawd/v4-trading-bot/reports \
  --v5_summary reports/runs/<run_id>/summary.json \
  --out /tmp/compare.md
```

## Notes / constraints

- No shorting in v5 phase-1.
- No leverage.
- Execution engine is dry-run scaffold; live execution + reconciliation gates are planned for the next phase.
