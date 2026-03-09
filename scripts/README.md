## Scripts Layout

This directory now keeps only scripts that are still relevant to the current repository.

### Ops: scheduled runtime

- `run_hourly_live_window.sh`
- `run_hourly_window.sh`
- `bills_sync.py`
- `ledger_once.py`
- `reconcile_guard_once.py`
- `rollup_last24h.py`
- `rollup_costs.py`
- `rollup_spreads.py`
- `health_check.py`
- `v5_status_report.py`
- `okx_private_selfcheck.py`
- `live_preflight_once.py`
- `orders_gc_once.py`
- `orders_repair_once.py`
- `compare_runs.py`

### Manual: operator-invoked recovery

- `emergency_close_all.py`
- `emergency_liquidate.py`
- `dust_cleaner.py`
- `fill_sync.py`
- `reconcile_once.py`
- `reconcile_with_retry.py`
- `sync_positions_from_okx.py`
- `direct_repay_guide.py`

### Ops: monitoring and support

- `trade_auditor.py`
- `trade_auditor_v2.py`
- `trade_auditor_v3.py`
- `trading_report.py`
- `smart_alert_check.py`
- `web_dashboard.py`
- `run_data_quality_check.py`
- `equity_anomaly_detector.py`
- `universe_guard.py`

### Non-production content

Archived scripts should live under local-only `scripts/archive/`:

- `scripts/archive/research/`: backtest, factor analysis, walk-forward, and model-training scripts
- `scripts/archive/devtools/`: one-off development helpers
- `scripts/archive/legacy/`: old 20u and deprecated launch scripts

Important:

- `scripts/archive/` is treated as a local archive area.
- it is ignored by GitHub according to the current `.gitignore`.
- the active GitHub repository should keep production and operator-relevant scripts only.
