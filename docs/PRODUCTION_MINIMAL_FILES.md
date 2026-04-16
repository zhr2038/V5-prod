# Production Minimal Files

## Purpose

This document defines the smallest repository surface that is still required to run the current live production workflow.

It is intended for:

- deployment packaging
- repository cleanup decisions
- future separation of production code from research history

## Required entry points

Live runtime:

- `main.py`
- `event_driven_check.py`

Required operational scripts:

- `scripts/run_hourly_live_window.sh`
- `scripts/collect_funding_sentiment.py`
- `scripts/collect_rss_sentiment.py`
- `scripts/bills_sync.py`
- `scripts/ledger_once.py`
- `scripts/reconcile_guard_once.py`
- `scripts/rollup_last24h.py`
- `scripts/rollup_costs.py`
- `scripts/rollup_spreads.py`
- `scripts/health_check.py`
- `scripts/v5_status_report.py`
- `scripts/okx_private_selfcheck.py`
- `scripts/live_preflight_once.py`
- `scripts/orders_gc_once.py`
- `scripts/orders_repair_once.py`
- `scripts/compare_runs.py`

## Required configuration

- `.env`
- `configs/live_prod.yaml`
- `configs/blacklist.json`
- `configs/borrow_prevention_rules.json`
- `requirements.txt`
- `pyproject.toml`

## Required deploy units

Primary production:

- `deploy/systemd/v5-prod.user.service`
- `deploy/systemd/v5-prod.user.timer`
- `deploy/systemd/v5-sentiment-collect.service`
- `deploy/systemd/v5-sentiment-collect.timer`

Important:

- `deploy/install_systemd.sh` installs these units, but does not auto-enable the live production timer.
- `deploy/sync_prod_release.py` syncs the production-only release payload to the target host.
- enabling `v5-prod.user.timer` should remain an explicit operator action.

Safety and support:

- `deploy/systemd/v5-reconcile.user.service`
- `deploy/systemd/v5-reconcile.timer`
- `deploy/systemd/v5-ledger.user.service`
- `deploy/systemd/v5-ledger.timer`
- `deploy/systemd/v5-cost-rollup-real.user.service`
- `deploy/systemd/v5-cost-rollup-real.user.timer`
- `deploy/systemd/v5-spread-rollup.service`
- `deploy/systemd/v5-spread-rollup.timer`
- `deploy/systemd/v5-event-driven.service`
- `deploy/systemd/v5-event-driven.timer`
- `deploy/install_systemd.sh`
- `deploy/sync_prod_release.py`

## Required source directories

These directories are still on the live path and should be treated as production code:

- `src/alpha/`
- `src/core/`
- `src/data/`
- `src/execution/`
- `src/factors/`
- `src/portfolio/`
- `src/regime/`
- `src/reporting/`
- `src/risk/`
- `src/strategy/`
- `src/utils/`
- `configs/`

## Required runtime output paths

These are generated at runtime and must remain writable:

- `reports/orders.sqlite`
- `reports/positions.sqlite`
- `reports/fills.sqlite`
- `reports/bills.sqlite`
- `reports/reconcile_status.json`
- `reports/reconcile_failure_state.json`
- `reports/kill_switch.json`
- `reports/ledger_status.json`
- `reports/ledger_state.json`
- `reports/order_state_machine.json`
- `reports/negative_expectancy_cooldown.json`
- `reports/topk_dropout_state.json`
- `reports/portfolio_optimizer_state.json`
- `reports/alpha_ic_monitor.json`
- `reports/runs/`
- `reports/cost_events/`
- `reports/cost_stats_real/`
- `reports/spread_stats/`

## Optional but operationally useful

- `scripts/trade_auditor.py`
- `scripts/trade_auditor_v2.py`
- `scripts/trade_auditor_v3.py`
- `scripts/trading_report.py`
- `scripts/smart_alert_check.py`
- `scripts/web_dashboard.py`
- `scripts/run_data_quality_check.py`
- `scripts/equity_anomaly_detector.py`
- `scripts/universe_guard.py`

These are not required for order execution itself, but they are useful for monitoring and investigation.

## Safe to exclude from a minimal production package

- `tests/`
- local-only `scripts/archive/`
- backtest-heavy configs under `configs/` that are not referenced by production deploy units
- `study_notes/`
- `v4_export/`
- historical analysis documents that are not current runbooks

## Practical minimal deployment tree

```text
v5-trading-bot/
  .env
  main.py
  event_driven_check.py
  requirements.txt
  pyproject.toml
  configs/
  deploy/systemd/
  scripts/
  src/
  reports/  # writable runtime state
```

## Current production naming convention

The canonical production service name is now:

- `v5-prod.user.service`
- `v5-prod.user.timer`

Retired names such as `v5-live-20u.user.service` have been removed from the active production paths and should not be used again.
