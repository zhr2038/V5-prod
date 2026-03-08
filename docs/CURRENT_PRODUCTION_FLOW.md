# Current Production Flow

## Scope

This document describes the current production runtime for the repository as it exists now.
It separates the live trading path from historical research, backtest, and one-off tooling.

## Production Entry Points

Primary runtime:
- `main.py`

Operational helpers:
- `scripts/collect_funding_sentiment.py`
- `scripts/collect_rss_sentiment.py`
- `event_driven_check.py`
- `scripts/run_hourly_live_window.sh`
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

Primary production units:
- `deploy/systemd/v5-prod.user.service`
- `deploy/systemd/v5-prod.user.timer`
- `deploy/systemd/v5-event-driven.service`
- `deploy/systemd/v5-event-driven.timer`
- `deploy/systemd/v5-sentiment-collect.service`
- `deploy/systemd/v5-sentiment-collect.timer`

Primary production config:
- `configs/live_prod.yaml`

Primary environment file:
- `.env` in the repository root

## Live Trading Flow

### 1. Process bootstrap

`main.py`:
- resolves config with priority `V5_CONFIG -> configs/live_prod.yaml -> configs/live_20u_real.yaml -> configs/config.yaml`
- loads `.env` from repository root
- builds logging, run id, and decision audit context

### 2. Market data and universe

`main.py`:
- builds provider with `build_provider()`
- in live mode, requires `V5_DATA_PROVIDER=okx`
- optionally refreshes the universe via `src/data/universe/okx_universe.py`
- fetches OHLCV through `src/data/okx_ccxt_provider.py`

### 3. Alpha and regime

`main.py` and `src/core/pipeline.py`:
- sentiment cache is refreshed by `v5-sentiment-collect.timer`
- alpha snapshot from `src/alpha/alpha_engine.py`
- regime from `src/regime/ensemble_regime_engine.py` or fallback `src/regime/regime_engine.py`
- optional trend cache read/write in `main.py`

### 4. Portfolio construction

`src/core/pipeline.py`:
- target weights from `src/portfolio/portfolio_engine.py`
- risk scaling from `src/risk/risk_engine.py`
- exit logic from `src/risk/exit_policy.py`
- additional runtime guards:
  - `src/execution/position_builder.py`
  - `src/execution/multi_level_stop_loss.py`
  - `src/risk/fixed_stop_loss.py`
  - `src/risk/profit_taking.py`
  - `src/risk/auto_risk_guard.py`
  - `src/risk/negative_expectancy_cooldown.py`

### 5. Account state and budget

`main.py`:
- loads local state from:
  - `src/execution/position_store.py`
  - `src/execution/account_store.py`
- marks positions to market
- checks live equity budget through `src/risk/live_equity_fetcher.py`

### 6. Pre-trade safety

Live mode only:
- `src/execution/live_preflight.py`

Current safety chain:
- bills/ledger status refresh
- reconcile status refresh
- kill-switch evaluation
- borrow and account-config buy gating

Important note:
- `SELL_ONLY` behavior still depends on `execution.preflight_fail_action` in `main.py`.

### 7. Order generation and arbitration

`main.py`:
- generates orders from pipeline output
- arbitrates conflicts via `src/execution/order_arbitrator.py`

### 8. Execution

Dry run:
- `src/execution/execution_engine.py`

Live:
- `src/execution/okx_private_client.py`
- `src/execution/live_execution_engine.py`
- `src/execution/order_store.py`

### 9. Post-trade synchronization

`main.py`:
- polls open orders
- syncs fills into `src/execution/fill_store.py`
- reconciles fills through `src/execution/fill_reconciler.py`
- updates summaries and run artifacts

### 10. Outputs

Primary runtime artifacts:
- `reports/orders.sqlite`
- `reports/positions.sqlite`
- `reports/fills.sqlite`
- `reports/reconcile_status.json`
- `reports/kill_switch.json`
- `reports/ledger_status.json`
- `reports/runs/<run_id>/...`

## Operational Flow Outside `main.py`

### Ledger path

- `scripts/bills_sync.py`
- `scripts/ledger_once.py`

Purpose:
- pull bills
- derive expected balances
- emit ledger status for preflight / monitoring

### Reconcile path

- `scripts/reconcile_guard_once.py`

Purpose:
- compare OKX balances and local state
- write reconcile status
- feed kill-switch / live preflight

### Event-driven path

- `event_driven_check.py`

Purpose:
- inspect latest state and signals
- trigger or coordinate event-driven trading checks

### Health and status

- `scripts/health_check.py`
- `scripts/v5_status_report.py`

Purpose:
- operational visibility for timers, DBs, API reachability, status summaries

### Daily rollups

- `scripts/rollup_last24h.py`
- `scripts/rollup_costs.py`
- `scripts/rollup_spreads.py`

Purpose:
- produce last-24h summaries and market microstructure rollups for ops review

## Repository Split

Keep as active production code:
- `main.py`
- `event_driven_check.py`
- `configs/live_prod.yaml`
- `src/`
- active ops scripts in `scripts/`

Treat as non-production support content:
- local-only `scripts/archive/`
- `study_notes/`
- `v4_export/`
- design and migration notes in `docs/`

## Scripts Layout

Active `scripts/` root now keeps three groups only:

- scheduled runtime and status scripts
- manual recovery / reconcile helpers
- monitoring and reporting helpers

Archived script groups:

- local `scripts/archive/research/`: backtest, factor research, walk-forward, and model-training scripts
- local `scripts/archive/devtools/`: one-off development helpers
- local `scripts/archive/legacy/`: old 20u and deprecated launch scripts

## Safe Cleanup Rules

Low-risk removal candidates:
- duplicate files
- backup files
- stray text dumps
- obsolete one-off outputs not referenced by code or deploy units

Do not remove without another review pass:
- `main.py` dependencies under `src/`
- scripts referenced by deploy units or manual runbooks
- backtest / research tools that may still be used offline

## This Cleanup Pass

Removed in this pass:
- root stray files `100`, `105`, `110`
- duplicate backup module `src/execution/reflection_agent_v1_backup.py`
- duplicate module `src/execution/reflection_agent_v2.py`
- archived non-production `scripts/` into:
  - `scripts/archive/research/`
  - `scripts/archive/devtools/`
  - `scripts/archive/legacy/`

Left intentionally in place:
- local `scripts/archive/`
- manual ops and monitoring scripts in `scripts/`
- design docs and study notes

Those areas need a second pass with usage confirmation before deletion.
