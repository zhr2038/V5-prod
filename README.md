# V5 Trading Bot

[简体中文](./README.zh-CN.md)

V5 is the current OKX spot trading workspace used for production operations. It is no longer just a research repo: it includes the live trading loop, event-driven checks, risk controls, reconciliation, a Flask dashboard, and ML training/promotion support.

## Overview

The repository currently covers:

- hourly production trading via `main.py`
- event-driven checks via `event_driven_check.py`
- OKX spot execution with explicit live arming
- pre-trade safety checks: bills, ledger, reconcile, kill-switch
- monitoring surfaces: health checks, audit artifacts, reports, web dashboard
- ML data collection, training, promotion gate, and optional live overlay

Current production runtime root:

- `/home/admin/clawd/v5-prod`

## Main Entry Points

Core runtime:

- `main.py`
- `event_driven_check.py`
- `scripts/web_dashboard.py`

Primary production config:

- `configs/live_prod.yaml`

Important production docs:

- [Current Production Flow](./docs/CURRENT_PRODUCTION_FLOW.md)
- [Production-Only Deployment](./docs/PRODUCTION_ONLY_DEPLOYMENT.md)
- [Production Minimal Files](./docs/PRODUCTION_MINIMAL_FILES.md)

## Architecture

Runtime flow:

1. Load `.env` and `configs/live_prod.yaml`
2. Pull market data from OKX public data APIs
3. Build alpha, regime, portfolio, and risk decisions
4. Run live preflight checks
5. Generate and execute orders
6. Persist orders, fills, positions, summaries, and audits under `reports/`

Current regime engine is ensemble-based:

- HMM
- funding sentiment
- RSS sentiment

Current monitoring surfaces include:

- dashboard APIs under `/api/*`
- health endpoints under `/health`, `/ready`, `/liveness`
- run artifacts under `reports/runs/<run_id>/`

## Dashboard

The dashboard backend lives in:

- `scripts/web_dashboard.py`

Frontend assets live in:

- `web/templates/`
- `web/static/`

Current dashboard highlights:

- single-page operational overview
- market state, risk tier, positions, trades, signals, health, and ML stages
- position spotlight with per-holding K-lines
- responsive layout for desktop and mobile

Useful endpoints:

- `/`
- `/monitor`
- `/api/dashboard`
- `/api/account`
- `/api/positions`
- `/api/market_state`
- `/api/position_kline`
- `/api/ml_training`

Run locally:

```bash
python scripts/web_dashboard.py
```

Default local bind:

- `http://127.0.0.1:5000`

## Quick Start

### 1. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Prepare `.env`

At minimum:

```env
EXCHANGE_API_KEY=...
EXCHANGE_API_SECRET=...
EXCHANGE_PASSPHRASE=...
```

### 3. Run locally

Dry-run style local run:

```bash
python main.py
```

Live-armed run:

```bash
export V5_CONFIG=configs/live_prod.yaml
export V5_DATA_PROVIDER=okx
export V5_LIVE_ARM=YES
python main.py
```

Important:

- live mode requires `V5_LIVE_ARM=YES`
- production data provider must be `okx`
- do not treat the dashboard as a trading frontend; it is an ops/monitoring surface

## Testing

Run the main regression suite:

```bash
pytest -q
```

Dashboard-focused tests:

```bash
pytest tests/test_web_dashboard.py
```

## Production Deployment

Recommended model:

- GitHub is the source of truth
- `/home/admin/clawd/v5-prod` is a synced runtime copy
- runtime state such as `reports/`, `logs/`, `.env`, `.venv/`, and server-side caches stays on the server

Sync the production release:

```bash
python deploy/sync_prod_release.py \
  --host <host> \
  --user root \
  --password '<password>' \
  --remote-root /home/admin/clawd/v5-prod \
  --service-user admin \
  --enable-prod-timer \
  --enable-event-driven-timer
```

The production sync payload now includes:

- `main.py`
- `event_driven_check.py`
- `configs/`
- `deploy/`
- `scripts/`
- `src/`
- `web/`
- current production docs

Install user-level systemd units manually if needed:

```bash
bash deploy/install_systemd.sh --user
```

If user timers must keep running after logout:

```bash
sudo loginctl enable-linger admin
```

## Repository Layout

Main directories:

- `src/`: core trading, execution, risk, regime, factors, reporting
- `configs/`: production and support configs
- `scripts/`: operational scripts, dashboard, reporting, recovery helpers
- `web/`: dashboard templates and static assets
- `deploy/`: systemd units and production sync helpers
- `reports/`: runtime outputs, SQLite databases, run artifacts
- `tests/`: regression tests

Non-production or historical content still present in the repo:

- `study_notes/`
- `v4_export/`
- `scripts/archive/`

## Operational Notes

- `reports/*` is runtime state; do not commit it to GitHub
- avoid `git pull` as the normal deployment strategy inside the live runtime directory
- do not use destructive Git commands on the production copy
- backport server-side hotfixes into the repository before the next sync

Key runtime outputs to inspect:

- `reports/runs/<run_id>/decision_audit.json`
- `reports/runs/<run_id>/summary.json`
- `reports/runs/<run_id>/trades.csv`
- `reports/reconcile_status.json`
- `reports/kill_switch.json`
- `reports/ledger_status.json`
- `reports/ml_runtime_status.json`

## Boundaries

- OKX spot only
- no leverage trading
- no short selling
- ML remains gated; it is not always promoted into live scoring
- historical research content exists, but this README documents the current production path
