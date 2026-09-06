# Production-Only Deployment

## Goal

Keep the production runtime root as a runnable synced copy without depending on manual server-side edits.

The production server should be treated as a sync target, not as the authoritative Git workspace.

## Why the old server copy became dirty

There were two structural reasons:

- repository files under `deploy/systemd/` and `scripts/` historically hardcoded a specific server root
- runtime state under `reports/` is tracked by Git, so a live server naturally accumulates local changes

The first class is fixed by rendering unit files for the target root during install and by making the hourly launch scripts derive the workspace root dynamically.

The second class is not a code bug. It is a deployment-model issue: a live runtime directory writes state, so it should not be treated as a normal Git working tree.

## Production deployment model

Authoritative source:

- local repository / GitHub

Production target example:

- `/home/ubuntu/clawd/v5-prod`

Persistent server-local state:

- `.env`
- `.venv/`
- `reports/`
- `logs/`
- `data/` caches produced on the server

Production Python dependencies come from `requirements.txt`. Optional ML/research-only dependencies live in `requirements-research.txt` and are not required for `live_prod`.

Synced code surface:

- `main.py`
- `event_driven_check.py`
- `requirements.txt`
- `requirements-research.txt`
- `pyproject.toml`
- `configs/`
- `deploy/`
- `scripts/`
- `src/`
- `web/`
- current production runbooks in `docs/`

## Standard deploy flow

1. Sync the production release:

```bash
python deploy/sync_prod_release.py \
  --host <host> \
  --user <user> \
  --password '***' \
  --remote-root /home/ubuntu/clawd/v5-prod \
  --service-user ubuntu \
  --enable-prod-timer \
  --enable-event-driven-timer
```

2. The sync script uploads only the production surface and then runs:

```bash
bash deploy/install_systemd.sh \
  --user \
  --production-only \
  --root /home/ubuntu/clawd/v5-prod \
  --enable-prod-timer \
  --enable-event-driven-timer
```

3. Rendered user units are installed under:

- `~/.config/systemd/user/`

The installed units point at the rendered target root, even if the repository source units still carry a historical default path.

## Operational rules

- Do not hand-edit files inside the live runtime root unless the goal is a hotfix.
- Do not rely on `git pull` inside the live directory as the normal deployment path.
- Treat `reports/` and `logs/` as mutable runtime state.
- If a hotfix is made directly on the server, backport it into the main repository before the next sync.

## Scope of the production install

The production-only systemd install covers:

- `v5-prod.user.service`
- `v5-prod.user.timer`
- `v5-event-driven.service`
- `v5-event-driven.timer`
- `v5-sentiment-collect.service`
- `v5-sentiment-collect.timer`
- `v5-reconcile.service`
- `v5-reconcile.timer`
- `v5-ledger.service`
- `v5-ledger.timer`
- `v5-cost-rollup-real.user.service`
- `v5-cost-rollup-real.user.timer`

Operational timers for sentiment refresh, reconcile, and ledger are enabled by default.
ML training, model promotion, and tuned XGBoost shadow timers are research-only and remain disabled in `live_prod`.

Live trading timers remain explicit operator choices:

- `--enable-prod-timer`
- `--enable-event-driven-timer`

## Optional Research-Only ML Shadow Deployment

The ML shadow workspace and scripts are retained for offline research only. They are not part of the `live_prod` execution path, and the default production install disables:

- `v5-daily-ml-training.timer`
- `v5-model-promotion-gate.timer`
- `v5-shadow-tuned-xgboost.user.timer`

Only use this section on an explicitly approved research host. Do not enable these timers on `live_prod` unless ML live overlay is explicitly re-approved.

Use a separate workspace on the production server for long-running dry-run observation.
This avoids mixing shadow `reports/` state with the live trading workspace.

Recommended shadow root:

- `/home/ubuntu/clawd/v5-shadow-tuned-xgboost`

Sync the code surface without touching the live timers:

```bash
python deploy/sync_prod_release.py \
  --host <host> \
  --user <user> \
  --password '***' \
  --remote-root /home/ubuntu/clawd/v5-shadow-tuned-xgboost \
  --service-user ubuntu \
  --skip-install
```

On the server, reuse the production virtualenv and `.env`:

```bash
ln -sfn /home/ubuntu/clawd/v5-prod/.venv /home/ubuntu/clawd/v5-shadow-tuned-xgboost/.venv
ln -sfn /home/ubuntu/clawd/v5-prod/.env /home/ubuntu/clawd/v5-shadow-tuned-xgboost/.env
```

Then render the shadow timer. It remains inert unless `/etc/v5-enable-ml-research-timers` exists on that research host:

```bash
uid=$(id -u ubuntu)
sudo -u ubuntu env \
  XDG_RUNTIME_DIR=/run/user/$uid \
  DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus \
  bash -lc '
    cd /home/ubuntu/clawd/v5-shadow-tuned-xgboost &&
    python3 deploy/render_systemd_units.py \
      --src-dir deploy/systemd \
      --dst-dir ~/.config/systemd/user \
      --root /home/ubuntu/clawd/v5-shadow-tuned-xgboost \
      --mapping v5-shadow-tuned-xgboost.user.service=v5-shadow-tuned-xgboost.user.service \
      --mapping v5-shadow-tuned-xgboost.user.timer=v5-shadow-tuned-xgboost.user.timer &&
    systemctl --user daemon-reload
  '
```

The tuned model artifacts needed by this shadow flow are part of the production sync surface:

- `models/ml_factor_model_gpu_tuned.json`
- `models/ml_factor_model_gpu_tuned_config.json`
# Runtime payload cleanup, 2026-09-06

The default production file selection ships `web/dist`, `web/static` and
`web/templates`. Frontend build sources and dependencies stay in the development
checkout. The standalone bundle test harness, repository tests, archived research,
and personal notes are not runtime payload. Full repository CI still runs all
tests, including the archived regression suite; no tests are deleted.

Use `production_snapshot` plus `iter_production_files` when creating an immutable
release. Generate the release manifest from exactly the selected committed bytes,
record excluded paths, and verify dependencies and runtime overrides before the
symlink switch. Do not prune a running release in place or edit its manifest to
make missing files pass. Keep the previous complete release as the rollback point.

All `src/` and research configuration files remain byte-identical during this
layout-only cleanup. Confirm `source_identity` against the active research ledger
before switching, retaining its existing directory and start time. Never remove
shared `.venv`, `.env`, models, runtime state, reports, orders or research ledgers.
Disabled shadow experiment code can be removed from the server only after checking
processes, units and cron references, archiving its exact source with file hashes,
and verifying a downloaded recovery copy. Preserve that experiment's data/models/
reports/logs and record its retired execution status separately.
