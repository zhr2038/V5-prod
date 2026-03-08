# Production-Only Deployment

## Goal

Keep `/home/admin/clawd/v5-prod` as a runnable production copy without depending on manual server-side edits.

The production server should be treated as a sync target, not as the authoritative Git workspace.

## Why the old server copy became dirty

There were two structural reasons:

- repository files under `deploy/systemd/` and `scripts/` hardcoded `/home/admin/clawd/v5-trading-bot`, so the server had to patch them to `/home/admin/clawd/v5-prod`
- runtime state under `reports/` is tracked by Git, so a live server naturally accumulates local changes

The first class is fixed by rendering unit files for the target root during install and by making the hourly launch scripts derive the workspace root dynamically.

The second class is not a code bug. It is a deployment-model issue: a live runtime directory writes state, so it should not be treated as a normal Git working tree.

## Production deployment model

Authoritative source:

- local repository / GitHub

Production target:

- `/home/admin/clawd/v5-prod`

Persistent server-local state:

- `.env`
- `.venv/`
- `reports/`
- `logs/`
- `data/` caches produced on the server

Synced code surface:

- `main.py`
- `event_driven_check.py`
- `requirements.txt`
- `pyproject.toml`
- `configs/`
- `deploy/`
- `scripts/`
- `src/`
- current production runbooks in `docs/`

## Standard deploy flow

1. Sync the production release:

```bash
python deploy/sync_prod_release.py \
  --host claw.hrhome.top \
  --user root \
  --password '***' \
  --remote-root /home/admin/clawd/v5-prod \
  --service-user admin \
  --enable-prod-timer \
  --enable-event-driven-timer
```

2. The sync script uploads only the production surface and then runs:

```bash
bash deploy/install_systemd.sh \
  --user \
  --production-only \
  --root /home/admin/clawd/v5-prod \
  --enable-prod-timer \
  --enable-event-driven-timer
```

3. Rendered user units are installed under:

- `/home/admin/.config/systemd/user/`

The installed units point at `/home/admin/clawd/v5-prod`, even if the repository source units still carry the historical default path.

## Operational rules

- Do not hand-edit files inside `/home/admin/clawd/v5-prod` unless the goal is a hotfix.
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

Live trading timers remain explicit operator choices:

- `--enable-prod-timer`
- `--enable-event-driven-timer`
