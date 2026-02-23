#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/deploy/systemd"

if [[ ! -d "$SRC" ]]; then
  echo "missing $SRC" >&2
  exit 1
fi

if [[ "${1:-}" == "--user" ]]; then
  DST="$HOME/.config/systemd/user"
  mkdir -p "$DST"
  # Copy shared timers and default services
  cp "$SRC"/*.timer "$DST/"
  cp "$SRC"/*.service "$DST/"
  # Override reconcile/ledger service units for user mode (avoid User=/Group=, which can fail with 216/GROUP)
  cp "$SRC"/v5-reconcile.user.service "$DST"/v5-reconcile.service
  cp "$SRC"/v5-ledger.user.service "$DST"/v5-ledger.service

  systemctl --user daemon-reload
  systemctl --user enable --now v5-hourly.timer
  systemctl --user enable --now v5-daily.timer
  systemctl --user enable --now v5-cost-rollup.timer
  systemctl --user enable --now v5-spread-rollup.timer
  systemctl --user enable --now v5-reconcile.timer
  systemctl --user enable --now v5-ledger.timer
  systemctl --user list-timers --all | grep -E "v5-(hourly|daily|cost-rollup|spread-rollup|reconcile|ledger)" || true
  exit 0
fi

echo "Installing system units to /etc/systemd/system (requires sudo)"
sudo cp "$SRC"/*.service "$SRC"/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now v5-hourly.timer
sudo systemctl enable --now v5-daily.timer
sudo systemctl enable --now v5-cost-rollup.timer
sudo systemctl enable --now v5-spread-rollup.timer
sudo systemctl enable --now v5-reconcile.timer
sudo systemctl enable --now v5-ledger.timer
systemctl list-timers --all | grep -E "v5-(hourly|daily|cost-rollup|spread-rollup|reconcile|ledger)" || true
