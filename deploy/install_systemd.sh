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
  cp "$SRC"/*.service "$SRC"/*.timer "$DST/"
  systemctl --user daemon-reload
  systemctl --user enable --now v5-hourly.timer
  systemctl --user enable --now v5-daily.timer
  systemctl --user enable --now v5-cost-rollup.timer
  systemctl --user enable --now v5-spread-rollup.timer
  systemctl --user list-timers --all | grep -E "v5-(hourly|daily|cost-rollup|spread-rollup)" || true
  exit 0
fi

echo "Installing system units to /etc/systemd/system (requires sudo)"
sudo cp "$SRC"/*.service "$SRC"/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now v5-hourly.timer
sudo systemctl enable --now v5-daily.timer
sudo systemctl enable --now v5-cost-rollup.timer
sudo systemctl enable --now v5-spread-rollup.timer
systemctl list-timers --all | grep -E "v5-(hourly|daily|cost-rollup|spread-rollup)" || true
