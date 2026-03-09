#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USER_MODE=0
PRODUCTION_ONLY=0
ENABLE_PROD_TIMER=0
ENABLE_EVENT_DRIVEN_TIMER=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      USER_MODE=1
      shift
      ;;
    --root)
      ROOT="$2"
      shift 2
      ;;
    --production-only)
      PRODUCTION_ONLY=1
      shift
      ;;
    --enable-prod-timer)
      ENABLE_PROD_TIMER=1
      shift
      ;;
    --enable-event-driven-timer)
      ENABLE_EVENT_DRIVEN_TIMER=1
      shift
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

SRC="$ROOT/deploy/systemd"
RENDERER="$ROOT/deploy/render_systemd_units.py"

if [[ ! -d "$SRC" ]]; then
  echo "missing $SRC" >&2
  exit 1
fi

if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "missing python interpreter" >&2
  exit 1
fi

render_units() {
  local dst="$1"
  shift
  "$PYTHON_BIN" "$RENDERER" --src-dir "$SRC" --dst-dir "$dst" --root "$ROOT" "$@"
}

if [[ "$USER_MODE" == "1" ]]; then
  if [[ -z "${XDG_RUNTIME_DIR:-}" ]]; then
    export XDG_RUNTIME_DIR="/run/user/$(id -u)"
  fi
  if [[ -z "${DBUS_SESSION_BUS_ADDRESS:-}" && -S "$XDG_RUNTIME_DIR/bus" ]]; then
    export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"
  fi

  DST="$HOME/.config/systemd/user"
  mkdir -p "$DST"

  if [[ "$PRODUCTION_ONLY" == "1" ]]; then
    render_units "$DST" \
      --mapping v5-prod.user.service=v5-prod.user.service \
      --mapping v5-prod.user.timer=v5-prod.user.timer \
      --mapping v5-event-driven.service=v5-event-driven.service \
      --mapping v5-event-driven.timer=v5-event-driven.timer \
      --mapping v5-sentiment-collect.service=v5-sentiment-collect.service \
      --mapping v5-sentiment-collect.timer=v5-sentiment-collect.timer \
      --mapping v5-auto-risk-eval.service=v5-auto-risk-eval.service \
      --mapping v5-auto-risk-eval.timer=v5-auto-risk-eval.timer \
      --mapping v5-reconcile.user.service=v5-reconcile.service \
      --mapping v5-reconcile.timer=v5-reconcile.timer \
      --mapping v5-ledger.user.service=v5-ledger.service \
      --mapping v5-ledger.timer=v5-ledger.timer \
      --mapping v5-cost-rollup-real.user.service=v5-cost-rollup-real.user.service \
      --mapping v5-cost-rollup-real.user.timer=v5-cost-rollup-real.user.timer
  else
    render_units "$DST" --copy-all \
      --mapping v5-reconcile.user.service=v5-reconcile.service \
      --mapping v5-ledger.user.service=v5-ledger.service
  fi

  systemctl --user daemon-reload

  if [[ "$PRODUCTION_ONLY" == "1" ]]; then
    systemctl --user enable --now v5-sentiment-collect.timer
    systemctl --user enable --now v5-auto-risk-eval.timer
    systemctl --user enable --now v5-reconcile.timer
    systemctl --user enable --now v5-ledger.timer
    systemctl --user enable --now v5-cost-rollup-real.user.timer
    if [[ "$ENABLE_PROD_TIMER" == "1" ]]; then
      systemctl --user enable --now v5-prod.user.timer
    fi
    if [[ "$ENABLE_EVENT_DRIVEN_TIMER" == "1" ]]; then
      systemctl --user enable --now v5-event-driven.timer
    fi
    systemctl --user list-timers --all | grep -E "v5-(prod|event-driven|sentiment-collect|auto-risk-eval|reconcile|ledger|cost-rollup-real)" || true
  else
    systemctl --user enable --now v5-hourly.timer
    systemctl --user enable --now v5-daily.timer
    systemctl --user enable --now v5-cost-rollup.timer
    systemctl --user enable --now v5-spread-rollup.timer
    systemctl --user enable --now v5-reconcile.timer
    systemctl --user enable --now v5-ledger.timer
    systemctl --user list-timers --all | grep -E "v5-(hourly|daily|cost-rollup|spread-rollup|reconcile|ledger)" || true
  fi
  exit 0
fi

echo "Installing system units to /etc/systemd/system (requires sudo)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
render_units "$TMP_DIR" --copy-all
sudo cp "$TMP_DIR"/*.service "$TMP_DIR"/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now v5-hourly.timer
sudo systemctl enable --now v5-daily.timer
sudo systemctl enable --now v5-cost-rollup.timer
sudo systemctl enable --now v5-spread-rollup.timer
sudo systemctl enable --now v5-reconcile.timer
sudo systemctl enable --now v5-ledger.timer
systemctl list-timers --all | grep -E "v5-(hourly|daily|cost-rollup|spread-rollup|reconcile|ledger)" || true
