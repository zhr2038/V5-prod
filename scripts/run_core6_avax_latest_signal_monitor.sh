#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/volume1/docker/V5-trading-bot/.venv_shadow/bin/python}"
LOCK_FILE="${LOCK_FILE:-/tmp/core6_avax_latest_signal_monitor.lock}"

cd "$ROOT_DIR"

if command -v flock >/dev/null 2>&1; then
  exec flock -n "$LOCK_FILE" "$PYTHON_BIN" scripts/run_pressure_probe.py \
    --label scheduled_latest_signal_monitor \
    --output-dir reports/research/remote_pressure \
    --sample-seconds 5 \
    -- \
    "$PYTHON_BIN" scripts/run_latest_signal_monitor.py \
    configs/research/core6_avax_latest_signal_monitor.yaml
fi

exec "$PYTHON_BIN" scripts/run_pressure_probe.py \
  --label scheduled_latest_signal_monitor \
  --output-dir reports/research/remote_pressure \
  --sample-seconds 5 \
  -- \
  "$PYTHON_BIN" scripts/run_latest_signal_monitor.py \
  configs/research/core6_avax_latest_signal_monitor.yaml
