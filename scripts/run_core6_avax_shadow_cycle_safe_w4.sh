#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/volume1/docker/V5-trading-bot/.venv_shadow/bin/python}"
LOCK_FILE="${LOCK_FILE:-/tmp/core6_avax_shadow_cycle_safe_w4.lock}"

cd "$ROOT_DIR"

if command -v flock >/dev/null 2>&1; then
  exec flock -n "$LOCK_FILE" "$PYTHON_BIN" scripts/run_pressure_probe.py \
    --label scheduled_shadow_cycle_safe_w4 \
    --output-dir reports/research/remote_pressure \
    --sample-seconds 5 \
    -- \
    "$PYTHON_BIN" scripts/run_core6_avax_shadow_cycle.py \
    --ab-config configs/research/core6_avax_shadow_compare_safe_w4.yaml \
    --shadow-config configs/research/core6_avax015_shadow_safe_w4.yaml \
    --output-json reports/research/core6_avax_shadow_cycle_safe_w4/latest.json \
    --output-md reports/research/core6_avax_shadow_cycle_safe_w4/latest.md
fi

exec "$PYTHON_BIN" scripts/run_pressure_probe.py \
  --label scheduled_shadow_cycle_safe_w4 \
  --output-dir reports/research/remote_pressure \
  --sample-seconds 5 \
  -- \
  "$PYTHON_BIN" scripts/run_core6_avax_shadow_cycle.py \
  --ab-config configs/research/core6_avax_shadow_compare_safe_w4.yaml \
  --shadow-config configs/research/core6_avax015_shadow_safe_w4.yaml \
  --output-json reports/research/core6_avax_shadow_cycle_safe_w4/latest.json \
  --output-md reports/research/core6_avax_shadow_cycle_safe_w4/latest.md
