#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WIN_ID="$(date -u +%Y%m%d_%H)"
NOW_EPOCH="$(date +%s)"
END_EPOCH=$(( NOW_EPOCH - (NOW_EPOCH % 3600) ))
START_EPOCH=$(( END_EPOCH - 3600 ))

echo "[V5-LIVE] WIN_ID=${WIN_ID} window=[${START_EPOCH}, ${END_EPOCH}) UTC"

LOCK="/tmp/v5_live.lock"
exec 9>"$LOCK"
flock -n 9 || exit 0

PYTHON_BIN="${V5_PYTHON_BIN:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="${V5_PYTHON_BIN:-python3}"
fi

export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export V5_DATA_PROVIDER="${V5_DATA_PROVIDER:-okx}"
export V5_CONFIG="${V5_CONFIG:-configs/live_prod.yaml}"
export V5_LIVE_ARM="${V5_LIVE_ARM:-YES}"
export V5_RUN_ID="$WIN_ID"
export V5_WINDOW_START_TS="${START_EPOCH}"
export V5_WINDOW_END_TS="${END_EPOCH}"

TREND_CACHE_PATH="${V5_TREND_CACHE_PATH:-$ROOT/reports/trend_cache.json}"
TREND_CACHE_MAX_AGE_SEC="${V5_TREND_CACHE_MAX_AGE_SEC:-300}"
if [[ -f "$TREND_CACHE_PATH" ]]; then
  cache_mtime="$(stat -c %Y "$TREND_CACHE_PATH" 2>/dev/null || stat -f %m "$TREND_CACHE_PATH" 2>/dev/null || echo 0)"
  cache_age=$(( NOW_EPOCH - cache_mtime ))
  if [[ "$cache_age" -ge 0 && "$cache_age" -le "$TREND_CACHE_MAX_AGE_SEC" ]]; then
    export V5_USE_CACHED_TREND="1"
  else
    unset V5_USE_CACHED_TREND
  fi
else
  unset V5_USE_CACHED_TREND
fi

"$PYTHON_BIN" main.py
