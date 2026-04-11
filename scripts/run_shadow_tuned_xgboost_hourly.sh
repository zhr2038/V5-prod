#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WIN_ID="shadow_tuned_xgboost_$(date +%Y%m%d_%H)"
NOW_EPOCH="$(date +%s)"
END_EPOCH=$(( NOW_EPOCH - (NOW_EPOCH % 3600) ))
START_EPOCH=$(( END_EPOCH - 3600 ))

echo "[V5-SHADOW-XGB] WIN_ID=${WIN_ID} window=[${START_EPOCH}, ${END_EPOCH}) CST"

LOCK="/tmp/v5_shadow_tuned_xgboost.lock"
exec 9>"$LOCK"
flock -n 9 || exit 0

PYTHON_BIN="${V5_PYTHON_BIN:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="${V5_PYTHON_BIN:-python3}"
fi

export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export V5_DATA_PROVIDER="${V5_DATA_PROVIDER:-okx}"
export V5_RUN_ID="$WIN_ID"
export V5_WINDOW_START_TS="${START_EPOCH}"
export V5_WINDOW_END_TS="${END_EPOCH}"
export V5_DISABLE_TOPLEVEL_ARTIFACTS="1"
export V5_PROJECT_ROOT="$ROOT"

resolve_trend_cache_path() {
  "$PYTHON_BIN" - <<'PY'
from pathlib import Path
import os

import yaml

root = Path(os.environ["V5_PROJECT_ROOT"])
cfg_path = root / "configs" / "shadow_tuned_xgboost_overrides.yaml"
order_store_path = "reports/shadow_tuned_xgboost/orders.sqlite"
try:
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    if isinstance(cfg, dict):
        execution = cfg.get("execution") or {}
        if isinstance(execution, dict):
            order_store_path = str(execution.get("order_store_path") or order_store_path)
except Exception:
    pass

path = Path(order_store_path)
if path.name == "orders.sqlite":
    trend_path = path.with_name("trend_cache.json")
elif "orders" in path.stem:
    trend_path = path.with_name(path.stem.replace("orders", "trend_cache", 1) + ".json")
else:
    trend_path = path.with_name("trend_cache.json")

if not trend_path.is_absolute():
    trend_path = root / trend_path
print(trend_path)
PY
}

resolve_trend_cache_timestamp() {
  local trend_cache_path="$1"
  TREND_CACHE_PATH_TO_READ="$trend_cache_path" "$PYTHON_BIN" - <<'PY'
from pathlib import Path
import json
import os

path = Path(os.environ["TREND_CACHE_PATH_TO_READ"])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(int(float(payload.get("timestamp", 0) or 0)))
except Exception:
    print(0)
PY
}

TREND_CACHE_PATH="${V5_TREND_CACHE_PATH:-$(resolve_trend_cache_path)}"
TREND_CACHE_MAX_AGE_SEC="${V5_TREND_CACHE_MAX_AGE_SEC:-300}"
if [[ -f "$TREND_CACHE_PATH" ]]; then
  cache_ts="$(resolve_trend_cache_timestamp "$TREND_CACHE_PATH")"
  cache_age=$(( NOW_EPOCH - cache_ts ))
  if [[ "$cache_age" -ge 0 && "$cache_age" -le "$TREND_CACHE_MAX_AGE_SEC" ]]; then
    export V5_USE_CACHED_TREND="1"
  else
    unset V5_USE_CACHED_TREND
  fi
else
  unset V5_USE_CACHED_TREND
fi

"$PYTHON_BIN" scripts/run_shadow_tuned_xgboost.py --provider "$V5_DATA_PROVIDER"
