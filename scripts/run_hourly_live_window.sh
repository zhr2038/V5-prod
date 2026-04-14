#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WIN_ID="$(date +%Y%m%d_%H)"
NOW_EPOCH="$(date +%s)"
END_EPOCH=$(( NOW_EPOCH - (NOW_EPOCH % 3600) ))
START_EPOCH=$(( END_EPOCH - 3600 ))

echo "[V5-LIVE] WIN_ID=${WIN_ID} window=[${START_EPOCH}, ${END_EPOCH}) CST"

LOCK="/tmp/v5_live.lock"
exec 9>"$LOCK"
flock -n 9 || exit 0

resolve_python_bin() {
  local requested="${V5_PYTHON_BIN:-}"
  local candidates=()
  local candidate

  if [[ -n "$requested" ]]; then
    candidates+=("$requested")
  fi
  candidates+=("$ROOT/.venv/bin/python" "python3" "python")

  for candidate in "${candidates[@]}"; do
    if [[ "$candidate" == */* ]]; then
      [[ -x "$candidate" ]] || continue
    elif ! command -v "$candidate" >/dev/null 2>&1; then
      continue
    fi

    if "$candidate" -c "import sys" >/dev/null 2>&1; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  echo "missing usable python interpreter" >&2
  return 1
}

PYTHON_BIN="$(resolve_python_bin)"

export V5_PROJECT_ROOT="$ROOT"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export V5_DATA_PROVIDER="${V5_DATA_PROVIDER:-okx}"
export V5_CONFIG="${V5_CONFIG:-configs/live_prod.yaml}"
export V5_LIVE_ARM="${V5_LIVE_ARM:-YES}"
export V5_RUN_ID="$WIN_ID"
export V5_WINDOW_START_TS="${START_EPOCH}"
export V5_WINDOW_END_TS="${END_EPOCH}"

resolve_trend_cache_path() {
  "$PYTHON_BIN" - <<'PY'
from pathlib import Path
import os

try:
    import yaml
except Exception:
    yaml = None

from src.execution.fill_store import derive_runtime_named_json_path

root = Path(os.environ["V5_PROJECT_ROOT"])
cfg_path = Path(os.environ.get("V5_CONFIG", "configs/live_prod.yaml"))
if not cfg_path.is_absolute():
    cfg_path = root / cfg_path

order_store_path = "reports/orders.sqlite"
if yaml is not None and cfg_path.exists():
    try:
        payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        execution = payload.get("execution") or {}
        candidate = execution.get("order_store_path")
        if candidate:
            order_store_path = str(candidate)
    except Exception:
        pass

order_store = Path(order_store_path)
if not order_store.is_absolute():
    order_store = root / order_store
print(derive_runtime_named_json_path(order_store, "trend_cache"))
PY
}

resolve_trend_cache_timestamp() {
  local cache_path="$1"
  "$PYTHON_BIN" - "$cache_path" <<'PY'
from pathlib import Path
import json
import sys

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(1)

timestamp = payload.get("timestamp")
try:
    print(int(float(timestamp)))
except Exception:
    raise SystemExit(1)
PY
}

TREND_CACHE_MAX_AGE_SEC="${TREND_CACHE_MAX_AGE_SEC:-300}"
TREND_CACHE_PATH="${V5_TREND_CACHE_PATH:-$(resolve_trend_cache_path)}"
unset V5_USE_CACHED_TREND
if [[ -f "$TREND_CACHE_PATH" ]]; then
  if cache_ts="$(resolve_trend_cache_timestamp "$TREND_CACHE_PATH" 2>/dev/null)"; then
    cache_age=$(( NOW_EPOCH - cache_ts ))
    if (( cache_age >= 0 && cache_age <= TREND_CACHE_MAX_AGE_SEC )); then
      export V5_USE_CACHED_TREND="1"
    fi
  fi
fi

"$PYTHON_BIN" main.py
