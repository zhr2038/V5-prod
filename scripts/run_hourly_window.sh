#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WIN_ID="$(date +%Y%m%d_%H)"
NOW_EPOCH="$(date +%s)"
END_EPOCH=$(( NOW_EPOCH - (NOW_EPOCH % 3600) ))
START_EPOCH=$(( END_EPOCH - 3600 ))

echo "[V5] WIN_ID=${WIN_ID} window=[${START_EPOCH}, ${END_EPOCH}) CST"

LOCK="/tmp/v5_dryrun.lock"
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

export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export V5_DATA_PROVIDER="${V5_DATA_PROVIDER:-okx}"
export V5_RUN_ID="$WIN_ID"
export V5_WINDOW_START_TS="${START_EPOCH}"
export V5_WINDOW_END_TS="${END_EPOCH}"

resolve_v4_reports_dir() {
  local requested="${V4_REPORTS_DIR:-}"
  local candidate

  if [[ -n "$requested" ]]; then
    if [[ "$requested" == /* ]]; then
      printf '%s\n' "$requested"
    else
      printf '%s\n' "$ROOT/$requested"
    fi
    return 0
  fi

  for candidate in "$ROOT/v4_export" "$ROOT/reports/compare/v4_export"; do
    if [[ -d "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  return 1
}

"$PYTHON_BIN" main.py

V5_SUMMARY="reports/runs/${WIN_ID}/summary.json"
COMPARE_OUT="reports/compare/hourly/compare_${WIN_ID}.md"

if [[ ! -f "$V5_SUMMARY" ]]; then
  echo "[V5] skip compare_runs: missing $V5_SUMMARY"
  exit 0
fi

if V4_DIR="$(resolve_v4_reports_dir 2>/dev/null)" && [[ -d "$V4_DIR" ]]; then
  "$PYTHON_BIN" scripts/compare_runs.py \
    --v4_reports_dir "$V4_DIR" \
    --v5_summary "$V5_SUMMARY" \
    --out "$COMPARE_OUT"
else
  echo "[V5] skip compare_runs: V4 reports dir unavailable"
fi
