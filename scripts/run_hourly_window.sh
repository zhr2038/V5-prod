#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WIN_ID="$(date -u +%Y%m%d_%H)"
NOW_EPOCH="$(date -u +%s)"
END_EPOCH=$(( NOW_EPOCH - (NOW_EPOCH % 3600) ))
START_EPOCH=$(( END_EPOCH - 3600 ))
V4_REPORTS_DIR="${V4_REPORTS_DIR:-$ROOT/v4_export}"

echo "[V5] WIN_ID=${WIN_ID} window=[${START_EPOCH}, ${END_EPOCH}) UTC"

LOCK="/tmp/v5_dryrun.lock"
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

"$PYTHON_BIN" main.py
"$PYTHON_BIN" scripts/compare_runs.py \
  --v4_reports_dir "$V4_REPORTS_DIR" \
  --v5_summary "reports/runs/${WIN_ID}/summary.json" \
  --out "reports/compare/hourly/compare_${WIN_ID}.md"
