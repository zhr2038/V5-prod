#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/admin/clawd/v5-trading-bot"
cd "$ROOT"

# UTC hour window id
WIN_ID="$(date -u +%Y%m%d_%H)"

# Window: last closed hour [start,end) in UTC seconds
NOW_EPOCH="$(date -u +%s)"
END_EPOCH=$(( NOW_EPOCH - (NOW_EPOCH % 3600) ))
START_EPOCH=$(( END_EPOCH - 3600 ))

echo "[V5-LIVE] WIN_ID=${WIN_ID} window=[${START_EPOCH}, ${END_EPOCH}) UTC"

# prevent overlap
LOCK="/tmp/v5_live.lock"
exec 9>"$LOCK"
flock -n 9 || exit 0

export PYTHONPATH="${PYTHONPATH:-.}"
export V5_DATA_PROVIDER="${V5_DATA_PROVIDER:-okx}"
export V5_CONFIG="${V5_CONFIG:-configs/live_small.yaml}"
export V5_LIVE_ARM="${V5_LIVE_ARM:-YES}"
export V5_RUN_ID="$WIN_ID"
export V5_WINDOW_START_TS="${START_EPOCH}"
export V5_WINDOW_END_TS="${END_EPOCH}"

python3 main.py
