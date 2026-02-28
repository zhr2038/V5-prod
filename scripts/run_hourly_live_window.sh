#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/admin/clawd/v5-trading-bot"
cd "$ROOT"

# Hour window id (使用本地时间/北京时间)
WIN_ID="$(date +%Y%m%d_%H)"

# Window: last closed hour [start,end) in local seconds
NOW_EPOCH="$(date +%s)"
END_EPOCH=$(( NOW_EPOCH - (NOW_EPOCH % 3600) ))
START_EPOCH=$(( END_EPOCH - 3600 ))

echo "[V5-LIVE] WIN_ID=${WIN_ID} window=[${START_EPOCH}, ${END_EPOCH}) CST"

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
export V5_USE_CACHED_TREND="1"  # 使用 :57 预计算的趋势缓存

/home/admin/clawd/v5-trading-bot/.venv/bin/python main.py
