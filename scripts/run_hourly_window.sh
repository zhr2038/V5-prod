#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/admin/clawd/v5-trading-bot"
cd "$ROOT"

# UTC hour window id
WIN_ID="$(date -u +%Y%m%d_%H)"

# 计算窗口时间：上一小时整点 -> 当前小时整点（UTC秒）
NOW_EPOCH="$(date -u +%s)"
# 当前小时整点（UTC），作为 window_end
END_EPOCH=$(( NOW_EPOCH - (NOW_EPOCH % 3600) ))
# 上一小时整点，作为 window_start
START_EPOCH=$(( END_EPOCH - 3600 ))

echo "[V5] WIN_ID=${WIN_ID} window=[${START_EPOCH}, ${END_EPOCH}) UTC"

# prevent overlap
LOCK="/tmp/v5_dryrun.lock"
exec 9>"$LOCK"
flock -n 9 || exit 0

export V5_DATA_PROVIDER="${V5_DATA_PROVIDER:-okx}"
export V5_RUN_ID="$WIN_ID"
export V5_WINDOW_START_TS="${START_EPOCH}"
export V5_WINDOW_END_TS="${END_EPOCH}"

python3 main.py

python3 scripts/compare_runs.py \
  --v4_reports_dir /home/admin/clawd/v4-trading-bot/reports \
  --v5_summary "reports/runs/${WIN_ID}/summary.json" \
  --out "reports/compare/hourly/compare_${WIN_ID}.md"
