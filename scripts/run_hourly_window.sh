#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/admin/clawd/v5-trading-bot"
cd "$ROOT"

# UTC hour window id
WIN_ID="$(date -u +%Y%m%d_%H)"

# prevent overlap
LOCK="/tmp/v5_dryrun.lock"
exec 9>"$LOCK"
flock -n 9 || exit 0

export V5_DATA_PROVIDER="${V5_DATA_PROVIDER:-okx}"
export V5_RUN_ID="$WIN_ID"

python3 main.py

python3 scripts/compare_runs.py \
  --v4_reports_dir /home/admin/clawd/v4-trading-bot/reports \
  --v5_summary "reports/runs/${WIN_ID}/summary.json" \
  --out "reports/compare/hourly/compare_${WIN_ID}.md"
