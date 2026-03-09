#!/usr/bin/env bash
set -euo pipefail

# Roll up yesterday's (UTC) cost events into daily stats.
# Intended to be called by cron.

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_DIR"

DAY_UTC="$(date -u +%Y%m%d)"

python3 scripts/rollup_costs.py --day "$DAY_UTC" --check_anomaly \
  --lookback_days 7 --anomaly_multiplier 2.0 --anomaly_abs_bps 30.0
