#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Ensure PYTHONPATH for local package imports
export PYTHONPATH=.

# Load env if present
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

# Roll up daily costs + anomaly detection (exit=2 on anomaly)
python3 scripts/rollup_costs.py "$@"
