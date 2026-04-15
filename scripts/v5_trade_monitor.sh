#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

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
exec "$PYTHON_BIN" "$ROOT/scripts/v5_trade_monitor.py" --silent "$@"
