#!/usr/bin/env bash
#
# Monitor event-driven trading status
#

echo "=================================="
echo "Event-Driven Trading Monitor"
echo "=================================="
echo ""

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
else
  PYTHON_BIN="$(command -v python)"
fi

STANDARD_TIMER_UNIT="v5-prod.user.timer"

EVENT_RUNTIME_INFO="$("$PYTHON_BIN" - "$ROOT" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root))

from configs.runtime_config import load_runtime_config, resolve_runtime_path
from src.execution.fill_store import derive_runtime_named_artifact_path, derive_runtime_named_json_path

cfg = load_runtime_config(project_root=root)
execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
order_store_path = Path(
    resolve_runtime_path(
        execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
        default="reports/orders.sqlite",
        project_root=root,
    )
).resolve()

log_path = derive_runtime_named_artifact_path(order_store_path, "event_driven_log", ".jsonl")
cooldown_path = derive_runtime_named_json_path(order_store_path, "cooldown_state")

print(f"LOG_FILE={log_path}")
print(f"COOLDOWN_FILE={cooldown_path}")
PY
)"

eval "$EVENT_RUNTIME_INFO"

# Check timer status
echo "[Service Status]"
echo "  Standard V5 Timer: $(systemctl --user is-active "$STANDARD_TIMER_UNIT" 2>/dev/null || echo 'unknown')"
echo "  Event-Driven Timer: $(systemctl --user is-active v5-event-driven.timer 2>/dev/null || echo 'not running')"
echo ""

# Check last run
echo "[Last Run Times]"
echo "  Standard V5:"
systemctl --user list-timers "$STANDARD_TIMER_UNIT" --no-pager 2>/dev/null | grep -E "LEFT|NEXT" || echo "    No data"
echo ""
echo "  Event-Driven:"
systemctl --user list-timers v5-event-driven.timer --no-pager 2>/dev/null | grep -E "LEFT|NEXT" || echo "    No data"
echo ""

# Check event-driven logs
echo "[Event-Driven Log - Last 10 entries]"
if [ -f "$LOG_FILE" ]; then
    echo "  Log file: $LOG_FILE"
    echo ""
    tail -10 "$LOG_FILE" 2>/dev/null | while read line; do
        echo "  $line" | python3 -m json.tool 2>/dev/null || echo "  $line"
    done
else
    echo "  No log file yet (waiting for first run)"
fi
echo ""

# Check cooldown state
echo "[Cooldown Status]"
if [ -f "$COOLDOWN_FILE" ]; then
    echo "  Cooldown file: $COOLDOWN_FILE"
    "$PYTHON_BIN" - "$COOLDOWN_FILE" << 'EOF' 2>/dev/null
import json
from pathlib import Path
import time
import sys

data = json.loads(Path(sys.argv[1]).read_text())
now = time.time()

print(f"  Last global trade: {time.strftime('%H:%M:%S', time.localtime(data['last_global_trade_ms']/1000))}")
print(f"  Symbol cooldowns: {len(data.get('symbol_cooldowns', {}))}")
for sym, ts in list(data.get('symbol_cooldowns', {}).items())[:3]:
    remaining = max(0, (ts/1000 + 3600) - now)
    print(f"    {sym}: {remaining/60:.1f} min remaining")
if len(data.get('symbol_cooldowns', {})) > 3:
    print(f"    ... and {len(data['symbol_cooldowns']) - 3} more")
print(f"  Pending confirmations: {len(data.get('pending_signals', {}))}")
EOF
else
    echo "  No cooldown state yet"
fi
echo ""

# Check recent events
echo "[Recent System Logs - Event-Driven]"
journalctl --user -u v5-event-driven.service --since "1 hour ago" --no-pager 2>/dev/null | tail -15 || echo "  No logs yet"
echo ""

echo "=================================="
echo "Press Ctrl+C to exit"
echo "Run with -w flag to watch continuously: ./monitor_event_driven.sh -w"
echo "=================================="

# Watch mode
if [ "$1" == "-w" ] || [ "$1" == "--watch" ]; then
    echo ""
    echo "Watching... (refresh every 30 seconds)"
    while true; do
        sleep 30
        clear
        $0
    done
fi
