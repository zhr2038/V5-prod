#!/usr/bin/env bash
#
# Monitor event-driven trading status
#

echo "=================================="
echo "Event-Driven Trading Monitor"
echo "=================================="
echo ""

ROOT="/home/admin/clawd/v5-trading-bot"

# Check timer status
echo "[Service Status]"
echo "  Standard V5 Timer: $(systemctl --user is-active v5-live-20u.user.timer 2>/dev/null || echo 'unknown')"
echo "  Event-Driven Timer: $(systemctl --user is-active v5-event-driven.timer 2>/dev/null || echo 'not running')"
echo ""

# Check last run
echo "[Last Run Times]"
echo "  Standard V5:"
systemctl --user list-timers v5-live-20u.user.timer --no-pager 2>/dev/null | grep -E "LEFT|NEXT" || echo "    No data"
echo ""
echo "  Event-Driven:"
systemctl --user list-timers v5-event-driven.timer --no-pager 2>/dev/null | grep -E "LEFT|NEXT" || echo "    No data"
echo ""

# Check event-driven logs
echo "[Event-Driven Log - Last 10 entries]"
LOG_FILE="$ROOT/reports/event_driven_log.jsonl"
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
COOLDOWN_FILE="$ROOT/reports/cooldown_state.json"
if [ -f "$COOLDOWN_FILE" ]; then
    echo "  Cooldown file: $COOLDOWN_FILE"
    python3 << 'EOF' 2>/dev/null
import json
from pathlib import Path
import time

data = json.loads(Path('/home/admin/clawd/v5-trading-bot/reports/cooldown_state.json').read_text())
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
