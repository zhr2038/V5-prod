#!/bin/bash
# V5 Trading Bot Monitor Script - Updated for v5-live-20u service

LOG_FILE="/home/admin/clawd/v5-prod/logs/monitor.log"
BOT_DIR="/home/admin/clawd/v5-prod"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$DATE] ====== V5 Bot Monitor Check ======" >> $LOG_FILE

# Check if v5-live-20u.timer is enabled (systemd user timer)
export XDG_RUNTIME_DIR=/run/user/$(id -u)
TIMER_STATUS=$(systemctl --user is-enabled v5-live-20u.timer 2>/dev/null)
if [ "$TIMER_STATUS" = "enabled" ]; then
    echo "[$DATE] ✅ v5-live-20u.timer is enabled" >> $LOG_FILE
    
    # Check last run time
    LAST_RUN=$(systemctl --user list-timers v5-live-20u.timer --no-pager 2>/dev/null | grep v5-live-20u | awk '{print $1, $2}')
    if [ -n "$LAST_RUN" ]; then
        echo "[$DATE] Last/Next run: $LAST_RUN" >> $LOG_FILE
    fi
else
    echo "[$DATE] ⚠️ v5-live-20u.timer is NOT enabled (status: $TIMER_STATUS)" >> $LOG_FILE
fi

# Check if there's a flock lock (indicating running job)
if [ -f /tmp/v5_live_20u_hourly.lock ]; then
    PID=$(flock -n /tmp/v5_live_20u_hourly.lock -c 'echo not_locked' 2>/dev/null || echo "locked")
    if [ "$PID" = "locked" ]; then
        echo "[$DATE] 🔒 v5-live-20u is currently RUNNING (flock active)" >> $LOG_FILE
    else
        echo "[$DATE] 🔓 v5-live-20u is NOT running (flock free)" >> $LOG_FILE
    fi
else
    echo "[$DATE] 🔓 No lock file found (not running)" >> $LOG_FILE
fi

# Check last runtime log
echo "[$DATE] --- Last Runtime Log (last 30 lines) ---" >> $LOG_FILE
if [ -f "$BOT_DIR/logs/v5_runtime.log" ]; then
    tail -30 $BOT_DIR/logs/v5_runtime.log >> $LOG_FILE 2>&1
else
    echo "[$DATE] No runtime log found" >> $LOG_FILE
fi

# Check for errors in runtime log
echo "[$DATE] --- Recent Errors/Warnings ---" >> $LOG_FILE
if [ -f "$BOT_DIR/logs/v5_runtime.log" ]; then
    ERRORS=$(grep -iE "error|exception|failed|warning|kill_switch" $BOT_DIR/logs/v5_runtime.log | tail -10)
    if [ -n "$ERRORS" ]; then
        echo "$ERRORS" >> $LOG_FILE
    else
        echo "[$DATE] No errors/warnings found" >> $LOG_FILE
    fi
fi

# Check trade audit
echo "[$DATE] --- Trade Audit ---" >> $LOG_FILE
if [ -f "$BOT_DIR/logs/trade_audit.log" ]; then
    tail -5 $BOT_DIR/logs/trade_audit.log >> $LOG_FILE
else
    echo "[$DATE] No audit log found" >> $LOG_FILE
fi

echo "[$DATE] ====== Monitor End ======" >> $LOG_FILE
echo "" >> $LOG_FILE
