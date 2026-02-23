#!/bin/bash
# V5 Trading Bot Monitor Script

LOG_FILE="/home/admin/clawd/v5-trading-bot/logs/monitor.log"
BOT_DIR="/home/admin/clawd/v5-trading-bot"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$DATE] ====== V5 Bot Monitor Check ======" >> $LOG_FILE

# Check if bot is running
PID=$(ps aux | grep 'main.py' | grep -v grep | awk '{print $2}')
if [ -z "$PID" ]; then
    echo "[$DATE] ⚠️ Bot is NOT running" >> $LOG_FILE
    
    # Check last log
    if [ -f "$BOT_DIR/logs/v5_runtime.log" ]; then
        LAST_LOG=$(tail -5 $BOT_DIR/logs/v5_runtime.log)
        echo "[$DATE] Last 5 lines of log:" >> $LOG_FILE
        echo "$LAST_LOG" >> $LOG_FILE
    fi
else
    echo "[$DATE] ✅ Bot is running (PID: $PID)" >> $LOG_FILE
fi

# Check for errors in last hour
if [ -f "$BOT_DIR/logs/v5_runtime.log" ]; then
    ERRORS=$(grep -i "error\|exception\|failed" $BOT_DIR/logs/v5_runtime.log | tail -10)
    if [ -n "$ERRORS" ]; then
        echo "[$DATE] ⚠️ Recent errors found:" >> $LOG_FILE
        echo "$ERRORS" >> $LOG_FILE
    fi
fi

# Check trade audit
if [ -f "$BOT_DIR/logs/trade_audit.log" ]; then
    LAST_AUDIT=$(tail -3 $BOT_DIR/logs/trade_audit.log)
    echo "[$DATE] Last audit:" >> $LOG_FILE
    echo "$LAST_AUDIT" >> $LOG_FILE
fi

echo "[$DATE] ====== Monitor End ======" >> $LOG_FILE
echo "" >> $LOG_FILE
