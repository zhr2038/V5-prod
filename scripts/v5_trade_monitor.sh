#!/bin/bash
# V5 交易监控报警脚本
# 每小时运行一次，检查交易状态

ALERT_HOURS=6  # 6小时无交易报警
CRITICAL_HOURS=12  # 12小时严重报警
CHAT_ID="5065024131"

# 获取最后一笔成交时间
LAST_TRADE=$(journalctl --user -u v5-prod.user.service --since "24 hours ago" --no-pager -n 500 | grep "FILLS_SYNC new_fills=" | grep -v "new_fills=0" | tail -1)

# 统计最近6小时成交
TRADE_COUNT=$(journalctl --user -u v5-prod.user.service --since "6 hours ago" --no-pager | grep -c "FILLS_SYNC new_fills=")
FILL_COUNT=$(journalctl --user -u v5-prod.user.service --since "6 hours ago" --no-pager | grep "FILLS_SYNC new_fills=" | grep -oP 'new_fills=\K[0-9]+' | awk '{sum+=$1} END {print sum+0}')

# 检查错误
BORROW_COUNT=$(journalctl --user -u v5-prod.user.service --since "2 hours ago" --no-pager | grep -c "borrow_detected")
ABORT_COUNT=$(journalctl --user -u v5-prod.user.service --since "2 hours ago" --no-pager | grep -c "ABORT")

# 获取当前市场状态
REGIME_FILE="/home/admin/clawd/v5-prod/reports/regime.json"
REGIME_STATE="unknown"
if [ -f "$REGIME_FILE" ]; then
    REGIME_STATE=$(cat "$REGIME_FILE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('state','unknown'))" 2>/dev/null || echo "unknown")
fi

# 分析是否需要报警
ALERT_MSG=""
PRIORITY="normal"

# 1. 检查是否有成交
if [ "$FILL_COUNT" -eq 0 ]; then
    # 6小时无成交
    if [ -n "$LAST_TRADE" ]; then
        ALERT_MSG="${ALERT_MSG}⚠️ 近6小时无成交\n"
    else
        ALERT_MSG="${ALERT_MSG}⚠️ 24小时内无成交记录\n"
        PRIORITY="critical"
    fi
fi

# 2. 检查错误
if [ "$BORROW_COUNT" -gt 0 ]; then
    ALERT_MSG="${ALERT_MSG}🚨 检测到借贷阻塞($BORROW_COUNT次)\n"
    PRIORITY="critical"
fi

if [ "$ABORT_COUNT" -gt 0 ]; then
    ALERT_MSG="${ALERT_MSG}🚨 交易被中止($ABORT_COUNT次)\n"
    PRIORITY="critical"
fi

# 3. 检查运行轮次
if [ "$TRADE_COUNT" -eq 0 ]; then
    ALERT_MSG="${ALERT_MSG}⚠️ 近6小时无交易轮次运行\n"
fi

# 4. 市场状态
if [ "$REGIME_STATE" = "Risk-Off" ]; then
    ALERT_MSG="${ALERT_MSG}ℹ️ 当前Risk-Off状态\n"
fi

# 发送报警
if [ -n "$ALERT_MSG" ]; then
    FULL_MSG="🤖 V5交易监控报警\n\n${ALERT_MSG}\n📊 统计: 近6小时${TRADE_COUNT}轮, ${FILL_COUNT}笔成交"
    
    # 使用 message 工具发送
    cd /home/admin/.openclaw/workspace
    /home/admin/.openclaw/workspace/.bin/openclaw message send --target "$CHAT_ID" --message "$FULL_MSG" 2>/dev/null || \
    echo "$FULL_MSG" >> /tmp/v5_alerts.log
    
    echo "[$(date)] 报警已发送: $ALERT_MSG"
else
    echo "[$(date)] 检查正常 - 近6小时${TRADE_COUNT}轮, ${FILL_COUNT}笔成交"
fi
