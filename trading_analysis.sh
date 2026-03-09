#!/bin/bash
# V5 Trading Analysis Script - 每小时分析交易情况

BOT_DIR="/home/admin/clawd/v5-prod"
DATE=$(date '+%Y-%m-%d %H:%M:%S')
REPORT_FILE="$BOT_DIR/logs/trading_analysis.log"

echo "========== V5 交易分析报告 [$DATE] ==========" >> $REPORT_FILE

# 获取最近一小时的交易数据
if [ -f "$BOT_DIR/logs/v5_runtime.log" ]; then
    # 查找最近一小时的交易记录
    RECENT_TRADES=$(grep -E "(TRADE_SAFETY|SELL_SAFETY|BUY_SAFETY|place failed)" $BOT_DIR/logs/v5_runtime.log | tail -50)
    
    if [ -n "$RECENT_TRADES" ]; then
        echo "--- 最近交易记录 ---" >> $REPORT_FILE
        echo "$RECENT_TRADES" >> $REPORT_FILE
        echo "" >> $REPORT_FILE
        
        # 分析买入操作
        BUYS=$(echo "$RECENT_TRADES" | grep -E "buy.*OPEN_LONG|buy.*REBALANCE")
        if [ -n "$BUYS" ]; then
            echo "--- 买入分析 ---" >> $REPORT_FILE
            echo "$BUYS" >> $REPORT_FILE
            
            # 检查是否有异常大额买入
            LARGE_BUYS=$(echo "$BUYS" | grep -E "notional=[5-9]|notional=[0-9]{2}")
            if [ -n "$LARGE_BUYS" ]; then
                echo "⚠️ 警告: 发现较大金额买入:" >> $REPORT_FILE
                echo "$LARGE_BUYS" >> $REPORT_FILE
            fi
            echo "" >> $REPORT_FILE
        fi
        
        # 分析卖出操作
        SELLS=$(echo "$RECENT_TRADES" | grep "sell")
        if [ -n "$SELLS" ]; then
            echo "--- 卖出分析 ---" >> $REPORT_FILE
            echo "$SELLS" >> $REPORT_FILE
            echo "" >> $REPORT_FILE
        fi
        
        # 检查失败交易
        FAILURES=$(echo "$RECENT_TRADES" | grep -E "place failed|NO_BORROW_SAFETY")
        if [ -n "$FAILURES" ]; then
            echo "❌ 失败交易:" >> $REPORT_FILE
            echo "$FAILURES" >> $REPORT_FILE
            echo "" >> $REPORT_FILE
        fi
    else
        echo "最近无交易记录" >> $REPORT_FILE
    fi
fi

# 检查市场状态
echo "--- 市场状态 ---" >> $REPORT_FILE
REGIME=$(grep "regime=" $BOT_DIR/logs/v5_runtime.log | tail -1)
if [ -n "$REGIME" ]; then
    echo "$REGIME" >> $REPORT_FILE
fi

# 检查 Kill Switch 触发情况
KILLS=$(grep -E "kill_switch|ABORT LIVE" $BOT_DIR/logs/v5_runtime.log | tail -5)
if [ -n "$KILLS" ]; then
    echo "" >> $REPORT_FILE
    echo "🛑 Kill Switch 记录:" >> $REPORT_FILE
    echo "$KILLS" >> $REPORT_FILE
fi

echo "========== 报告结束 ==========" >> $REPORT_FILE
echo "" >> $REPORT_FILE
