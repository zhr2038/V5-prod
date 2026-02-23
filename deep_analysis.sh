#!/bin/bash
# V5 Trading Deep Analysis Script - 深度分析交易逻辑

BOT_DIR="/home/admin/clawd/v5-trading-bot"
DATE=$(date '+%Y-%m-%d %H:%M:%S')
REPORT_FILE="$BOT_DIR/logs/deep_analysis.log"

echo "========== V5 深度交易分析 [$DATE] ==========" >> $REPORT_FILE

# 获取最新日志
LATEST_LOG=$(ls -t $BOT_DIR/logs/v5_runtime.log 2>/dev/null | head -1)
if [ -z "$LATEST_LOG" ]; then
    echo "错误: 找不到运行日志" >> $REPORT_FILE
    exit 1
fi

# 分析市场状态
echo "--- 市场状态分析 ---" >> $REPORT_FILE
REGIME=$(grep "regime=" $LATEST_LOG | tail -1)
if [ -n "$REGIME" ]; then
    echo "$REGIME" >> $REPORT_FILE
    
    # 提取市场模式
    MODE=$(echo "$REGIME" | grep -oE "Risk-Off|Risk-On|Trending|Sideways" | head -1)
    echo "当前市场模式: $MODE" >> $REPORT_FILE
    
    case "$MODE" in
        "Risk-Off")
            echo "📉 风险规避模式: 降低仓位，优先卖出" >> $REPORT_FILE
            ;;
        "Risk-On")
            echo "📈 风险开启模式: 正常交易" >> $REPORT_FILE
            ;;
        "Trending")
            echo "🚀 趋势模式: 追涨杀跌" >> $REPORT_FILE
            ;;
        "Sideways")
            echo "↔️ 震荡模式: 高抛低吸" >> $REPORT_FILE
            ;;
    esac
fi

# 分析买入决策
echo "" >> $REPORT_FILE
echo "--- 买入决策分析 ---" >> $REPORT_FILE
BUYS=$(grep "TRADE_SAFETY.*buy" $LATEST_LOG | tail -20)
if [ -n "$BUYS" ]; then
    echo "$BUYS" | while read line; do
        SYMBOL=$(echo "$line" | grep -oE "buy [A-Z]+-USDT" | sed 's/buy //')
        INTENT=$(echo "$line" | grep -oE "intent=[A-Z_]+" | sed 's/intent=//')
        NOTIONAL=$(echo "$line" | grep -oE "notional=[0-9.]+" | sed 's/notional=//')
        
        echo "买入 $SYMBOL" >> $REPORT_FILE
        echo "  意图: $INTENT" >> $REPORT_FILE
        echo "  金额: \$${NOTIONAL}" >> $REPORT_FILE
        
        # 分析意图
        case "$INTENT" in
            "OPEN_LONG")
                echo "  📊 代码逻辑: 开新仓 - Alpha信号触发，该币种排名靠前" >> $REPORT_FILE
                ;;
            "REBALANCE")
                echo "  ⚖️ 代码逻辑: 再平衡 - 调整仓位至目标权重" >> $REPORT_FILE
                ;;
            "ADD_LONG")
                echo "  ➕ 代码逻辑: 加仓 - 趋势确认，增加持仓" >> $REPORT_FILE
                ;;
        esac
        echo "" >> $REPORT_FILE
    done
else
    echo "本周期无买入操作" >> $REPORT_FILE
fi

# 分析卖出决策
echo "" >> $REPORT_FILE
echo "--- 卖出决策分析 ---" >> $REPORT_FILE
SELLS=$(grep "TRADE_SAFETY.*sell" $LATEST_LOG | tail -30)
if [ -n "$SELLS" ]; then
    # 统计卖出原因
    REGIME_EXIT=$(echo "$SELLS" | grep -c "regime_exit")
    REBALANCE=$(echo "$SELLS" | grep -c "REBALANCE")
    STOP_LOSS=$(echo "$SELLS" | grep -c "STOP_LOSS")
    
    echo "卖出统计:" >> $REPORT_FILE
    echo "  - 市场状态退出: $REGIME_EXIT 笔" >> $REPORT_FILE
    echo "  - 再平衡: $REBALANCE 笔" >> $REPORT_FILE
    echo "  - 止损: $STOP_LOSS 笔" >> $REPORT_FILE
    echo "" >> $REPORT_FILE
    
    # 详细分析
    echo "$SELLS" | head -10 | while read line; do
        SYMBOL=$(echo "$line" | grep -oE "sell [A-Z]+-USDT" | sed 's/sell //')
        REASON=$(echo "$line" | grep -oE "reason=[a-z_]+" | sed 's/reason=//')
        NOTIONAL=$(echo "$line" | grep -oE "notional=[0-9.]+" | sed 's/notional=//')
        QTY=$(echo "$line" | grep -oE "local_qty=[0-9.e-]+" | sed 's/local_qty=//')
        
        if [ -n "$SYMBOL" ]; then
            echo "卖出 $SYMBOL" >> $REPORT_FILE
            echo "  原因: $REASON" >> $REPORT_FILE
            echo "  金额: \$${NOTIONAL}, 数量: $QTY" >> $REPORT_FILE
            
            case "$REASON" in
                "regime_exit")
                    echo "  📉 代码逻辑: 市场状态退出 - 该币种不在选中列表，清仓处理" >> $REPORT_FILE
                    ;;
                "rebalance_exit")
                    echo "  ⚖️ 代码逻辑: 再平衡卖出 - 仓位超过目标权重" >> $REPORT_FILE
                    ;;
                "stop_loss")
                    echo "  🛑 代码逻辑: 止损 - 价格跌破止损线" >> $REPORT_FILE
                    ;;
            esac
            echo "" >> $REPORT_FILE
        fi
    done
else
    echo "本周期无卖出操作" >> $REPORT_FILE
fi

# 分析未交易原因
echo "" >> $REPORT_FILE
echo "--- 未交易分析 ---" >> $REPORT_FILE

# 检查 Kill Switch
KILLS=$(grep -E "Kill Switch|kill_switch|ABORT LIVE" $LATEST_LOG | tail -5)
if [ -n "$KILLS" ]; then
    echo "🛑 Kill Switch 触发记录:" >> $REPORT_FILE
    echo "$KILLS" >> $REPORT_FILE
    echo "" >> $REPORT_FILE
    echo "代码逻辑: Kill Switch 是风控机制，当满足以下条件时触发:" >> $REPORT_FILE
    echo "  - 回撤超过阈值 (drawdown_trigger)" >> $REPORT_FILE
    echo "  - 波动率异常" >> $REPORT_FILE
    echo "  - 手动触发" >> $REPORT_FILE
fi

# 检查预检失败
PREFLIGHT=$(grep "LIVE_PREFLIGHT.*ABORT" $LATEST_LOG | tail -3)
if [ -n "$PREFLIGHT" ]; then
    echo "⚠️ 预检失败记录:" >> $REPORT_FILE
    echo "$PREFLIGHT" >> $REPORT_FILE
    echo "" >> $REPORT_FILE
fi

# 分析 Alpha 选择
echo "" >> $REPORT_FILE
echo "--- Alpha 选择分析 ---" >> $REPORT_FILE
ALPHA=$(grep "selected=" $LATEST_LOG | tail -1)
if [ -n "$ALPHA" ]; then
    SELECTED=$(echo "$ALPHA" | grep -oE "selected=\[[^\]]+\]")
    echo "选中币种: $SELECTED" >> $REPORT_FILE
    echo "" >> $REPORT_FILE
    echo "代码逻辑 (Alpha Engine):" >> $REPORT_FILE
    echo "  1. 计算多因子得分 (momentum, volume, volatility)" >> $REPORT_FILE
    echo "  2. 按得分排名，取前 20%" >> $REPORT_FILE
    echo "  3. 根据市场状态调整仓位" >> $REPORT_FILE
fi

# 合理性评估
echo "" >> $REPORT_FILE
echo "--- 交易合理性评估 ---" >> $REPORT_FILE

# 检查是否有异常
ERRORS=$(grep -iE "error|exception|failed" $LATEST_LOG | grep -v "fills sync/export" | tail -5)
if [ -n "$ERRORS" ]; then
    echo "❌ 发现错误:" >> $REPORT_FILE
    echo "$ERRORS" >> $REPORT_FILE
    echo "" >> $REPORT_FILE
fi

# 综合评估
echo "✅ 合理性判断:" >> $REPORT_FILE
if [ "$MODE" = "Risk-Off" ] && [ "$REGIME_EXIT" -gt 10 ]; then
    echo "  - Risk-Off 模式下大量卖出: ✅ 符合策略逻辑" >> $REPORT_FILE
fi

if echo "$BUYS" | grep -q "REBALANCE"; then
    echo "  - 再平衡买入: ✅ 仓位调整正常" >> $REPORT_FILE
fi

if [ -n "$KILLS" ]; then
    echo "  - Kill Switch 触发: ⚠️ 风控介入，需关注" >> $REPORT_FILE
fi

echo "" >> $REPORT_FILE
echo "========== 分析结束 ==========" >> $REPORT_FILE
echo "" >> $REPORT_FILE
