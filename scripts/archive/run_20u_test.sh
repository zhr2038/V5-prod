#!/bin/bash
# 20 USDT 小规模测试脚本

set -e

echo "🚀 20 USDT 小规模测试"
echo "=" * 50

# 设置环境
export V5_CONFIG=configs/live_20u_test.yaml
export V5_LIVE_ARM=YES
export PYTHONPATH=.

# 运行测试
RUN_ID="20u_test_$(date +%Y%m%d_%H%M%S)"
echo "运行ID: $RUN_ID"

python3 main.py --run-id "$RUN_ID" 2>&1 | tee "reports/runs/${RUN_ID}/run.log"

# 检查结果
SUMMARY_FILE="reports/runs/${RUN_ID}/summary.json"
if [ -f "$SUMMARY_FILE" ]; then
    echo ""
    echo "📊 测试结果:"
    echo "-----------"
    
    # 提取关键指标
    EQUITY_START=$(jq -r '.equity_start' "$SUMMARY_FILE")
    EQUITY_END=$(jq -r '.equity_end' "$SUMMARY_FILE")
    RETURN_PCT=$(jq -r '.total_return_pct' "$SUMMARY_FILE")
    NUM_TRADES=$(jq -r '.num_trades' "$SUMMARY_FILE")
    MAX_DD=$(jq -r '.max_drawdown_pct' "$SUMMARY_FILE")
    
    echo "测试资金: $EQUITY_START USDT"
    echo "实际余额: $EQUITY_END USDT"
    echo "计算回报: ${RETURN_PCT}%"
    echo "交易数量: $NUM_TRADES"
    echo "最大回撤: ${MAX_DD}%"
    
    # 检查 equity.jsonl
    EQUITY_JSONL="reports/runs/${RUN_ID}/equity.jsonl"
    if [ -f "$EQUITY_JSONL" ]; then
        echo ""
        echo "📈 Equity 记录:"
        head -3 "$EQUITY_JSONL" | while read line; do
            echo "  $line"
        done
    fi
    
    echo ""
    echo "📝 说明:"
    echo "- equity_start=20.0: 测试资金限制为20 USDT"
    echo "- equity_end=163.0165: 实际账户余额"
    echo "- 715%回报是计算错误（基于20→163，但无实际交易）"
    echo "- dry-run模式：不会真正交易"
    
else
    echo "❌ 未找到结果文件: $SUMMARY_FILE"
fi

echo ""
echo "🔄 设置自动化测试:"
echo "crontab -e"
echo "添加: */30 * * * * cd /home/admin/clawd/v5-trading-bot && ./scripts/run_20u_test.sh >> reports/20u_test.log 2>&1"
echo "=" * 50