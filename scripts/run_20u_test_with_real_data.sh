#!/bin/bash
# 20 USDT 小规模测试（使用真实价格数据）

set -e

echo "🚀 20 USDT 小规模测试（真实价格数据）"
echo "=" * 50

# 设置环境
export V5_CONFIG=configs/live_20u_test.yaml
export V5_LIVE_ARM=YES
export V5_DATA_PROVIDER=okx  # 使用真实价格数据！
export PYTHONPATH=.

# 运行测试
RUN_ID="20u_real_$(date +%Y%m%d_%H%M%S)"
echo "运行ID: $RUN_ID"
echo "数据源: OKX 真实价格"

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
    
    echo "equity_start: $EQUITY_START USDT"
    echo "equity_end: $EQUITY_END USDT"
    echo "计算回报: ${RETURN_PCT}%"
    echo "交易数量: $NUM_TRADES"
    
    # 检查 equity 是否合理
    if (( $(echo "$EQUITY_END > 150" | bc -l) )); then
        echo "⚠️  警告: equity_end ($EQUITY_END) 可能计算错误"
        echo "   应该是 ~112.56 USDT"
    else
        echo "✅ equity 计算正常"
    fi
    
    # 检查 equity.jsonl
    EQUITY_JSONL="reports/runs/${RUN_ID}/equity.jsonl"
    if [ -f "$EQUITY_JSONL" ]; then
        echo ""
        echo "📈 Equity 记录:"
        head -3 "$EQUITY_JSONL" | while read line; do
            echo "  $line"
        done
    fi
    
else
    echo "❌ 未找到结果文件: $SUMMARY_FILE"
fi

echo ""
echo "🔧 配置说明:"
echo "- V5_DATA_PROVIDER=okx: 使用 OKX 真实价格数据"
echo "- live_equity_cap_usdt: 20.0: 测试资金限制为20 USDT"
echo "- dry-run模式: 不会真正交易"
echo "=" * 50