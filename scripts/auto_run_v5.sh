#!/bin/bash
# V5 小资金测试自动化运行脚本

set -e

echo "🚀 V5 小资金测试自动化运行"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=" * 60

# 设置环境变量
export V5_CONFIG=configs/live_20u_test.yaml
export V5_LIVE_ARM=YES
export V5_DATA_PROVIDER=okx  # 使用真实价格数据
export PYTHONPATH=.

# 运行ID
RUN_ID="auto_$(date +%Y%m%d_%H%M%S)"
echo "运行ID: $RUN_ID"
echo "配置: $V5_CONFIG"
echo "数据源: $V5_DATA_PROVIDER"

# 创建运行目录
RUN_DIR="reports/runs/$RUN_ID"
mkdir -p "$RUN_DIR"

# 运行 V5
echo "开始运行 V5..."
START_TIME=$(date +%s)

python3 main.py --run-id "$RUN_ID" 2>&1 | tee "$RUN_DIR/run.log"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "运行完成，耗时: ${DURATION}秒"

# 检查结果
SUMMARY_FILE="$RUN_DIR/summary.json"
if [ -f "$SUMMARY_FILE" ]; then
    echo ""
    echo "📊 运行结果:"
    echo "-----------"
    
    # 提取关键指标
    EQUITY_START=$(jq -r '.equity_start' "$SUMMARY_FILE")
    EQUITY_END=$(jq -r '.equity_end' "$SUMMARY_FILE")
    RETURN_PCT=$(jq -r '.total_return_pct' "$SUMMARY_FILE")
    NUM_TRADES=$(jq -r '.num_trades' "$SUMMARY_FILE")
    MAX_DD=$(jq -r '.max_drawdown_pct' "$SUMMARY_FILE")
    SHARPE=$(jq -r '.sharpe' "$SUMMARY_FILE")
    
    echo "测试资金: $EQUITY_START USDT"
    echo "实际余额: $EQUITY_END USDT"
    echo "计算回报: ${RETURN_PCT}%"
    echo "交易数量: $NUM_TRADES"
    echo "最大回撤: ${MAX_DD}%"
    echo "夏普比率: ${SHARPE}"
    
    # 检查 budget 数据
    if jq -e '.budget' "$SUMMARY_FILE" > /dev/null; then
        EXCEEDED=$(jq -r '.budget.exceeded' "$SUMMARY_FILE")
        FILLS_COUNT=$(jq -r '.budget.fills_count_today' "$SUMMARY_FILE")
        COST_BPS=$(jq -r '.budget.cost_used_bps' "$SUMMARY_FILE")
        
        echo ""
        echo "💰 Budget 状态:"
        echo "  超支: $EXCEEDED"
        echo "  今日fills: $FILLS_COUNT"
        echo "  成本使用: ${COST_BPS} bps"
    fi
    
    # 记录到日志文件
    LOG_ENTRY="$(date '+%Y-%m-%d %H:%M:%S'),$RUN_ID,$EQUITY_START,$EQUITY_END,$RETURN_PCT,$NUM_TRADES,$DURATION"
    echo "$LOG_ENTRY" >> "reports/auto_runs.csv"
    
    echo ""
    echo "✅ 运行记录已保存到: reports/auto_runs.csv"
    
else
    echo "❌ 未找到结果文件: $SUMMARY_FILE"
fi

# 运行数据采集（每3次运行执行一次）
RUN_COUNT=$(wc -l < "reports/auto_runs.csv" 2>/dev/null || echo "0")
if [ $((RUN_COUNT % 3)) -eq 0 ]; then
    echo ""
    echo "🔄 执行数据采集..."
    python3 scripts/auto_data_collector.py 2>&1 | tail -5
fi

# 运行借币监控（每5次运行执行一次）
if [ $((RUN_COUNT % 5)) -eq 0 ]; then
    echo ""
    echo "🔍 执行借币监控..."
    python3 scripts/borrow_monitor.py 2>&1 | head -10
fi

# 🔧 预防性修复：确保持仓同步和统计正确
echo ""
echo "🔧 运行预防性修复..."
python3 scripts/prevent_position_sync_bug.py "$RUN_ID"

echo ""
echo "📋 运行统计:"
echo "总运行次数: $RUN_COUNT"
echo "运行目录: $RUN_DIR"
echo "日志文件: reports/auto_runs.csv"
echo "=" * 60