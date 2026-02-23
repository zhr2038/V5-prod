#!/bin/bash
# 20 USDT 实盘小资金测试启动脚本

set -e

echo "🚀 20 USDT 实盘小资金测试启动"
echo "=" * 60
echo "⚠️  警告：这将进行真实交易！"
echo "⚠️  测试资金：20 USDT"
echo "=" * 60

# 确认
read -p "确认开始 20 USDT 实盘测试？(输入 YES 确认): " -r
echo
if [[ ! $REPLY =~ ^YES$ ]]; then
    echo "❌ 测试取消"
    exit 1
fi

# 设置环境变量
export V5_CONFIG=configs/live_20u_real.yaml
export V5_LIVE_ARM=YES
export V5_DATA_PROVIDER=okx
export PYTHONPATH=.

# 运行ID
RUN_ID="real_20u_$(date +%Y%m%d_%H%M%S)"
echo "运行ID: $RUN_ID"
echo "配置: $V5_CONFIG"
echo "模式: 实盘交易 (dry_run=false)"

# 安全检查
echo ""
echo "🔒 安全检查:"
echo "1. 检查账户余额..."
python3 -c "
from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient
cfg = load_config('$V5_CONFIG', env_path='.env')
okx = OKXPrivateClient(exchange=cfg.exchange)
resp = okx.get_balance()
if resp.data and 'data' in resp.data:
    account = resp.data['data'][0]
    total_eq = float(account.get('totalEq', 0))
    print(f'✅ 账户总权益: {total_eq:.4f} USDT')
    
    # 检查 USDT 余额
    for detail in account.get('details', []):
        if detail.get('ccy') == 'USDT':
            eq = float(detail.get('eq', 0))
            avail = float(detail.get('availBal', 0))
            print(f'✅ USDT 余额: {eq:.4f}')
            print(f'✅ 可用余额: {avail:.4f}')
            
            if avail < 20:
                print(f'❌ 可用余额不足 20 USDT (当前: {avail:.4f})')
                exit(1)
else:
    print('❌ 无法获取账户数据')
    exit(1)
" 2>&1

if [ $? -ne 0 ]; then
    echo "❌ 安全检查失败"
    exit 1
fi

echo "2. 检查借币状态..."
python3 scripts/borrow_monitor.py 2>&1 | head -10

echo "3. 检查 kill switch..."
if [ -f "reports/kill_switch.json" ]; then
    KILL_STATUS=$(jq -r '.enabled' reports/kill_switch.json 2>/dev/null || echo "false")
    if [ "$KILL_STATUS" = "true" ]; then
        echo "❌ kill switch 已启用，无法交易"
        exit 1
    else
        echo "✅ kill switch 未启用"
    fi
else
    echo "✅ 无 kill switch 文件"
fi

# 确认开始
echo ""
echo "=" * 60
echo "🎯 准备开始实盘测试"
echo "测试资金: 20 USDT"
echo "风险限制: 单币种最大仓位 25%"
echo "交易模式: 真实交易"
echo "=" * 60

read -p "最后确认：开始 20 USDT 实盘测试？(输入 CONFIRM 确认): " -r
echo
if [[ ! $REPLY =~ ^CONFIRM$ ]]; then
    echo "❌ 测试取消"
    exit 1
fi

# 开始运行
echo ""
echo "🚀 开始实盘交易..."
START_TIME=$(date +%s)

python3 main.py --run-id "$RUN_ID" 2>&1 | tee "reports/runs/$RUN_ID/run.log"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "✅ 实盘测试完成，耗时: ${DURATION}秒"

# 检查结果
SUMMARY_FILE="reports/runs/$RUN_ID/summary.json"
if [ -f "$SUMMARY_FILE" ]; then
    echo ""
    echo "📊 实盘测试结果:"
    echo "---------------"
    
    # 提取关键指标
    EQUITY_START=$(jq -r '.equity_start' "$SUMMARY_FILE")
    EQUITY_END=$(jq -r '.equity_end' "$SUMMARY_FILE")
    RETURN_PCT=$(jq -r '.total_return_pct' "$SUMMARY_FILE")
    NUM_TRADES=$(jq -r '.num_trades' "$SUMMARY_FILE")
    FEES=$(jq -r '.fees_usdt_total' "$SUMMARY_FILE")
    COST=$(jq -r '.cost_usdt_total' "$SUMMARY_FILE")
    
    echo "测试资金: $EQUITY_START USDT"
    echo "结束权益: $EQUITY_END USDT"
    echo "回报率: ${RETURN_PCT}%"
    echo "交易数量: $NUM_TRADES"
    echo "手续费: ${FEES} USDT"
    echo "总成本: ${COST} USDT"
    
    # 检查是否有真实交易
    if [ "$NUM_TRADES" -gt 0 ]; then
        echo "🎉 完成 $NUM_TRADES 笔真实交易！"
        
        # 检查交易记录
        TRADES_FILE="reports/runs/$RUN_ID/trades.csv"
        if [ -f "$TRADES_FILE" ]; then
            echo ""
            echo "📋 交易记录:"
            cat "$TRADES_FILE"
        fi
    else
        echo "⚠️  无交易执行（可能被风控阻止）"
    fi
    
    # 记录到实盘日志
    LOG_ENTRY="$(date '+%Y-%m-%d %H:%M:%S'),$RUN_ID,$EQUITY_START,$EQUITY_END,$RETURN_PCT,$NUM_TRADES,$FEES,$COST"
    echo "$LOG_ENTRY" >> "reports/real_trading_log.csv"
    
    echo ""
    echo "📝 记录已保存到: reports/real_trading_log.csv"
    
else
    echo "❌ 未找到结果文件"
fi

# 更新自动化配置（切换到实盘）
echo ""
echo "🔄 更新自动化配置..."
sed -i 's/dry_run: true/dry_run: false/g' configs/live_20u_real.yaml 2>/dev/null || true
sed -i 's/dry_run: false  # 关键：启用真实交易！/dry_run: false/g' configs/live_20u_real.yaml 2>/dev/null || true

echo ""
echo "📋 后续步骤:"
echo "1. 查看详细日志: tail -f reports/runs/$RUN_ID/run.log"
echo "2. 检查账户余额: python3 scripts/borrow_monitor.py"
echo "3. 监控运行状态: python3 scripts/monitor_auto_runs.py"
echo "4. 查看交易记录: cat reports/runs/$RUN_ID/trades.csv"
echo "=" * 60