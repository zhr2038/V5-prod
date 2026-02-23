#!/bin/bash
# 方案B启动脚本

echo "🚀 启动方案B：正常交易加速积累真实数据"
echo "=" * 60

# 1. 检查当前状态
echo "📊 检查当前状态..."
CURRENT_FILLS=$(sqlite3 reports/orders.sqlite "SELECT COUNT(*) FROM orders WHERE state = 'FILLED';" 2>/dev/null || echo "0")
echo "当前已有FILLED订单: $CURRENT_FILLS 个"

# 2. 备份当前配置
echo "🔧 备份当前配置..."
cp configs/live_20u_test.yaml configs/live_20u_test_backup_$(date +%Y%m%d_%H%M%S).yaml 2>/dev/null || true

# 3. 应用方案B配置
echo "⚙️ 应用方案B配置..."
cp configs/live_normal_accelerated.yaml configs/live_20u_test.yaml 2>/dev/null || true

# 4. 显示配置摘要
echo ""
echo "📋 方案B配置摘要:"
echo "  - 目标: 3天内积累50+真实交易数据"
echo "  - 策略: 适度加速，保持稳定性"
echo "  - 调仓间隔: 45分钟（适度加速）"
echo "  - Deadband: 降低至0.04（增加敏感度）"
echo "  - 币种选择: 动态universe，前25个"
echo "  - 风险控制: 适度分散，最大仓位20%"
echo "  - 模式: dry-run（安全）"

# 5. 启动交易机器人
echo ""
echo "🤖 启动交易机器人..."
echo "命令: python3 src/main.py --config configs/live_20u_test.yaml --start"
echo ""
echo "📝 建议在后台运行:"
echo "  nohup python3 src/main.py --config configs/live_20u_test.yaml --start > logs/plan_b_$(date +%Y%m%d_%H%M%S).log 2>&1 &"
echo ""
echo "📊 查看日志:"
echo "  tail -f logs/plan_b_*.log"

# 6. 启动监控
echo ""
echo "👁️ 启动进度监控..."
echo "命令: python3 scripts/plan_b_monitor.py"
echo ""
echo "📈 监控功能:"
echo "  - 实时跟踪fills积累进度"
echo "  - 自适应参数调整建议"
echo "  - 分布分析和质量检查"
echo "  - 预计完成时间计算"

# 7. 预期时间表
echo ""
echo "⏱️ 预期时间表:"
echo "  第1天: 目标15-20个fills"
echo "  第2天: 累计30-40个fills"
echo "  第3天: 达成50+ fills目标"
echo ""
echo "📊 每日检查点:"
echo "  09:00 - 检查前一日进度"
echo "  14:00 - 检查当日进度，调整参数"
echo "  20:00 - 检查全天进度，总结"

# 8. 风险提示
echo ""
echo "⚠️ 风险提示:"
echo "  - 保持dry-run模式，无资金风险"
echo "  - 适度加速，避免过度交易"
echo "  - 监控系统负载，确保稳定性"
echo "  - 定期备份数据，防止意外"

# 9. 成功标准
echo ""
echo "🎯 成功标准:"
echo "  ✅ 3天内积累50+真实交易fills"
echo "  ✅ 币种分布合理（≥8个不同币种）"
echo "  ✅ 时间分布均匀（覆盖不同时段）"
echo "  ✅ 成本数据真实（非默认6bps费用）"

# 10. 下一步行动
echo ""
echo "🚀 下一步行动:"
echo "  1. 启动交易机器人（上述命令）"
echo "  2. 启动监控脚本（上述命令）"
echo "  3. 每日检查进度报告"
echo "  4. 根据建议调整参数"
echo "  5. 达成目标后恢复原配置"

echo ""
echo "=" * 60
echo "✅ 方案B启动准备完成"
echo "=" * 60

# 11. 可选：直接启动
echo ""
read -p "是否直接启动交易机器人？(y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "启动交易机器人..."
    nohup python3 src/main.py --config configs/live_20u_test.yaml --start > logs/plan_b_$(date +%Y%m%d_%H%M%S).log 2>&1 &
    echo "✅ 交易机器人已启动（后台运行）"
    echo "日志文件: logs/plan_b_$(date +%Y%m%d_%H%M%S).log"
    
    echo ""
    read -p "是否启动监控脚本？(y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "启动监控脚本..."
        python3 scripts/plan_b_monitor.py
    else
        echo "可以稍后手动运行: python3 scripts/plan_b_monitor.py"
    fi
else
    echo "可以稍后手动执行上述命令"
fi

echo ""
echo "📞 如有问题，检查日志文件或重新运行此脚本"