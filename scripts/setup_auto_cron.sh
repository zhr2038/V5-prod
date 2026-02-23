#!/bin/bash
# 设置 V5 自动化运行 crontab

set -e

echo "🕒 设置 V5 自动化运行调度"
echo "=" * 60

# 当前目录
WORKDIR="/home/admin/clawd/v5-trading-bot"
AUTO_SCRIPT="$WORKDIR/scripts/auto_run_v5.sh"
LOG_FILE="$WORKDIR/reports/auto_runs.log"
CSV_FILE="$WORKDIR/reports/auto_runs.csv"

# 创建必要的目录和文件
mkdir -p "$WORKDIR/reports/runs"
touch "$LOG_FILE"
touch "$CSV_FILE"

# 创建 crontab 配置
CRON_CONFIG="# ============================================
# V5 小资金测试自动化运行配置
# ============================================

# 每小时运行 V5（收集 alpha 数据）
0 * * * * cd $WORKDIR && $AUTO_SCRIPT >> $LOG_FILE 2>&1

# 每30分钟运行数据采集
*/30 * * * * cd $WORKDIR && export V5_DATA_PROVIDER=okx && python3 scripts/auto_data_collector.py >> $WORKDIR/reports/data_collector.log 2>&1

# 每天凌晨2点运行借币监控
0 2 * * * cd $WORKDIR && export V5_LIVE_ARM=YES && export PYTHONPATH=. && python3 scripts/borrow_monitor.py >> $WORKDIR/reports/borrow_monitor.log 2>&1

# 每天凌晨3点运行 IC 分析
0 3 * * * cd $WORKDIR && python3 scripts/quick_ic_analysis.py >> $WORKDIR/reports/ic_analysis.log 2>&1

# 每天凌晨4点清理旧日志（保留7天）
0 4 * * * find $WORKDIR/reports -name \"*.log\" -mtime +7 -delete
0 4 * * * find $WORKDIR/reports/runs -type d -mtime +14 -exec rm -rf {} \; 2>/dev/null || true

# 每周日凌晨5点运行参数优化
0 5 * * 0 cd $WORKDIR && python3 scripts/evaluate_parameter_sweep.py >> $WORKDIR/reports/parameter_sweep.log 2>&1
"

echo "Crontab 配置:"
echo "-------------"
echo "$CRON_CONFIG"
echo "-------------"

# 询问是否添加到 crontab
read -p "是否添加到 crontab？(y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    # 备份当前 crontab
    BACKUP_FILE="/tmp/crontab_backup_$(date +%Y%m%d_%H%M%S)"
    crontab -l > "$BACKUP_FILE" 2>/dev/null || true
    echo "✅ 当前 crontab 已备份到: $BACKUP_FILE"
    
    # 添加新配置
    (crontab -l 2>/dev/null; echo "$CRON_CONFIG") | crontab -
    
    echo "✅ Crontab 配置已添加"
    echo ""
    echo "📋 当前 crontab:"
    crontab -l
else
    echo "⚠️  跳过 crontab 设置"
    echo "手动运行命令: $AUTO_SCRIPT"
fi

echo ""
echo "🔧 手动测试命令:"
echo "1. 运行 V5: $AUTO_SCRIPT"
echo "2. 数据采集: cd $WORKDIR && python3 scripts/auto_data_collector.py"
echo "3. IC 分析: cd $WORKDIR && python3 scripts/quick_ic_analysis.py"
echo "4. 借币监控: cd $WORKDIR && export V5_LIVE_ARM=YES && python3 scripts/borrow_monitor.py"
echo ""
echo "📁 日志文件:"
echo "- V5 运行: $LOG_FILE"
echo "- 数据采集: $WORKDIR/reports/data_collector.log"
echo "- IC 分析: $WORKDIR/reports/ic_analysis.log"
echo "- 运行记录: $CSV_FILE"
echo ""
echo "📊 监控命令:"
echo "1. 查看最新运行: ls -la $WORKDIR/reports/runs/ | tail -5"
echo "2. 查看运行统计: tail -10 $CSV_FILE"
echo "3. 查看日志: tail -f $LOG_FILE"
echo "4. 数据状态: python3 scripts/data_monitor.py"
echo "=" * 60