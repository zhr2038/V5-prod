#!/bin/bash
# 设置自动化数据采集的 crontab

set -e

echo "🕒 设置自动化数据采集调度"
echo "=" * 50

# 当前目录
WORKDIR="/home/admin/clawd/v5-trading-bot"
LOGFILE="$WORKDIR/reports/data_collector.log"

# 创建 crontab 配置
CRON_CONFIG="# V5 数据自动化采集
# 每30分钟收集市场数据
*/30 * * * * cd $WORKDIR && python3 scripts/auto_data_collector.py >> $LOGFILE 2>&1

# 每小时运行 V5（收集 alpha 数据）
0 * * * * cd $WORKDIR && export V5_CONFIG=configs/live_small.yaml && export V5_LIVE_ARM=YES && python3 main.py --run-id \"auto_\$(date +%Y%m%d_%H%M%S)\" >> $WORKDIR/reports/auto_runs.log 2>&1

# 每天凌晨清理旧日志
0 2 * * * find $WORKDIR/reports -name \"*.log\" -mtime +7 -delete
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
    crontab -l > /tmp/crontab_backup_$(date +%Y%m%d_%H%M%S) 2>/dev/null || true
    
    # 添加新配置
    (crontab -l 2>/dev/null; echo "$CRON_CONFIG") | crontab -
    
    echo "✅ Crontab 配置已添加"
    echo "当前 crontab:"
    crontab -l
else
    echo "⚠️  跳过 crontab 设置"
    echo "手动运行: python3 scripts/auto_data_collector.py"
fi

echo ""
echo "📋 手动测试命令:"
echo "1. 数据采集: cd $WORKDIR && python3 scripts/auto_data_collector.py"
echo "2. V5 运行: cd $WORKDIR && export V5_CONFIG=configs/live_small.yaml && export V5_LIVE_ARM=YES && python3 main.py"
echo "3. 查看日志: tail -f $LOGFILE"
echo ""
echo "🔍 监控命令:"
echo "1. 数据状态: python3 scripts/quick_ic_analysis.py"
echo "2. 数据库查询: sqlite3 reports/alpha_history.db"
echo "3. 采集状态: sqlite3 reports/alpha_history.db \"SELECT * FROM data_collection_status ORDER BY id DESC LIMIT 5;\""
echo "=" * 50