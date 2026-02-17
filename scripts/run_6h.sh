#!/bin/bash
# 6小时运行一次的 V5 脚本
# 配合 cron: 0 */6 * * * cd /home/admin/clawd/v5-trading-bot && ./scripts/run_6h.sh

set -e

cd /home/admin/clawd/v5-trading-bot

# 设置环境变量
export V5_CONFIG=configs/live_small.yaml
export V5_LIVE_ARM=YES

# 运行时间戳
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_ID="6h_${TIMESTAMP}"

echo "========================================"
echo "V5 6h 运行开始: ${RUN_ID}"
echo "时间: $(date)"
echo "========================================"

# 运行 V5
python3 main.py --run-id "${RUN_ID}" 2>&1 | tee "reports/runs/${RUN_ID}/run.log"

echo "========================================"
echo "V5 6h 运行完成: ${RUN_ID}"
echo "时间: $(date)"
echo "========================================"

# 发送通知（可选）
# python3 scripts/send_notification.py --message "V5 6h run completed: ${RUN_ID}"