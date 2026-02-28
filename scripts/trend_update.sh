#!/bin/bash
# V5 趋势更新脚本 - 在交易前运行
# 在 :57 执行，计算趋势并缓存，供 :00 的交易程序使用

cd /home/admin/clawd/v5-trading-bot
source .venv/bin/activate

export V5_TREND_UPDATE_ONLY=1
export V5_CONFIG=configs/live_20u_real.yaml
export V5_DATA_PROVIDER=okx

# 运行趋势更新
python3 main.py 2>&1 | tee -a logs/trend_update.log

exit_code=${PIPESTATUS[0]}

if [ $exit_code -eq 0 ]; then
    echo "[$(date)] Trend update completed successfully" | tee -a logs/trend_update.log
else
    echo "[$(date)] Trend update failed with exit code $exit_code" | tee -a logs/trend_update.log
fi

exit $exit_code