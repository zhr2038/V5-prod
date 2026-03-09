#!/bin/bash
# 启动新的 V5 Monitor Dashboard

echo "Starting V5 Monitor Dashboard..."
cd /home/admin/clawd/v5-prod

# 检查依赖
if [ ! -d ".venv" ]; then
    echo "Error: .venv not found"
    exit 1
fi

source .venv/bin/activate

# 启动 Flask 服务
export PYTHONPATH=/home/admin/clawd/v5-prod
export FLASK_APP=scripts/web_dashboard.py

# 使用 monitor.html 作为入口
python3 -c "
import sys
sys.path.insert(0, '/home/admin/clawd/v5-prod')

from flask import Flask, send_from_directory
from scripts.web_dashboard import app

# 添加 monitor.html 路由
@app.route('/monitor')
def monitor():
    return send_from_directory('/home/admin/clawd/v5-prod/web', 'monitor.html')

# 也作为默认首页
@app.route('/')
def index():
    return send_from_directory('/home/admin/clawd/v5-prod/web', 'monitor.html')

if __name__ == '__main__':
    print('='*60)
    print('V5 Monitor Dashboard')
    print('='*60)
    print('访问地址: http://0.0.0.0:5000/monitor')
    print('或: http://0.0.0.0:5000/')
    print('='*60)
    app.run(host='0.0.0.0', port=5000, debug=False)
"