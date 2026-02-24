#!/usr/bin/env python3
"""
V5 Web Dashboard - 交易可视化界面

功能：
- 账户总览
- 交易历史
- 币种评分
- K线图表
- 系统状态
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import os
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from flask import Flask, render_template, jsonify, send_from_directory
import pandas as pd
import yaml

app = Flask(__name__, template_folder='web/templates', static_folder='web/static')

# 配置路径
WORKSPACE = Path('/home/admin/clawd/v5-trading-bot')
REPORTS_DIR = WORKSPACE / 'reports'
CONFIG_PATH = WORKSPACE / 'configs/live_20u_real.yaml'


def get_db_connection():
    """获取数据库连接"""
    db_path = REPORTS_DIR / 'orders.sqlite'
    if db_path.exists():
        return sqlite3.connect(db_path)
    return None


def load_config():
    """加载配置"""
    try:
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    except:
        return {}


@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')


@app.route('/api/account')
def api_account():
    """账户信息API"""
    try:
        # 读取reconcile状态
        reconcile_file = REPORTS_DIR / 'reconcile_status.json'
        if reconcile_file.exists():
            with open(reconcile_file, 'r') as f:
                reconcile = json.load(f)
        else:
            reconcile = {}
        
        # 获取最新权益
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN side='buy' AND state='FILLED' THEN notional_usdt ELSE 0 END) as total_buy,
                    SUM(CASE WHEN side='sell' AND state='FILLED' THEN notional_usdt ELSE 0 END) as total_sell,
                    SUM(CASE WHEN state='FILLED' THEN fee ELSE 0 END) as total_fees
                FROM orders
            """)
            row = cursor.fetchone()
            conn.close()
            
            total_trades, total_buy, total_sell, total_fees = row
            
            # 计算已实现盈亏
            realized_pnl = (total_sell or 0) - (total_buy or 0) + (total_fees or 0)
        else:
            total_trades = total_buy = total_sell = total_fees = realized_pnl = 0
        
        # 从reconcile获取现金
        cash = reconcile.get('local_snapshot', {}).get('cash_usdt', 0)
        
        return jsonify({
            'cash_usdt': round(float(cash), 2),
            'total_trades': total_trades or 0,
            'total_buy': round(total_buy or 0, 2),
            'total_sell': round(total_sell or 0, 2),
            'total_fees': round(total_fees or 0, 4),
            'realized_pnl': round(realized_pnl, 2),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/trades')
def api_trades():
    """交易历史API"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify([])
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                inst_id, side, notional_usdt, fee, state,
                datetime(created_ts/1000, 'unixepoch') as time
            FROM orders 
            WHERE state='FILLED'
            ORDER BY created_ts DESC
            LIMIT 100
        """)
        
        trades = []
        for row in cursor.fetchall():
            trades.append({
                'symbol': row[0],
                'side': row[1],
                'amount': round(row[2], 4),
                'fee': round(row[3], 6),
                'state': row[4],
                'time': row[5]
            })
        
        conn.close()
        return jsonify(trades)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/positions')
def api_positions():
    """持仓信息API"""
    try:
        # 读取reconcile状态
        reconcile_file = REPORTS_DIR / 'reconcile_status.json'
        if not reconcile_file.exists():
            return jsonify([])
        
        with open(reconcile_file, 'r') as f:
            reconcile = json.load(f)
        
        positions = []
        ccy_qty = reconcile.get('local_snapshot', {}).get('ccy_qty', {})
        
        for symbol, qty in ccy_qty.items():
            if symbol != 'USDT' and float(qty) > 0.000001:
                positions.append({
                    'symbol': symbol,
                    'qty': float(qty),
                    'value_usdt': 0  # 需要实时价格
                })
        
        return jsonify(positions)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scores')
def api_scores():
    """币种评分API"""
    try:
        # 查找最新的决策文件
        runs_dir = REPORTS_DIR / 'runs'
        if not runs_dir.exists():
            return jsonify([])
        
        # 获取最新的run目录
        run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()], reverse=True)
        if not run_dirs:
            return jsonify([])
        
        latest_run = run_dirs[0]
        decision_file = latest_run / 'decision_audit.json'
        
        if not decision_file.exists():
            return jsonify([])
        
        with open(decision_file, 'r') as f:
            decision = json.load(f)
        
        scores = []
        top_scores = decision.get('top_scores', [])
        for item in top_scores[:20]:
            scores.append({
                'symbol': item['symbol'],
                'score': round(item['score'], 4)
            })
        
        return jsonify({
            'regime': decision.get('regime', 'Unknown'),
            'scores': scores
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/status')
def api_status():
    """系统状态API"""
    try:
        config = load_config()
        
        # 检查timer状态
        import subprocess
        result = subprocess.run(
            ['systemctl', '--user', 'status', 'v5-live-20u.user.timer'],
            capture_output=True, text=True
        )
        timer_active = 'active' in result.stdout.lower()
        
        return jsonify({
            'timer_active': timer_active,
            'mode': config.get('execution', {}).get('mode', 'unknown'),
            'dry_run': config.get('execution', {}).get('dry_run', True),
            'equity_cap': config.get('budget', {}).get('live_equity_cap_usdt', 0),
            'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/equity_history')
def api_equity_history():
    """权益曲线历史"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify([])
        
        # 按日期汇总盈亏
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                date(created_ts/1000, 'unixepoch') as date,
                SUM(CASE WHEN side='sell' THEN notional_usdt ELSE -notional_usdt END) as net_flow,
                SUM(fee) as fees
            FROM orders 
            WHERE state='FILLED'
            GROUP BY date
            ORDER BY date
        """)
        
        data = []
        for row in cursor.fetchall():
            data.append({
                'date': row[0],
                'net_flow': round(row[1] or 0, 2),
                'fees': round(row[2] or 0, 4)
            })
        
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("="*60)
    print("V5 Web Dashboard 启动中...")
    print("="*60)
    print(f"访问地址: http://0.0.0.0:5000")
    print("="*60)
    app.run(host='0.0.0.0', port=5000, debug=False)
