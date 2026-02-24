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

app = Flask(__name__, 
            template_folder='/home/admin/clawd/v5-trading-bot/web/templates', 
            static_folder='/home/admin/clawd/v5-trading-bot/web/static')

# 配置路径
WORKSPACE = Path('/home/admin/clawd/v5-trading-bot')
REPORTS_DIR = WORKSPACE / 'reports'
CONFIG_PATH = WORKSPACE / 'configs/live_20u_real.yaml'

# 排除测试/异常数据
EXCLUDED_SYMBOLS = ['PEPE-USDT', 'MERL-USDT', 'SPACE-USDT']


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
    """主页面 - 综合监控面板"""
    return render_template('index.html')


@app.route('/monitor')
def monitor():
    """旧版监控页面（保留兼容）"""
    return render_template('monitor.html')


@app.route('/simple')
def simple_dashboard():
    """简洁版监控页"""
    return render_template('monitor.html')


@app.route('/<path:filename>')
def static_files(filename):
    """提供React静态文件"""
    react_build_path = '/home/admin/v5-trading-dashboard/dist'
    file_path = os.path.join(react_build_path, filename)
    
    # 检查文件是否存在
    if os.path.exists(file_path) and os.path.isfile(file_path):
        # 根据扩展名设置Content-Type
        content_types = {
            '.js': 'application/javascript',
            '.css': 'text/css',
            '.html': 'text/html',
            '.json': 'application/json',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.svg': 'image/svg+xml',
        }
        ext = os.path.splitext(filename)[1]
        content_type = content_types.get(ext, 'application/octet-stream')
        
        with open(file_path, 'rb') as f:
            return f.read(), 200, {'Content-Type': content_type}
    
    # 如果文件不存在，返回index.html（支持React Router）
    index_path = os.path.join(react_build_path, 'index.html')
    if os.path.exists(index_path):
        with open(index_path, 'r') as f:
            return f.read(), 200, {'Content-Type': 'text/html'}
    
    return 'Not found', 404


@app.route('/api/account')
def api_account():
    """账户信息API"""
    try:
        # 读取reconcile状态
        reconcile_file = REPORTS_DIR / 'reconcile_status.json'
        cash = 0
        if reconcile_file.exists():
            with open(reconcile_file, 'r') as f:
                reconcile = json.load(f)
            cash = reconcile.get('local_snapshot', {}).get('cash_usdt', 0)
        
        # 获取最新权益 - 排除异常数据
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            # 使用参数化查询排除异常币种
            placeholders = ','.join(['?' for _ in EXCLUDED_SYMBOLS])
            query = f"""
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN side='buy' AND state='FILLED' THEN notional_usdt ELSE 0 END) as total_buy,
                    SUM(CASE WHEN side='sell' AND state='FILLED' THEN notional_usdt ELSE 0 END) as total_sell,
                    SUM(CASE WHEN state='FILLED' THEN fee ELSE 0 END) as total_fees
                FROM orders
                WHERE inst_id NOT IN ({placeholders})
                AND notional_usdt < 1000  -- 排除异常大额
            """
            cursor.execute(query, EXCLUDED_SYMBOLS)
            row = cursor.fetchone()
            conn.close()
            
            total_trades = row[0] or 0
            total_buy = row[1] or 0
            total_sell = row[2] or 0
            total_fees = row[3] or 0
            
            # 计算已实现盈亏
            realized_pnl = float(total_sell) - float(total_buy) + float(total_fees)
        else:
            total_trades = total_buy = total_sell = total_fees = realized_pnl = 0
        
        return jsonify({
            'cash_usdt': round(float(cash), 2),
            'total_trades': int(total_trades),
            'total_buy': round(float(total_buy), 2),
            'total_sell': round(float(total_sell), 2),
            'total_fees': round(float(total_fees), 4),
            'realized_pnl': round(float(realized_pnl), 2),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/trades')
def api_trades():
    """交易历史API"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify([])
        
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in EXCLUDED_SYMBOLS])
        cursor.execute(f"""
            SELECT 
                inst_id, side, notional_usdt, fee, state,
                datetime(created_ts/1000, 'unixepoch') as time
            FROM orders 
            WHERE state='FILLED'
            AND inst_id NOT IN ({placeholders})
            AND notional_usdt < 1000
            ORDER BY created_ts DESC
            LIMIT 100
        """, EXCLUDED_SYMBOLS)
        
        trades = []
        for row in cursor.fetchall():
            try:
                trades.append({
                    'symbol': str(row[0]),
                    'side': str(row[1]),
                    'amount': round(float(row[2]), 4),
                    'fee': round(float(row[3]), 6),
                    'state': str(row[4]),
                    'time': str(row[5])
                })
            except (TypeError, ValueError) as e:
                # 跳过异常数据
                continue
        
        conn.close()
        return jsonify(trades)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


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
            try:
                qty_float = float(qty)
                # 只显示有实际持仓的（大于最小精度）
                if symbol != 'USDT' and qty_float > 0.0001:
                    positions.append({
                        'symbol': symbol,
                        'qty': round(qty_float, 8),
                        'value_usdt': 0  # TODO: 需要实时价格
                    })
            except (TypeError, ValueError):
                continue
        
        return jsonify(positions)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scores')
def api_scores():
    """币种评分API - 包含与上次结果的比较"""
    try:
        # 查找最新的决策文件
        runs_dir = REPORTS_DIR / 'runs'
        if not runs_dir.exists():
            return jsonify({'regime': 'Unknown', 'scores': []})
        
        # 获取所有run目录并按修改时间排序（最新的在前）
        run_dirs = [d for d in runs_dir.iterdir() if d.is_dir()]
        run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        if not run_dirs:
            return jsonify({'regime': 'Unknown', 'scores': []})
        
        # 尝试找到有decision_audit.json的最近run
        current_scores = []
        current_regime = 'Unknown'
        
        for run_dir in run_dirs[:10]:  # 检查最近10个
            decision_file = run_dir / 'decision_audit.json'
            if decision_file.exists():
                try:
                    with open(decision_file, 'r') as f:
                        decision = json.load(f)
                    
                    current_regime = decision.get('regime', 'Unknown')
                    top_scores = decision.get('top_scores', [])
                    
                    for item in top_scores[:20]:
                        try:
                            current_scores.append({
                                'symbol': item.get('symbol', 'Unknown'),
                                'score': round(float(item.get('score', 0)), 4)
                            })
                        except (TypeError, ValueError):
                            continue
                    
                    break  # 找到了当前数据
                except (json.JSONDecodeError, KeyError) as e:
                    continue
        
        # 加载历史评分数据
        history_file = REPORTS_DIR / 'scores_history.json'
        previous_ranking = {}
        
        if history_file.exists():
            try:
                with open(history_file, 'r') as f:
                    history = json.load(f)
                
                # 获取最近一次的数据（排除当前）
                current_time = datetime.now().isoformat()
                for entry in reversed(history):
                    entry_time = entry.get('timestamp', '')
                    # 如果这条记录比当前时间早至少5分钟，认为是上一次的数据
                    if entry_time < current_time:
                        prev_scores = entry.get('scores', [])
                        for idx, s in enumerate(prev_scores):
                            previous_ranking[s.get('symbol')] = {
                                'rank': idx + 1,
                                'score': s.get('score', 0)
                            }
                        break
            except Exception as e:
                print(f"加载历史评分失败: {e}")
        
        # 比较排名变化
        scores_with_trend = []
        for idx, s in enumerate(current_scores):
            symbol = s['symbol']
            current_rank = idx + 1
            prev_info = previous_ranking.get(symbol)
            
            if prev_info:
                rank_change = prev_info['rank'] - current_rank  # 正值表示排名上升
                score_change = s['score'] - prev_info['score']
                
                if rank_change > 0:
                    trend = 'up'  # 排名上升（数字变小）
                elif rank_change < 0:
                    trend = 'down'  # 排名下降（数字变大）
                else:
                    trend = 'stable'  # 排名不变
                
                scores_with_trend.append({
                    **s,
                    'rank': current_rank,
                    'previous_rank': prev_info['rank'],
                    'rank_change': rank_change,
                    'score_change': round(score_change, 4),
                    'trend': trend
                })
            else:
                scores_with_trend.append({
                    **s,
                    'rank': current_rank,
                    'previous_rank': None,
                    'rank_change': None,
                    'score_change': None,
                    'trend': 'new'  # 新上榜
                })
        
        # 保存当前评分到历史
        try:
            history = []
            if history_file.exists():
                with open(history_file, 'r') as f:
                    history = json.load(f)
            
            # 添加新记录
            history.append({
                'timestamp': datetime.now().isoformat(),
                'regime': current_regime,
                'scores': current_scores
            })
            
            # 只保留最近100条记录
            if len(history) > 100:
                history = history[-100:]
            
            with open(history_file, 'w') as f:
                json.dump(history, f, indent=2)
        except Exception as e:
            print(f"保存历史评分失败: {e}")
        
        return jsonify({
            'regime': current_regime,
            'scores': scores_with_trend,
            'last_update': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'regime': 'Error', 'scores': [], 'error': str(e)}), 500


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


def calculate_market_indicators():
    """从BTC K线数据计算市场指标"""
    try:
        # 读取BTC缓存数据
        cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/cache')
        btc_files = list(cache_dir.glob('BTC_USDT_1H_*.csv'))
        
        if not btc_files:
            return {'ma20': 0, 'ma60': 0, 'atr_percent': 1.0, 'price': 0}
        
        # 读取最新的BTC数据
        latest_file = max(btc_files, key=lambda x: x.stat().st_mtime)
        df = pd.read_csv(latest_file)
        
        if len(df) < 60:
            return {'ma20': 0, 'ma60': 0, 'atr_percent': 1.0, 'price': 0}
        
        # 计算MA20和MA60
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma60'] = df['close'].rolling(window=60).mean()
        
        # 计算ATR
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = abs(df['high'] - df['close'].shift())
        df['low_close'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=14).mean()
        
        # 获取最新值
        latest = df.iloc[-1]
        price = latest['close']
        ma20 = latest['ma20']
        ma60 = latest['ma60']
        atr = latest['atr']
        atr_percent = (atr / price * 100) if price > 0 else 1.0
        
        return {
            'ma20': round(ma20, 2) if not pd.isna(ma20) else 0,
            'ma60': round(ma60, 2) if not pd.isna(ma60) else 0,
            'atr_percent': round(atr_percent, 2) if not pd.isna(atr_percent) else 1.0,
            'price': round(price, 2)
        }
    except Exception as e:
        print(f"计算市场指标失败: {e}")
        return {'ma20': 0, 'ma60': 0, 'atr_percent': 1.0, 'price': 0}


@app.route('/api/market_state')
def api_market_state():
    """市场状态API"""
    try:
        # 获取评分数据中的regime
        scores_data = api_scores().get_json()
        regime = scores_data.get('regime', 'Risk-Off')
        
        # 计算市场指标
        indicators = calculate_market_indicators()
        
        # 根据regime确定仓位乘数
        multiplier_map = {
            'Risk-Off': 0.0,
            'RISK_OFF': 0.0,
            'Trending': 1.0,
            'TRENDING': 1.0,
            'Sideways': 0.5,
            'SIDEWAYS': 0.5
        }
        multiplier = multiplier_map.get(regime, 0.3)
        
        # 描述
        descriptions = {
            'Risk-Off': '风险规避模式，空仓保护中',
            'RISK_OFF': '风险规避模式，空仓保护中',
            'Trending': '趋势行情，增加仓位暴露',
            'TRENDING': '趋势行情，增加仓位暴露',
            'Sideways': '震荡行情，正常仓位',
            'SIDEWAYS': '震荡行情，正常仓位'
        }
        
        return jsonify({
            'state': regime.upper().replace('-', '_'),
            'ma20': indicators['ma20'],
            'ma60': indicators['ma60'],
            'atr_percent': indicators['atr_percent'],
            'price': indicators['price'],
            'position_multiplier': multiplier,
            'description': descriptions.get(regime, '市场状态监控中')
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
        
        # 按日期汇总盈亏 - 排除异常数据
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in EXCLUDED_SYMBOLS])
        cursor.execute(f"""
            SELECT 
                date(created_ts/1000, 'unixepoch') as date,
                SUM(CASE WHEN side='sell' THEN notional_usdt ELSE -notional_usdt END) as net_flow,
                SUM(fee) as fees
            FROM orders 
            WHERE state='FILLED'
            AND inst_id NOT IN ({placeholders})
            AND notional_usdt < 1000
            GROUP BY date
            ORDER BY date
        """, EXCLUDED_SYMBOLS)
        
        data = []
        cumulative = 100  # 初始权益
        for row in cursor.fetchall():
            try:
                net_flow = float(row[1] or 0)
                cumulative += net_flow
                data.append({
                    'timestamp': str(row[0]) + 'T00:00:00Z',
                    'value': round(cumulative, 2)
                })
            except (TypeError, ValueError):
                continue
        
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/equity_curve')
def api_equity_curve():
    """权益曲线 - 新版格式（支持图表库）"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'dates': [], 'values': [], 'pnl': []})
        
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in EXCLUDED_SYMBOLS])
        cursor.execute(f"""
            SELECT 
                date(created_ts/1000, 'unixepoch') as date,
                SUM(CASE WHEN side='sell' THEN notional_usdt ELSE -notional_usdt END) as net_flow,
                SUM(fee) as fees
            FROM orders 
            WHERE state='FILLED'
            AND inst_id NOT IN ({placeholders})
            AND notional_usdt < 1000
            GROUP BY date
            ORDER BY date
        """, EXCLUDED_SYMBOLS)
        
        dates = []
        values = []
        pnls = []
        cumulative = 100
        
        for row in cursor.fetchall():
            try:
                date_str = str(row[0])
                net_flow = float(row[1] or 0)
                fees = float(row[2] or 0)
                pnl = net_flow - fees
                cumulative += pnl
                
                dates.append(date_str)
                values.append(round(cumulative, 2))
                pnls.append(round(pnl, 2))
            except (TypeError, ValueError):
                continue
        
        conn.close()
        
        return jsonify({
            'dates': dates,
            'values': values,
            'pnl': pnls,
            'initial': 100,
            'current': values[-1] if values else 100,
            'total_return': round((values[-1] - 100) / 100 * 100, 2) if values else 0
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard')
def api_dashboard():
    """Dashboard 完整数据API"""
    try:
        # 获取账户数据
        account_data = api_account().get_json()
        
        # 获取持仓
        positions_data = api_positions().get_json()
        
        # 获取交易
        trades_data = api_trades().get_json()
        
        # 获取评分
        scores_data = api_scores().get_json()
        
        # 获取状态
        status_data = api_status().get_json()
        
        # 获取权益曲线
        equity_data = api_equity_history().get_json()
        
        # 获取市场状态
        market_state_data = api_market_state().get_json()
        
        # 转换持仓格式
        positions = []
        for pos in positions_data:
            positions.append({
                'symbol': pos.get('symbol', ''),
                'qty': pos.get('qty', 0),
                'avgPrice': 0,
                'currentPrice': 0,
                'value': pos.get('value_usdt', 0),
                'pnl': 0,
                'pnlPercent': 0
            })
        
        # 转换交易格式
        trades = []
        for i, trade in enumerate(trades_data[:20]):
            trades.append({
                'id': str(i),
                'timestamp': trade.get('time', '') + 'Z' if trade.get('time') else '',
                'symbol': trade.get('symbol', '').replace('-USDT', '/USDT'),
                'side': trade.get('side', 'buy'),
                'type': 'REBALANCE',
                'price': 0,
                'qty': 0,
                'value': trade.get('amount', 0),
                'fee': abs(trade.get('fee', 0))
            })
        
        # 转换Alpha评分
        alpha_scores = []
        for i, score in enumerate(scores_data.get('scores', [])[:10]):
            alpha_scores.append({
                'symbol': score.get('symbol', '').replace('-USDT', '/USDT'),
                'score': score.get('score', 0),
                'f1_mom_5d': 0,
                'f2_mom_20d': 0,
                'f3_vol_adj': 0,
                'f4_volume': 0,
                'f5_rsi': 0,
                'weight': 0.1
            })
        
        dashboard_data = {
            'account': {
                'totalEquity': account_data.get('cash_usdt', 0),
                'cash': account_data.get('cash_usdt', 0),
                'totalPnl': account_data.get('realized_pnl', 0),
                'totalPnlPercent': round((account_data.get('realized_pnl', 0) / 100) * 100, 2) if account_data.get('cash_usdt', 0) > 0 else 0,
                'todayPnl': 0,
                'todayPnlPercent': 0,
                'sharpeRatio': 0,
                'maxDrawdown': 0,
                'winRate': 0,
                'totalTrades': account_data.get('total_trades', 0)
            },
            'positions': positions,
            'trades': trades,
            'alphaScores': alpha_scores,
            'marketState': market_state_data,
            'systemStatus': {
                'isRunning': status_data.get('timer_active', False),
                'mode': 'live' if not status_data.get('dry_run', True) else 'dry_run',
                'lastUpdate': account_data.get('last_update', ''),
                'killSwitch': False,
                'errors': []
            },
            'equityCurve': equity_data if isinstance(equity_data, list) else [],
            # 新增：系统进度数据
            'timers': api_timers().get_json() if hasattr(api_timers(), 'get_json') else {'timers': []},
            'costCalibration': api_cost_calibration().get_json() if hasattr(api_cost_calibration(), 'get_json') else {'status': 'unknown'},
            'icDiagnostics': api_ic_diagnostics().get_json() if hasattr(api_ic_diagnostics(), 'get_json') else {'status': 'no_data'},
            'mlTraining': api_ml_training().get_json() if hasattr(api_ml_training(), 'get_json') else {'status': 'unknown'},
            'reflectionReports': api_reflection_reports().get_json() if hasattr(api_reflection_reports(), 'get_json') else {'reports': []}
        }
        
        return jsonify(dashboard_data)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/timer')
def api_timer():
    """定时任务信息API"""
    try:
        import subprocess
        import re
        from datetime import datetime, timedelta
        
        # 获取timer状态 - 使用status命令获取更准确的信息
        result = subprocess.run(
            ['systemctl', '--user', 'show', 'v5-live-20u.user.timer', 
             '--property=OnCalendar', '--property=Trigger'],
            capture_output=True, text=True
        )
        
        next_run = None
        countdown_seconds = 0
        interval_minutes = 60  # 默认1小时
        
        # 解析配置获取间隔
        for line in result.stdout.split('\n'):
            if line.startswith('OnCalendar='):
                calendar_str = line.split('=', 1)[1].strip()
                # 解析 OnCalendar 格式
                if '0/2' in calendar_str or '00/2' in calendar_str:
                    interval_minutes = 120  # 2小时
                elif 'hourly' in calendar_str.lower():
                    interval_minutes = 60  # 1小时
                elif '0/1' in calendar_str:
                    interval_minutes = 60  # 1小时
            
            if line.startswith('Trigger='):
                trigger_str = line.split('=', 1)[1].strip()
                if trigger_str and trigger_str != 'n/a':
                    try:
                        # 解析 "Tue 2026-02-24 14:52:00 CST" 格式
                        # 去掉时区缩写
                        trigger_clean = re.sub(r'\s+[A-Z]{3}$', '', trigger_str)
                        next_run_dt = datetime.strptime(trigger_clean, '%a %Y-%m-%d %H:%M:%S')
                        next_run = next_run_dt.strftime('%Y-%m-%d %H:%M:%S')
                        
                        # 计算倒计时
                        now = datetime.now()
                        diff = next_run_dt - now
                        countdown_seconds = max(0, int(diff.total_seconds()))
                    except Exception as e:
                        print(f"解析Trigger时间失败: {e}, trigger_str={trigger_str}")
        
        # 如果上面的方法失败，尝试使用list-timers
        if not next_run:
            result2 = subprocess.run(
                ['systemctl', '--user', 'list-timers', 'v5-live-20u.user.timer', '--no-pager'],
                capture_output=True, text=True
            )
            
            for line in result2.stdout.split('\n'):
                if 'v5-live-20u.user.timer' in line:
                    # 格式: "Tue 2026-02-24 14:52:00 CST  1min 4s left ..."
                    # 或: "n/a  n/a  ..."
                    match = re.search(r'(\w{3}\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
                    if match:
                        time_str = match.group(1)
                        try:
                            next_run_dt = datetime.strptime(time_str, '%a %Y-%m-%d %H:%M:%S')
                            next_run = next_run_dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                            now = datetime.now()
                            diff = next_run_dt - now
                            countdown_seconds = max(0, int(diff.total_seconds()))
                            
                            # 尝试解析LEFT列获取倒计时
                            left_match = re.search(r'\d{2}:\d{2}:\d{2}\s+([\d\w\s]+?)\s+\w{3}\s+v5-live', line)
                            if left_match:
                                left_str = left_match.group(1).strip()
                                # 解析类似 "1min 4s" 或 "45s" 或 "1h 30min"
                                total_seconds = 0
                                for part in left_str.split():
                                    if 'h' in part:
                                        total_seconds += int(part.replace('h', '')) * 3600
                                    elif 'min' in part:
                                        total_seconds += int(part.replace('min', '')) * 60
                                    elif 's' in part:
                                        total_seconds += int(part.replace('s', ''))
                                if total_seconds > 0:
                                    countdown_seconds = total_seconds
                        except Exception as e:
                            print(f"解析list-timers失败: {e}")
                    break
        
        return jsonify({
            'timer_name': 'v5-live-20u.user.timer',
            'next_run': next_run,
            'countdown_seconds': countdown_seconds,
            'interval_minutes': interval_minutes,
            'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        import traceback
        return jsonify({
            'timer_name': 'v5-live-20u.user.timer',
            'next_run': None,
            'countdown_seconds': 0,
            'interval_minutes': 120,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/timers')
def api_timers():
    """所有定时任务状态API"""
    try:
        import subprocess
        import re
        
        # 定义要监控的timer
        timer_configs = [
            {'name': 'v5-live-20u.user.timer', 'desc': '实盘交易执行', 'icon': '🔄'},
            {'name': 'v5-reconcile.timer', 'desc': '对账状态刷新', 'icon': '🔍'},
            {'name': 'v5-daily-ml-training.timer', 'desc': 'ML模型训练', 'icon': '🧠'},
            {'name': 'v5-reflection-agent.timer', 'desc': '交易后分析', 'icon': '📊'},
        ]
        
        timers = []
        
        for config in timer_configs:
            timer_name = config['name']
            
            # 获取timer状态
            result = subprocess.run(
                ['systemctl', '--user', 'show', timer_name,
                 '--property=UnitFileState', '--property=ActiveState'],
                capture_output=True, text=True
            )
            
            enabled = False
            active = False
            
            for line in result.stdout.split('\n'):
                if line.startswith('UnitFileState='):
                    enabled = line.split('=', 1)[1].strip() == 'enabled'
                if line.startswith('ActiveState='):
                    active = line.split('=', 1)[1].strip() == 'active'
            
            # 获取下次执行时间
            result2 = subprocess.run(
                ['systemctl', '--user', 'list-timers', timer_name, '--no-pager'],
                capture_output=True, text=True
            )
            
            next_run = None
            left_str = None
            
            for line in result2.stdout.split('\n'):
                if timer_name in line:
                    # 解析 LEFT 列
                    parts = line.split()
                    if len(parts) >= 4:
                        left_str = parts[-3] if 'left' in line else None
                        # 解析下次执行时间
                        match = re.search(r'(\w{3}\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
                        if match:
                            next_run = match.group(1)
                    break
            
            timers.append({
                'name': timer_name,
                'desc': config['desc'],
                'icon': config['icon'],
                'enabled': enabled,
                'active': active,
                'next_run': next_run,
                'time_left': left_str
            })
        
        return jsonify({
            'timers': timers,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/cost_calibration')
def api_cost_calibration():
    """F2成本校准进度API - 从原始数据计算"""
    try:
        cost_dir = REPORTS_DIR / 'cost_stats'
        events_dir = REPORTS_DIR / 'cost_events'
        
        calibration_data = []
        total_days = 0
        avg_slippage_bps = 0
        avg_fee_bps = 0
        total_trade_count = 0
        
        # 优先从cost_stats读取已汇总的数据
        if cost_dir.exists():
            stats_files = sorted(cost_dir.glob('daily_cost_stats_*.json'))
            
            for stats_file in stats_files[-30:]:  # 最近30天
                try:
                    with open(stats_file, 'r') as f:
                        stats = json.load(f)
                    
                    day = stats_file.stem.replace('daily_cost_stats_', '')
                    
                    # 从嵌套的buckets计算平均值
                    buckets = stats.get('buckets', {})
                    day_slippage = []
                    day_fee = []
                    day_trade_count = 0
                    
                    for bucket_name, bucket_data in buckets.items():
                        slippage_data = bucket_data.get('slippage_bps', {})
                        fee_data = bucket_data.get('fee_bps', {})
                        
                        if slippage_data.get('count', 0) > 0 and slippage_data.get('mean') is not None:
                            day_slippage.append(slippage_data['mean'])
                        if fee_data.get('count', 0) > 0 and fee_data.get('mean') is not None:
                            day_fee.append(fee_data['mean'])
                        
                        day_trade_count += slippage_data.get('count', 0)
                    
                    avg_day_slippage = sum(day_slippage) / len(day_slippage) if day_slippage else 0
                    avg_day_fee = sum(day_fee) / len(day_fee) if day_fee else 0
                    
                    calibration_data.append({
                        'date': day,
                        'slippage_bps': round(avg_day_slippage, 4),
                        'fee_bps': round(avg_day_fee, 4),
                        'total_cost_bps': round(avg_day_slippage + avg_day_fee, 4),
                        'trade_count': day_trade_count
                    })
                    
                    total_days += 1
                    avg_slippage_bps += avg_day_slippage
                    avg_fee_bps += avg_day_fee
                    total_trade_count += day_trade_count
                except Exception as e:
                    print(f"处理 {stats_file} 失败: {e}")
                    continue
        
        # 如果没有stats数据，从cost_events原始数据计算
        if total_days == 0 and events_dir.exists():
            event_files = sorted(events_dir.glob('*.jsonl'))
            
            # 按日期分组统计
            daily_stats = {}
            
            for event_file in event_files[-30:]:  # 最近30天
                try:
                    # 从文件名提取日期 (YYYYMMDD.jsonl)
                    day = event_file.stem
                    if not day.isdigit() or len(day) != 8:
                        continue
                    
                    slippage_list = []
                    fee_list = []
                    
                    with open(event_file, 'r') as f:
                        for line in f:
                            try:
                                event = json.loads(line.strip())
                                # 提取滑点和费率
                                slippage = event.get('slippage_bps') or event.get('slippage_usdt', 0) / event.get('notional_usdt', 1) * 10000
                                fee = event.get('fee_bps') or event.get('fee', 0) / event.get('notional_usdt', 1) * 10000
                                
                                if slippage is not None and not isinstance(slippage, str):
                                    slippage_list.append(float(slippage))
                                if fee is not None and not isinstance(fee, str):
                                    fee_list.append(float(fee))
                            except:
                                continue
                    
                    if slippage_list or fee_list:
                        avg_s = sum(slippage_list) / len(slippage_list) if slippage_list else 0
                        avg_f = sum(fee_list) / len(fee_list) if fee_list else 0
                        
                        daily_stats[day] = {
                            'date': day,
                            'slippage_bps': round(avg_s, 4),
                            'fee_bps': round(avg_f, 4),
                            'total_cost_bps': round(avg_s + avg_f, 4),
                            'trade_count': len(slippage_list) + len(fee_list)
                        }
                except Exception as e:
                    print(f"处理 {event_file} 失败: {e}")
                    continue
            
            # 转换为列表并计算平均值
            calibration_data = list(daily_stats.values())
            calibration_data.sort(key=lambda x: x['date'])
            
            total_days = len(calibration_data)
            for d in calibration_data:
                avg_slippage_bps += d['slippage_bps']
                avg_fee_bps += d['fee_bps']
                total_trade_count += d['trade_count']
        
        # 计算平均值
        if total_days > 0:
            avg_slippage_bps /= total_days
            avg_fee_bps /= total_days
        
        # 获取事件文件数
        event_count = 0
        if events_dir.exists():
            event_count = len(list(events_dir.glob('*.jsonl')))
        
        return jsonify({
            'status': 'calibrated' if total_days >= 7 else 'calibrating',
            'total_days': total_days,
            'avg_slippage_bps': round(avg_slippage_bps, 4),
            'avg_fee_bps': round(avg_fee_bps, 4),
            'avg_total_cost_bps': round(avg_slippage_bps + avg_fee_bps, 4),
            'event_files': event_count,
            'total_trades': total_trade_count,
            'daily_stats': calibration_data[-7:],  # 最近7天
            'progress_percent': min(100, int(total_days / 7 * 100)),
            'data_source': 'events' if total_days > 0 and not (cost_dir.exists() and list(cost_dir.glob('*.json'))) else 'stats',
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/ic_diagnostics')
def api_ic_diagnostics():
    """IC诊断进度API"""
    try:
        # 查找最新的IC诊断文件
        ic_files = sorted(REPORTS_DIR.glob('ic_diagnostics_*.json'))
        
        if not ic_files:
            return jsonify({
                'status': 'no_data',
                'message': '暂无IC诊断数据'
            })
        
        latest_ic = ic_files[-1]
        with open(latest_ic, 'r') as f:
            ic_data = json.load(f)
        
        # 解析IC数据 - 新版结构在overall_tradable.ic下
        overall_tradable = ic_data.get('overall_tradable', {})
        overall_raw = ic_data.get('overall_raw', {})
        
        # 获取IC数据
        ic_by_factor = overall_tradable.get('ic', {})
        
        # 计算整体IC（所有因子的平均）
        all_ic_values = []
        for factor_data in ic_by_factor.values():
            mean_ic = factor_data.get('mean')
            if mean_ic is not None:
                all_ic_values.append(mean_ic)
        
        overall_ic_mean = sum(all_ic_values) / len(all_ic_values) if all_ic_values else 0
        
        # 计算各因子IC
        factors = []
        for factor_name, factor_data in ic_by_factor.items():
            mean_ic = factor_data.get('mean', 0)
            p50_ic = factor_data.get('p50', 0)
            count = factor_data.get('count', 0)
            
            # 简化计算IR (IC / std)，如果std不可用则用近似值
            std_approx = (factor_data.get('p75', 0) - factor_data.get('p25', 0)) / 1.35 if factor_data.get('p75') else 0.1
            ir = mean_ic / std_approx if std_approx > 0 else 0
            
            factors.append({
                'name': factor_name,
                'ic': round(mean_ic, 4),
                'ic_median': round(p50_ic, 4),
                'ic_std': round(std_approx, 4),
                'ir': round(ir, 4),
                'sample_count': count
            })
        
        # 按Regime分组 - 从by_regime数据中提取
        regimes = []
        regime_data = ic_data.get('by_regime', {})
        for regime_name, regime_info in regime_data.items():
            regime_ic_data = regime_info.get('ic', {})
            if regime_ic_data:
                # 计算该regime下所有因子的平均IC
                regime_ic_values = []
                for factor_ic in regime_ic_data.values():
                    if isinstance(factor_ic, dict) and 'mean' in factor_ic:
                        regime_ic_values.append(factor_ic['mean'])
                    elif isinstance(factor_ic, (int, float)):
                        regime_ic_values.append(factor_ic)
                
                avg_regime_ic = sum(regime_ic_values) / len(regime_ic_values) if regime_ic_values else 0
                
                regimes.append({
                    'name': regime_name,
                    'ic': round(avg_regime_ic, 4),
                    'sample_count': regime_info.get('n', 0)
                })
        
        # 计算整体IR
        overall_std = 0.1  # 默认值
        if factors:
            overall_std = sum(f['ic_std'] for f in factors) / len(factors)
        overall_ir = overall_ic_mean / overall_std if overall_std > 0 else 0
        
        return jsonify({
            'status': 'ready',
            'overall_ic': round(overall_ic_mean, 4),
            'overall_ir': round(overall_ir, 4),
            'sample_count': overall_tradable.get('used_points', 0),
            'timestamps_count': overall_tradable.get('used_timestamps', 0),
            'lookback_days': ic_data.get('lookback_days', 30),
            'factors': factors,
            'regimes': regimes,
            'last_update': datetime.fromtimestamp(latest_ic.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/ml_training')
def api_ml_training():
    """机器学习训练进度API"""
    try:
        # 检查模型目录
        model_dir = WORKSPACE / 'models'
        latest_model = None
        model_files = sorted(model_dir.glob('lgb_model_*.pkl')) if model_dir.exists() else []
        
        if model_files:
            latest_model = model_files[-1]
            model_time = datetime.fromtimestamp(latest_model.stat().st_mtime)
        else:
            model_time = None
        
        # 检查数据收集进度
        data_dir = WORKSPACE / 'data' / 'ml_training'
        data_files = list(data_dir.glob('training_data_*.csv')) if data_dir.exists() else []
        
        total_samples = 0
        for df in data_files:
            try:
                import pandas as pd
                data = pd.read_csv(df)
                total_samples += len(data)
            except:
                continue
        
        # 检查训练日志
        training_log = WORKSPACE / 'logs' / 'ml_training.log'
        last_training = None
        last_ic = None
        
        if training_log.exists():
            try:
                with open(training_log, 'r') as f:
                    lines = f.readlines()
                    # 查找最后一行包含IC的
                    for line in reversed(lines):
                        if 'IC:' in line or 'ic:' in line:
                            import re
                            ic_match = re.search(r'IC[:\s]+([\d.]+)', line)
                            if ic_match:
                                last_ic = float(ic_match.group(1))
                                break
                    if lines:
                        last_training = lines[-1][:50]  # 最后一条日志
            except:
                pass
        
        # 确定状态
        if model_time and (datetime.now() - model_time).days < 1:
            status = 'trained_today'
        elif total_samples >= 100:
            status = 'ready_to_train'
        elif total_samples > 0:
            status = 'collecting_data'
        else:
            status = 'no_data'
        
        return jsonify({
            'status': status,
            'total_samples': total_samples,
            'samples_needed': 100,
            'progress_percent': min(100, int(total_samples / 100 * 100)),
            'latest_model': latest_model.name if latest_model else None,
            'model_date': model_time.strftime('%Y-%m-%d %H:%M') if model_time else None,
            'last_ic': round(last_ic, 4) if last_ic else None,
            'data_files': len(data_files),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reflection_reports')
def api_reflection_reports():
    """反思Agent报告列表API"""
    try:
        reflection_dir = REPORTS_DIR / 'reflection'
        
        if not reflection_dir.exists():
            return jsonify({'reports': [], 'message': '暂无反思报告'})
        
        reports = []
        report_files = sorted(reflection_dir.glob('reflection_*.json'), reverse=True)
        
        for report_file in report_files[:10]:  # 最近10份
            try:
                with open(report_file, 'r') as f:
                    data = json.load(f)
                
                # 提取关键信息
                metrics = data.get('overall_metrics', {})
                insights = data.get('insights', [])
                
                # 统计洞察
                high_severity = sum(1 for i in insights if i.get('severity') == 'high')
                medium_severity = sum(1 for i in insights if i.get('severity') == 'medium')
                
                reports.append({
                    'filename': report_file.name,
                    'date': report_file.stem.replace('reflection_', ''),
                    'total_pnl': round(metrics.get('total_pnl', 0), 2),
                    'trade_count': metrics.get('total_trades', 0),
                    'symbols': metrics.get('unique_symbols', 0),
                    'insights_count': len(insights),
                    'high_priority': high_severity,
                    'medium_priority': medium_severity
                })
            except:
                continue
        
        return jsonify({
            'reports': reports,
            'total_reports': len(report_files),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("="*60)
    print("V5 Web Dashboard 启动中...")
    print("="*60)
    print(f"访问地址: http://0.0.0.0:5000")
    print("="*60)
    app.run(host='0.0.0.0', port=5000, debug=False)
