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
import requests
import subprocess

app = Flask(__name__, 
            template_folder='/home/admin/clawd/v5-trading-bot/web/templates', 
            static_folder='/home/admin/clawd/v5-trading-bot/web/static')

# 注册健康检查蓝图
try:
    sys.path.insert(0, "/home/admin/clawd/v5-trading-bot")
    from src.reporting.health import health_bp
    app.register_blueprint(health_bp)
    print("[WebDashboard] Health check endpoints registered: /health, /ready, /liveness")
except Exception as e:
    print(f"[WebDashboard] Failed to register health blueprint: {e}")


@app.after_request
def add_no_cache_headers(resp):
    """避免移动端缓存旧前端，确保样式/图表脚本及时生效。"""
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

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
    """主页面 - 新版监控面板"""
    return render_template('monitor_v2.html')


@app.route('/monitor')
def monitor():
    """旧版监控页面（保留兼容）"""
    return render_template('monitor_v2.html')


@app.route('/simple')
def simple_dashboard():
    """简洁版监控页"""
    return render_template('monitor_v2.html')


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
    """账户信息API - 优先OKX实时数据"""
    try:
        # 优先从OKX获取实时余额
        cash = 0
        try:
            import os, time, hmac, hashlib, base64, requests
            from dotenv import load_dotenv
            load_dotenv(str(WORKSPACE / '.env'))
            
            key = os.getenv('EXCHANGE_API_KEY')
            sec = os.getenv('EXCHANGE_API_SECRET')
            pp = os.getenv('EXCHANGE_PASSPHRASE')
            
            if key and sec and pp:
                ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
                path = '/api/v5/account/balance'
                msg = ts + 'GET' + path
                sig = base64.b64encode(hmac.new(sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
                headers = {
                    'OK-ACCESS-KEY': key,
                    'OK-ACCESS-SIGN': sig,
                    'OK-ACCESS-TIMESTAMP': ts,
                    'OK-ACCESS-PASSPHRASE': pp,
                }
                resp = requests.get('https://www.okx.com' + path, headers=headers, timeout=8)
                data = resp.json()
                if data.get('code') == '0' and data.get('data'):
                    for d in data['data'][0].get('details', []):
                        if d.get('ccy') == 'USDT':
                            cash = float(d.get('eq', 0))
                            break
        except Exception as e:
            print(f"[account] OKX API错误: {e}")
        
        # 如果OKX获取失败，回退到reconcile文件
        if cash <= 0:
            reconcile_file = REPORTS_DIR / 'reconcile_status.json'
            if reconcile_file.exists():
                try:
                    with open(reconcile_file, 'r') as f:
                        reconcile = json.load(f)
                    cash = (
                        reconcile.get('exchange_snapshot', {}).get('ccy_cashBal', {}).get('USDT')
                        or reconcile.get('local_snapshot', {}).get('cash_usdt', 0)
                    )
                except Exception:
                    pass
        
        # 获取最新权益 - 排除异常数据
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            # 使用参数化查询排除异常币种
            placeholders = ','.join(['?' for _ in EXCLUDED_SYMBOLS])
            query = f"""
                SELECT 
                    SUM(CASE WHEN state='FILLED' THEN 1 ELSE 0 END) as total_trades,
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
        
        # 持仓市值与 /api/positions 保持同口径
        positions_value = 0.0
        positions_rows = []
        try:
            pos_payload = api_positions().get_json() or {}
            if isinstance(pos_payload, dict):
                positions_rows = pos_payload.get('positions', []) or []
            elif isinstance(pos_payload, list):
                # 兼容旧格式
                positions_rows = pos_payload
            positions_value = sum(float(x.get('value_usdt') or x.get('value') or 0.0) for x in positions_rows)
        except Exception:
            pass

        total_equity = float(cash or 0) + positions_value
        
        # 计算盈亏百分比（假设初始资金为120 USDT）
        initial_capital = 120.0
        total_pnl_pct = (total_equity - initial_capital) / initial_capital if initial_capital > 0 else 0
        
        # 获取持仓数量
        positions_count = 0
        try:
            rows = positions_rows if isinstance(positions_rows, list) else []
            positions_count = len([p for p in rows if float(p.get('value_usdt') or p.get('value') or 0) > 1])
        except Exception:
            pass
        
        # 计算回撤（基于资金上限）
        # 修复：小资金测试时，不应使用历史大资金峰值
        config = load_config()
        budget_cap = float(config.get('budget', {}).get('live_equity_cap_usdt', 0) or 0)
        drawdown_pct = 0.0
        peak_equity = initial_capital  # 默认使用初始资金作为峰值
        
        if budget_cap > 0:
            # 如果设置了资金上限，使用上限作为峰值基准
            peak_equity = budget_cap
            drawdown_pct = (peak_equity - total_equity) / peak_equity if peak_equity > 0 else 0
            # 如果当前权益超过峰值，回撤为0（不更新峰值，只是计算）
            if total_equity > peak_equity:
                drawdown_pct = 0.0
        else:
            # 未设置资金上限，使用传统计算方式
            # 从数据库读取峰值
            try:
                conn2 = sqlite3.connect(str(REPORTS_DIR / 'positions.sqlite'))
                cursor2 = conn2.cursor()
                cursor2.execute("SELECT equity_peak_usdt FROM account_state WHERE k='default'")
                row2 = cursor2.fetchone()
                if row2 and row2[0]:
                    db_peak = float(row2[0])
                    # 如果数据库峰值超过当前权益太多（超过2倍），可能是历史数据
                    # 使用当前权益和初始资金的较大值
                    if db_peak > total_equity * 2:
                        peak_equity = max(total_equity, initial_capital)
                    else:
                        peak_equity = db_peak
                conn2.close()
            except Exception:
                peak_equity = max(total_equity, initial_capital)
            
            drawdown_pct = (peak_equity - total_equity) / peak_equity if peak_equity > 0 else 0
        
        # 确保回撤在合理范围
        drawdown_pct = max(0.0, min(1.0, drawdown_pct))

        return jsonify({
            'cash_usdt': round(float(cash), 2),
            'positions_value_usdt': round(float(positions_value), 4),
            'total_equity_usdt': round(float(total_equity), 4),
            'total_pnl_pct': round(float(total_pnl_pct), 4),
            'drawdown_pct': round(float(drawdown_pct), 4),
            'peak_equity_usdt': round(float(peak_equity), 2),
            'budget_cap_usdt': round(float(budget_cap), 2) if budget_cap > 0 else None,
            'positions_count': positions_count,
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
    """交易历史API（优先OKX实时成交，回退DB，再回退runs/*/trades.csv）"""
    try:
        trades = []

        # 0) 优先OKX实时成交
        try:
            import os, time, hmac, hashlib, base64
            from dotenv import load_dotenv
            load_dotenv(str(WORKSPACE / '.env'))
            key = os.getenv('EXCHANGE_API_KEY')
            sec = os.getenv('EXCHANGE_API_SECRET')
            pp = os.getenv('EXCHANGE_PASSPHRASE')
            if not (key and sec and pp):
                envp = WORKSPACE / '.env'
                if envp.exists():
                    for ln in envp.read_text(encoding='utf-8', errors='ignore').splitlines():
                        if not ln or ln.strip().startswith('#') or '=' not in ln:
                            continue
                        k, v = ln.split('=', 1)
                        k = k.strip(); v = v.strip().strip('"').strip("'")
                        if k == 'EXCHANGE_API_KEY' and not key:
                            key = v
                        elif k == 'EXCHANGE_API_SECRET' and not sec:
                            sec = v
                        elif k == 'EXCHANGE_PASSPHRASE' and not pp:
                            pp = v

            if key and sec and pp:
                ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
                path = '/api/v5/trade/fills?limit=100'
                msg = ts + 'GET' + path
                sig = base64.b64encode(hmac.new(sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
                headers = {
                    'OK-ACCESS-KEY': key,
                    'OK-ACCESS-SIGN': sig,
                    'OK-ACCESS-TIMESTAMP': ts,
                    'OK-ACCESS-PASSPHRASE': pp,
                }
                resp = requests.get('https://www.okx.com' + path, headers=headers, timeout=8)
                data = resp.json()
                if data.get('code') == '0':
                    for r in data.get('data', []):
                        try:
                            inst = str(r.get('instId', ''))
                            if (not inst) or (inst in EXCLUDED_SYMBOLS):
                                continue
                            ts_ms = int(r.get('ts') or 0)
                            t = datetime.utcfromtimestamp(ts_ms / 1000.0) + timedelta(hours=8)
                            px = float(r.get('fillPx') or 0)
                            sz = float(r.get('fillSz') or 0)
                            amount = px * sz
                            fee = float(r.get('fee') or 0)
                            trades.append({
                                'symbol': inst,
                                'side': str(r.get('side', '')),
                                'amount': round(amount, 4),
                                'fee': round(fee, 6),
                                'state': 'FILLED',
                                'time': t.strftime('%Y-%m-%d %H:%M:%S')
                            })
                        except Exception:
                            continue
        except Exception:
            pass

        # 1) 回退订单库
        if not trades:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in EXCLUDED_SYMBOLS])
                cursor.execute(f"""
                    SELECT 
                        inst_id, side, notional_usdt, fee, state,
                        datetime(created_ts/1000, 'unixepoch', '+8 hours') as time
                    FROM orders 
                    WHERE state='FILLED'
                    AND inst_id NOT IN ({placeholders})
                    AND notional_usdt < 1000
                    ORDER BY created_ts DESC
                    LIMIT 100
                """, EXCLUDED_SYMBOLS)

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
                    except (TypeError, ValueError):
                        continue
                conn.close()

        # 2) 回退 runs/*/trades.csv
        if not trades:
            runs_dir = REPORTS_DIR / 'runs'
            if runs_dir.exists():
                run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True)
                for run_dir in run_dirs[:24]:
                    p = run_dir / 'trades.csv'
                    if not p.exists():
                        continue
                    try:
                        import csv
                        with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                            reader = csv.DictReader(f)
                            for r in reader:
                                sym = str(r.get('symbol', '') or '')
                                if not sym:
                                    continue
                                if any(ex in sym for ex in EXCLUDED_SYMBOLS):
                                    continue
                                trades.append({
                                    'symbol': sym.replace('/USDT', '-USDT'),
                                    'side': str(r.get('side', '')),
                                    'amount': round(float(r.get('notional_usdt') or 0), 4),
                                    'fee': round(float(r.get('fee_usdt') or 0), 6),
                                    'state': 'FILLED',
                                    'time': str(r.get('ts', '')),
                                })
                    except Exception:
                        continue
                    if len(trades) >= 100:
                        break

        return jsonify({'trades': trades[:100]})
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trades': []}), 500


@app.route('/api/positions')
def api_positions():
    """持仓信息API（优先 positions.sqlite，回退最新 run 的 positions.jsonl）"""
    try:
        hidden_symbols = {
            'PROMPT', 'XAUT', 'WLFI', 'SPACE', 'KITE', 'AGLD', 'MERL', 'USDG', 'J', 'PEPE'
        }

        def get_last_price_usdt(symbol: str) -> float:
            """获取币种最新价格，优先OKX实时API"""
            # 1) 优先OKX实时API
            try:
                r = requests.get(f"https://www.okx.com/api/v5/market/ticker?instId={symbol}-USDT", timeout=5)
                j = r.json()
                if j.get('code') == '0' and j.get('data'):
                    return float(j['data'][0].get('last') or 0)
            except Exception:
                pass
            
            # 2) Fallback: 缓存文件（检查时间，超过15分钟废弃）
            try:
                import time
                cache_dir = WORKSPACE / 'data' / 'cache'
                files = sorted(cache_dir.glob(f'{symbol}_USDT_1H_*.csv'))
                if files:
                    # 检查文件修改时间
                    file_mtime = files[-1].stat().st_mtime
                    if time.time() - file_mtime < 900:  # 15分钟内
                        df = pd.read_csv(files[-1])
                        if len(df) > 0 and 'close' in df.columns:
                            return float(df.iloc[-1]['close'])
            except Exception:
                pass
            
            return 0.0

        pos_db = REPORTS_DIR / 'positions.sqlite'
        positions = []
        live_okx_used = False

        # 0) 优先实时OKX余额（与用户手动操作一致）
        okx_error = None
        try:
            import os, time, hmac, hashlib, base64
            from dotenv import load_dotenv
            load_dotenv(str(WORKSPACE / '.env'))
            key = os.getenv('EXCHANGE_API_KEY')
            sec = os.getenv('EXCHANGE_API_SECRET')
            pp = os.getenv('EXCHANGE_PASSPHRASE')
            # fallback: parse .env manually when process env not populated
            if not (key and sec and pp):
                try:
                    envp = WORKSPACE / '.env'
                    if envp.exists():
                        for ln in envp.read_text(encoding='utf-8', errors='ignore').splitlines():
                            if not ln or ln.strip().startswith('#') or '=' not in ln:
                                continue
                            k, v = ln.split('=', 1)
                            k = k.strip(); v = v.strip().strip('"').strip("'")
                            if k == 'EXCHANGE_API_KEY' and not key:
                                key = v
                            elif k == 'EXCHANGE_API_SECRET' and not sec:
                                sec = v
                            elif k == 'EXCHANGE_PASSPHRASE' and not pp:
                                pp = v
                except Exception as e:
                    okx_error = f"env_parse_error: {e}"
            if key and sec and pp:
                ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
                path = '/api/v5/account/balance'
                msg = ts + 'GET' + path
                sig = base64.b64encode(hmac.new(sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
                headers = {
                    'OK-ACCESS-KEY': key,
                    'OK-ACCESS-SIGN': sig,
                    'OK-ACCESS-TIMESTAMP': ts,
                    'OK-ACCESS-PASSPHRASE': pp,
                }
                resp = requests.get('https://www.okx.com' + path, headers=headers, timeout=8)
                data = resp.json()
                if data.get('code') == '0' and data.get('data'):
                    live_okx_used = True
                    details = data['data'][0].get('details', [])
                    for d in details:
                        try:
                            ccy = str(d.get('ccy') or '')
                            if not ccy or ccy == 'USDT' or ccy in hidden_symbols:
                                continue
                            qty_float = float(d.get('eq') or 0)
                            if qty_float <= 0:
                                continue
                            px = get_last_price_usdt(ccy)
                            if px <= 0:
                                continue
                            value = qty_float * px
                            if value < 0.5:
                                continue
                            positions.append({
                                'symbol': ccy,
                                'qty': round(qty_float, 8),
                                'avg_px': 0.0,
                                'last_price': round(px, 6),
                                'value_usdt': round(value, 4)
                            })
                        except Exception:
                            continue
        except Exception as e:
            import traceback
            okx_error = f"{e}\n{traceback.format_exc()}"
            print(f"[positions] OKX API错误: {okx_error}")

        # 1) 回退 positions.sqlite（仅当实时OKX不可用且positions为空）
        # 注意：如果OKX API成功调用但返回空持仓，说明真的没持仓，不应回退到缓存
        fallback_source = None
        if not live_okx_used and not positions and pos_db.exists():
            fallback_source = "positions.sqlite"
            conn = sqlite3.connect(str(pos_db))
            cur = conn.cursor()
            cur.execute("SELECT symbol, qty, avg_px, last_mark_px FROM positions")
            rows = cur.fetchall()
            conn.close()

            for symbol_raw, qty, avg_px, last_mark_px in rows:
                try:
                    symbol_raw = str(symbol_raw or '')
                    base = symbol_raw.split('/')[0] if '/' in symbol_raw else symbol_raw.split('-')[0]
                    if base == 'USDT' or base in hidden_symbols:
                        continue
                    print(f"[positions] sqlite: {base}, qty={qty}")

                    qty_float = float(qty or 0)
                    if qty_float <= 0:
                        continue

                    px = float(last_mark_px or 0) if last_mark_px else 0.0
                    if px <= 0:
                        px = get_last_price_usdt(base)
                    if px <= 0 and avg_px:
                        px = float(avg_px)

                    value = qty_float * px if px > 0 else 0.0
                    if value < 0.5:
                        continue

                    positions.append({
                        'symbol': base,
                        'qty': round(qty_float, 8),
                        'avg_px': round(float(avg_px or 0), 6),
                        'last_price': round(px, 6),
                        'value_usdt': round(value, 4)
                    })
                except Exception:
                    continue

        # 2) 回退：DB为空且OKX不可用时读取最新 runs/*/positions.jsonl
        if not live_okx_used and not positions:
            runs_dir = REPORTS_DIR / 'runs'
            if runs_dir.exists():
                run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True)
                for run_dir in run_dirs[:12]:
                    p = run_dir / 'positions.jsonl'
                    if not p.exists():
                        continue
                    try:
                        with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                try:
                                    row = json.loads(line)
                                    symbol_raw = str(row.get('symbol', '') or '')
                                    base = symbol_raw.split('/')[0] if '/' in symbol_raw else symbol_raw.split('-')[0]
                                    if base == 'USDT' or base in hidden_symbols:
                                        continue
                                    qty_float = float(row.get('qty') or 0)
                                    if qty_float <= 0:
                                        continue
                                    px = float(row.get('mark_px') or 0)
                                    if px <= 0:
                                        px = get_last_price_usdt(base)
                                    if px <= 0:
                                        px = float(row.get('avg_px') or 0)
                                    value = qty_float * px if px > 0 else 0.0
                                    if value < 0.5:
                                        continue
                                    positions.append({
                                        'symbol': base,
                                        'qty': round(qty_float, 8),
                                        'avg_px': round(float(row.get('avg_px') or 0), 6),
                                        'last_price': round(px, 6),
                                        'value_usdt': round(value, 4)
                                    })
                                except Exception:
                                    continue
                    except Exception:
                        continue
                    if positions:
                        break

        positions.sort(key=lambda x: x.get('value_usdt', 0), reverse=True)
        
        # 从交易记录计算真实成本价和盈亏（使用FIFO方法）
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                for p in positions:
                    symbol = p.get('symbol', '')
                    if not symbol:
                        continue
                    
                    current_qty = float(p.get('qty', 0))
                    if current_qty <= 0:
                        continue
                    
                    # 查询该币种所有成交记录（按时间正序，FIFO）
                    cursor.execute("""
                        SELECT side, notional_usdt, sz, avg_px, created_ts
                        FROM orders 
                        WHERE inst_id LIKE ? AND state='FILLED'
                        ORDER BY created_ts ASC
                    """, (f"%{symbol}%",))
                    
                    rows = cursor.fetchall()
                    
                    # 构建买入队列（FIFO）
                    buy_queue = []  # [(qty, cost_per_unit), ...]
                    
                    for row in rows:
                        side, notional, sz, avg_px, ts = row
                        notional_val = float(notional or 0)
                        
                        # 计算数量
                        if sz and float(sz) > 0:
                            qty_val = float(sz)
                        elif avg_px and float(avg_px) > 0:
                            qty_val = notional_val / float(avg_px)
                        else:
                            continue
                        
                        cost_per_unit = notional_val / qty_val if qty_val > 0 else 0
                        
                        if side == 'buy':
                            # 买入加入队列
                            buy_queue.append([qty_val, cost_per_unit])
                        elif side == 'sell':
                            # 卖出按FIFO减少买入队列
                            sell_qty = qty_val
                            while sell_qty > 0 and buy_queue:
                                first_buy = buy_queue[0]
                                if first_buy[0] <= sell_qty:
                                    # 第一笔买入全部卖出
                                    sell_qty -= first_buy[0]
                                    buy_queue.pop(0)
                                else:
                                    # 第一笔买入部分卖出
                                    first_buy[0] -= sell_qty
                                    sell_qty = 0
                    
                    # 计算剩余持仓的成本
                    total_cost = 0.0
                    total_qty = 0.0
                    for qty, cost in buy_queue:
                        total_cost += qty * cost
                        total_qty += qty
                    
                    # 当前持仓应该和队列剩余匹配
                    if total_qty > 0 and abs(total_qty - current_qty) < 0.001:
                        avg_cost = total_cost / total_qty
                        p['avg_px'] = round(avg_cost, 6)
                    elif current_qty > 0:
                        # 如果不匹配，用最新一次买入价格
                        # 查询最近买入记录
                        cursor.execute("""
                            SELECT avg_px, notional_usdt, sz
                            FROM orders 
                            WHERE inst_id LIKE ? AND state='FILLED' AND side='buy'
                            ORDER BY created_ts DESC
                            LIMIT 1
                        """, (f"%{symbol}%",))
                        last_buy = cursor.fetchone()
                        if last_buy:
                            px, notional, sz = last_buy
                            if px:
                                p['avg_px'] = round(float(px), 6)
                            elif notional and sz and float(sz) > 0:
                                p['avg_px'] = round(float(notional) / float(sz), 6)
                
                conn.close()
        except Exception as e:
            import traceback
            print(f"[positions] FIFO成本计算错误: {e}")
            print(traceback.format_exc())
        
        # 计算持仓盈亏
        for p in positions:
            avg_px = float(p.get('avg_px', 0))
            last_px = float(p.get('last_price', 0))
            if avg_px > 0 and last_px > 0:
                p['pnl_pct'] = round((last_px - avg_px) / avg_px, 4)
                # 盈亏金额
                qty = float(p.get('qty', 0))
                p['pnl_value'] = round((last_px - avg_px) * qty, 4)
            else:
                p['pnl_pct'] = 0.0
                p['pnl_value'] = 0.0
            p['price'] = last_px
            p['value'] = p.get('value_usdt', 0)
            p['quantity'] = p.get('qty', 0)
        
        return jsonify({'positions': positions})
    except Exception as e:
        return jsonify({'error': str(e), 'positions': []}), 500


@app.route('/api/scores')
def api_scores():
    """币种评分API（当前run vs 上一个run 的排名变化）"""
    try:
        runs_dir = REPORTS_DIR / 'runs'
        if not runs_dir.exists():
            return jsonify({'regime': 'Unknown', 'scores': []})

        run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
        run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        if not run_dirs:
            return jsonify({'regime': 'Unknown', 'scores': []})

        def load_scores(run_dir: Path):
            with open(run_dir / 'decision_audit.json', 'r', encoding='utf-8') as f:
                decision = json.load(f)
            items = []
            for item in decision.get('top_scores', [])[:20]:
                try:
                    items.append({'symbol': item.get('symbol', 'Unknown'), 'score': round(float(item.get('score', 0)), 4)})
                except Exception:
                    continue
            regime = decision.get('regime', 'Unknown')
            return regime, items

        current_run = run_dirs[0]
        current_regime, current_scores = load_scores(current_run)

        previous_scores = []
        previous_run_id = None
        if len(run_dirs) > 1:
            previous_run_id = run_dirs[1].name
            _, previous_scores = load_scores(run_dirs[1])

        previous_ranking = {}
        for idx, s in enumerate(previous_scores):
            previous_ranking[s['symbol']] = {'rank': idx + 1, 'score': s.get('score', 0)}

        scores_with_trend = []
        for idx, s in enumerate(current_scores):
            symbol = s['symbol']
            current_rank = idx + 1
            prev_info = previous_ranking.get(symbol)
            if prev_info:
                rank_change = prev_info['rank'] - current_rank
                score_change = round(float(s['score']) - float(prev_info['score']), 4)
                trend = 'up' if rank_change > 0 else 'down' if rank_change < 0 else 'stable'
                scores_with_trend.append({
                    **s,
                    'rank': current_rank,
                    'previous_rank': prev_info['rank'],
                    'rank_change': rank_change,
                    'score_change': score_change,
                    'trend': trend,
                })
            else:
                scores_with_trend.append({
                    **s,
                    'rank': current_rank,
                    'previous_rank': None,
                    'rank_change': None,
                    'score_change': None,
                    'trend': 'new',
                })

        return jsonify({
            'regime': current_regime,
            'current_run': current_run.name,
            'previous_run': previous_run_id,
            'scores': scores_with_trend,
            'last_update': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'regime': 'Error', 'scores': [], 'error': str(e)}), 500


@app.route('/api/sentiment')
def api_sentiment():
    """情绪分析API（优先读取本地缓存，避免阻塞UI）"""
    try:
        # 动态展示：主流币 + 当前评分Top币，避免TRX等未显示
        symbols = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']
        try:
            top_scores = api_scores().get_json().get('scores', [])[:8]
            for row in top_scores:
                sym = str(row.get('symbol', '')).replace('/USDT', '-USDT')
                if sym and sym not in symbols:
                    symbols.append(sym)
        except Exception:
            pass
        cache_dir = WORKSPACE / 'data/sentiment_cache'
        results = {}

        for symbol in symbols:
            try:
                # 优先顺序: rss > funding > deepseek > 通用
                files = []
                
                # 1. 尝试RSS币种特定缓存
                rss_files = sorted(cache_dir.glob(f'rss_{symbol}_*.json'))
                if rss_files:
                    files = rss_files
                
                # 2. 尝试RSS市场通用缓存 (rss_MARKET_*)
                if not files:
                    rss_market_files = sorted(cache_dir.glob('rss_MARKET_*.json'))
                    if rss_market_files:
                        files = rss_market_files
                
                # 3. 尝试funding缓存
                if not files:
                    funding_files = sorted(cache_dir.glob(f'funding_{symbol}_*.json'))
                    if funding_files:
                        files = funding_files
                
                # 4. 尝试deepseek缓存
                if not files:
                    files = sorted(cache_dir.glob(f'deepseek_{symbol}_*.json'))
                
                # 5. 回退通用缓存
                if not files:
                    files = sorted(cache_dir.glob(f'{symbol}_*.json'))

                if not files:
                    results[symbol] = {'error': 'no_cache'}
                    continue

                latest = files[-1]
                with open(latest, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                results[symbol] = {
                    'sentiment': float(data.get('f6_sentiment', 0.0)),
                    'fear_greed': float(data.get('f6_fear_greed_index', 50.0)),
                    'stage': data.get('f6_market_stage', 'unknown'),
                    'summary': data.get('f6_sentiment_summary', ''),
                    'source': data.get('f6_sentiment_source', 'cache'),
                    'cache_file': latest.name,
                    'cache_mtime': datetime.fromtimestamp(latest.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                }
            except Exception as e:
                results[symbol] = {'error': str(e)}

        valid_scores = [r['sentiment'] for r in results.values() if 'sentiment' in r]
        valid_fg = [r['fear_greed'] for r in results.values() if 'fear_greed' in r]
        avg_sentiment = sum(valid_scores) / len(valid_scores) if valid_scores else 0.0
        avg_fear_greed = sum(valid_fg) / len(valid_fg) if valid_fg else 50.0

        if avg_sentiment > 0.5:
            market_mood = '贪婪'
            mood_color = '#22c55e'
        elif avg_sentiment < -0.5:
            market_mood = '恐慌'
            mood_color = '#ef4444'
        else:
            market_mood = '中性'
            mood_color = '#64748b'

        return jsonify({
            'overall': {
                'sentiment': round(avg_sentiment, 4),
                'fear_greed': int(round(avg_fear_greed)),
                'mood': market_mood,
                'mood_color': mood_color
            },
            'by_symbol': results,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


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
    """市场状态API（显示Ensemble三种方法）"""
    try:
        # 优先从最新 decision_audit 读取，避免 scores 接口与 market_state 口径不一致
        regime = 'Risk-Off'
        ensemble_data = {}
        audit = {}
        try:
            runs_dir = REPORTS_DIR / 'runs'
            if runs_dir.exists():
                run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
                run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                if run_dirs:
                    with open(run_dirs[0] / 'decision_audit.json', 'r') as f:
                        audit = json.load(f)
                        regime = audit.get('regime', regime)
                        if 'regime_details' in audit:
                            ensemble_data = audit['regime_details']
                            # 若有final_state，优先使用它作为最终状态
                            regime = ensemble_data.get('final_state', regime)
        except Exception:
            # 回退到 scores 口径
            try:
                scores_data = api_scores().get_json()
                regime = scores_data.get('regime', regime)
            except Exception:
                pass
        
        # 计算市场指标
        indicators = calculate_market_indicators()
        
        # 仓位乘数：优先使用最新审计中的真实值，回退到配置映射
        multiplier_map = {
            'Risk-Off': 0.0,
            'RISK_OFF': 0.0,
            'Trending': 1.2,
            'TRENDING': 1.2,
            'Sideways': 0.8,
            'SIDEWAYS': 0.8
        }
        multiplier = float(audit.get('regime_multiplier', multiplier_map.get(regime, 0.3)))
        
        # 描述
        descriptions = {
            'Risk-Off': '风险规避模式，空仓保护中',
            'RISK_OFF': '风险规避模式，空仓保护中',
            'Trending': '趋势行情，增加仓位暴露',
            'TRENDING': '趋势行情，增加仓位暴露',
            'Sideways': '震荡行情，正常仓位',
            'SIDEWAYS': '震荡行情，正常仓位'
        }
        
        # 构建响应（显示三种判断标准）
        response = {
            'state': regime.upper().replace('-', '_'),
            'position_multiplier': multiplier,
            'description': descriptions.get(regime, '市场状态监控中'),
            'method': ensemble_data.get('method', '传统MA'),
            'votes': {
                'hmm': ensemble_data.get('votes', {}).get('hmm', {'state': 'N/A', 'weight': 0}),
                'funding': ensemble_data.get('votes', {}).get('funding', {'state': 'N/A', 'weight': 0}),
                'rss': ensemble_data.get('votes', {}).get('rss', {'state': 'N/A', 'weight': 0})
            },
            'final_score': ensemble_data.get('final_score', 0),
            'price': indicators['price']
        }
        
        return jsonify(response)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


def _load_equity_points(limit: int = 800):
    """从 reports/runs/*/equity.jsonl 聚合权益点（真实口径：cash+持仓市值）。"""
    runs_dir = REPORTS_DIR / 'runs'
    points = []
    if not runs_dir.exists():
        return points

    run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()])
    for run_dir in run_dirs:
        eq_file = run_dir / 'equity.jsonl'
        if not eq_file.exists():
            continue
        try:
            with open(eq_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                        ts = row.get('ts')
                        eq = row.get('equity')
                        if ts is None or eq is None:
                            continue
                        points.append((str(ts), float(eq)))
                    except Exception:
                        continue
        except Exception:
            continue

    # 去重并排序
    dedup = {}
    for ts, eq in points:
        dedup[ts] = eq
    points = sorted(dedup.items(), key=lambda x: x[0])
    if len(points) > limit:
        points = points[-limit:]
    return points


@app.route('/api/equity_history')
def api_equity_history():
    """权益曲线历史（基于运行时equity快照）"""
    try:
        points = _load_equity_points()
        data = [{'timestamp': ts, 'value': round(eq, 4)} for ts, eq in points]
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/equity_curve')
def api_equity_curve():
    """权益曲线 - 新版格式（基于运行时equity快照，默认展示最近48小时并做时间分桶）"""
    try:
        points = _load_equity_points()
        if not points:
            return jsonify({'dates': [], 'values': [], 'pnl': [], 'initial': 0, 'current': 0, 'total_return': 0, 'days': 0})

        # 解析时间，保留最近48小时，避免全历史挤在一起
        parsed = []
        for ts, eq in points:
            try:
                dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
                parsed.append((dt, float(eq)))
            except Exception:
                continue
        if not parsed:
            return jsonify({'dates': [], 'values': [], 'pnl': [], 'initial': 0, 'current': 0, 'total_return': 0, 'days': 0})

        end_dt = parsed[-1][0]
        start_dt = end_dt - timedelta(hours=48)
        recent = [(dt, eq) for dt, eq in parsed if dt >= start_dt]
        if len(recent) < 10:
            recent = parsed[-200:]  # 回退：至少给一些点

        # 15分钟分桶，取每桶最后一个点
        bucketed = {}
        for dt, eq in recent:
            b = dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)
            bucketed[b] = eq
        series = sorted(bucketed.items(), key=lambda x: x[0])

        dates, values, pnls = [], [], []
        prev = None
        for dt, eq in series:
            dates.append(dt.isoformat())
            values.append(round(eq, 4))
            pnls.append(round(eq - prev, 4) if prev is not None else 0.0)
            prev = eq

        initial = values[0] if values else 0
        current = values[-1] if values else 0
        total_return = ((current - initial) / initial * 100) if initial else 0
        days = len({d.split('T')[0] for d in dates})

        return jsonify({
            'dates': dates,
            'values': values,
            'pnl': pnls,
            'initial': round(initial, 4),
            'current': round(current, 4),
            'total_return': round(total_return, 2),
            'days': int(days)
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
            avg_price = float(pos.get('avg_px', 0) or 0)
            cur_price = float(pos.get('last_price', 0) or 0)
            qty = float(pos.get('qty', 0) or 0)
            value = float(pos.get('value_usdt', 0) or 0)
            pnl = (cur_price - avg_price) * qty if avg_price > 0 and cur_price > 0 else 0
            pnl_pct = ((cur_price - avg_price) / avg_price * 100) if avg_price > 0 and cur_price > 0 else 0
            positions.append({
                'symbol': pos.get('symbol', ''),
                'qty': qty,
                'avgPrice': round(avg_price, 6),
                'currentPrice': round(cur_price, 6),
                'value': round(value, 4),
                'pnl': round(pnl, 4),
                'pnlPercent': round(pnl_pct, 2)
            })
        
        # 转换交易格式
        trades = []
        for i, trade in enumerate(trades_data[:20]):
            trades.append({
                'id': str(i),
                'timestamp': trade.get('time', '') if trade.get('time') else '',
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
        
        positions_value = sum(float(p.get('value', 0) or 0) for p in positions)
        cash_usdt = float(account_data.get('cash_usdt', 0) or 0)
        total_equity = cash_usdt + positions_value
        realized_pnl = float(account_data.get('realized_pnl', 0) or 0)

        dashboard_data = {
            'account': {
                'totalEquity': round(total_equity, 4),
                'cash': round(cash_usdt, 4),
                'positionsValue': round(positions_value, 4),
                'totalPnl': round(realized_pnl, 4),
                'totalPnlPercent': round((realized_pnl / max(total_equity, 1e-9)) * 100, 2) if total_equity > 0 else 0,
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
    """F2成本校准进度API - 从定时任务生成的真实成本数据计算"""
    try:
        # 优先读取定时任务生成的真实成本数据 (cost_stats_real)
        cost_dir = REPORTS_DIR / 'cost_stats_real'
        if not cost_dir.exists():
            cost_dir = REPORTS_DIR / 'cost_stats'  # 兼容旧路径
        events_dir = REPORTS_DIR / 'cost_events'
        
        calibration_data = []
        total_days = 0
        avg_slippage_bps = 0
        avg_fee_bps = 0
        total_trade_count = 0
        
        # 优先从cost_stats_real读取已汇总的数据
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
                    total_day_cost = avg_day_slippage + avg_day_fee
                    
                    # 过滤异常值（成本 > 1000 bps 或 < 0 视为异常）
                    if total_day_cost > 1000 or total_day_cost < 0:
                        print(f"[CostCalibration] Skipping abnormal day {day}: cost={total_day_cost:.2f} bps")
                        continue
                    
                    calibration_data.append({
                        'date': day,
                        'slippage_bps': round(avg_day_slippage, 4),
                        'fee_bps': round(avg_day_fee, 4),
                        'total_cost_bps': round(total_day_cost, 4),
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
        # 查找IC诊断文件（按修改时间，避免文件名排序误判）
        ic_files = list(REPORTS_DIR.glob('ic_diagnostics_*.json'))

        if not ic_files:
            return jsonify({
                'status': 'no_data',
                'message': '暂无IC诊断数据'
            })

        ic_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        # 优先使用“有可用因子IC”的最新文件；否则回退到最近文件
        latest_ic = ic_files[0]
        ic_data = None
        fallback_reason = None
        for f in ic_files:
            try:
                with open(f, 'r', encoding='utf-8') as fh:
                    d = json.load(fh)
                
                # 检查新格式 (fresh文件)
                if 'factors' in d and isinstance(d['factors'], dict):
                    for factor_info in d['factors'].values():
                        if factor_info.get('count', 0) > 0:
                            latest_ic = f
                            ic_data = d
                            break
                    if ic_data:
                        break
                    continue
                
                # 检查旧格式
                ic_by_factor = (d.get('overall_tradable') or {}).get('ic', {})
                has_valid_data = False
                if isinstance(ic_by_factor, dict) and len(ic_by_factor) > 0:
                    for factor_data in ic_by_factor.values():
                        if isinstance(factor_data, dict) and factor_data.get('count', 0) > 0:
                            has_valid_data = True
                            break
                if has_valid_data:
                    latest_ic = f
                    ic_data = d
                    break
            except Exception:
                continue

        if ic_data is None:
            with open(latest_ic, 'r', encoding='utf-8') as f:
                ic_data = json.load(f)
            fallback_reason = 'latest_file_has_no_valid_factor_ic'
        
        # 检查是否是新的简化格式（fresh文件）
        if 'factors' in ic_data and isinstance(ic_data['factors'], dict):
            # 新格式：直接有factors字段
            factors_data = ic_data['factors']
            factors = []
            all_ic_values = []
            for factor_name, factor_info in factors_data.items():
                ic_val = factor_info.get('ic', 0)
                count = factor_info.get('count', 0)
                all_ic_values.append(ic_val)
                factors.append({
                    'name': factor_name,
                    'ic': round(ic_val, 4),
                    'ic_median': round(ic_val, 4),  # 简化为相同值
                    'ic_std': 0.1,
                    'ir': round(ic_val / 0.1, 4),
                    'sample_count': count
                })
            
            overall_ic_mean = sum(all_ic_values) / len(all_ic_values) if all_ic_values else 0
            overall_std = 0.1
            overall_ir = overall_ic_mean / overall_std
            
            # 处理by_regime
            regimes = []
            regime_data = ic_data.get('by_regime', {})
            for regime_name, regime_factors in regime_data.items():
                if isinstance(regime_factors, dict):
                    regime_ic_values = [v for v in regime_factors.values() if isinstance(v, (int, float))]
                    avg_ic = sum(regime_ic_values) / len(regime_ic_values) if regime_ic_values else 0
                    regimes.append({
                        'name': regime_name[:20],  # 截断长名称
                        'ic': round(avg_ic, 4),
                        'sample_count': 0
                    })
            
            return jsonify({
                'status': 'ready',
                'overall_ic': round(overall_ic_mean, 4),
                'overall_ir': round(overall_ir, 4),
                'sample_count': ic_data.get('total_samples', 0),
                'timestamps_count': 0,
                'lookback_days': 14,
                'factors': factors,
                'regimes': regimes,
                'source_file': latest_ic.name,
                'fallback_reason': 'fresh_format',
                'last_update': datetime.fromtimestamp(latest_ic.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # 解析IC数据 - 旧版结构在overall_tradable.ic下
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
        def _num(v, default=0.0):
            try:
                if v is None:
                    return float(default)
                return float(v)
            except Exception:
                return float(default)

        factors = []
        for factor_name, factor_data in ic_by_factor.items():
            mean_ic = _num(factor_data.get('mean', 0.0), 0.0)
            p50_ic = _num(factor_data.get('p50', 0.0), 0.0)
            count = int(_num(factor_data.get('count', 0), 0))

            # 简化计算IR (IC / std)，如果std不可用则用近似值
            p75 = _num(factor_data.get('p75', 0.0), 0.0)
            p25 = _num(factor_data.get('p25', 0.0), 0.0)
            std_approx = ((p75 - p25) / 1.35) if (p75 != 0 or p25 != 0) else 0.1
            std_approx = std_approx if std_approx > 1e-9 else 0.1
            ir = mean_ic / std_approx

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
            'source_file': latest_ic.name,
            'fallback_reason': fallback_reason,
            'last_update': datetime.fromtimestamp(latest_ic.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/ml_training')
def api_ml_training():
    """机器学习训练进度API（对齐当前项目文件结构）"""
    try:
        model_dir = WORKSPACE / 'models'
        model_candidates = []
        if model_dir.exists():
            model_candidates += list(model_dir.glob('ml_factor_model.txt'))
            model_candidates += list(model_dir.glob('lgb_model_*.pkl'))
        latest_model = max(model_candidates, key=lambda p: p.stat().st_mtime) if model_candidates else None
        model_time = datetime.fromtimestamp(latest_model.stat().st_mtime) if latest_model else None

        # 优先从 SQLite 统计样本（当前真实路径）
        total_samples = 0
        labeled_samples = 0
        db_path = REPORTS_DIR / 'ml_training_data.db'
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM feature_snapshots')
            total_samples = int(cur.fetchone()[0] or 0)
            cur.execute('SELECT COUNT(*) FROM feature_snapshots WHERE label_filled = 1')
            labeled_samples = int(cur.fetchone()[0] or 0)
            conn.close()

        # 兼容旧CSV路径
        data_dir = WORKSPACE / 'data' / 'ml_training'
        data_files = list(data_dir.glob('training_data_*.csv')) if data_dir.exists() else []

        # 训练日志（支持负数/NaN）
        training_log = WORKSPACE / 'logs' / 'ml_training.log'
        last_ic = None
        if training_log.exists():
            try:
                with open(training_log, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                import re
                for line in reversed(lines):
                    m = re.search(r'Valid IC[:\s]+([+-]?\d*\.?\d+|nan)', line, re.IGNORECASE)
                    if m:
                        v = m.group(1).lower()
                        last_ic = None if v == 'nan' else float(v)
                        break
            except Exception:
                pass

        effective_samples = labeled_samples if labeled_samples > 0 else total_samples
        if model_time and (datetime.now() - model_time).days < 1:
            status = 'trained_today'
        elif effective_samples >= 100:
            status = 'ready_to_train'
        elif effective_samples > 0:
            status = 'collecting_data'
        else:
            status = 'no_data'

        return jsonify({
            'status': status,
            'total_samples': total_samples,
            'labeled_samples': labeled_samples,
            'samples_needed': 100,
            'progress_percent': min(100, int((effective_samples / 100) * 100)) if effective_samples else 0,
            'latest_model': latest_model.name if latest_model else None,
            'model_date': model_time.strftime('%Y-%m-%d %H:%M') if model_time else None,
            'last_ic': round(last_ic, 4) if last_ic is not None else None,
            'data_files': len(data_files),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reflection_reports')
def api_reflection_reports():
    """反思Agent报告列表API（兼容V1/V2结构）"""
    try:
        reflection_dir = REPORTS_DIR / 'reflection'

        if not reflection_dir.exists():
            return jsonify({'reports': [], 'message': '暂无反思报告'})

        reports = []
        report_files = sorted(reflection_dir.glob('reflection_*.json'), reverse=True)

        for report_file in report_files[:10]:
            try:
                with open(report_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # V2: summary/alerts; V1: overall_metrics/insights
                summary = data.get('summary', {})
                metrics = data.get('overall_metrics', {})
                alerts = data.get('alerts', [])
                insights = data.get('insights', [])

                total_pnl = summary.get('total_realized_pnl', metrics.get('total_pnl', 0))
                trade_count = summary.get('total_trades', metrics.get('total_trades', 0))
                symbols = summary.get('total_symbols', metrics.get('unique_symbols', 0))

                high_priority = sum(1 for a in alerts if str(a.get('level', '')).lower() in ('high', 'critical'))
                medium_priority = sum(1 for a in alerts if str(a.get('level', '')).lower() in ('medium', 'warning'))
                if not alerts and insights:
                    high_priority = sum(1 for i in insights if str(i.get('severity', '')).lower() == 'high')
                    medium_priority = sum(1 for i in insights if str(i.get('severity', '')).lower() == 'medium')

                reports.append({
                    'filename': report_file.name,
                    'date': report_file.stem.replace('reflection_', ''),
                    'total_pnl': round(float(total_pnl or 0), 2),
                    'trade_count': int(trade_count or 0),
                    'symbols': int(symbols or 0),
                    'insights_count': len(alerts) if alerts else len(insights),
                    'high_priority': high_priority,
                    'medium_priority': medium_priority
                })
            except Exception:
                continue

        return jsonify({
            'reports': reports,
            'total_reports': len(report_files),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/decision_chain')
def api_decision_chain():
    """决策归因面板API - 展示策略信号到执行的完整链路"""
    try:
        # 获取最近5轮决策记录
        runs_dir = REPORTS_DIR / 'runs'
        if not runs_dir.exists():
            return jsonify({'rounds': [], 'message': '暂无决策记录'})

        run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
        run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)

        rounds = []
        for run_dir in run_dirs[:5]:
            try:
                with open(run_dir / 'decision_audit.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # 提取决策链信息
                run_id = run_dir.name
                ts = data.get('now_ts') or data.get('window_start_ts')
                if ts:
                    # 判断是旧数据(UTC)还是新数据(CST)
                    # 通过比较 run_id 小时和文件修改时间来判断
                    import os
                    mtime = os.path.getmtime(run_dir)
                    mtime_dt = datetime.fromtimestamp(mtime)
                    
                    # 如果 run_id 小时与本地修改时间相差很大，说明是旧UTC数据
                    run_hour = int(run_id.split('_')[-1]) if '_' in run_id else 0
                    local_hour = mtime_dt.hour
                    
                    # UTC数据的特征：run_id小时 = 本地小时 - 8 (或 +16)
                    hour_diff = (run_hour - local_hour) % 24
                    if hour_diff >= 16:  # 相差16小时以上，说明是UTC命名的旧数据
                        # 旧数据：时间戳是UTC，需要+8转为CST
                        run_time = datetime.fromtimestamp(ts + 8*3600).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # 新数据：时间戳已经是CST
                        run_time = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    run_time = run_id

                # 1. 策略层信号
                selected_scores = data.get('top_scores', [])
                strategy_signals = []
                for item in selected_scores[:5]:
                    strategy_signals.append({
                        'symbol': item.get('symbol'),
                        'score': round(float(item.get('score', 0)), 4),
                        'rank': item.get('rank', 0)
                    })

                # 2. 风控层状态
                risk_state = {
                    'regime': data.get('regime', 'Unknown'),
                    'regime_multiplier': data.get('regime_multiplier', 1.0),
                    'dd_multiplier': None,
                    'deadband': data.get('rebalance_deadband_pct')
                }
                # 从notes中提取DD multiplier
                for note in data.get('notes', []):
                    if 'DD multiplier' in note:
                        try:
                            import re
                            m = re.search(r'DD multiplier:\s*([\d.]+)', note)
                            if m:
                                risk_state['dd_multiplier'] = float(m.group(1))
                        except:
                            pass
                    if 'drawdown' in note.lower():
                        try:
                            import re
                            m = re.search(r'drawdown:\s*([\d.]+)%', note, re.IGNORECASE)
                            if m:
                                risk_state['drawdown_pct'] = float(m.group(1))
                        except:
                            pass

                # 3. 执行层结果
                counts = data.get('counts', {})
                execution_result = {
                    'selected': int(counts.get('selected', 0) or 0),
                    'targets_pre_risk': int(counts.get('targets_pre_risk', 0) or 0),
                    'orders_rebalance': int(counts.get('orders_rebalance', 0) or 0),
                    'orders_exit': int(counts.get('orders_exit', 0) or 0)
                }

                # 4. 阻塞原因统计
                router_decisions = data.get('router_decisions', [])
                block_reasons = {}
                for rd in router_decisions:
                    reason = rd.get('reason', 'unknown')
                    block_reasons[reason] = block_reasons.get(reason, 0) + 1

                # 5. 被拦截的Top信号
                blocked_signals = []
                for rd in (router_decisions or []):
                    if rd.get('reason') == 'deadband':
                        try:
                            drift_v = float(rd.get('drift') or 0.0)
                        except Exception:
                            drift_v = 0.0
                        try:
                            deadband_v = float(rd.get('deadband') or 0.0)
                        except Exception:
                            deadband_v = 0.0
                        blocked_signals.append({
                            'symbol': rd.get('symbol'),
                            'drift': round(drift_v, 4),
                            'deadband': round(deadband_v, 4)
                        })
                # 按漂移排序
                blocked_signals.sort(key=lambda x: abs(float(x.get('drift', 0) or 0)), reverse=True)

                rounds.append({
                    'run_id': run_id,
                    'time': run_time,
                    'strategy_signals': strategy_signals,
                    'risk_state': risk_state,
                    'execution_result': execution_result,
                    'block_reasons': block_reasons,
                    'blocked_top': blocked_signals[:3]
                })
            except Exception as e:
                # 保留可观测性，避免静默失败导致前端长期显示空白
                rounds.append({
                    'run_id': run_dir.name,
                    'time': run_dir.name,
                    'strategy_signals': [],
                    'risk_state': {'regime': 'Error'},
                    'execution_result': {'selected': 0, 'targets_pre_risk': 0, 'orders_rebalance': 0, 'orders_exit': 0},
                    'block_reasons': {'parse_error': 1},
                    'blocked_top': [],
                    'error': str(e)
                })
                continue

        return jsonify({
            'rounds': rounds,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/shadow_test')
def api_shadow_test():
    """参数A/B影子测试API - 对比当前参数与候选参数的历史表现"""
    try:
        import sys
        sys.path.insert(0, str(WORKSPACE))
        
        # 获取最近7天的运行数据用于对比
        runs_dir = REPORTS_DIR / 'runs'
        if not runs_dir.exists():
            return jsonify({'status': 'no_data', 'message': '暂无运行数据'})
        
        run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
        run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # 取最近7天（最多50轮）
        recent_runs = run_dirs[:50]
        
        current_stats = {
            'rounds': 0,
            'total_selected': 0,
            'total_rebalance': 0,
            'total_exit': 0,
            'deadband_blocks': 0,
            'avg_deadband_skip': 0
        }
        
        # 模拟：新参数效果（deadband从0.04->0.03的预估影响）
        # 实际实现需要重新跑历史数据，这里用启发式估算
        simulated_stats = {
            'rounds': 0,
            'total_selected': 0,
            'total_rebalance': 0,
            'total_exit': 0,
            'deadband_blocks': 0,
            'avg_deadband_skip': 0,
            'estimated_improvement': 0
        }
        
        deadband_skips = []
        
        for run_dir in recent_runs:
            try:
                with open(run_dir / 'decision_audit.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                counts = data.get('counts', {})
                current_stats['rounds'] += 1
                current_stats['total_selected'] += counts.get('selected', 0)
                current_stats['total_rebalance'] += counts.get('orders_rebalance', 0)
                current_stats['total_exit'] += counts.get('orders_exit', 0)
                
                # 统计deadband拦截
                router_decisions = data.get('router_decisions', [])
                deadband_count = sum(1 for rd in router_decisions if rd.get('reason') == 'deadband')
                current_stats['deadband_blocks'] += deadband_count
                
                # 记录被拦漂移值用于模拟
                for rd in router_decisions:
                    if rd.get('reason') == 'deadband':
                        drift = abs(float(rd.get('drift', 0)))
                        deadband_skips.append(drift)
                        
                        # 模拟：如果deadband是0.03而不是0.04，有多少能成交
                        if drift > 0.03:  # 新阈值下能通过
                            simulated_stats['estimated_improvement'] += 1
                            
            except Exception:
                continue
        
        # 计算当前统计
        if current_stats['rounds'] > 0:
            current_stats['avg_selected'] = round(current_stats['total_selected'] / current_stats['rounds'], 2)
            current_stats['avg_rebalance'] = round(current_stats['total_rebalance'] / current_stats['rounds'], 2)
            current_stats['conversion_rate'] = round(
                (current_stats['total_rebalance'] / current_stats['total_selected'] * 100) if current_stats['total_selected'] > 0 else 0, 
                1
            )
        
        if deadband_skips:
            current_stats['avg_deadband_skip'] = round(sum(deadband_skips) / len(deadband_skips), 4)
        
        # 读取/刷新 A/B gate 评估（建议是否切参）
        ab_gate = None
        try:
            gate_path = REPORTS_DIR / 'ab_gate_status.json'
            need_refresh = True
            if gate_path.exists():
                age_sec = max(0, (datetime.now().timestamp() - gate_path.stat().st_mtime))
                need_refresh = age_sec > 1800  # 30分钟
            if need_refresh:
                subprocess.run(
                    [str(WORKSPACE / '.venv/bin/python'), str(WORKSPACE / 'scripts/ab_decision_gate.py')],
                    cwd=str(WORKSPACE),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=12,
                    check=False,
                )
            if gate_path.exists():
                with open(gate_path, 'r', encoding='utf-8') as f:
                    ab_gate = json.load(f)
        except Exception:
            ab_gate = None

        # 生成A/B对比报告
        ab_report = {
            'status': 'ready',
            'window_days': 7,
            'window_rounds': current_stats['rounds'],
            'current_params': {
                'deadband_sideways': 0.04,
                'description': '当前参数'
            },
            'proposed_params': {
                'deadband_sideways': 0.03,
                'description': '建议参数（更激进）'
            },
            'comparison': {
                'current': {
                    'avg_selected_per_round': current_stats.get('avg_selected', 0),
                    'avg_rebalance_per_round': current_stats.get('avg_rebalance', 0),
                    'conversion_rate': current_stats.get('conversion_rate', 0),
                    'total_deadband_blocks': current_stats['deadband_blocks'],
                    'avg_drift_when_blocked': current_stats['avg_deadband_skip']
                },
                'estimated_with_proposed': {
                    'avg_rebalance_per_round': round(
                        current_stats.get('avg_rebalance', 0) + 
                        (simulated_stats['estimated_improvement'] / max(current_stats['rounds'], 1)), 
                        2
                    ),
                    'estimated_conversion_rate': round(
                        ((current_stats['total_rebalance'] + simulated_stats['estimated_improvement']) / 
                         max(current_stats['total_selected'], 1)) * 100,
                        1
                    ),
                    'additional_trades': simulated_stats['estimated_improvement'],
                    'risk_note': '成交增加，但可能包含更多弱信号'
                }
            },
            'recommendation': {
                'action': 'cautious_try' if simulated_stats['estimated_improvement'] > 5 else 'keep_current',
                'reason': f"过去{current_stats['rounds']}轮中，约{simulated_stats['estimated_improvement']}笔额外交易可成交" if simulated_stats['estimated_improvement'] > 0 else "当前参数下成交率已合理",
                'suggested_next_step': '将 deadband_sideways 从 0.04 调至 0.03，观察24小时' if simulated_stats['estimated_improvement'] > 5 else '保持当前参数'
            },
            'matrix': [
                {'name': 'A(当前)', 'params': {'deadband_sideways': 0.03, 'min_trade_notional_base': 2.0, 'pos_mult_sideways': 0.8}},
                {'name': 'B1', 'params': {'deadband_sideways': 0.025}},
                {'name': 'B2', 'params': {'min_trade_notional_base': 2.5}},
                {'name': 'B3', 'params': {'pos_mult_sideways': 0.7}},
            ],
            'ab_gate': ab_gate,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return jsonify(ab_report)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/smart_alerts')
def api_smart_alerts():
    """智能告警API - 返回当前活跃的告警"""
    try:
        from src.monitoring.smart_alert import SmartAlertEngine
        
        engine = SmartAlertEngine()
        alerts = engine.run_all_checks()
        
        return jsonify({
            'alerts': alerts,
            'count': len(alerts),
            'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'alert' if alerts else 'normal'
        })
    except Exception as e:
        return jsonify({'error': str(e), 'alerts': [], 'status': 'error'}), 500


@app.route('/api/auto_risk_guard')
def api_auto_risk_guard():
    """自动风险档位API - 显示当前风险档位和配置"""
    try:
        from src.risk.auto_risk_guard import get_auto_risk_guard
        guard = get_auto_risk_guard()
        config = guard.get_current_config()
        return jsonify({
            'current_level': guard.current_level,
            'config': config,
            'history': guard.history[-5:],  # 最近5次切换
            'metrics': guard.metrics,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/decision_audit')
def api_decision_audit():
    """获取最新决策审计数据"""
    try:
        # 找到最新的决策审计文件
        runs_dir = REPORTS_DIR / 'runs'
        if not runs_dir.exists():
            return jsonify({'error': 'No runs directory'}), 404
        
        run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
        if not run_dirs:
            return jsonify({'error': 'No audit files found'}), 404
        
        # 按修改时间排序，取最新的
        run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        latest_run_dir = run_dirs[0]
        latest_audit_file = latest_run_dir / 'decision_audit.json'
        
        with open(latest_audit_file, 'r') as f:
            audit_data = json.load(f)
        
        # 使用文件修改时间作为时间戳（now_ts可能不正确）
        file_mtime = latest_run_dir.stat().st_mtime
        
        # 同时尝试读取策略信号审计
        strategy_signals = []
        strategy_file = latest_run_dir / 'strategy_signals.json'
        if strategy_file.exists():
            with open(strategy_file, 'r') as f:
                strategy_data = json.load(f)
                strategy_signals = strategy_data.get('strategies', [])
        
        return jsonify({
            'run_id': audit_data.get('run_id'),
            'timestamp': file_mtime,  # 使用文件修改时间
            'regime': audit_data.get('regime'),
            'regime_details': audit_data.get('regime_details', {}),
            'counts': audit_data.get('counts', {}),
            'strategy_signals': strategy_signals,
            'notes': audit_data.get('notes', [])[:10]  # 只返回前10条
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health')
def api_health():
    """系统健康检查API"""
    try:
        checks = []
        overall_status = 'healthy'
        
        # 1. 检查定时任务
        try:
            import subprocess
            result = subprocess.run(
                ['systemctl', '--user', 'is-active', 'v5-live-20u.user.timer'],
                capture_output=True, text=True, timeout=5
            )
            timer_active = result.returncode == 0
            
            if timer_active:
                checks.append({'name': '定时任务', 'status': 'healthy', 'detail': 'v5-live-20u运行中'})
            else:
                checks.append({'name': '定时任务', 'status': 'critical', 'detail': 'v5-live-20u未运行'})
                overall_status = 'critical'
        except Exception as e:
            checks.append({'name': '定时任务', 'status': 'warning', 'detail': str(e)})
            overall_status = 'warning'
        
        # 2. 检查数据库
        try:
            orders_db = REPORTS_DIR / 'orders.sqlite'
            if orders_db.exists():
                conn = sqlite3.connect(str(orders_db))
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM orders")
                count = cursor.fetchone()[0]
                conn.close()
                checks.append({'name': '数据库', 'status': 'healthy', 'detail': f'{count}条订单记录'})
            else:
                checks.append({'name': '数据库', 'status': 'critical', 'detail': 'orders.sqlite不存在'})
                overall_status = 'critical'
        except Exception as e:
            checks.append({'name': '数据库', 'status': 'warning', 'detail': str(e)})
        
        # 3. 检查OKX API
        try:
            import os, time, hmac, hashlib, base64, requests
            from dotenv import load_dotenv
            load_dotenv(str(WORKSPACE / '.env'))
            
            key = os.getenv('EXCHANGE_API_KEY')
            sec = os.getenv('EXCHANGE_API_SECRET')
            pp = os.getenv('EXCHANGE_PASSPHRASE')
            
            if key and sec and pp:
                start = time.time()
                ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
                path = '/api/v5/account/balance'
                msg = ts + 'GET' + path
                sig = base64.b64encode(hmac.new(sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
                
                headers = {
                    'OK-ACCESS-KEY': key,
                    'OK-ACCESS-SIGN': sig,
                    'OK-ACCESS-TIMESTAMP': ts,
                    'OK-ACCESS-PASSPHRASE': pp,
                }
                
                resp = requests.get('https://www.okx.com' + path, headers=headers, timeout=8)
                latency = (time.time() - start) * 1000
                
                if resp.status_code == 200 and resp.json().get('code') == '0':
                    checks.append({'name': 'OKX API', 'status': 'healthy', 'detail': f'{latency:.0f}ms'})
                else:
                    checks.append({'name': 'OKX API', 'status': 'critical', 'detail': 'API响应异常'})
                    overall_status = 'critical'
            else:
                checks.append({'name': 'OKX API', 'status': 'warning', 'detail': 'API密钥未配置'})
        except Exception as e:
            checks.append({'name': 'OKX API', 'status': 'warning', 'detail': str(e)})
        
        # 4. 检查磁盘空间
        try:
            import shutil
            total, used, free = shutil.disk_usage(WORKSPACE)
            free_gb = free / (1024**3)
            used_percent = used / total * 100
            
            if free_gb < 1:
                checks.append({'name': '磁盘空间', 'status': 'critical', 'detail': f'仅剩{free_gb:.1f}GB'})
                overall_status = 'critical'
            elif used_percent > 90:
                checks.append({'name': '磁盘空间', 'status': 'warning', 'detail': f'已用{used_percent:.1f}%'})
                if overall_status == 'healthy':
                    overall_status = 'warning'
            else:
                checks.append({'name': '磁盘空间', 'status': 'healthy', 'detail': f'{free_gb:.1f}GB可用'})
        except Exception as e:
            checks.append({'name': '磁盘空间', 'status': 'warning', 'detail': str(e)})
        
        return jsonify({
            'status': overall_status,
            'checks': checks,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


if __name__ == '__main__':
    print("="*60)
    print("V5 Web Dashboard 启动中...")
    print("="*60)
    print(f"访问地址: http://0.0.0.0:5000")
    print("="*60)
    app.run(host='0.0.0.0', port=5000, debug=False)
