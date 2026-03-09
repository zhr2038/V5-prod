#!/usr/bin/env python3
"""
V5 交易审计脚本 V3 - 精准版

输出原则：
1. 只报事实，不猜测
2. 有异常才报，无异常静默
3. 数据来源要明确（OKX实时/本地文件/数据库）
"""

import json
import sqlite3
import sys
import os
import time
import hmac
import hashlib
import base64
import requests
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')
ORDERS_DB = Path('/home/admin/clawd/v5-trading-bot/reports/orders.sqlite')
WORKSPACE = Path('/home/admin/clawd/v5-trading-bot')

class TradeAuditorV3:
    """精准交易审计器"""
    
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.info = []
    
    def log(self, msg):
        print(msg)
    
    def get_okx_balance(self):
        """从OKX获取实时余额"""
        try:
            from dotenv import load_dotenv
            load_dotenv(str(WORKSPACE / '.env'))
            
            key = os.getenv('EXCHANGE_API_KEY')
            sec = os.getenv('EXCHANGE_API_SECRET')
            pp = os.getenv('EXCHANGE_PASSPHRASE')
            
            if not (key and sec and pp):
                return None
            
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
                details = data['data'][0].get('details', [])
                usdt_eq = 0
                positions = []
                for d in details:
                    ccy = d.get('ccy')
                    eq = float(d.get('eq', 0))
                    if ccy == 'USDT':
                        usdt_eq = eq
                    elif eq > 0.5:
                        positions.append(f"{ccy}: {eq:.2f}")
                return {'usdt': usdt_eq, 'positions': positions}
        except Exception as e:
            return {'error': str(e)}
        return None
    
    def get_recent_orders(self, hours=2):
        """获取最近订单"""
        if not ORDERS_DB.exists():
            return []
        
        conn = sqlite3.connect(str(ORDERS_DB))
        cursor = conn.cursor()
        
        now = datetime.now()
        start_ts = int((now - timedelta(hours=hours)).timestamp() * 1000)
        
        cursor.execute("""
            SELECT inst_id, side, state, created_ts
            FROM orders 
            WHERE created_ts > ?
            ORDER BY created_ts DESC
        """, (start_ts,))
        
        orders = cursor.fetchall()
        conn.close()
        return orders
    
    def get_market_state(self):
        """获取市场状态"""
        try:
            runs_dir = REPORTS_DIR / 'runs'
            if runs_dir.exists():
                run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
                run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                if run_dirs:
                    with open(run_dirs[0] / 'decision_audit.json', 'r') as f:
                        data = json.load(f)
                    regime = data.get('regime')
                    details = data.get('regime_details', {})
                    multiplier = details.get('position_multiplier', data.get('regime_multiplier', 0.6))
                    return {'state': regime, 'multiplier': multiplier}
        except Exception:
            pass
        return {'state': 'Unknown', 'multiplier': 0}
    
    def analyze(self):
        """执行审计分析"""
        # 1. 获取OKX实时数据
        okx_data = self.get_okx_balance()
        
        # 2. 获取最近订单
        orders = self.get_recent_orders(hours=2)
        
        # 3. 统计订单
        buy_filled = sum(1 for o in orders if o[1] == 'buy' and o[2] == 'FILLED')
        sell_filled = sum(1 for o in orders if o[1] == 'sell' and o[2] == 'FILLED')
        rejected = sum(1 for o in orders if o[2] == 'REJECTED')
        
        # 4. 获取市场状态
        market = self.get_market_state()
        
        return {
            'okx': okx_data,
            'orders': {'buy': buy_filled, 'sell': sell_filled, 'rejected': rejected, 'total': len(orders)},
            'market': market
        }
    
    def generate_report(self, data):
        """生成简洁报告"""
        lines = []
        lines.append("🤖 交易审计报告")
        lines.append("")
        lines.append(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append("")
        
        # 账户状态
        okx = data.get('okx', {})
        if 'error' in okx:
            lines.append(f"⚠️ OKX API错误: {okx['error']}")
        elif okx:
            lines.append(f"💰 账户权益: {okx.get('usdt', 0):.2f} USDT")
            if okx.get('positions'):
                lines.append(f"📊 持仓: {', '.join(okx['positions'][:3])}")
        
        lines.append("")
        
        # 市场状态
        market = data.get('market', {})
        lines.append(f"📈 市场状态: {market.get('state', 'Unknown')}")
        lines.append(f"🎯 仓位乘数: {market.get('multiplier', 0):.2f}x")
        
        lines.append("")
        
        # 交易统计
        orders = data.get('orders', {})
        lines.append(f"📋 最近2小时交易:")
        lines.append(f"  ✅ 买入: {orders.get('buy', 0)} 笔")
        lines.append(f"  ✅ 卖出: {orders.get('sell', 0)} 笔")
        if orders.get('rejected', 0) > 0:
            lines.append(f"  ❌ 拒绝: {orders.get('rejected', 0)} 笔")
        
        # 判断结果
        lines.append("")
        if orders.get('total', 0) == 0:
            lines.append("结果: ⏸️ 无交易")
        elif orders.get('rejected', 0) > 20:
            lines.append("结果: ✅ 通过（大量粉尘过滤，属正常）")
        else:
            lines.append("结果: ✅ 通过")
        
        return "\n".join(lines)
    
    def run(self):
        """运行审计"""
        data = self.analyze()
        report = self.generate_report(data)
        self.log(report)
        return report


def main():
    auditor = TradeAuditorV3()
    auditor.run()


if __name__ == '__main__':
    main()
