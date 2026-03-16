#!/usr/bin/env python3
"""
V5 交易审计脚本 - 详细版
由 Codex 模型风格编写，深度审查每笔交易
"""

import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports/runs')
ORDERS_DB = Path('/home/admin/clawd/v5-trading-bot/reports/orders.sqlite')
LOG_FILE = Path('/home/admin/clawd/v5-trading-bot/logs/trade_audit.log')
ALERT_FILE = Path('/home/admin/clawd/v5-trading-bot/logs/trade_alert.json')

def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')

def get_latest_orders(limit=20):
    """从 SQLite 获取最新订单"""
    if not ORDERS_DB.exists():
        return []
    
    conn = sqlite3.connect(str(ORDERS_DB))
    cursor = conn.cursor()
    cursor.execute("""
        SELECT cl_ord_id, inst_id, side, state, intent, ord_id, last_error_code, last_error_msg
        FROM orders 
        ORDER BY rowid DESC 
        LIMIT ?
    """, (limit,))
    orders = cursor.fetchall()
    conn.close()
    return orders

def analyze_orders(orders):
    """深度分析订单"""
    issues = []
    buy_orders = []
    sell_orders = []
    rejected = []
    
    for order in orders:
        cl_ord_id, inst_id, side, state, intent, ord_id, err_code, err_msg = order
        symbol = inst_id.replace('-USDT', '/USDT')
        
        # 分类
        if side == 'buy':
            buy_orders.append(order)
        else:
            sell_orders.append(order)
        
        # 检查拒绝订单
        if state == 'REJECTED' or (err_code and err_code != '0'):
            rejected.append({
                'symbol': symbol,
                'side': side,
                'intent': intent,
                'error': err_msg or f'code:{err_code}'
            })
            
        # 检查异常状态
        if state not in ['FILLED', 'CANCELED', 'REJECTED', 'LIVE', 'OPEN']:
            issues.append(f"⚠️ 异常状态: {symbol} {side} 状态={state}")
    
    return {
        'issues': issues,
        'buy_count': len(buy_orders),
        'sell_count': len(sell_orders),
        'rejected': rejected
    }

def check_risk_limits():
    """检查风控限制"""
    issues = []
    
    # 检查 kill_switch
    kill_switch = Path('/home/admin/clawd/v5-trading-bot/reports/kill_switch.json')
    if kill_switch.exists():
        with open(kill_switch) as f:
            ks = json.load(f)
            if ks.get('enabled'):
                issues.append(f"🚨 Kill Switch 已启用: {ks.get('reason', 'unknown')}")
    
    # 检查 reconcile 状态
    reconcile = Path('/home/admin/clawd/v5-trading-bot/reports/reconcile_status.json')
    if reconcile.exists():
        with open(reconcile) as f:
            rc = json.load(f)
            if not rc.get('ok'):
                issues.append(f"⚠️ 对账异常: {rc.get('reason', 'unknown')}")
    
    return issues

def main():
    LOG_FILE.parent.mkdir(exist_ok=True)
    
    log("=" * 60)
    log("🔍 V5 交易审计启动")
    log("=" * 60)
    
    # 获取最新订单
    orders = get_latest_orders(30)
    if not orders:
        log("❌ 未找到订单记录")
        return
    
    log(f"分析最近 {len(orders)} 笔订单...")
    
    # 分析订单
    analysis = analyze_orders(orders)
    
    # 检查风控
    risk_issues = check_risk_limits()
    
    # 汇总问题
    all_issues = analysis['issues'] + risk_issues
    
    # 生成报告
    log(f"\n📊 交易统计:")
    log(f"  买入: {analysis['buy_count']} 笔")
    log(f"  卖出: {analysis['sell_count']} 笔")
    log(f"  拒绝: {len(analysis['rejected'])} 笔")
    
    if analysis['rejected']:
        log(f"\n❌ 被拒绝订单:")
        for r in analysis['rejected'][:5]:
            log(f"  - {r['symbol']} {r['side']} ({r['intent']}): {r['error']}")
    
    if all_issues:
        log(f"\n🚨 发现 {len(all_issues)} 个问题:")
        for issue in all_issues:
            log(f"  {issue}")
        
        # 写入告警文件
        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'issue_count': len(all_issues),
            'issues': all_issues,
            'rejected_orders': analysis['rejected'],
            'summary': {
                'buy_count': analysis['buy_count'],
                'sell_count': analysis['sell_count'],
                'rejected_count': len(analysis['rejected'])
            }
        }
        with open(ALERT_FILE, 'w') as f:
            json.dump(alert_data, f, indent=2, ensure_ascii=False)
        
        log(f"\n⚠️  告警已保存至: {ALERT_FILE}")
    else:
        log(f"\n✅ 审计通过，未发现异常")
        # 清理告警文件
        if ALERT_FILE.exists():
            ALERT_FILE.unlink()
    
    log("=" * 60)
    log("审计完成")
    log("=" * 60)

if __name__ == '__main__':
    main()
