#!/usr/bin/env python3
"""
紧急清仓脚本 - 卖出所有非灰尘持仓
忽略灰尘仓位（< 0.1 USDT的无法处理的持仓）
"""
import os
import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

# 加载环境变量
env_path = '/home/admin/clawd/v5-trading-bot/.env'
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                key, val = line.strip().split('=', 1)
                val = val.strip('"').strip("'")
                os.environ[key] = val

import ccxt

# 连接OKX
exchange = ccxt.okx({
    'apiKey': os.getenv('EXCHANGE_API_KEY'),
    'secret': os.getenv('EXCHANGE_API_SECRET'),
    'password': os.getenv('EXCHANGE_PASSPHRASE'),
    'enableRateLimit': True
})

# 获取余额
balance = exchange.fetch_balance()

print("=" * 60)
print("🚨 紧急清仓 - 卖出所有非灰尘持仓")
print("=" * 60)

# 灰尘阈值 (< 0.5 USDT 的不处理，视为灰尘)
DUST_THRESHOLD = 0.5

sold = []
errors = []
skipped = []

for coin, amount in balance.get('total', {}).items():
    if coin == 'USDT' or amount <= 0:
        continue
    
    try:
        # 获取当前价格
        ticker = exchange.fetch_ticker(f"{coin}/USDT")
        price = ticker.get('last', 0)
        value = amount * price
        
        # 跳过灰尘仓 (< 0.5 USDT)
        if value < DUST_THRESHOLD:
            skipped.append({'coin': coin, 'amount': amount, 'value': value})
            print(f"[灰尘仓 忽略] {coin}: {amount:.6f} (${value:.2f})")
            continue
        
        # 下单卖出
        symbol = f"{coin}/USDT"
        print(f"[卖出] {symbol}: {amount:.6f} 价值: ${value:.2f}")
        
        order = exchange.create_market_sell_order(symbol, amount)
        sold.append({'coin': coin, 'amount': amount, 'value': value, 'order': order})
        print(f"  ✅ 成交")
        
    except Exception as e:
        errors.append({'coin': coin, 'error': str(e)})
        print(f"  ❌ 错误: {e}")

print()
print("=" * 60)
print(f"📊 清仓总结:")
print(f"  卖出: {len(sold)} 个币种")
print(f"  忽略(灰尘): {len(skipped)} 个币种")  
print(f"  错误: {len(errors)} 个")

if sold:
    total_sold = sum(s['value'] for s in sold)
    print(f"  卖出总价值: ${total_sold:.2f} USDT")

if skipped:
    total_skipped = sum(s['value'] for s in skipped)
    print(f"  灰尘仓总价值: ${total_skipped:.2f} USDT (忽略)")

print()
# 显示最终余额
balance = exchange.fetch_balance()
usdt_total = balance.get('total', {}).get('USDT', 0)
print(f"💰 最终 USDT 余额: {usdt_total:.2f} USDT")
print("=" * 60)

# 写入清仓报告
import json
from datetime import datetime
report = {
    'timestamp': datetime.now().isoformat(),
    'sold': sold,
    'skipped_dust': skipped,
    'errors': errors,
    'final_usdt': usdt_total
}
with open('reports/emergency_close_report.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print("📄 报告已保存: reports/emergency_close_report.json")
