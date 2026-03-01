#!/usr/bin/env python3
"""
IC验证脚本 - 使用实际价格数据重新计算IC
"""
import os
import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 连接OKX
exchange = ccxt.okx({
    'apiKey': os.getenv('EXCHANGE_API_KEY'),
    'secret': os.getenv('EXCHANGE_API_SECRET'),
    'password': os.getenv('EXCHANGE_PASSPHRASE'),
    'enableRateLimit': True
})

print("=" * 60)
print("IC验证 - 使用实际价格数据")
print("=" * 60)

# 测试币种
symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT']

# 获取历史数据（过去30天）
print("\n获取历史数据...")
all_data = {}

for sym in symbols:
    try:
        ohlcv = exchange.fetch_ohlcv(sym, '1h', limit=24*30)  # 30天
        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
        all_data[sym] = df
        print(f"  {sym}: {len(df)} 条记录")
    except Exception as e:
        print(f"  {sym}: 获取失败 - {e}")

# 计算因子和未来收益
print("\n计算因子...")

results = []

for sym, df in all_data.items():
    if len(df) < 25:
        continue
    
    # 计算动量因子 (mom_20d)
    df['mom_20d'] = (df['close'] - df['close'].shift(24*20)) / df['close'].shift(24*20)
    
    # 计算未来6小时收益
    df['future_ret_6h'] = (df['close'].shift(-6) - df['close']) / df['close']
    
    # 计算未来24小时收益
    df['future_ret_24h'] = (df['close'].shift(-24) - df['close']) / df['close']
    
    # 只取有完整数据的部分
    df_valid = df.dropna()
    
    if len(df_valid) < 10:
        continue
    
    # 计算IC (因子与未来收益的相关系数)
    ic_6h = np.corrcoef(df_valid['mom_20d'], df_valid['future_ret_6h'])[0, 1]
    ic_24h = np.corrcoef(df_valid['mom_20d'], df_valid['future_ret_24h'])[0, 1]
    
    results.append({
        'symbol': sym,
        'ic_6h': ic_6h,
        'ic_24h': ic_24h,
        'mom_mean': df_valid['mom_20d'].mean(),
        'ret_6h_mean': df_valid['future_ret_6h'].mean(),
        'ret_24h_mean': df_valid['future_ret_24h'].mean(),
    })

# 显示结果
print("\n" + "=" * 60)
print("IC结果 (mom_20d vs 未来收益)")
print("=" * 60)
print(f"{'币种':12s} {'IC(6h)':10s} {'IC(24h)':10s}")
print("-" * 40)

for r in results:
    print(f"{r['symbol']:12s} {r['ic_6h']:+.4f}    {r['ic_24h']:+.4f}")

# 平均IC
if results:
    avg_ic_6h = np.mean([r['ic_6h'] for r in results])
    avg_ic_24h = np.mean([r['ic_24h'] for r in results])
    
    print("-" * 40)
    print(f"{'平均':12s} {avg_ic_6h:+.4f}    {avg_ic_24h:+.4f}")
    
    print("\n" + "=" * 60)
    print("结论分析")
    print("=" * 60)
    
    if avg_ic_6h > 0.05:
        print(f"🟢 6h IC = {avg_ic_6h:+.4f} > 0.05")
        print("   动量因子有效：涨得好的币未来继续涨（追涨）")
        print("   ✅ 应该买高因子值的币")
    elif avg_ic_6h < -0.05:
        print(f"🔴 6h IC = {avg_ic_6h:+.4f} < -0.05")
        print("   动量因子反向：涨得好的币未来跌（均值回归）")
        print("   ✅ 应该买低因子值的币（反向）")
    else:
        print(f"🟡 6h IC = {avg_ic_6h:+.4f} (接近0)")
        print("   动量因子不明显")
        
    print()
    
    if avg_ic_24h > 0.05:
        print(f"🟢 24h IC = {avg_ic_24h:+.4f} > 0.05")
        print("   长期动量有效")
    elif avg_ic_24h < -0.05:
        print(f"🔴 24h IC = {avg_ic_24h:+.4f} < -0.05")
        print("   长期均值回归")
    else:
        print(f"🟡 24h IC = {avg_ic_24h:+.4f} (接近0)")

print("=" * 60)
