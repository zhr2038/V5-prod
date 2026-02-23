#!/usr/bin/env python3
"""
修复的历史数据回填脚本
"""

import requests
import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def get_all_symbols():
    """获取所有币种"""
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    return symbols

def fetch_historical_data(symbol, before_time, limit=100):
    """
    获取历史数据 - 修复版本
    before_time: 获取这个时间之前的数据
    """
    inst_id = symbol.replace('/', '-')
    url = "https://www.okx.com/api/v5/market/history-candles"
    
    # OKX API: after参数是结束时间（毫秒），获取这个时间之前的数据
    params = {
        'instId': inst_id,
        'bar': '1H',
        'after': str(int(before_time * 1000)),  # 毫秒
        'limit': limit
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        
        if data.get('code') == '0':
            candles = []
            for candle in data.get('data', []):
                ts = int(candle[0]) // 1000
                candles.append({
                    'timestamp': ts,
                    'symbol': symbol,
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4]),
                    'volume': float(candle[5]),
                    'volume_ccy': float(candle[6])
                })
            return candles
        else:
            print(f"  ❌ API错误: {data.get('msg')}")
            return []
    except Exception as e:
        print(f"  ❌ 请求失败: {e}")
        return []

def backfill_symbol_fixed(symbol, days=30):
    """修复的回填函数"""
    print(f"\n🔄 处理 {symbol}...")
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 获取当前最早数据时间（不是最新！）
    cursor.execute("SELECT MIN(timestamp) FROM market_data_1h WHERE symbol = ?", (symbol,))
    earliest_ts = cursor.fetchone()[0]
    
    # 计算需要回填的结束时间（最早数据时间之前）
    end_time = int(time.time())
    target_start_time = end_time - (days * 24 * 3600)  # 30天前
    
    if earliest_ts:
        # 如果已有数据，从最早数据时间向前获取更早的数据
        # 我们需要获取 earliest_ts 之前的数据
        fetch_before_time = earliest_ts - 3600  # 获取最早数据时间之前1小时的数据
        print(f"  当前最早数据: {datetime.fromtimestamp(earliest_ts)}")
        print(f"  目标最早数据: {datetime.fromtimestamp(target_start_time)}")
        
        # 如果已经达到30天数据，跳过
        if (end_time - earliest_ts) >= (days * 24 * 3600):
            print(f"  ✓ 已有足够数据（{days}天），跳过")
            conn.close()
            return 0
    else:
        # 没有数据，从当前时间开始向前获取
        fetch_before_time = end_time
        print(f"  ⚠️ 没有现有数据，从当前时间开始")
    
    print(f"  需要获取 {fetch_before_time - target_start_time:.0f} 秒的历史数据")
    print(f"  约 {(fetch_before_time - target_start_time) / (24*3600):.1f} 天")
    
    added = 0
    current_before = fetch_before_time
    
    # 分批获取数据，直到达到目标时间或没有更多数据
    max_batches = 20  # 防止无限循环
    batch_count = 0
    
    while current_before > target_start_time and batch_count < max_batches:
        batch_count += 1
        
        print(f"  批次 {batch_count}: 获取 {datetime.fromtimestamp(current_before)} 之前的数据")
        
        candles = fetch_historical_data(symbol, current_before, limit=100)
        
        if not candles:
            print(f"    ⚠️ 没有获取到数据，停止")
            break
        
        # 过滤重复数据
        df_new = pd.DataFrame(candles)
        
        # 检查数据库中是否已存在
        timestamps = df_new['timestamp'].tolist()
        if timestamps:
            placeholders = ','.join(['?'] * len(timestamps))
            cursor.execute(f"SELECT timestamp FROM market_data_1h WHERE symbol = ? AND timestamp IN ({placeholders})", 
                          [symbol] + timestamps)
            existing = {row[0] for row in cursor.fetchall()}
            
            df_new = df_new[~df_new['timestamp'].isin(existing)]
        
        if len(df_new) > 0:
            df_new.to_sql('market_data_1h', conn, if_exists='append', index=False)
            added += len(df_new)
            print(f"    + 添加 {len(df_new)} 条记录")
        
        # 更新当前时间为这批数据的最早时间，继续向前获取
        earliest_in_batch = min(c['timestamp'] for c in candles)
        current_before = earliest_in_batch - 3600
        
        # 避免请求过于频繁
        time.sleep(0.5)
    
    conn.close()
    
    if added > 0:
        print(f"  ✅ 完成，共添加 {added} 条记录")
    else:
        print(f"  ⚠️  没有新数据")
    
    return added

def simple_backfill_strategy(symbol, days=30):
    """简单回填策略：直接获取30天数据，忽略现有数据"""
    print(f"\n🔄 简单回填 {symbol} ({days}天)...")
    
    conn = sqlite3.connect("reports/alpha_history.db")
    
    # 计算时间范围
    end_time = int(time.time())
    start_time = end_time - (days * 24 * 3600)
    
    print(f"  时间范围: {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
    
    added = 0
    current_before = end_time
    
    # 分批获取数据
    batch_count = 0
    while current_before > start_time and batch_count < 10:
        batch_count += 1
        
        candles = fetch_historical_data(symbol, current_before, limit=100)
        
        if not candles:
            break
        
        # 保存所有数据（让数据库处理重复）
        df = pd.DataFrame(candles)
        df.to_sql('market_data_1h', conn, if_exists='append', index=False)
        added += len(df)
        
        print(f"   批次 {batch_count}: 添加 {len(df)} 条记录")
        
        # 更新当前时间
        earliest = min(c['timestamp'] for c in candles)
        current_before = earliest - 3600
        
        time.sleep(0.5)
    
    conn.close()
    
    if added > 0:
        print(f"  ✅ 完成，共添加 {added} 条记录")
    else:
        print(f"  ⚠️  没有获取到数据")
    
    return added

def main():
    print("🚀 修复的历史数据回填")
    print("=" * 60)
    
    # 获取所有币种
    symbols = get_all_symbols()
    print(f"📊 总币种数: {len(symbols)}")
    print(f"📅 回填天数: 30天")
    
    # 显示当前状态
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total_records FROM market_data_1h")
    pre_total = cursor.fetchone()[0]
    conn.close()
    
    print(f"📈 当前总记录数: {pre_total}")
    
    # 选择回填策略
    print("\n🔧 选择回填策略:")
    print("1. 智能回填（从现有数据向前扩展）")
    print("2. 简单回填（直接获取30天数据）")
    
    choice = input("请选择策略 (1或2): ").strip()
    
    if choice == "1":
        backfill_func = backfill_symbol_fixed
        print("\n🔄 使用智能回填策略...")
    else:
        backfill_func = simple_backfill_strategy
        print("\n🔄 使用简单回填策略...")
    
    # 开始回填
    start_time = time.time()
    total_added = 0
    successful = []
    
    # 先测试几个主要币种
    test_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    
    print(f"\n🧪 先测试 {len(test_symbols)} 个主要币种...")
    
    for i, symbol in enumerate(test_symbols, 1):
        print(f"\n[{i}/{len(test_symbols)}] ", end="")
        try:
            added = backfill_func(symbol, days=30)
            if added > 0:
                total_added += added
                successful.append(symbol)
        except Exception as e:
            print(f"  ❌ 错误: {e}")
        
        time.sleep(1)
    
    # 如果测试成功，继续其他币种
    if successful:
        print(f"\n✅ 测试成功！继续其他币种...")
        
        remaining_symbols = [s for s in symbols if s not in test_symbols]
        
        for i, symbol in enumerate(remaining_symbols, 1):
            print(f"\n[{i}/{len(remaining_symbols)}] ", end="")
            try:
                added = backfill_func(symbol, days=30)
                if added > 0:
                    total_added += added
                    successful.append(symbol)
            except Exception as e:
                print(f"  ❌ 错误: {e}")
            
            time.sleep(1)
    
    duration = time.time() - start_time
    
    # 显示结果
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total_records FROM market_data_1h")
    post_total = cursor.fetchone()[0]
    conn.close()
    
    # 计算覆盖率
    symbol_count = len(symbols)
    theoretical_max_30d = symbol_count * 720  # 30天 × 24小时
    coverage = (post_total / theoretical_max_30d) * 100 if theoretical_max_30d > 0 else 0
    
    print("\n" + "=" * 60)
    print("✅ 历史数据回填完成!")
    print("=" * 60)
    print(f"⏱️  总耗时: {duration:.2f}秒")
    print(f"📊 成功币种: {len(successful)}/{len(symbols)}")
    print(f"📈 添加记录: {total_added}条")
    print(f"📊 总记录数: {pre_total} → {post_total}")
    print(f"🎯 30天数据覆盖率: {coverage:.2f}%")
    
    if successful:
        print(f"\n📋 成功币种 ({len(successful)}个):")
        for i in range(0, len(successful), 5):
            print(f"  {', '.join(successful[i:i+5])}")
    
    print("\n💡 下一步:")
    print("1. 运行数据质量检查")
    print("2. 计算forward returns")
    print("3. 验证数据完整性")
    print("=" * 60)

if __name__ == "__main__":
    main()