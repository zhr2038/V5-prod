#!/usr/bin/env python3
"""
快速历史数据回填 - 所有币种30天数据
"""

import requests
import time
import sqlite3
import pandas as pd
from datetime import datetime

def get_symbols():
    """获取所有币种"""
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    return symbols

def fetch_candles(symbol, end_time, limit=100):
    """获取K线数据"""
    inst_id = symbol.replace('/', '-')
    url = "https://www.okx.com/api/v5/market/history-candles"
    
    params = {
        'instId': inst_id,
        'bar': '1H',
        'after': str(int(end_time * 1000)),
        'limit': limit
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
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
            print(f"  {symbol}: API错误 - {data.get('msg')}")
            return []
    except Exception as e:
        print(f"  {symbol}: 请求失败 - {e}")
        return []

def backfill_all_symbols(days=30):
    """回填所有币种"""
    symbols = get_symbols()
    print(f"🚀 开始回填 {len(symbols)} 个币种的 {days} 天数据")
    print("=" * 60)
    
    total_added = 0
    successful = []
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] 处理 {symbol}")
        
        conn = sqlite3.connect("reports/alpha_history.db")
        
        # 检查当前数据
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM market_data_1h WHERE symbol = ?", (symbol,))
        latest_ts = cursor.fetchone()[0]
        
        # 计算时间范围
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 3600)
        
        if latest_ts:
            # 如果已有数据，从最新数据后开始
            actual_start = latest_ts + 3600
            if actual_start > start_time:
                start_time = actual_start
        
        # 如果已经达到30天数据，跳过
        if latest_ts and (end_time - latest_ts) >= (days * 24 * 3600):
            print(f"  ✓ 已有足够数据，跳过")
            conn.close()
            successful.append(symbol)
            continue
        
        print(f"  ⏰ 时间范围: {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
        
        added = 0
        current_end = end_time
        
        # 分批获取数据
        batch_count = 0
        while current_end > start_time and batch_count < 10:  # 最多10批，避免无限循环
            batch_count += 1
            
            candles = fetch_candles(symbol, current_end)
            
            if candles:
                # 保存新数据
                df = pd.DataFrame(candles)
                
                # 检查重复
                timestamps = df['timestamp'].tolist()
                placeholders = ','.join(['?'] * len(timestamps))
                cursor.execute(f"SELECT timestamp FROM market_data_1h WHERE symbol = ? AND timestamp IN ({placeholders})", 
                              [symbol] + timestamps)
                existing = {row[0] for row in cursor.fetchall()}
                
                new_df = df[~df['timestamp'].isin(existing)]
                
                if len(new_df) > 0:
                    new_df.to_sql('market_data_1h', conn, if_exists='append', index=False)
                    added += len(new_df)
                    print(f"  + 添加 {len(new_df)} 条记录")
                
                # 更新结束时间
                earliest = min(c['timestamp'] for c in candles)
                current_end = earliest - 3600
            else:
                # 没有数据，向前移动
                current_end = current_end - (100 * 3600)
            
            time.sleep(0.5)  # 避免请求过于频繁
        
        conn.close()
        
        if added > 0:
            total_added += added
            successful.append(symbol)
            print(f"  ✅ 完成，共添加 {added} 条记录")
        else:
            print(f"  ⚠️  没有新数据")
        
        # 币种间延迟
        time.sleep(1)
    
    return symbols, successful, total_added

def calculate_coverage():
    """计算数据覆盖率"""
    conn = sqlite3.connect("reports/alpha_history.db")
    
    # 获取统计
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT symbol) as symbol_count, COUNT(*) as total_records FROM market_data_1h")
    symbol_count, total_records = cursor.fetchone()
    
    # 计算30天理论最大
    theoretical_max_30d = symbol_count * 720  # 30天 × 24小时
    coverage = (total_records / theoretical_max_30d) * 100 if theoretical_max_30d > 0 else 0
    
    # 获取时间范围
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM market_data_1h")
    min_ts, max_ts = cursor.fetchone()
    
    conn.close()
    
    return {
        'symbol_count': symbol_count,
        'total_records': total_records,
        'theoretical_max_30d': theoretical_max_30d,
        'coverage': coverage,
        'time_range_days': (max_ts - min_ts) / (24 * 3600) if min_ts and max_ts else 0,
        'earliest': datetime.fromtimestamp(min_ts) if min_ts else None,
        'latest': datetime.fromtimestamp(max_ts) if max_ts else None
    }

def main():
    print("🚀 快速30天历史数据回填")
    print("=" * 60)
    
    # 显示当前状态
    stats_before = calculate_coverage()
    print(f"📊 当前状态:")
    print(f"  币种数量: {stats_before['symbol_count']}")
    print(f"  总记录数: {stats_before['total_records']}")
    print(f"  时间范围: {stats_before['time_range_days']:.1f}天")
    print(f"  30天覆盖率: {stats_before['coverage']:.2f}%")
    print(f"  最早数据: {stats_before['earliest']}")
    print(f"  最新数据: {stats_before['latest']}")
    print("")
    
    # 确认
    confirm = input("⚠️  确认开始回填所有币种的30天历史数据？(yes/no): ")
    if confirm.lower() != 'yes':
        print("❌ 操作已取消")
        return
    
    print("\n🔄 开始回填...")
    start_time = time.time()
    
    # 运行回填
    all_symbols, successful, total_added = backfill_all_symbols(days=30)
    
    duration = time.time() - start_time
    
    # 显示结果
    stats_after = calculate_coverage()
    
    print("\n" + "=" * 60)
    print("✅ 回填完成!")
    print("=" * 60)
    print(f"⏱️  总耗时: {duration:.2f}秒")
    print(f"📊 币种统计: {len(successful)}/{len(all_symbols)} 成功")
    print(f"📈 添加记录: {total_added}条")
    print("")
    print(f"📊 更新后状态:")
    print(f"  总记录数: {stats_before['total_records']} → {stats_after['total_records']}")
    print(f"  时间范围: {stats_before['time_range_days']:.1f}天 → {stats_after['time_range_days']:.1f}天")
    print(f"  30天覆盖率: {stats_before['coverage']:.2f}% → {stats_after['coverage']:.2f}%")
    print("")
    print(f"📋 成功币种 ({len(successful)}个):")
    for i in range(0, len(successful), 5):
        print(f"  {', '.join(successful[i:i+5])}")
    print("=" * 60)

if __name__ == "__main__":
    main()