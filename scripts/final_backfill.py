#!/usr/bin/env python3
"""
最终历史数据回填 - 所有币种30天数据
"""

import requests
import time
import sqlite3
import pandas as pd
from datetime import datetime
import sys

def get_all_symbols():
    """获取所有币种"""
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    return symbols

def fetch_historical_data(symbol, end_time):
    """获取历史数据"""
    inst_id = symbol.replace('/', '-')
    url = "https://www.okx.com/api/v5/market/history-candles"
    
    params = {
        'instId': inst_id,
        'bar': '1H',
        'after': str(int(end_time * 1000)),
        'limit': 100
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

def backfill_symbol(symbol, days=30):
    """回填单个币种"""
    print(f"\n🔄 处理 {symbol}...")
    
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
        return 0
    
    print(f"  ⏰ 需要补充: {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
    
    added = 0
    current_end = end_time
    
    # 分批获取数据
    while current_end > start_time:
        candles = fetch_historical_data(symbol, current_end)
        
        if not candles:
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
            print(f"  + 添加 {len(df_new)} 条记录")
        
        # 更新结束时间为这批数据的最早时间
        earliest = min(c['timestamp'] for c in candles)
        current_end = earliest - 3600
        
        # 避免请求过于频繁
        time.sleep(0.3)
    
    conn.close()
    
    if added > 0:
        print(f"  ✅ 完成，共添加 {added} 条记录")
    else:
        print(f"  ⚠️  没有新数据")
    
    return added

def main():
    print("🚀 开始完整30天历史数据回填")
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
    print(f"🎯 目标30天覆盖率: 20-30%")
    print("")
    
    # 开始回填
    start_time = time.time()
    total_added = 0
    successful = []
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] ", end="")
        try:
            added = backfill_symbol(symbol, days=30)
            if added > 0:
                total_added += added
                successful.append(symbol)
        except Exception as e:
            print(f"  ❌ 错误: {e}")
        
        # 币种间延迟
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
    print(f"📊 币种统计: {len(successful)}/{len(symbols)} 成功")
    print(f"📈 添加记录: {total_added}条")
    print(f"📊 总记录数: {pre_total} → {post_total}")
    print(f"🎯 30天数据覆盖率: {coverage:.2f}%")
    
    # 显示成功币种
    if successful:
        print(f"\n📋 成功回填的币种 ({len(successful)}个):")
        for i in range(0, len(successful), 5):
            print(f"  {', '.join(successful[i:i+5])}")
    
    print("\n💡 下一步:")
    print("1. 运行数据质量检查: python3 scripts/auto_data_collector.py")
    print("2. 验证forward returns计算")
    print("3. 继续每小时自动收集保持数据更新")
    print("=" * 60)
    
    # 保存报告
    report = {
        'timestamp': time.time(),
        'duration_seconds': duration,
        'symbols_total': len(symbols),
        'symbols_successful': len(successful),
        'records_added': total_added,
        'records_before': pre_total,
        'records_after': post_total,
        'coverage_30d': coverage,
        'successful_symbols': successful
    }
    
    import json
    with open('reports/backfill_summary.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n📋 报告已保存: reports/backfill_summary.json")

if __name__ == "__main__":
    main()