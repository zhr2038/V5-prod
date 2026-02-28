#!/usr/bin/env python3
"""
自动历史数据回填 - 无需交互
"""

import requests
import time
import sqlite3
import pandas as pd
from datetime import datetime

def get_all_symbols():
    """获取所有币种"""
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    return symbols

def fetch_candles(symbol, before_time, limit=100):
    """获取K线数据"""
    inst_id = symbol.replace('/', '-')
    url = "https://www.okx.com/api/v5/market/history-candles"
    
    params = {
        'instId': inst_id,
        'bar': '1H',
        'after': str(int(before_time * 1000)),  # 毫秒，获取这个时间之前的数据
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

def backfill_symbol_simple(symbol, days=30):
    """简单回填：直接获取指定天数的数据"""
    print(f"\n[{symbol}] 开始回填{days}天数据...")
    
    conn = sqlite3.connect("reports/alpha_history.db")
    
    # 计算时间范围
    end_time = int(time.time())
    start_time = end_time - (days * 24 * 3600)
    
    print(f"  时间范围: {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
    
    added = 0
    current_before = end_time
    batch_count = 0
    
    # 分批获取数据
    while current_before > start_time and batch_count < 15:  # 最多15批
        batch_count += 1
        
        candles = fetch_candles(symbol, current_before, limit=100)
        
        if not candles:
            print(f"   批次{batch_count}: 没有数据，停止")
            break
        
        # 过滤掉时间范围之外的数据
        valid_candles = [c for c in candles if c['timestamp'] >= start_time]
        
        if not valid_candles:
            print(f"   批次{batch_count}: 数据都在目标时间范围外，停止")
            break
        
        # 保存数据
        df = pd.DataFrame(valid_candles)
        
        # 使用INSERT OR IGNORE避免重复
        for _, row in df.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO market_data_1h 
                    (timestamp, symbol, open, high, low, close, volume, volume_ccy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(row['timestamp']),
                    row['symbol'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['volume_ccy']
                ))
                if cursor.rowcount > 0:
                    added += 1
            except Exception as e:
                # 忽略重复键错误
                if "UNIQUE constraint" not in str(e):
                    print(f"   插入错误: {e}")
        
        conn.commit()
        
        print(f"   批次{batch_count}: 获取{len(candles)}条，有效{len(valid_candles)}条，新增{added - added_before}条")
        
        # 更新当前时间为这批数据的最早时间
        earliest = min(c['timestamp'] for c in candles)
        current_before = earliest - 3600
        
        # 避免请求过于频繁
        time.sleep(0.5)
    
    conn.close()
    
    if added > 0:
        print(f"  ✅ 完成，共添加 {added} 条记录")
    else:
        print(f"  ⚠️  没有新数据")
    
    return added

def main():
    print("🚀 自动历史数据回填 - 修复版本")
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
    
    # 计算当前覆盖率
    symbol_count = len(symbols)
    theoretical_max_30d = symbol_count * 720  # 30天 × 24小时
    pre_coverage = (pre_total / theoretical_max_30d) * 100 if theoretical_max_30d > 0 else 0
    
    print(f"📈 当前总记录数: {pre_total}")
    print(f"🎯 当前30天覆盖率: {pre_coverage:.2f}%")
    print(f"🎯 目标覆盖率: 20-30%")
    print(f"📊 需要补充记录数: {int(theoretical_max_30d * 0.2) - pre_total}条 (目标20%)")
    
    conn.close()
    
    print("\n🔄 开始回填...")
    start_time = time.time()
    total_added = 0
    successful = []
    
    # 先测试几个主要币种
    test_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
    
    print(f"\n🧪 第一阶段: 测试{len(test_symbols)}个主要币种")
    
    for i, symbol in enumerate(test_symbols, 1):
        print(f"\n[{i}/{len(test_symbols)}] ", end="")
        try:
            added = backfill_symbol_simple(symbol, days=30)
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
        
        print(f"\n🚀 第二阶段: 回填剩余{len(remaining_symbols)}个币种")
        
        for i, symbol in enumerate(remaining_symbols, 1):
            print(f"\n[{i}/{len(remaining_symbols)}] ", end="")
            try:
                added = backfill_symbol_simple(symbol, days=30)
                if added > 0:
                    total_added += added
                    successful.append(symbol)
            except Exception as e:
                print(f"  ❌ 错误: {e}")
            
            time.sleep(1)
    else:
        print(f"\n❌ 测试失败，停止回填")
    
    duration = time.time() - start_time
    
    # 显示结果
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total_records FROM market_data_1h")
    post_total = cursor.fetchone()[0]
    conn.close()
    
    # 计算更新后的覆盖率
    post_coverage = (post_total / theoretical_max_30d) * 100 if theoretical_max_30d > 0 else 0
    
    print("\n" + "=" * 60)
    print("✅ 历史数据回填完成!")
    print("=" * 60)
    print(f"⏱️  总耗时: {duration:.2f}秒")
    print(f"📊 成功币种: {len(successful)}/{len(symbols)}")
    print(f"📈 添加记录: {total_added}条")
    print(f"📊 总记录数: {pre_total} → {post_total}")
    print(f"🎯 30天数据覆盖率: {pre_coverage:.2f}% → {post_coverage:.2f}%")
    
    if successful:
        print(f"\n📋 成功币种 ({len(successful)}个):")
        for i in range(0, len(successful), 5):
            print(f"  {', '.join(successful[i:i+5])}")
    
    print("\n💡 下一步:")
    print("1. 运行数据质量检查: python3 scripts/auto_data_collector.py")
    print("2. 计算forward returns")
    print("3. 验证数据完整性")
    print("=" * 60)
    
    # 保存报告
    import json
    report = {
        'timestamp': time.time(),
        'duration_seconds': duration,
        'symbols_total': len(symbols),
        'symbols_successful': len(successful),
        'records_added': total_added,
        'records_before': pre_total,
        'records_after': post_total,
        'coverage_before': pre_coverage,
        'coverage_after': post_coverage,
        'successful_symbols': successful
    }
    
    with open('reports/backfill_fixed_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n📋 报告已保存: reports/backfill_fixed_report.json")

if __name__ == "__main__":
    main()