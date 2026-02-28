#!/usr/bin/env python3
"""
简单历史数据回填 - 使用OKX API
"""

import requests
import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def get_historical_candles(symbol, start_time, end_time, limit=100):
    """获取历史K线数据"""
    # 转换符号格式
    inst_id = symbol.replace('/', '-')
    
    url = "https://www.okx.com/api/v5/market/history-candles"
    
    # OKX参数：after是结束时间（毫秒），limit是条数
    params = {
        'instId': inst_id,
        'bar': '1H',  # 1小时K线
        'after': str(int(end_time * 1000)),  # 毫秒
        'limit': limit
    }
    
    try:
        print(f"  请求 {symbol}: {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get('code') == '0':
            candles = data.get('data', [])
            records = []
            for candle in candles:
                ts = int(candle[0]) // 1000  # 毫秒转秒
                # 只保留在时间范围内的数据
                if start_time <= ts <= end_time:
                    records.append({
                        'timestamp': ts,
                        'symbol': symbol,
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5]),
                        'volume_ccy': float(candle[6])
                    })
            return records
        else:
            print(f"  API错误: {data.get('msg')}")
            return []
            
    except Exception as e:
        print(f"  请求失败: {e}")
        return []

def backfill_symbol(symbol, days=30):
    """回填单个币种"""
    print(f"\n🔄 回填 {symbol} {days}天数据...")
    
    db_path = "reports/alpha_history.db"
    conn = sqlite3.connect(db_path)
    
    # 获取当前最新数据时间
    cursor = conn.cursor()
    cursor.execute(
        "SELECT MAX(timestamp) FROM market_data_1h WHERE symbol = ?",
        (symbol,)
    )
    latest_ts = cursor.fetchone()[0]
    
    # 计算时间范围
    end_time = int(time.time())
    start_time = end_time - (days * 24 * 3600)
    
    if latest_ts:
        # 如果已有数据，从最新数据后开始
        actual_start = latest_ts + 3600
        if actual_start > start_time:
            start_time = actual_start
    
    print(f"  时间范围: {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
    
    total_added = 0
    current_end = end_time
    
    # 分批获取数据
    while current_end > start_time:
        batch_start = current_end - (100 * 3600)  # 每次100小时
        if batch_start < start_time:
            batch_start = start_time
        
        candles = get_historical_candles(symbol, batch_start, current_end)
        
        if candles:
            # 保存到数据库
            df = pd.DataFrame(candles)
            df.to_sql('market_data_1h', conn, if_exists='append', index=False)
            total_added += len(candles)
            print(f"  已添加 {len(candles)} 条记录")
            
            # 更新结束时间为这批数据的最早时间
            earliest = min(c['timestamp'] for c in candles)
            current_end = earliest - 3600
        else:
            # 没有数据，向前移动
            current_end = batch_start - 3600
        
        # 避免请求过于频繁
        time.sleep(0.5)
    
    conn.close()
    print(f"✅ 完成 {symbol}，共添加 {total_added} 条记录")
    return total_added

def main():
    print("🚀 简单历史数据回填")
    print("=" * 50)
    
    # 先测试几个主要币种
    test_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    
    total_added = 0
    for symbol in test_symbols:
        added = backfill_symbol(symbol, days=30)
        total_added += added
    
    print(f"\n📊 回填完成!")
    print(f"总添加记录: {total_added}")
    print(f"测试币种: {test_symbols}")
    
    # 检查更新后的数据
    print(f"\n🔍 更新后数据统计:")
    db_path = "reports/alpha_history.db"
    conn = sqlite3.connect(db_path)
    
    for symbol in test_symbols:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as count, MIN(timestamp) as earliest, MAX(timestamp) as latest FROM market_data_1h WHERE symbol = ?",
            (symbol,)
        )
        count, earliest, latest = cursor.fetchone()
        hours = (latest - earliest) / 3600 if earliest and latest else 0
        print(f"  {symbol}: {count} 条记录, {hours:.1f} 小时 ({hours/24:.1f} 天)")
    
    conn.close()

if __name__ == "__main__":
    main()