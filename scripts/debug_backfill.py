#!/usr/bin/env python3
"""
调试历史数据回填问题
"""

import requests
import time
import sqlite3
import json
from datetime import datetime

def debug_api():
    """调试API请求"""
    print("🔍 调试OKX API请求")
    print("=" * 50)
    
    # 测试一个币种
    symbol = "BTC/USDT"
    inst_id = symbol.replace('/', '-')
    
    # 测试1: 当前时间的数据
    print(f"\n1. 测试当前时间数据 ({symbol}):")
    url = "https://www.okx.com/api/v5/market/history-candles"
    end_time = int(time.time())
    
    params = {
        'instId': inst_id,
        'bar': '1H',
        'after': str(int(end_time * 1000)),
        'limit': 10  # 只请求10条
    }
    
    print(f"   请求参数: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"   状态码: {response.status_code}")
        print(f"   响应头: {dict(response.headers)}")
        
        data = response.json()
        print(f"   响应数据: {json.dumps(data, indent=2)[:500]}...")
        
        if data.get('code') == '0':
            candles = data.get('data', [])
            print(f"   获取到 {len(candles)} 条K线")
            if candles:
                print(f"   第一条数据: {candles[0]}")
                print(f"   最后一条数据: {candles[-1]}")
        else:
            print(f"   API错误代码: {data.get('code')}")
            print(f"   错误信息: {data.get('msg')}")
            
    except Exception as e:
        print(f"   请求异常: {e}")
    
    # 测试2: 较早时间的数据
    print(f"\n2. 测试较早时间数据 ({symbol}):")
    early_time = end_time - (7 * 24 * 3600)  # 7天前
    
    params2 = {
        'instId': inst_id,
        'bar': '1H',
        'after': str(int(early_time * 1000)),
        'limit': 5
    }
    
    print(f"   请求参数: {params2}")
    print(f"   请求时间: {datetime.fromtimestamp(early_time)}")
    
    try:
        response2 = requests.get(url, params=params2, timeout=10)
        data2 = response2.json()
        
        if data2.get('code') == '0':
            candles2 = data2.get('data', [])
            print(f"   获取到 {len(candles2)} 条K线")
            if candles2:
                for i, candle in enumerate(candles2):
                    ts = int(candle[0]) // 1000
                    print(f"     {i+1}. 时间: {datetime.fromtimestamp(ts)}, O: {candle[1]}, H: {candle[2]}, L: {candle[3]}, C: {candle[4]}")
        else:
            print(f"   API错误: {data2.get('msg')}")
            
    except Exception as e:
        print(f"   请求异常: {e}")
    
    # 测试3: 检查数据库当前状态
    print(f"\n3. 检查数据库当前状态:")
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 检查BTC/USDT数据
    cursor.execute("""
        SELECT COUNT(*) as count, 
               MIN(timestamp) as earliest, 
               MAX(timestamp) as latest 
        FROM market_data_1h 
        WHERE symbol = ?
    """, (symbol,))
    
    count, earliest, latest = cursor.fetchone()
    print(f"   {symbol} 现有数据:")
    print(f"     记录数: {count}")
    print(f"     最早时间: {datetime.fromtimestamp(earliest) if earliest else '无'}")
    print(f"     最晚时间: {datetime.fromtimestamp(latest) if latest else '无'}")
    
    if earliest and latest:
        hours = (latest - earliest) / 3600
        print(f"     时间范围: {hours:.1f}小时 ({hours/24:.1f}天)")
    
    # 检查数据示例
    cursor.execute("SELECT timestamp, close FROM market_data_1h WHERE symbol = ? ORDER BY timestamp DESC LIMIT 3", (symbol,))
    print(f"   最新3条数据:")
    for ts, close in cursor.fetchall():
        print(f"     {datetime.fromtimestamp(ts)}: {close}")
    
    conn.close()

def debug_time_calculation():
    """调试时间计算"""
    print("\n🔧 调试时间计算逻辑")
    print("=" * 50)
    
    symbol = "BTC/USDT"
    days = 30
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 获取当前最新数据时间
    cursor.execute("SELECT MAX(timestamp) FROM market_data_1h WHERE symbol = ?", (symbol,))
    latest_ts = cursor.fetchone()[0]
    
    # 计算时间范围
    end_time = int(time.time())
    start_time = end_time - (days * 24 * 3600)
    
    print(f"当前时间: {datetime.fromtimestamp(end_time)}")
    print(f"30天前: {datetime.fromtimestamp(start_time)}")
    print(f"最新数据时间: {datetime.fromtimestamp(latest_ts) if latest_ts else '无数据'}")
    
    if latest_ts:
        # 如果已有数据，从最新数据后开始
        actual_start = latest_ts + 3600
        print(f"实际开始时间 (最新数据+1小时): {datetime.fromtimestamp(actual_start)}")
        
        if actual_start > start_time:
            start_time = actual_start
            print(f"调整后开始时间: {datetime.fromtimestamp(start_time)}")
    
    # 检查是否已经达到30天数据
    if latest_ts and (end_time - latest_ts) >= (days * 24 * 3600):
        print(f"⚠️ 已经达到30天数据，不需要回填")
        print(f"  数据时间范围: {datetime.fromtimestamp(latest_ts - (days * 24 * 3600))} 到 {datetime.fromtimestamp(latest_ts)}")
    else:
        print(f"需要回填的时间范围:")
        print(f"  开始: {datetime.fromtimestamp(start_time)}")
        print(f"  结束: {datetime.fromtimestamp(end_time)}")
        hours = (end_time - start_time) / 3600
        print(f"  总小时数: {hours:.1f}小时")
        print(f"  预计记录数: {int(hours)}条")
    
    conn.close()

def debug_duplicate_check():
    """调试重复数据检查"""
    print("\n🔄 调试重复数据检查逻辑")
    print("=" * 50)
    
    # 模拟一些测试数据
    test_timestamps = [
        int(time.time()) - 3600,  # 1小时前
        int(time.time()) - 7200,  # 2小时前
        int(time.time()) - 10800, # 3小时前
    ]
    
    print(f"测试时间戳: {[datetime.fromtimestamp(ts) for ts in test_timestamps]}")
    
    # 检查数据库中是否已存在
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 创建测试表（如果不存在）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_duplicates (
            timestamp INTEGER,
            symbol TEXT,
            value REAL
        )
    """)
    
    # 清空测试表
    cursor.execute("DELETE FROM test_duplicates")
    
    # 插入一些测试数据
    for i, ts in enumerate(test_timestamps[:2]):  # 只插入前2个
        cursor.execute("INSERT INTO test_duplicates (timestamp, symbol, value) VALUES (?, ?, ?)", 
                      (ts, "TEST", 100.0 + i))
    
    conn.commit()
    
    # 现在检查重复
    placeholders = ','.join(['?'] * len(test_timestamps))
    query = f"SELECT timestamp FROM test_duplicates WHERE symbol = ? AND timestamp IN ({placeholders})"
    
    print(f"查询SQL: {query}")
    print(f"查询参数: ['TEST'] + {test_timestamps}")
    
    cursor.execute(query, ["TEST"] + test_timestamps)
    existing = {row[0] for row in cursor.fetchall()}
    
    print(f"已存在的时间戳: {existing}")
    print(f"需要过滤的时间戳: {[ts for ts in test_timestamps if ts in existing]}")
    
    # 清理
    cursor.execute("DROP TABLE test_duplicates")
    conn.commit()
    conn.close()

def main():
    print("🚀 历史数据回填问题调试")
    print("=" * 60)
    
    # 1. 调试API请求
    debug_api()
    
    # 2. 调试时间计算
    debug_time_calculation()
    
    # 3. 调试重复检查
    debug_duplicate_check()
    
    print("\n" + "=" * 60)
    print("📋 问题诊断:")
    print("1. 检查API是否返回数据")
    print("2. 检查时间范围计算是否正确")
    print("3. 检查重复数据过滤逻辑")
    print("4. 检查数据库连接和写入权限")
    print("=" * 60)

if __name__ == "__main__":
    main()