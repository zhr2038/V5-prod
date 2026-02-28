#!/usr/bin/env python3
"""
迷你历史数据回填 - 先测试几个主要币种
"""

import requests
import time
import sqlite3
from datetime import datetime

def test_backfill():
    """测试回填功能"""
    print("🔧 测试历史数据回填功能")
    print("=" * 50)
    
    # 测试币种
    test_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    
    for symbol in test_symbols:
        print(f"\n测试 {symbol}:")
        
        # 转换符号格式
        inst_id = symbol.replace('/', '-')
        url = "https://www.okx.com/api/v5/market/history-candles"
        
        # 获取最近100小时数据
        end_time = int(time.time())
        params = {
            'instId': inst_id,
            'bar': '1H',
            'after': str(int(end_time * 1000)),
            'limit': 100
        }
        
        try:
            print(f"  请求API...")
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get('code') == '0':
                candles = data.get('data', [])
                print(f"  获取到 {len(candles)} 条K线数据")
                
                if candles:
                    # 显示第一条和最后一条数据的时间
                    first_ts = int(candles[-1][0]) // 1000  # 最早的
                    last_ts = int(candles[0][0]) // 1000   # 最新的
                    print(f"  时间范围: {datetime.fromtimestamp(first_ts)} 到 {datetime.fromtimestamp(last_ts)}")
                    print(f"  数据跨度: {(last_ts - first_ts)/3600:.1f} 小时")
                    
                    # 检查数据质量
                    sample = candles[0]
                    print(f"  数据格式: 时间={sample[0]}, O={sample[1]}, H={sample[2]}, L={sample[3]}, C={sample[4]}")
                else:
                    print(f"  ⚠️ 没有获取到数据")
            else:
                print(f"  ❌ API错误: {data.get('msg', 'Unknown')}")
                
        except Exception as e:
            print(f"  ❌ 请求失败: {e}")
        
        time.sleep(1)  # 避免请求过于频繁
    
    print("\n" + "=" * 50)
    print("📋 测试完成!")
    print("下一步: 如果API工作正常，可以开始完整回填")

def check_current_data():
    """检查当前数据状态"""
    print("\n📊 当前数据状态检查")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 总统计
    cursor.execute("SELECT COUNT(DISTINCT symbol) as symbol_count, COUNT(*) as total_records FROM market_data_1h")
    symbol_count, total_records = cursor.fetchone()
    
    print(f"币种数量: {symbol_count}")
    print(f"总记录数: {total_records}")
    
    # 按币种统计
    cursor.execute("""
        SELECT symbol, COUNT(*) as count, 
               MIN(timestamp) as earliest, MAX(timestamp) as latest 
        FROM market_data_1h 
        GROUP BY symbol 
        ORDER BY count DESC
        LIMIT 10
    """)
    
    print(f"\n📈 前10个币种数据统计:")
    for symbol, count, earliest, latest in cursor.fetchall():
        hours = (latest - earliest) / 3600 if earliest and latest else 0
        print(f"  {symbol}: {count}条, {hours:.1f}小时 ({hours/24:.1f}天)")
    
    conn.close()
    
    # 计算覆盖率
    theoretical_max_30d = symbol_count * 720  # 30天 × 24小时
    coverage = (total_records / theoretical_max_30d) * 100 if theoretical_max_30d > 0 else 0
    
    print(f"\n🎯 30天数据覆盖率分析:")
    print(f"  理论最大记录数 (30天): {theoretical_max_30d}条")
    print(f"  当前覆盖率: {coverage:.2f}%")
    print(f"  需要补充记录数 (目标30%): {int(theoretical_max_30d * 0.3) - total_records}条")

def main():
    print("🚀 迷你历史数据回填测试")
    print("=" * 60)
    
    # 检查当前数据
    check_current_data()
    
    print("\n" + "=" * 60)
    print("🔧 测试API功能...")
    
    # 测试API
    test_backfill()
    
    print("\n" + "=" * 60)
    print("💡 建议:")
    print("1. 如果API测试成功，可以运行完整回填")
    print("2. 完整回填预计需要: 26个币种 × 2秒/币种 ≈ 52秒")
    print("3. 目标: 将30天覆盖率从当前水平提升到20-30%")
    print("=" * 60)
    
    # 询问是否开始完整回填
    confirm = input("\n⚠️  是否开始完整回填所有26个币种的30天数据？(yes/no): ")
    if confirm.lower() == 'yes':
        print("\n🔄 开始完整回填...")
        # 这里可以调用完整回填函数
        print("(完整回填功能需要实现)")
    else:
        print("\n❌ 操作已取消")

if __name__ == "__main__":
    main()