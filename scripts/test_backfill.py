#!/usr/bin/env python3
"""
测试历史数据回填 - 先测试一个币种
"""

import os
import sys
import time
import sqlite3
import pandas as pd
import requests
from datetime import datetime

# 简单测试一个币种
def test_backfill_one_symbol():
    print("🔍 测试历史数据回填 - BTC/USDT")
    
    db_path = "reports/alpha_history.db"
    symbol = "BTC/USDT"
    
    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 检查当前数据
    cursor.execute("SELECT COUNT(*) as count, MIN(timestamp) as earliest, MAX(timestamp) as latest FROM market_data_1h WHERE symbol = ?", (symbol,))
    result = cursor.fetchone()
    
    if result:
        count, earliest, latest = result
        print(f"当前 {symbol} 数据:")
        print(f"  记录数: {count}")
        print(f"  时间范围: {datetime.fromtimestamp(earliest) if earliest else 'N/A'} 到 {datetime.fromtimestamp(latest) if latest else 'N/A'}")
        if earliest and latest:
            hours = (latest - earliest) / 3600
            print(f"  小时数: {hours:.1f} 小时 ({hours/24:.1f} 天)")
    
    # 检查所有币种
    cursor.execute("SELECT symbol, COUNT(*) as count FROM market_data_1h GROUP BY symbol ORDER BY count DESC")
    all_symbols = cursor.fetchall()
    
    print(f"\n📊 所有币种数据统计:")
    for sym, cnt in all_symbols[:10]:  # 显示前10个
        print(f"  {sym}: {cnt} 条记录")
    
    conn.close()
    
    # 计算理论最大数据量
    print(f"\n📈 数据覆盖率分析:")
    print(f"  当前币种数: {len(all_symbols)}")
    print(f"  当前时间范围: 180小时 (7.5天)")
    print(f"  理论最大记录数: {len(all_symbols)} × 180 = {len(all_symbols)*180} 条")
    
    total_records = sum(cnt for _, cnt in all_symbols)
    print(f"  实际记录数: {total_records} 条")
    print(f"  当前覆盖率: {total_records/(len(all_symbols)*180)*100:.2f}%")
    
    # 30天回填后的预期
    print(f"\n🎯 30天回填后预期:")
    print(f"  时间范围: 720小时 (30天)")
    print(f"  理论最大记录数: {len(all_symbols)} × 720 = {len(all_symbols)*720} 条")
    print(f"  预期覆盖率: {total_records/(len(all_symbols)*720)*100:.2f}% → 目标: 20-30%")
    
    print(f"\n💡 建议:")
    print(f"  1. 先测试回填1-2个主要币种 (BTC/USDT, ETH/USDT)")
    print(f"  2. 验证数据质量和API限制")
    print(f"  3. 然后批量回填所有币种")
    print(f"  4. 预计需要时间: 约{len(all_symbols)*2}秒 (按每个币种2秒估算)")

if __name__ == "__main__":
    test_backfill_one_symbol()