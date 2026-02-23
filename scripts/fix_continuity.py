#!/usr/bin/env python3
# 时间连续性修复脚本

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

def fix_continuity():
    """修复时间连续性"""
    conn = sqlite3.connect('reports/alpha_history.db')
    
    # 分析时间缺口
    cursor = conn.cursor()
    
    # 获取所有币种
    cursor.execute('SELECT DISTINCT symbol FROM market_data_1h')
    symbols = [row[0] for row in cursor.fetchall()]
    
    fixes_applied = 0
    
    for symbol in symbols:
        # 获取该币种的时间序列
        df = pd.read_sql_query(f'''
            SELECT timestamp, open, high, low, close, volume
            FROM market_data_1h
            WHERE symbol = '{symbol}'
            ORDER BY timestamp
        ''', conn)
        
        if len(df) < 2:
            continue
        
        # 检查时间连续性
        df['time_gap'] = df['timestamp'].diff()
        large_gaps = df[df['time_gap'] > 3600]
        
        if len(large_gaps) > 0:
            print(f'{symbol}: 发现 {len(large_gaps)} 个时间缺口')
            
            # 这里可以添加具体的填充逻辑
            # 例如: 对于每个缺口，插入插值数据
            
            fixes_applied += 1
    
    conn.close()
    print(f'\n总共需要修复 {fixes_applied} 个币种的时间连续性')
    return fixes_applied

if __name__ == '__main__':
    fix_continuity()
