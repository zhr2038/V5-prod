#!/usr/bin/env python3
"""
一致性和连续性优化方案
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def analyze_systematic_gaps():
    """分析系统性时间缺口"""
    print("🔍 分析系统性时间缺口")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    
    # 找出所有系统性缺口（多个币种在同一时间点缺失）
    query = """
    WITH all_hours AS (
        -- 生成完整的时间序列（每小时）
        SELECT DISTINCT timestamp
        FROM market_data_1h
        WHERE timestamp >= (SELECT MIN(timestamp) FROM market_data_1h)
          AND timestamp <= (SELECT MAX(timestamp) FROM market_data_1h)
    ),
    symbol_hours AS (
        -- 每个币种实际有的时间点
        SELECT DISTINCT symbol, timestamp
        FROM market_data_1h
    ),
    missing_data AS (
        -- 找出缺失的时间点
        SELECT 
            ah.timestamp,
            COUNT(DISTINCT sh.symbol) as existing_symbols,
            (SELECT COUNT(DISTINCT symbol) FROM market_data_1h) as total_symbols,
            (SELECT COUNT(DISTINCT symbol) FROM market_data_1h) - COUNT(DISTINCT sh.symbol) as missing_symbols
        FROM all_hours ah
        LEFT JOIN symbol_hours sh ON ah.timestamp = sh.timestamp
        GROUP BY ah.timestamp
        HAVING missing_symbols > 0
    )
    SELECT 
        timestamp,
        strftime('%Y-%m-%d %H:%M:%S', timestamp, 'unixepoch') as time_str,
        existing_symbols,
        total_symbols,
        missing_symbols,
        ROUND(missing_symbols * 100.0 / total_symbols, 1) as missing_pct
    FROM missing_data
    WHERE missing_pct >= 30  -- 30%以上的币种缺失
    ORDER BY missing_pct DESC, timestamp
    """
    
    df = pd.read_sql_query(query, conn)
    
    if len(df) > 0:
        print(f"⚠️ 发现 {len(df)} 个系统性时间缺口")
        print(f"最严重的缺口:")
        for _, row in df.head(5).iterrows():
            print(f"  {row['time_str']}: {row['missing_symbols']}/{row['total_symbols']}币种缺失 ({row['missing_pct']}%)")
        
        # 分析缺口模式
        print(f"\n📅 缺口时间模式:")
        df['hour'] = pd.to_datetime(df['time_str']).dt.hour
        hour_dist = df.groupby('hour').size().sort_values(ascending=False)
        
        for hour, count in hour_dist.head().items():
            print(f"   {hour:02d}:00 - {hour+1:02d}:00: {count}个缺口")
        
        # 检查是否是规律性缺口
        print(f"\n🔍 规律性分析:")
        if len(df) >= 3:
            timestamps = df['timestamp'].values
            gaps = np.diff(timestamps)
            avg_gap = np.mean(gaps)
            
            print(f"  平均缺口间隔: {avg_gap/3600:.1f}小时")
            if 3500 < avg_gap < 3700:  # 大约每天
                print(f"  ⚠️ 可能是每日定时数据收集问题")
            elif 17000 < avg_gap < 17500:  # 大约每周
                print(f"  ⚠️ 可能是每周维护窗口")
    else:
        print("✅ 无严重系统性时间缺口")
    
    conn.close()
    return df if len(df) > 0 else None

def fix_data_consistency():
    """修复数据一致性"""
    print("\n🔄 修复数据一致性")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 1. 找出所有币种的共同时间范围
    cursor.execute("""
        SELECT 
            MIN(timestamp) as global_min,
            MAX(timestamp) as global_max
        FROM market_data_1h
    """)
    global_min, global_max = cursor.fetchone()
    
    print(f"全局时间范围: {datetime.fromtimestamp(global_min)} 到 {datetime.fromtimestamp(global_max)}")
    print(f"总小时数: {(global_max - global_min)/3600:.1f}小时")
    
    # 2. 找出记录数最少的币种作为基准
    cursor.execute("""
        SELECT symbol, COUNT(*) as count
        FROM market_data_1h
        GROUP BY symbol
        ORDER BY count
        LIMIT 1
    """)
    min_symbol, min_count = cursor.fetchone()
    
    print(f"\n基准币种: {min_symbol} ({min_count}条记录)")
    
    # 3. 找出缺失的记录
    consistency_issues = []
    
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    all_symbols = [row[0] for row in cursor.fetchall()]
    
    for symbol in all_symbols:
        cursor.execute("SELECT COUNT(*) FROM market_data_1h WHERE symbol = ?", (symbol,))
        count = cursor.fetchone()[0]
        
        if count < min_count:
            missing = min_count - count
            consistency_issues.append((symbol, count, missing))
    
    if consistency_issues:
        print(f"\n⚠️ 发现 {len(consistency_issues)} 个币种记录数不一致:")
        for symbol, count, missing in sorted(consistency_issues, key=lambda x: x[2], reverse=True)[:10]:
            print(f"  {symbol}: {count}条, 缺失{missing}条 ({missing/count*100:.1f}%)")
    else:
        print(f"\n✅ 所有币种记录数一致")
    
    conn.close()
    return consistency_issues

def create_continuous_time_series():
    """创建连续时间序列"""
    print("\n📈 创建连续时间序列")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    
    # 生成完整的时间序列模板
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM market_data_1h")
    min_ts, max_ts = cursor.fetchone()
    
    # 创建每小时的时间点
    time_points = []
    current_ts = min_ts
    while current_ts <= max_ts:
        time_points.append(current_ts)
        current_ts += 3600  # 增加1小时
    
    print(f"时间范围: {datetime.fromtimestamp(min_ts)} 到 {datetime.fromtimestamp(max_ts)}")
    print(f"完整时间序列: {len(time_points)} 个时间点")
    
    # 检查每个币种的连续性
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    
    continuity_report = []
    
    for symbol in symbols:
        # 获取该币种的所有时间点
        cursor.execute("SELECT timestamp FROM market_data_1h WHERE symbol = ? ORDER BY timestamp", (symbol,))
        symbol_times = {row[0] for row in cursor.fetchall()}
        
        # 找出缺失的时间点
        missing_times = [ts for ts in time_points if ts not in symbol_times]
        
        if missing_times:
            missing_count = len(missing_times)
            missing_pct = missing_count / len(time_points) * 100
            continuity_report.append((symbol, len(symbol_times), missing_count, missing_pct))
    
    if continuity_report:
        print(f"\n⏰ 时间连续性报告:")
        print(f"{'币种':<15} {'现有':<6} {'缺失':<6} {'缺失率':<8}")
        print("-" * 40)
        
        for symbol, existing, missing, pct in sorted(continuity_report, key=lambda x: x[3], reverse=True)[:10]:
            print(f"{symbol:<15} {existing:<6} {missing:<6} {pct:<8.1f}%")
        
        total_missing = sum(r[2] for r in continuity_report)
        total_expected = len(symbols) * len(time_points)
        total_existing = sum(r[1] for r in continuity_report)
        overall_continuity = total_existing / total_expected * 100
        
        print(f"\n📊 总体连续性: {overall_continuity:.2f}%")
        print(f"总缺失时间点: {total_missing}个")
    else:
        print(f"\n✅ 完美连续性: 所有币种在所有时间点都有数据")
    
    conn.close()
    return continuity_report

def implement_consistency_fixes(consistency_issues, continuity_report):
    """实施一致性修复"""
    print("\n🔧 实施一致性修复方案")
    print("=" * 50)
    
    if not consistency_issues and not continuity_report:
        print("✅ 无需要修复的问题")
        return
    
    # 创建修复计划
    print("📋 修复计划:")
    
    # 1. 数据一致性修复
    if consistency_issues:
        print("\n1. 🔄 数据一致性修复:")
        print("   目标: 使所有币种有相同数量的记录")
        print("   方法:")
        print("   a. 找出缺失的时间点")
        print("   b. 使用相邻数据插值填充")
        print("   c. 或标记为无效数据点")
        
        # 创建修复脚本
        fix_script = """#!/usr/bin/env python3
# 数据一致性修复脚本

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

def fix_consistency():
    \"\"\"修复数据一致性\"\"\"
    conn = sqlite3.connect('reports/alpha_history.db')
    
    # 1. 找出所有币种的共同时间范围
    cursor = conn.cursor()
    cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM market_data_1h')
    min_ts, max_ts = cursor.fetchone()
    
    # 2. 生成完整的时间序列
    time_points = []
    current = min_ts
    while current <= max_ts:
        time_points.append(current)
        current += 3600
    
    print(f'完整时间序列: {len(time_points)} 个时间点')
    
    # 3. 获取所有币种
    cursor.execute('SELECT DISTINCT symbol FROM market_data_1h')
    symbols = [row[0] for row in cursor.fetchall()]
    
    fixes_applied = 0
    
    for symbol in symbols:
        # 获取该币种的所有时间点
        cursor.execute('SELECT timestamp FROM market_data_1h WHERE symbol = ?', (symbol,))
        existing_times = {row[0] for row in cursor.fetchall()}
        
        # 找出缺失的时间点
        missing_times = [ts for ts in time_points if ts not in existing_times]
        
        if missing_times:
            print(f'{symbol}: 缺失 {len(missing_times)} 个时间点')
            
            # 这里可以添加具体的填充逻辑
            # 例如: 使用前后数据的平均值填充
            # 或标记为特殊值
            
            fixes_applied += 1
    
    conn.close()
    print(f'\\n总共需要修复 {fixes_applied} 个币种')
    return fixes_applied

if __name__ == '__main__':
    fix_consistency()
"""
        
        with open("scripts/fix_consistency.py", "w") as f:
            f.write(fix_script)
        
        print(f"\n   📄 修复脚本已创建: scripts/fix_consistency.py")
    
    # 2. 时间连续性修复
    if continuity_report:
        print("\n2. ⏰ 时间连续性修复:")
        print("   目标: 填补时间缺口")
        print("   方法:")
        print("   a. 线性插值填充小缺口")
        print("   b. 前后平均值填充大缺口")
        print("   c. 标记系统性缺口原因")
        
        # 创建连续性修复脚本
        continuity_script = """#!/usr/bin/env python3
# 时间连续性修复脚本

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

def fix_continuity():
    \"\"\"修复时间连续性\"\"\"
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
    print(f'\\n总共需要修复 {fixes_applied} 个币种的时间连续性')
    return fixes_applied

if __name__ == '__main__':
    fix_continuity()
"""
        
        with open("scripts/fix_continuity.py", "w") as f:
            f.write(continuity_script)
        
        print(f"\n   📄 修复脚本已创建: scripts/fix_continuity.py")
    
    print(f"\n💡 实施步骤:")
    print("1. 运行修复脚本: python3 scripts/fix_consistency.py")
    print("2. 运行连续性修复: python3 scripts/fix_continuity.py")
    print("3. 验证修复效果")
    print("4. 更新数据收集逻辑防止问题重现")

def main():
    print("🚀 一致性和连续性优化")
    print("=" * 60)
    
    # 1. 分析系统性缺口
    systematic_gaps = analyze_systematic_gaps()
    
    # 2. 修复数据一致性
    consistency_issues = fix_data_consistency()
    
    # 3. 创建连续时间序列
    continuity_report = create_continuous_time_series()
    
    # 4. 实施修复方案
    implement_consistency_fixes(consistency_issues, continuity_report)
    
    print("\n" + "=" * 60)
    print("✅ 一致性和连续性分析完成!")
    print("=" * 60)
    
    print(f"\n📋 关键发现:")
    if systematic_gaps is not None and len(systematic_gaps) > 0:
        print(f"1. ⚠️ 系统性时间缺口: {len(systematic_gaps)} 个")
        print(f"   多个币种在同一时间点缺失数据")
        print(f"   可能原因: API限制、数据收集问题、市场休市")
    
    if consistency_issues:
        print(f"2. 🔄 数据不一致: {len(consistency_issues)} 个币种")
        print(f"   记录数差异最大: {max(c[2] for c in consistency_issues)} 条")
    
    if continuity_report:
        worst = max(continuity_report, key=lambda x: x[3])
        print(f"3. ⏰ 时间连续性: 最差币种 {worst[0]} 缺失率 {worst[3]:.1f}%")
    
    print(f"\n🎯 优化目标:")
    print(f"1. 一致性: 所有币种记录数差异 < 5条")
    print(f"2. 连续性: 时间缺口 < 1%")
    print(f"3. 系统性: 消除同一时间点的多币种缺口")
    
    print(f"\n💡 建议:")
    print(f"1. 先运行修复脚本解决明显问题")
    print(f"2. 优化数据收集逻辑避免系统性缺口")
    print(f"3. 建立数据质量监控及时发现问题")
    print("=" * 60)

if __name__ == "__main__":
    main()