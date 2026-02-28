#!/usr/bin/env python3
"""
优化时间连续性 - 填充缺失的时间点
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def analyze_time_gaps():
    """分析时间缺口"""
    print("🔍 分析时间连续性缺口")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    
    # 分析每个币种的时间缺口
    query = """
    WITH time_series AS (
        SELECT 
            symbol,
            timestamp,
            LAG(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_ts
        FROM market_data_1h
    ),
    gaps AS (
        SELECT 
            symbol,
            timestamp,
            prev_ts,
            timestamp - prev_ts as gap_seconds,
            CASE 
                WHEN timestamp - prev_ts > 3600 THEN 1
                ELSE 0
            END as is_large_gap
        FROM time_series
        WHERE prev_ts IS NOT NULL
    )
    SELECT 
        symbol,
        COUNT(*) as total_gaps,
        SUM(is_large_gap) as large_gaps,
        MAX(gap_seconds) as max_gap,
        AVG(gap_seconds) as avg_gap,
        GROUP_CONCAT(
            CASE WHEN is_large_gap = 1 THEN 
                strftime('%Y-%m-%d %H:%M:%S', prev_ts, 'unixepoch') || ' -> ' || 
                strftime('%Y-%m-%d %H:%M:%S', timestamp, 'unixepoch') || 
                ' (' || (gap_seconds/3600) || 'h)'
            ELSE NULL END, ' | '
        ) as gap_details
    FROM gaps
    GROUP BY symbol
    HAVING large_gaps > 0
    ORDER BY large_gaps DESC, max_gap DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    print(f"📊 发现 {len(df)} 个币种有时间缺口")
    print(f"总缺口数: {df['large_gaps'].sum()} 个")
    print(f"最大缺口: {df['max_gap'].max()/3600:.1f} 小时")
    
    # 显示前5个最严重的币种
    print("\n🔴 最严重的时间缺口 (前5个):")
    for _, row in df.head().iterrows():
        print(f"  {row['symbol']}: {row['large_gaps']}个大缺口, 最大{row['max_gap']/3600:.1f}小时")
        if row['gap_details']:
            gaps = str(row['gap_details']).split(' | ')[:2]  # 只显示前2个缺口
            for gap in gaps:
                if gap and gap != 'None':
                    print(f"    • {gap}")
    
    conn.close()
    return df

def fill_time_gaps_interpolation():
    """使用插值法填充时间缺口"""
    print("\n🔄 使用插值法填充时间缺口")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    
    # 获取所有币种
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    
    total_filled = 0
    
    for symbol in symbols:
        print(f"\n处理 {symbol}...")
        
        # 读取该币种的所有数据
        df = pd.read_sql_query(f"""
            SELECT timestamp, open, high, low, close, volume 
            FROM market_data_1h 
            WHERE symbol = '{symbol}' 
            ORDER BY timestamp
        """, conn)
        
        if len(df) < 2:
            print(f"  ⚠️ 数据不足，跳过")
            continue
        
        # 创建完整的时间序列（每小时）
        min_ts = df['timestamp'].min()
        max_ts = df['timestamp'].max()
        
        # 生成完整的时间序列
        full_timestamps = pd.date_range(
            start=datetime.fromtimestamp(min_ts),
            end=datetime.fromtimestamp(max_ts),
            freq='1H'
        )
        
        full_df = pd.DataFrame({
            'timestamp': [int(ts.timestamp()) for ts in full_timestamps],
            'symbol': symbol
        })
        
        # 合并现有数据
        merged_df = pd.merge(full_df, df, on=['timestamp', 'symbol'], how='left')
        
        # 检查缺失的数据点
        missing_before = merged_df['open'].isna().sum()
        
        if missing_before == 0:
            print(f"  ✅ 无缺失数据点")
            continue
        
        print(f"  发现 {missing_before} 个缺失数据点")
        
        # 使用前向填充和后向填充结合
        # 对于价格数据，使用线性插值
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            merged_df[col] = merged_df[col].interpolate(method='linear')
        
        # 对于成交量，使用前后平均值
        merged_df['volume'] = merged_df['volume'].fillna(merged_df['volume'].rolling(3, min_periods=1).mean())
        
        # 检查填充结果
        missing_after = merged_df['open'].isna().sum()
        filled = missing_before - missing_after
        
        if filled > 0:
            # 只插入新填充的数据
            new_data = merged_df[merged_df['open'].notna() & df.set_index('timestamp').index.isin(merged_df['timestamp']).values]
            
            # 检查是否已存在
            for _, row in new_data.iterrows():
                cursor.execute("""
                    SELECT COUNT(*) FROM market_data_1h 
                    WHERE symbol = ? AND timestamp = ?
                """, (symbol, int(row['timestamp'])))
                
                if cursor.fetchone()[0] == 0:
                    cursor.execute("""
                        INSERT INTO market_data_1h 
                        (symbol, timestamp, open, high, low, close, volume, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        int(row['timestamp']),
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        float(row['volume']),
                        int(datetime.now().timestamp())
                    ))
                    total_filled += 1
            
            conn.commit()
            print(f"  ✅ 填充 {filled} 个数据点")
        else:
            print(f"  ⚠️ 无法填充数据点")
    
    conn.close()
    print(f"\n📈 总共填充 {total_filled} 个缺失数据点")
    return total_filled

def add_database_indexes():
    """添加数据库索引"""
    print("\n⚡ 添加数据库索引")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    indexes = [
        ("idx_md_timestamp", "CREATE INDEX idx_md_timestamp ON market_data_1h(timestamp)"),
        ("idx_md_symbol_ts_composite", "CREATE INDEX idx_md_symbol_ts_composite ON market_data_1h(symbol, timestamp)"),
        ("idx_md_ts_symbol_composite", "CREATE INDEX idx_md_ts_symbol_composite ON market_data_1h(timestamp, symbol)")
    ]
    
    added_count = 0
    
    for index_name, create_sql in indexes:
        # 检查是否已存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?", (index_name,))
        
        if not cursor.fetchone():
            try:
                cursor.execute(create_sql)
                print(f"  ✅ 添加索引: {index_name}")
                added_count += 1
            except Exception as e:
                print(f"  ❌ 添加索引失败 {index_name}: {e}")
        else:
            print(f"  ✓ 索引已存在: {index_name}")
    
    conn.commit()
    conn.close()
    
    print(f"\n📊 索引添加完成: {added_count} 个新索引")
    return added_count

def verify_optimization_results():
    """验证优化结果"""
    print("\n🔍 验证优化结果")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 检查优化后的数据统计
    cursor.execute("SELECT COUNT(*) as total_records FROM market_data_1h")
    total_records = cursor.fetchone()[0]
    
    # 检查时间连续性改进
    cursor.execute("""
        WITH time_gaps AS (
            SELECT 
                symbol,
                timestamp - LAG(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) as gap_seconds
            FROM market_data_1h
        )
        SELECT 
            COUNT(*) as total_gaps,
            SUM(CASE WHEN gap_seconds > 3600 THEN 1 ELSE 0 END) as large_gaps,
            MAX(gap_seconds) as max_gap
        FROM time_gaps 
        WHERE gap_seconds IS NOT NULL
    """)
    
    total_gaps, large_gaps, max_gap = cursor.fetchone()
    
    print(f"📊 优化后数据统计:")
    print(f"  总记录数: {total_records}")
    print(f"  总时间缺口: {total_gaps} 个")
    print(f"  大时间缺口 (>1小时): {large_gaps} 个")
    print(f"  最大缺口: {max_gap/3600:.1f} 小时" if max_gap else "无数据")
    
    # 检查索引
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'market_data_1h'")
    indexes = [row[0] for row in cursor.fetchall()]
    
    print(f"\n📋 数据库索引 ({len(indexes)}个):")
    for idx in indexes:
        print(f"  • {idx}")
    
    conn.close()
    
    return {
        'total_records': total_records,
        'large_gaps': large_gaps,
        'max_gap_hours': max_gap/3600 if max_gap else 0,
        'index_count': len(indexes)
    }

def main():
    print("🚀 时间连续性优化工具")
    print("=" * 60)
    
    # 1. 分析时间缺口
    gap_analysis = analyze_time_gaps()
    
    # 2. 填充时间缺口
    filled_count = fill_time_gaps_interpolation()
    
    # 3. 添加数据库索引
    index_count = add_database_indexes()
    
    # 4. 验证优化结果
    results = verify_optimization_results()
    
    print("\n" + "=" * 60)
    print("✅ 数据优化完成!")
    print("=" * 60)
    
    print(f"\n📊 优化成果总结:")
    print(f"1. 📈 数据填充: {filled_count} 个缺失数据点")
    print(f"2. ⚡ 性能优化: {index_count} 个新数据库索引")
    print(f"3. 📊 当前状态: {results['total_records']} 条记录")
    print(f"4. ⏰ 时间连续性: {results['large_gaps']} 个大缺口")
    
    if results['large_gaps'] > 0:
        print(f"\n⚠️ 仍然存在 {results['large_gaps']} 个大时间缺口")
        print("   原因: 某些币种在特定时段确实没有交易")
        print("   建议: 对于策略回测，可以:")
        print("     • 使用有交易的币种")
        print("     • 在回测中处理数据缺口")
        print("     • 或继续补充更早的历史数据")
    else:
        print(f"\n🎉 完美! 无大时间缺口")
    
    print(f"\n💡 下一步建议:")
    print("1. 运行策略回测验证数据质量")
    print("2. 监控查询性能提升")
    print("3. 定期运行数据质量检查")
    print("=" * 60)

if __name__ == "__main__":
    main()