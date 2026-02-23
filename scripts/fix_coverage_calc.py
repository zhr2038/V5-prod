#!/usr/bin/env python3
"""
修复覆盖率计算逻辑
"""

import sqlite3

def fix_coverage_calculation():
    """修复覆盖率计算"""
    print("🔧 修复覆盖率计算逻辑")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 获取当前数据统计
    cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as symbol_count,
            MIN(timestamp) as min_ts,
            MAX(timestamp) as max_ts
        FROM market_data_1h
    """)
    
    total, symbols, min_ts, max_ts = cursor.fetchone()
    
    if min_ts and max_ts:
        hours_range = (max_ts - min_ts) / 3600
        # 正确的覆盖率计算
        theoretical_max = symbols * hours_range
        actual_coverage = (total / theoretical_max) * 100 if theoretical_max > 0 else 0
        
        # 30天覆盖率
        theoretical_max_30d = symbols * 720  # 30天 × 24小时
        coverage_30d = (total / theoretical_max_30d) * 100 if theoretical_max_30d > 0 else 0
        
        print(f"📊 当前数据统计:")
        print(f"  总记录数: {total}")
        print(f"  币种数量: {symbols}")
        print(f"  时间范围: {hours_range:.1f}小时 ({hours_range/24:.1f}天)")
        print(f"  理论最大 (实际时间范围): {theoretical_max:.0f}条")
        print(f"  实际覆盖率 (实际时间范围): {actual_coverage:.2f}%")
        print(f"  理论最大 (30天): {theoretical_max_30d}条")
        print(f"  30天覆盖率: {coverage_30d:.2f}%")
        
        # 修复建议
        print(f"\n💡 修复建议:")
        print(f"  原公式: total / (symbols * 168 * 7)")
        print(f"  错误原因: 168*7=1176小时 (49天)，与实际时间范围不匹配")
        print(f"  正确公式: total / (symbols * hours_range)")
        print(f"  或使用30天基准: total / (symbols * 720)")
    
    conn.close()
    
    return True

def create_optimized_queries():
    """创建优化查询"""
    print(f"\n📈 创建优化数据查询")
    print("=" * 50)
    
    queries = {
        "数据完整性检查": """
            -- 检查数据完整性
            SELECT 
                symbol,
                COUNT(*) as record_count,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                (MAX(timestamp)-MIN(timestamp))/3600 as hours_range,
                ROUND(COUNT(*)*100.0/720, 2) as coverage_30d_pct,
                CASE 
                    WHEN COUNT(*) >= 700 THEN '优秀'
                    WHEN COUNT(*) >= 650 THEN '良好'
                    WHEN COUNT(*) >= 600 THEN '一般'
                    ELSE '需改进'
                END as quality
            FROM market_data_1h 
            GROUP BY symbol 
            ORDER BY coverage_30d_pct DESC
        """,
        
        "数据质量问题检测": """
            -- 检测数据质量问题
            WITH issues AS (
                SELECT 
                    symbol,
                    SUM(CASE WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 1 ELSE 0 END) as invalid_prices,
                    SUM(CASE WHEN volume < 0 THEN 1 ELSE 0 END) as negative_volume,
                    SUM(CASE WHEN high < low THEN 1 ELSE 0 END) as high_low_inverted,
                    SUM(CASE WHEN close < low OR close > high THEN 1 ELSE 0 END) as close_out_of_range
                FROM market_data_1h 
                GROUP BY symbol
            )
            SELECT 
                symbol,
                invalid_prices,
                negative_volume,
                high_low_inverted,
                close_out_of_range,
                CASE 
                    WHEN invalid_prices + negative_volume + high_low_inverted + close_out_of_range = 0 THEN '✅ 优秀'
                    ELSE '⚠️ 需检查'
                END as quality_status
            FROM issues
            ORDER BY quality_status, symbol
        """,
        
        "时间连续性分析": """
            -- 分析时间连续性
            WITH time_gaps AS (
                SELECT 
                    symbol,
                    timestamp,
                    timestamp - LAG(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) as gap_seconds
                FROM market_data_1h
            ),
            gap_stats AS (
                SELECT 
                    symbol,
                    COUNT(*) as total_gaps,
                    SUM(CASE WHEN gap_seconds > 3600 THEN 1 ELSE 0 END) as large_gaps,
                    MAX(gap_seconds) as max_gap_seconds,
                    AVG(gap_seconds) as avg_gap_seconds
                FROM time_gaps 
                WHERE gap_seconds IS NOT NULL
                GROUP BY symbol
            )
            SELECT 
                symbol,
                total_gaps,
                large_gaps,
                max_gap_seconds,
                ROUND(avg_gap_seconds, 0) as avg_gap_seconds,
                CASE 
                    WHEN large_gaps = 0 THEN '✅ 连续'
                    WHEN large_gaps <= 5 THEN '⚠️ 少量缺口'
                    ELSE '❌ 需优化'
                END as continuity_status
            FROM gap_stats
            ORDER BY large_gaps DESC, symbol
        """
    }
    
    # 保存优化查询到文件
    with open("reports/optimized_data_queries.sql", "w") as f:
        f.write("-- 优化数据查询脚本\n")
        f.write("-- 生成时间: 2026-02-18\n")
        f.write("-- 用于数据质量分析和优化\n\n")
        
        for name, query in queries.items():
            f.write(f"-- {name}\n")
            f.write(query)
            f.write("\n\n")
    
    print(f"✅ 优化查询已保存: reports/optimized_data_queries.sql")
    print(f"📋 包含 {len(queries)} 个优化查询:")
    for name in queries.keys():
        print(f"  • {name}")
    
    return True

def optimize_database_indexes():
    """优化数据库索引"""
    print(f"\n⚡ 优化数据库索引")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 检查现有索引
    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'market_data_1h'")
    existing_indexes = cursor.fetchall()
    
    print(f"📊 现有索引 ({len(existing_indexes)}个):")
    for name, sql in existing_indexes:
        print(f"  • {name}")
    
    # 建议的新索引
    suggested_indexes = [
        ("idx_md_timestamp", "CREATE INDEX idx_md_timestamp ON market_data_1h(timestamp)"),
        ("idx_md_symbol_ts_composite", "CREATE INDEX idx_md_symbol_ts_composite ON market_data_1h(symbol, timestamp)"),
        ("idx_md_ts_symbol_composite", "CREATE INDEX idx_md_ts_symbol_composite ON market_data_1h(timestamp, symbol)")
    ]
    
    print(f"\n💡 建议的新索引:")
    for name, sql in suggested_indexes:
        # 检查是否已存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?", (name,))
        if not cursor.fetchone():
            print(f"  + {name}")
        else:
            print(f"  ✓ {name} (已存在)")
    
    conn.close()
    
    return True

def main():
    print("🚀 数据优化工具")
    print("=" * 60)
    
    # 1. 修复覆盖率计算
    fix_coverage_calculation()
    
    # 2. 创建优化查询
    create_optimized_queries()
    
    # 3. 优化数据库索引
    optimize_database_indexes()
    
    print("\n" + "=" * 60)
    print("✅ 数据优化完成!")
    print("=" * 60)
    print("\n📋 优化成果:")
    print("1. ✅ 覆盖率计算逻辑分析完成")
    print("2. ✅ 优化查询脚本已生成")
    print("3. ✅ 数据库索引优化建议")
    print("\n💡 下一步:")
    print("1. 运行优化查询检查数据质量")
    print("2. 考虑添加建议的数据库索引")
    print("3. 定期运行数据质量检查")
    print("=" * 60)

if __name__ == "__main__":
    main()