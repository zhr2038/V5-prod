#!/usr/bin/env python3
"""
简单数据优化 - 主要优化数据库索引和覆盖率计算
"""

import sqlite3

def add_optimized_indexes():
    """添加优化的数据库索引"""
    print("⚡ 添加优化的数据库索引")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 建议的索引
    indexes = [
        ("idx_market_data_timestamp", "CREATE INDEX IF NOT EXISTS idx_market_data_timestamp ON market_data_1h(timestamp)"),
        ("idx_market_data_symbol_timestamp", "CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timestamp ON market_data_1h(symbol, timestamp)"),
        ("idx_market_data_timestamp_symbol", "CREATE INDEX IF NOT EXISTS idx_market_data_timestamp_symbol ON market_data_1h(timestamp, symbol)"),
        ("idx_forward_returns_symbol_ts", "CREATE INDEX IF NOT EXISTS idx_forward_returns_symbol_ts ON forward_returns(symbol, timestamp)"),
    ]
    
    added = 0
    existing = 0
    
    for index_name, create_sql in indexes:
        # 检查是否已存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?", (index_name,))
        
        if cursor.fetchone():
            print(f"  ✓ {index_name} (已存在)")
            existing += 1
        else:
            try:
                cursor.execute(create_sql)
                print(f"  ✅ {index_name} (已添加)")
                added += 1
            except Exception as e:
                print(f"  ❌ {index_name} 创建失败: {e}")
    
    conn.commit()
    
    # 分析索引效果
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name LIKE '%market_data%' OR tbl_name LIKE '%forward_returns%'")
    all_indexes = [row[0] for row in cursor.fetchall()]
    
    print(f"\n📊 索引统计:")
    print(f"  新增索引: {added} 个")
    print(f"  现有索引: {existing} 个")
    print(f"  总索引数: {len(all_indexes)} 个")
    
    conn.close()
    return added

def analyze_query_performance():
    """分析查询性能"""
    print("\n📈 分析查询性能")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 常见查询的性能分析
    queries = [
        ("按币种和时间查询", "SELECT * FROM market_data_1h WHERE symbol = 'BTC/USDT' AND timestamp >= 1771300000 AND timestamp <= 1771380000"),
        ("按时间范围查询", "SELECT COUNT(*) FROM market_data_1h WHERE timestamp >= 1771300000 AND timestamp <= 1771380000"),
        ("按币种统计", "SELECT symbol, COUNT(*) FROM market_data_1h GROUP BY symbol"),
        ("Forward Returns查询", "SELECT * FROM forward_returns WHERE symbol = 'BTC/USDT' AND return_1h IS NOT NULL"),
    ]
    
    print("常见查询分析:")
    for name, query in queries:
        try:
            # 使用EXPLAIN QUERY PLAN分析
            cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            plan = cursor.fetchall()
            
            print(f"\n🔍 {name}:")
            print(f"  查询: {query[:60]}...")
            print(f"  执行计划:")
            for row in plan[:3]:  # 只显示前3行
                print(f"    {row[3]}")
        except Exception as e:
            print(f"\n❌ {name} 分析失败: {e}")
    
    conn.close()

def update_coverage_calculation():
    """更新覆盖率计算逻辑"""
    print("\n🔧 更新覆盖率计算逻辑")
    print("=" * 50)
    
    # 分析当前覆盖率计算问题
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 正确的覆盖率计算
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
        
        # 多种覆盖率计算方式
        coverage_actual = (total / (symbols * hours_range)) * 100 if symbols * hours_range > 0 else 0
        coverage_30d = (total / (symbols * 720)) * 100 if symbols * 720 > 0 else 0
        
        print(f"📊 覆盖率计算分析:")
        print(f"  实际时间范围: {hours_range:.1f}小时 ({hours_range/24:.1f}天)")
        print(f"  30天基准: 720小时 (30天)")
        print(f"  实际覆盖率: {coverage_actual:.2f}%")
        print(f"  30天覆盖率: {coverage_30d:.2f}%")
        
        # 原错误计算
        wrong_coverage = (total / (symbols * 168 * 7)) * 100 if symbols > 0 else 0
        print(f"  原错误计算: {wrong_coverage:.2f}% (使用168*7=1176小时)")
        
        print(f"\n💡 修复建议:")
        print(f"  在 auto_data_collector.py 中:")
        print(f"  第377行: coverage_pct = (total / (symbols * 168 * 7)) * 100")
        print(f"  应改为: coverage_pct = (total / (symbols * 720)) * 100  # 30天基准")
        print(f"  或改为: coverage_pct = (total / (symbols * hours_range)) * 100  # 实际时间范围")
    
    conn.close()
    
    # 创建修复脚本（写入到文件，不在本脚本内执行）
    fix_script = r'''#!/usr/bin/env python3
# 覆盖率计算修复脚本
# 运行此脚本修复 auto_data_collector.py 中的覆盖率计算错误

import re

def fix_coverage_calculation():
    """修复覆盖率计算"""
    file_path = "scripts/auto_data_collector.py"
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # 查找并替换错误的覆盖率计算
    old_pattern = r"'coverage_pct': \(total / \(symbols \* 168 \* 7\)\) \* 100 if symbols > 0 else 0"
    new_text = "'coverage_pct': (total / (symbols * 720)) * 100 if symbols > 0 else 0  # 30天基准"
    
    if old_pattern in content:
        content = content.replace(old_pattern, new_text)
        
        with open(file_path, 'w') as f:
            f.write(content)
        
        print("✅ 覆盖率计算已修复")
        print(f"  原公式: total / (symbols * 168 * 7)")
        print(f"  新公式: total / (symbols * 720)  # 30天基准")
        return True
    else:
        print("⚠️ 未找到需要修复的代码")
        return False

if __name__ == "__main__":
    fix_coverage_calculation()
'''
    
    with open("scripts/fix_coverage.py", "w") as f:
        f.write(fix_script)
    
    print(f"\n📋 修复脚本已生成: scripts/fix_coverage.py")
    print("运行: python3 scripts/fix_coverage.py")

def create_data_quality_monitor():
    """创建数据质量监控"""
    print("\n📊 创建数据质量监控")
    print("=" * 50)
    
    monitor_script = r'''#!/usr/bin/env python3
"""
数据质量监控脚本
每日运行检查数据质量
"""

import sqlite3
import json
from datetime import datetime, timedelta

def check_data_quality():
    """检查数据质量"""
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    checks = {}
    
    # 1. 数据完整性检查
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT symbol) as symbol_count,
            COUNT(*) as total_records,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest
        FROM market_data_1h
    """)
    symbols, total, earliest, latest = cursor.fetchone()
    
    hours_range = (latest - earliest) / 3600 if earliest and latest else 0
    coverage_30d = (total / (symbols * 720)) * 100 if symbols * 720 > 0 else 0
    
    checks['completeness'] = {
        'symbol_count': symbols,
        'total_records': total,
        'time_range_hours': hours_range,
        'coverage_30d': coverage_30d,
        'status': '✅ 优秀' if coverage_30d >= 95 else '⚠️ 需改进' if coverage_30d >= 80 else '❌ 严重'
    }
    
    # 2. 数据质量检查
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 1 ELSE 0 END) as invalid_prices,
            SUM(CASE WHEN volume < 0 THEN 1 ELSE 0 END) as negative_volume
        FROM market_data_1h
    """)
    invalid_prices, negative_volume = cursor.fetchone()
    
    checks['quality'] = {
        'invalid_prices': invalid_prices,
        'negative_volume': negative_volume,
        'invalid_pct': (invalid_prices / total * 100) if total > 0 else 0,
        'status': '✅ 优秀' if invalid_prices == 0 and negative_volume == 0 else '❌ 存在问题'
    }
    
    # 3. 时间连续性检查
    cursor.execute("""
        WITH time_gaps AS (
            SELECT 
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
    
    checks['continuity'] = {
        'total_gaps': total_gaps,
        'large_gaps': large_gaps,
        'max_gap_hours': max_gap / 3600 if max_gap else 0,
        'status': '✅ 优秀' if large_gaps == 0 else '⚠️ 少量缺口' if large_gaps <= 10 else '❌ 需优化'
    }
    
    conn.close()
    
    # 保存检查结果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"reports/data_quality_{timestamp}.json"
    
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'checks': checks,
            'summary': generate_summary(checks)
        }, f, indent=2)
    
    return checks, report_path

def generate_summary(checks):
    """生成总结"""
    summary = []
    
    # 完整性
    comp = checks['completeness']
    summary.append(f"完整性: {comp['status']} ({comp['coverage_30d']:.1f}%覆盖率)")
    
    # 质量
    qual = checks['quality']
    if qual['invalid_prices'] > 0 or qual['negative_volume'] > 0:
        summary.append(f"质量: {qual['status']} ({qual['invalid_prices']}无效价格)")
    else:
        summary.append(f"质量: {qual['status']}")
    
    # 连续性
    cont = checks['continuity']
    if cont['large_gaps'] > 0:
        summary.append(f"连续性: {cont['status']} ({cont['large_gaps']}个大缺口)")
    else:
        summary.append(f"连续性: {cont['status']}")
    
    return "; ".join(summary)

def main():
    print("🔍 数据质量监控")
    print("=" * 50)
    
    checks, report_path = check_data_quality()
    
    print("📊 检查结果:")
    for check_name, check_data in checks.items():
        print(f"\n{check_name.upper()}:")
        for key, value in check_data.items():
            if key != 'status':
                print(f"  {key}: {value}")
    
    print(f"\n📋 总结: {generate_summary(checks)}")
    print(f"\n📁 报告已保存: {report_path}")

if __name__ == "__main__":
    main()
'''
    
    with open("scripts/data_quality_monitor.py", "w") as f:
        f.write(monitor_script)
    
    print(f"✅ 数据质量监控脚本已创建: scripts/data_quality_monitor.py")
    print("💡 建议添加到cron每日运行:")
    print("   0 9 * * * cd /home/admin/clawd/v5-trading-bot && python3 scripts/data_quality_monitor.py")

def main():
    print("🚀 简单数据优化工具")
    print("=" * 60)
    
    # 1. 添加数据库索引
    indexes_added = add_optimized_indexes()
    
    # 2. 分析查询性能
    analyze_query_performance()
    
    # 3. 更新覆盖率计算
    update_coverage_calculation()
    
    # 4. 创建数据质量监控
    create_data_quality_monitor()
    
    print("\n" + "=" * 60)
    print("✅ 数据优化完成!")
    print("=" * 60)
    
    print(f"\n📋 优化成果:")
    print(f"1. ⚡ 数据库索引: {indexes_added} 个新索引")
    print(f"2. 🔧 覆盖率计算: 已分析并生成修复脚本")
    print(f"3. 📊 质量监控: 创建了数据质量监控脚本")
    
    print(f"\n💡 下一步:")
    print("1. 运行修复脚本: python3 scripts/fix_coverage.py")
    print("2. 测试数据质量监控: python3 scripts/data_quality_monitor.py")
    print("3. 将质量监控添加到cron定期运行")
    print("4. 运行策略回测验证数据质量")
    print("=" * 60)

if __name__ == "__main__":
    main()