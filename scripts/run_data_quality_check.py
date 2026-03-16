#!/usr/bin/env python3
"""
运行数据质量检查
"""

import sqlite3
from datetime import datetime

def run_data_quality_checks():
    """运行数据质量检查"""
    print("🔍 运行数据质量检查")
    print("=" * 60)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 1. 数据完整性检查
    print("\n📊 1. 数据完整性检查")
    print("-" * 40)
    
    cursor.execute("""
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
    """)
    
    results = cursor.fetchall()
    
    print(f"币种数量: {len(results)}")
    print(f"{'币种':<15} {'记录数':<8} {'覆盖率':<8} {'质量':<8}")
    print("-" * 40)
    
    excellent = 0
    good = 0
    average = 0
    need_improvement = 0
    
    for symbol, count, earliest, latest, hours, coverage, quality in results[:10]:  # 显示前10个
        print(f"{symbol:<15} {count:<8} {coverage:<8.1f}% {quality:<8}")
        
        if quality == '优秀':
            excellent += 1
        elif quality == '良好':
            good += 1
        elif quality == '一般':
            average += 1
        else:
            need_improvement += 1
    
    if len(results) > 10:
        print(f"... 还有 {len(results)-10} 个币种")
    
    print(f"\n📈 质量分布:")
    print(f"  优秀: {excellent}个 ({excellent/len(results)*100:.1f}%)")
    print(f"  良好: {good}个 ({good/len(results)*100:.1f}%)")
    print(f"  一般: {average}个 ({average/len(results)*100:.1f}%)")
    print(f"  需改进: {need_improvement}个 ({need_improvement/len(results)*100:.1f}%)")
    
    # 2. 数据质量问题检测
    print("\n🚨 2. 数据质量问题检测")
    print("-" * 40)
    
    cursor.execute("""
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
    """)
    
    quality_results = cursor.fetchall()
    
    perfect_count = sum(1 for r in quality_results if r[5] == '✅ 优秀')
    issues_count = len(quality_results) - perfect_count
    
    print(f"完美币种: {perfect_count}个 ({perfect_count/len(quality_results)*100:.1f}%)")
    print(f"存在问题: {issues_count}个 ({issues_count/len(quality_results)*100:.1f}%)")
    
    if issues_count > 0:
        print("\n⚠️ 存在问题的币种:")
        for symbol, invalid, negative, inverted, out_of_range, status in quality_results:
            if status != '✅ 优秀':
                issues = []
                if invalid > 0:
                    issues.append(f"无效价格:{invalid}")
                if negative > 0:
                    issues.append(f"负成交量:{negative}")
                if inverted > 0:
                    issues.append(f"高低价倒置:{inverted}")
                if out_of_range > 0:
                    issues.append(f"收盘价越界:{out_of_range}")
                print(f"  {symbol}: {', '.join(issues)}")
    
    # 3. 时间连续性分析
    print("\n⏰ 3. 时间连续性分析")
    print("-" * 40)
    
    cursor.execute("""
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
    """)
    
    continuity_results = cursor.fetchall()
    
    continuous = sum(1 for r in continuity_results if r[5] == '✅ 连续')
    small_gaps = sum(1 for r in continuity_results if r[5] == '⚠️ 少量缺口')
    need_optimization = sum(1 for r in continuity_results if r[5] == '❌ 需优化')
    
    print(f"完全连续: {continuous}个 ({continuous/len(continuity_results)*100:.1f}%)")
    print(f"少量缺口: {small_gaps}个 ({small_gaps/len(continuity_results)*100:.1f}%)")
    print(f"需优化: {need_optimization}个 ({need_optimization/len(continuity_results)*100:.1f}%)")
    
    if need_optimization > 0:
        print("\n❌ 需要优化的币种 (缺口>5个):")
        for symbol, total_gaps, large_gaps, max_gap, avg_gap, status in continuity_results:
            if status == '❌ 需优化':
                print(f"  {symbol}: 总缺口{total_gaps}个, 大缺口{large_gaps}个, 最大缺口{max_gap}秒")
    
    conn.close()
    
    return {
        'total_symbols': len(results),
        'excellent_quality': excellent,
        'good_quality': good,
        'average_quality': average,
        'need_improvement': need_improvement,
        'perfect_data_quality': perfect_count,
        'data_issues': issues_count,
        'continuous': continuous,
        'small_gaps': small_gaps,
        'need_optimization': need_optimization
    }

def create_data_optimization_plan(stats):
    """创建数据优化计划"""
    print("\n🎯 4. 数据优化计划")
    print("=" * 60)
    
    print("📋 优化优先级:")
    
    # 优先级1: 数据质量问题
    if stats['data_issues'] > 0:
        print("1. 🔴 高优先级: 修复数据质量问题")
        print("   • 检查并修复无效价格、负成交量等问题")
        print("   • 影响: 数据准确性，可能导致错误的分析结果")
    
    # 优先级2: 时间连续性
    if stats['need_optimization'] > 0:
        print("2. 🟡 中优先级: 优化时间连续性")
        print("   • 补充缺失的时间点数据")
        print("   • 影响: 时间序列分析的准确性")
    
    # 优先级3: 覆盖率提升
    if stats['need_improvement'] > 0:
        print("3. 🟢 低优先级: 提升数据覆盖率")
        print("   • 补充缺失的记录")
        print("   • 影响: 数据完整性，但当前覆盖率已很高")
    
    # 优先级4: 性能优化
    print("4. ⚡ 性能优化: 添加数据库索引")
    print("   • 添加 timestamp 单列索引")
    print("   • 添加 (symbol, timestamp) 复合索引")
    print("   • 添加 (timestamp, symbol) 复合索引")
    print("   • 影响: 查询性能提升")
    
    print("\n💡 具体优化步骤:")
    print("1. 运行数据修复脚本清理无效数据")
    print("2. 运行时间连续性优化补充缺失时间点")
    print("3. 添加建议的数据库索引")
    print("4. 更新覆盖率计算逻辑")
    
    return True

def main():
    print("🚀 数据质量检查与优化")
    print("=" * 60)
    
    # 运行数据质量检查
    stats = run_data_quality_checks()
    
    # 创建优化计划
    create_data_optimization_plan(stats)
    
    print("\n" + "=" * 60)
    print("✅ 数据质量检查完成!")
    print("=" * 60)
    
    # 总结报告
    print("\n📊 数据质量总结:")
    print(f"• 币种总数: {stats['total_symbols']}")
    print(f"• 数据质量: {stats['excellent_quality']}优秀, {stats['good_quality']}良好, {stats['average_quality']}一般, {stats['need_improvement']}需改进")
    print(f"• 数据准确性: {stats['perfect_data_quality']}完美, {stats['data_issues']}存在问题")
    print(f"• 时间连续性: {stats['continuous']}连续, {stats['small_gaps']}少量缺口, {stats['need_optimization']}需优化")
    
    # 总体评分
    total_score = 100
    if stats['data_issues'] > 0:
        total_score -= 20
    if stats['need_optimization'] > 0:
        total_score -= 10
    if stats['need_improvement'] > 0:
        total_score -= 5
    
    print(f"\n🎯 总体质量评分: {total_score}/100")
    
    if total_score >= 90:
        print("⭐ 优秀: 数据质量很高，适合进行策略回测和分析")
    elif total_score >= 80:
        print("✅ 良好: 数据质量不错，可以进行策略回测")
    elif total_score >= 70:
        print("⚠️ 一般: 建议先进行数据优化")
    else:
        print("❌ 需改进: 需要优先进行数据优化")
    
    print("\n💡 建议:")
    if total_score >= 90:
        print("可以直接进行策略回测和分析")
    else:
        print("建议先运行数据优化脚本")
    
    print("=" * 60)

if __name__ == "__main__":
    main()