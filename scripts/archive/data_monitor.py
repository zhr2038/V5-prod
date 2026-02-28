#!/usr/bin/env python3
"""
数据监控面板
显示自动化数据采集的状态和关键指标
"""

import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta
import sys


def print_header(title):
    """打印标题"""
    print(f"\n{'='*60}")
    print(f"📊 {title}")
    print(f"{'='*60}")


def monitor_data_status():
    """监控数据状态"""
    db_path = "reports/alpha_history.db"
    
    print_header("数据状态监控")
    
    conn = sqlite3.connect(db_path)
    
    # 1. 市场数据状态
    cursor = conn.cursor()
    cursor.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT symbol) as symbols,
        MIN(timestamp) as min_ts,
        MAX(timestamp) as max_ts,
        MAX(updated_at) as last_updated
    FROM market_data_1h
    """)
    
    total, symbols, min_ts, max_ts, last_updated = cursor.fetchone()
    
    if min_ts and max_ts:
        min_dt = datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M')
        max_dt = datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M')
        hours = (max_ts - min_ts) / 3600
        
        print(f"📈 市场数据:")
        print(f"  记录数: {total:,}")
        print(f"  币种数: {symbols}")
        print(f"  时间范围: {min_dt} 到 {max_dt}")
        print(f"  覆盖时长: {hours:.0f} 小时 ({hours/24:.1f} 天)")
        
        if last_updated:
            last_dt = datetime.fromtimestamp(last_updated).strftime('%Y-%m-%d %H:%M:%S')
            age = (time.time() - last_updated) / 60  # 分钟
            print(f"  最后更新: {last_dt} ({age:.0f} 分钟前)")
    
    # 2. Forward returns 状态
    cursor.execute("""
    SELECT 
        COUNT(*) as total,
        AVG(CASE WHEN return_1h IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_1h,
        AVG(CASE WHEN return_6h IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_6h,
        AVG(CASE WHEN return_24h IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_24h
    FROM forward_returns
    """)
    
    total, pct_1h, pct_6h, pct_24h = cursor.fetchone()
    
    print(f"\n📊 Forward Returns:")
    print(f"  记录数: {total:,}")
    print(f"  完整度: 1h={pct_1h:.1f}%, 6h={pct_6h:.1f}%, 24h={pct_24h:.1f}%")
    
    # 3. Alpha 数据状态
    try:
        cursor.execute("SELECT COUNT(*) FROM alpha_snapshots")
        alpha_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT run_id) FROM alpha_snapshots")
        run_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MAX(ts) FROM alpha_snapshots")
        last_alpha_ts = cursor.fetchone()[0]
        
        print(f"\n🧠 Alpha 数据:")
        print(f"  记录数: {alpha_count:,}")
        print(f"  运行次数: {run_count}")
        
        if last_alpha_ts:
            last_dt = datetime.fromtimestamp(last_alpha_ts).strftime('%Y-%m-%d %H:%M:%S')
            age = (time.time() - last_alpha_ts) / 3600  # 小时
            print(f"  最后运行: {last_dt} ({age:.1f} 小时前)")
    except:
        pass
    
    # 4. 采集状态
    cursor.execute("""
    SELECT 
        task_type,
        COUNT(*) as runs,
        SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_runs,
        AVG(duration_seconds) as avg_duration,
        MAX(timestamp) as last_run
    FROM data_collection_status 
    WHERE timestamp > strftime('%s', 'now') - 86400  -- 最近24小时
    GROUP BY task_type
    ORDER BY task_type
    """)
    
    collection_stats = cursor.fetchall()
    
    if collection_stats:
        print(f"\n🔄 采集状态 (最近24小时):")
        for task_type, runs, success_runs, avg_duration, last_run in collection_stats:
            success_rate = (success_runs / runs * 100) if runs > 0 else 0
            last_dt = datetime.fromtimestamp(last_run).strftime('%H:%M') if last_run else "N/A"
            
            print(f"  {task_type:15s}: {runs:2d} 次, 成功率: {success_rate:.0f}%, 平均耗时: {avg_duration:.1f}s, 最后: {last_dt}")
    
    conn.close()


def monitor_ic_status():
    """监控 IC 状态"""
    print_header("IC 状态监控")
    
    db_path = "reports/alpha_history.db"
    conn = sqlite3.connect(db_path)
    
    try:
        # 使用 ic_calculation_view
        query = """
        SELECT 
            COUNT(*) as total_points,
            COUNT(DISTINCT symbol) as symbols,
            MIN(timestamp) as min_ts,
            MAX(timestamp) as max_ts,
            AVG(score) as avg_score
        FROM ic_calculation_view
        WHERE return_1h IS NOT NULL
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        
        if row and row[0] > 0:
            total, symbols, min_ts, max_ts, avg_score = row
            
            print(f"📈 IC 计算数据:")
            print(f"  有效数据点: {total:,}")
            print(f"  币种数量: {symbols}")
            
            if min_ts and max_ts:
                days = (max_ts - min_ts) / 86400
                print(f"  时间范围: {days:.1f} 天")
            
            print(f"  平均分数: {avg_score:.3f}")
            
            # 计算最新 IC
            query = """
            SELECT 
                timestamp,
                symbol,
                score,
                return_1h,
                return_6h,
                return_24h
            FROM ic_calculation_view
            WHERE return_1h IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 100
            """
            
            df = pd.read_sql_query(query, conn)
            
            if not df.empty:
                print(f"\n📊 最新 IC 值:")
                
                horizons = ['1h', '6h', '24h']
                for horizon in horizons:
                    col = f'return_{horizon}'
                    if col in df.columns:
                        valid = df[df[col].notna()]
                        if len(valid) > 10:
                            ic = valid['score'].corr(valid[col])
                            print(f"  IC({horizon}): {ic:.4f} (n={len(valid)})")
        
    except Exception as e:
        print(f"❌ IC 监控错误: {e}")
    
    conn.close()


def check_data_gaps():
    """检查数据缺口"""
    print_header("数据缺口检查")
    
    db_path = "reports/alpha_history.db"
    conn = sqlite3.connect(db_path)
    
    # 检查每个币种的最近数据时间
    cursor = conn.cursor()
    cursor.execute("""
    SELECT 
        symbol,
        MAX(timestamp) as last_ts,
        COUNT(*) as record_count,
        (strftime('%s', 'now') - MAX(timestamp)) / 3600 as hours_since_update
    FROM market_data_1h
    GROUP BY symbol
    ORDER BY hours_since_update DESC
    LIMIT 10
    """)
    
    gaps = cursor.fetchall()
    
    if gaps:
        print("⏰ 数据更新延迟 (Top 10):")
        print("  Symbol          最后更新           延迟(小时)  记录数")
        print("  " + "-" * 50)
        
        for symbol, last_ts, count, delay in gaps:
            if last_ts:
                last_dt = datetime.fromtimestamp(last_ts).strftime('%m-%d %H:%M')
                print(f"  {symbol:12s}  {last_dt:12s}  {delay:8.1f}  {count:8d}")
                
                if delay > 3:
                    print(f"    ⚠️  数据延迟超过3小时!")
    
    conn.close()


def system_recommendations():
    """系统建议"""
    print_header("系统建议")
    
    db_path = "reports/alpha_history.db"
    conn = sqlite3.connect(db_path)
    
    recommendations = []
    
    # 检查数据量
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM alpha_snapshots")
    alpha_count = cursor.fetchone()[0]
    
    if alpha_count < 1000:
        recommendations.append(f"📈 需要更多 alpha 数据 (当前: {alpha_count}, 目标: 1000+)")
    
    # 检查时间范围
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM market_data_1h")
    min_ts, max_ts = cursor.fetchone()
    
    if min_ts and max_ts:
        days = (max_ts - min_ts) / 86400
        if days < 7:
            recommendations.append(f"📅 需要更长时间范围的数据 (当前: {days:.1f} 天, 目标: 30+ 天)")
    
    # 检查采集状态
    cursor.execute("""
    SELECT task_type, MAX(timestamp) 
    FROM data_collection_status 
    GROUP BY task_type
    """)
    
    for task_type, last_ts in cursor.fetchall():
        if last_ts:
            age = (time.time() - last_ts) / 3600  # 小时
            if age > 2 and task_type == 'market_data':
                recommendations.append(f"🔄 {task_type} 采集已停止 {age:.1f} 小时")
    
    conn.close()
    
    if recommendations:
        for rec in recommendations:
            print(f"  {rec}")
    else:
        print("  ✅ 系统运行正常")
    
    print(f"\n📋 维护命令:")
    print(f"  1. 手动采集: python3 scripts/auto_data_collector.py")
    print(f"  2. 运行 V5: export V5_CONFIG=configs/live_small.yaml && export V5_LIVE_ARM=YES && python3 main.py")
    print(f"  3. IC 分析: python3 scripts/quick_ic_analysis.py")


def main():
    import time
    
    print("🚀 数据监控面板")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    monitor_data_status()
    monitor_ic_status()
    check_data_gaps()
    system_recommendations()
    
    print(f"\n{'='*60}")
    print("🎯 监控完成")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()