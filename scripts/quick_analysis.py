#!/usr/bin/env python3
"""
快速数据分析 - 检查数据收集状态和计算基础指标
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime


def check_data_status():
    """检查数据状态"""
    db_path = "reports/alpha_history.db"
    
    print("📊 数据收集状态检查")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    
    # 检查各表记录数
    tables = ['alpha_snapshots', 'market_data_1h', 'forward_returns', 'run_metadata']
    
    for table in tables:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:20s}: {count:6d} 条记录")
    
    print("\n📈 Alpha 数据详情:")
    cursor = conn.cursor()
    cursor.execute("""
    SELECT 
        COUNT(DISTINCT run_id) as run_count,
        COUNT(DISTINCT symbol) as symbol_count,
        MIN(ts) as first_ts,
        MAX(ts) as last_ts
    FROM alpha_snapshots
    """)
    row = cursor.fetchone()
    
    if row:
        run_count, symbol_count, first_ts, last_ts = row
        first_dt = datetime.fromtimestamp(first_ts).strftime('%Y-%m-%d %H:%M')
        last_dt = datetime.fromtimestamp(last_ts).strftime('%Y-%m-%d %H:%M')
        
        print(f"  运行次数: {run_count}")
        print(f"  币种数量: {symbol_count}")
        print(f"  时间范围: {first_dt} 到 {last_dt}")
        print(f"  数据天数: {(last_ts - first_ts) / 86400:.1f} 天")
    
    print("\n📊 市场数据详情:")
    cursor.execute("""
    SELECT 
        COUNT(DISTINCT symbol) as symbol_count,
        MIN(timestamp) as first_ts,
        MAX(timestamp) as last_ts,
        COUNT(*) as total_bars
    FROM market_data_1h
    """)
    row = cursor.fetchone()
    
    if row:
        symbol_count, first_ts, last_ts, total_bars = row
        if first_ts:
            first_dt = datetime.fromtimestamp(first_ts).strftime('%Y-%m-%d %H:%M')
            last_dt = datetime.fromtimestamp(last_ts).strftime('%Y-%m-%d %H:%M')
            hours = (last_ts - first_ts) / 3600
            
            print(f"  币种数量: {symbol_count}")
            print(f"  时间范围: {first_dt} 到 {last_dt}")
            print(f"  总小时数: {hours:.0f}h")
            print(f"  K线数量: {total_bars}")
            print(f"  平均每个币种: {total_bars/symbol_count:.0f} 条")
    
    print("\n📈 Forward Returns 完整性:")
    cursor.execute("""
    SELECT 
        AVG(CASE WHEN return_1h IS NOT NULL THEN 1 ELSE 0 END) as pct_1h,
        AVG(CASE WHEN return_6h IS NOT NULL THEN 1 ELSE 0 END) as pct_6h,
        AVG(CASE WHEN return_24h IS NOT NULL THEN 1 ELSE 0 END) as pct_24h
    FROM forward_returns
    """)
    row = cursor.fetchone()
    
    if row:
        pct_1h, pct_6h, pct_24h = row
        print(f"  return_1h 完整度:  {pct_1h*100:.1f}%")
        print(f"  return_6h 完整度:  {pct_6h*100:.1f}%")
        print(f"  return_24h 完整度: {pct_24h*100:.1f}%")
    
    conn.close()


def calculate_basic_ic():
    """计算基础 IC"""
    print("\n🔍 计算基础 IC 指标")
    print("=" * 50)
    
    db_path = "reports/alpha_history.db"
    conn = sqlite3.connect(db_path)
    
    # 获取 alpha scores 和 forward returns
    query = """
    SELECT 
        a.ts,
        a.symbol,
        a.score,
        f.return_1h,
        f.return_6h,
        f.return_24h
    FROM alpha_snapshots a
    LEFT JOIN forward_returns f ON a.symbol = f.symbol AND a.ts = f.timestamp
    WHERE a.score IS NOT NULL 
      AND f.return_1h IS NOT NULL
    ORDER BY a.ts
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("❌ 没有足够的数据计算 IC")
        return
    
    print(f"有效数据点: {len(df)}")
    print(f"时间范围: {datetime.fromtimestamp(df['ts'].min()).strftime('%Y-%m-%d')} 到 {datetime.fromtimestamp(df['ts'].max()).strftime('%Y-%m-%d')}")
    
    # 计算 IC
    horizons = ['1h', '6h', '24h']
    ic_results = {}
    
    for horizon in horizons:
        return_col = f'return_{horizon}'
        if return_col in df.columns:
            valid_data = df[df[return_col].notna()]
            if len(valid_data) > 10:
                ic = valid_data['score'].corr(valid_data[return_col])
                ic_results[horizon] = {
                    'ic': ic,
                    'n': len(valid_data),
                    'horizon': horizon
                }
    
    print("\n📈 IC 衰减曲线:")
    print("-" * 30)
    for horizon, result in ic_results.items():
        print(f"  IC({horizon}): {result['ic']:.4f} (n={result['n']})")
    
    # 计算 IC 衰减
    if '1h' in ic_results and '24h' in ic_results:
        decay = ic_results['24h']['ic'] / ic_results['1h']['ic'] if ic_results['1h']['ic'] != 0 else 0
        print(f"  IC衰减(1h→24h): {decay:.2%}")
    
    # 按时间段分析
    print("\n📅 按时间段分析:")
    df['date'] = pd.to_datetime(df['ts'], unit='s').dt.date
    
    daily_ic = df.groupby('date').apply(
        lambda x: x['score'].corr(x['return_1h']) if len(x) > 5 else np.nan
    )
    
    if not daily_ic.isna().all():
        print(f"  日均IC: {daily_ic.mean():.4f}")
        print(f"  IC标准差: {daily_ic.std():.4f}")
        print(f"  IC_IR: {daily_ic.mean()/max(daily_ic.std(), 0.001):.2f}")
        print(f"  IC正比例: {(daily_ic > 0).mean():.1%}")
    
    return ic_results


def main():
    check_data_status()
    ic_results = calculate_basic_ic()
    
    print("\n" + "=" * 50)
    print("🎯 数据收集总结")
    print("=" * 50)
    
    # 评估数据质量
    db_path = "reports/alpha_history.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM alpha_snapshots WHERE score IS NOT NULL")
    score_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM forward_returns WHERE return_1h IS NOT NULL")
    returns_count = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"✅ Alpha scores: {score_count} 条")
    print(f"✅ Forward returns: {returns_count} 条")
    
    if score_count > 100 and returns_count > 100:
        print("✅ 数据量足够进行统计分析")
        
        if ic_results and '1h' in ic_results:
            ic_1h = ic_results['1h']['ic']
            if ic_1h > 0.02:
                print(f"✅ IC(1h) = {ic_1h:.4f} > 0.02，信号有效")
            else:
                print(f"⚠️  IC(1h) = {ic_1h:.4f}，信号较弱")
    else:
        print("⚠️  数据量不足，需要更多运行")
    
    print("\n📋 建议:")
    print("1. 继续运行 V5 收集更多 alpha snapshot")
    print("2. 定期运行 simple_data_collect.py 更新市场数据")
    print("3. 运行完整 evaluate_alpha_historical.py 进行深入分析")
    print("=" * 50)


if __name__ == "__main__":
    main()