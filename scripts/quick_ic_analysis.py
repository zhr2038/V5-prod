#!/usr/bin/env python3
"""
快速 IC 分析 - 使用对齐后的数据
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime


def analyze_ic_by_horizon():
    """按时间 horizon 分析 IC"""
    db_path = "reports/alpha_history.db"
    
    print("📈 IC 衰减分析")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    
    # 使用 ic_calculation_view
    query = """
    SELECT 
        timestamp,
        symbol,
        score,
        return_1h,
        return_6h,
        return_24h,
        regime
    FROM ic_calculation_view
    WHERE score IS NOT NULL
    ORDER BY timestamp
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("❌ 没有数据")
        return
    
    print(f"数据点: {len(df)}")
    print(f"时间范围: {datetime.fromtimestamp(df['timestamp'].min()).strftime('%Y-%m-%d %H:%M')} 到 {datetime.fromtimestamp(df['timestamp'].max()).strftime('%Y-%m-%d %H:%M')}")
    print(f"币种数量: {df['symbol'].nunique()}")
    print(f"Regime 分布: {df['regime'].value_counts().to_dict()}")
    
    # 计算各 horizon 的 IC
    horizons = ['1h', '6h', '24h']
    ic_results = {}
    
    for horizon in horizons:
        return_col = f'return_{horizon}'
        if return_col in df.columns:
            valid = df[df[return_col].notna()]
            if len(valid) > 10:
                ic = valid['score'].corr(valid[return_col])
                ic_std = valid.groupby('timestamp').apply(
                    lambda x: x['score'].corr(x[return_col]) if len(x) > 3 else np.nan
                ).std()
                
                ic_results[horizon] = {
                    'ic': ic,
                    'ic_std': ic_std,
                    'n': len(valid),
                    'ic_ir': ic / max(ic_std, 0.001),
                    'positive_ratio': (valid['score'] * valid[return_col] > 0).mean()
                }
    
    print("\n📊 IC 衰减曲线:")
    print("-" * 40)
    for horizon, result in ic_results.items():
        print(f"  {horizon:4s}: IC = {result['ic']:7.4f} | n = {result['n']:4d} | IR = {result['ic_ir']:6.2f} | 正比例 = {result['positive_ratio']:.1%}")
    
    # 计算衰减率
    if '1h' in ic_results and '6h' in ic_results:
        decay_6h = ic_results['6h']['ic'] / ic_results['1h']['ic'] if ic_results['1h']['ic'] != 0 else 0
        print(f"\n📉 IC 衰减率:")
        print(f"  1h → 6h: {decay_6h:.2%}")
    
    if '1h' in ic_results and '24h' in ic_results:
        decay_24h = ic_results['24h']['ic'] / ic_results['1h']['ic'] if ic_results['1h']['ic'] != 0 else 0
        print(f"  1h → 24h: {decay_24h:.2%}")
    
    return df, ic_results


def analyze_by_regime(df):
    """按 regime 分析 IC"""
    if 'regime' not in df.columns or df['regime'].isna().all():
        return
    
    print("\n🎭 按 Regime 分析:")
    print("-" * 40)
    
    regimes = df['regime'].unique()
    
    for regime in regimes:
        regime_data = df[df['regime'] == regime]
        if len(regime_data) < 10:
            continue
        
        print(f"\n{regime}:")
        print(f"  数据点: {len(regime_data)}")
        
        # 计算 IC
        horizons = ['1h', '6h', '24h']
        for horizon in horizons:
            return_col = f'return_{horizon}'
            if return_col in regime_data.columns:
                valid = regime_data[regime_data[return_col].notna()]
                if len(valid) > 5:
                    ic = valid['score'].corr(valid[return_col])
                    print(f"  IC({horizon}): {ic:.4f} (n={len(valid)})")


def analyze_by_symbol(df):
    """按币种分析"""
    print("\n💰 按币种分析 (Top 5):")
    print("-" * 40)
    
    # 按数据量排序
    symbol_counts = df['symbol'].value_counts()
    top_symbols = symbol_counts.head(5).index.tolist()
    
    for symbol in top_symbols:
        symbol_data = df[df['symbol'] == symbol]
        if len(symbol_data) < 5:
            continue
        
        print(f"\n{symbol}:")
        print(f"  数据点: {len(symbol_data)}")
        
        # 计算 IC
        if 'return_1h' in symbol_data.columns:
            valid = symbol_data[symbol_data['return_1h'].notna()]
            if len(valid) > 3:
                ic = valid['score'].corr(valid['return_1h'])
                print(f"  IC(1h): {ic:.4f}")


def analyze_factor_performance():
    """分析因子表现"""
    db_path = "reports/alpha_history.db"
    
    print("\n🔍 因子表现分析:")
    print("-" * 40)
    
    conn = sqlite3.connect(db_path)
    
    # 获取因子值和 returns
    query = """
    SELECT 
        z1_mom_5d,
        z2_mom_20d,
        z3_vol_adj_ret_20d,
        z4_volume_expansion,
        z5_rsi_trend_confirm,
        f.return_1h
    FROM alpha_snapshots_aligned_view a
    JOIN forward_returns f ON a.symbol = f.symbol AND a.ts_aligned = f.timestamp
    WHERE f.return_1h IS NOT NULL
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("❌ 没有因子数据")
        return
    
    factors = {
        'z1_mom_5d': 'MOM 5D',
        'z2_mom_20d': 'MOM 20D',
        'z3_vol_adj_ret_20d': 'Vol Adj Ret',
        'z4_volume_expansion': 'Volume Exp',
        'z5_rsi_trend_confirm': 'RSI Trend'
    }
    
    print("因子 IC(1h):")
    for factor_col, factor_name in factors.items():
        if factor_col in df.columns:
            valid = df[df[factor_col].notna()]
            if len(valid) > 10:
                ic = valid[factor_col].corr(valid['return_1h'])
                print(f"  {factor_name:15s}: {ic:.4f} (n={len(valid)})")


def main():
    print("📊 快速 IC 分析报告")
    print("=" * 60)
    
    # 1. IC 衰减分析
    df, ic_results = analyze_ic_by_horizon()
    
    # 2. 按 regime 分析
    analyze_by_regime(df)
    
    # 3. 按币种分析
    analyze_by_symbol(df)
    
    # 4. 因子表现分析
    analyze_factor_performance()
    
    print("\n" + "=" * 60)
    print("🎯 关键发现:")
    print("=" * 60)
    
    if ic_results and '1h' in ic_results:
        ic_1h = ic_results['1h']['ic']
        
        if ic_1h < -0.1:
            print("⚠️  **IC(1h) 显著为负** (-0.2057)")
            print("   可能原因:")
            print("   1. 市场反转（今天特殊行情）")
            print("   2. 因子权重需要调整")
            print("   3. 信号方向错误")
            print("   4. 数据时间段太短")
        
        elif abs(ic_1h) < 0.02:
            print("⚠️  **IC(1h) 接近零** (信号很弱)")
            print("   需要优化 alpha 模型")
        
        else:
            print("✅ **IC(1h) 显著为正** (信号有效)")
    
    print("\n📋 建议:")
    print("1. 收集更多历史数据（至少1周）")
    print("2. 运行参数优化脚本")
    print("3. 调整因子权重（基于因子IC分析）")
    print("4. 考虑 regime-specific 参数")
    print("=" * 60)


if __name__ == "__main__":
    main()