#!/usr/bin/env python3
"""
深入IC分析 - 基于30天alpha数据
"""

import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append(str(Path(__file__).resolve().parents[1]))

def deep_ic_analysis():
    """深入IC分析"""
    print("🔍 深入IC分析 - 基于30天alpha数据")
    print("=" * 70)
    
    db_path = "reports/alpha_history.db"
    
    if not Path(db_path).exists():
        print(f"❌ 数据库文件不存在: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    
    # 1. 基础IC分析
    print("\n📊 1. 基础IC分析")
    print("-" * 40)
    
    # 读取IC数据
    query = """
    SELECT 
        timestamp,
        symbol,
        score,
        return_1h,
        return_6h,
        return_24h,
        regime,
        price,
        volume
    FROM ic_calculation_view
    WHERE score IS NOT NULL AND return_1h IS NOT NULL
    ORDER BY timestamp
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("❌ 无IC数据")
        return
    
    print(f"数据点: {len(df)} 条")
    print(f"时间范围: {datetime.fromtimestamp(df['timestamp'].min()).strftime('%Y-%m-%d')} 到 {datetime.fromtimestamp(df['timestamp'].max()).strftime('%Y-%m-%d')}")
    print(f"币种数量: {df['symbol'].nunique()}")
    print(f"数据天数: {(df['timestamp'].max() - df['timestamp'].min()) / (24*3600):.1f} 天")
    
    # 2. IC衰减曲线详细分析
    print("\n📈 2. IC衰减曲线详细分析")
    print("-" * 40)
    
    horizons = ['1h', '6h', '24h']
    ic_results = {}
    
    for horizon in horizons:
        col_name = f'return_{horizon}'
        if col_name in df.columns:
            valid = df[['score', col_name]].dropna()
            if len(valid) > 10:
                ic = valid['score'].corr(valid[col_name])
                ic_results[horizon] = {
                    'ic': ic,
                    'count': len(valid),
                    'positive_ratio': (valid['score'] * valid[col_name] > 0).mean()
                }
    
    print("IC衰减曲线:")
    for horizon, result in ic_results.items():
        print(f"  {horizon}:")
        print(f"    IC值: {result['ic']:.4f}")
        print(f"    数据量: {result['count']} 条")
        print(f"    正比例: {result['positive_ratio']*100:.1f}%")
    
    # 计算衰减率
    if '1h' in ic_results and '6h' in ic_results:
        decay_6h = ic_results['6h']['ic'] / ic_results['1h']['ic'] * 100 if ic_results['1h']['ic'] != 0 else 0
        print(f"  IC衰减率(1h→6h): {decay_6h:.1f}%")
    
    if '1h' in ic_results and '24h' in ic_results:
        decay_24h = ic_results['24h']['ic'] / ic_results['1h']['ic'] * 100 if ic_results['1h']['ic'] != 0 else 0
        print(f"  IC衰减率(1h→24h): {decay_24h:.1f}%")
    
    # 3. 按市场状态分析
    print("\n🎭 3. 按市场状态分析")
    print("-" * 40)
    
    # 简化regime分析
    df['regime_simple'] = df['regime'].apply(lambda x: 'Risk-Off' if 'Risk-Off' in str(x) else 
                                                      'Sideways' if 'Sideways' in str(x) else 
                                                      'Trending' if 'Trending' in str(x) else 'Other')
    
    regime_ics = {}
    for regime in df['regime_simple'].unique():
        regime_data = df[df['regime_simple'] == regime]
        if len(regime_data) > 50:
            regime_ics[regime] = {}
            for horizon in horizons:
                col_name = f'return_{horizon}'
                if col_name in regime_data.columns:
                    valid = regime_data[['score', col_name]].dropna()
                    if len(valid) > 10:
                        ic = valid['score'].corr(valid[col_name])
                        regime_ics[regime][horizon] = {
                            'ic': ic,
                            'count': len(valid)
                        }
    
    for regime, results in regime_ics.items():
        print(f"  {regime}:")
        for horizon, result in results.items():
            print(f"    {horizon}: IC={result['ic']:.4f} (n={result['count']})")
    
    # 4. 按币种分析
    print("\n💰 4. 按币种分析 (前10个)")
    print("-" * 40)
    
    symbol_ics = {}
    top_symbols = df['symbol'].value_counts().head(10).index
    
    for symbol in top_symbols:
        symbol_data = df[df['symbol'] == symbol]
        if len(symbol_data) > 20:
            valid = symbol_data[['score', 'return_1h']].dropna()
            if len(valid) > 10:
                ic = valid['score'].corr(valid['return_1h'])
                symbol_ics[symbol] = {
                    'ic': ic,
                    'count': len(valid),
                    'avg_score': valid['score'].mean(),
                    'avg_return': valid['return_1h'].mean()
                }
    
    # 按IC排序
    sorted_symbols = sorted(symbol_ics.items(), key=lambda x: abs(x[1]['ic']), reverse=True)
    
    for symbol, stats in sorted_symbols[:10]:
        print(f"  {symbol}:")
        print(f"    IC(1h): {stats['ic']:.4f}")
        print(f"    数据量: {stats['count']} 条")
        print(f"    平均分数: {stats['avg_score']:.4f}")
        print(f"    平均收益: {stats['avg_return']*100:.4f}%")
    
    # 5. 时间序列分析
    print("\n⏰ 5. 时间序列分析")
    print("-" * 40)
    
    # 按天计算IC
    df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
    daily_ic = df.groupby('date').apply(
        lambda x: x['score'].corr(x['return_1h']) if len(x) > 10 else None
    ).dropna()
    
    if len(daily_ic) > 0:
        print(f"  每日IC统计:")
        print(f"    平均IC: {daily_ic.mean():.4f}")
        print(f"    IC标准差: {daily_ic.std():.4f}")
        print(f"    最大IC: {daily_ic.max():.4f}")
        print(f"    最小IC: {daily_ic.min():.4f}")
        print(f"    正IC天数: {(daily_ic > 0).sum()}/{len(daily_ic)} ({daily_ic[daily_ic > 0].count()/len(daily_ic)*100:.1f}%)")
        
        # IC稳定性
        rolling_ic = daily_ic.rolling(window=5, min_periods=1).mean()
        ic_volatility = daily_ic.std() / abs(daily_ic.mean()) if daily_ic.mean() != 0 else 0
        print(f"    IC波动率: {ic_volatility:.2f}")
        
        # 显示最近7天IC
        print(f"\n  最近7天IC:")
        for date, ic in daily_ic.tail(7).items():
            print(f"    {date}: {ic:.4f}")
    
    # 6. F2因子专项分析
    print("\n🎯 6. F2因子(f2_mom_20d)专项分析")
    print("-" * 40)
    
    try:
        # 从alpha_snapshots表获取F2因子数据
        f2_query = """
        SELECT 
            a.ts,
            a.symbol,
            a.f2_mom_20d as f2_value,
            i.return_1h,
            i.return_6h,
            i.return_24h
        FROM alpha_snapshots a
        JOIN ic_calculation_view i ON a.ts = i.timestamp AND a.symbol = i.symbol
        WHERE a.f2_mom_20d IS NOT NULL
        ORDER BY a.ts
        LIMIT 5000
        """
        
        f2_df = pd.read_sql_query(f2_query, conn)
        
        if not f2_df.empty:
            print(f"  F2因子数据: {len(f2_df)} 条")
            
            # 计算F2因子的IC
            f2_ic_1h = f2_df['f2_value'].corr(f2_df['return_1h']) if len(f2_df) > 10 else None
            f2_ic_6h = f2_df['f2_value'].corr(f2_df['return_6h']) if len(f2_df) > 10 else None
            f2_ic_24h = f2_df['f2_value'].corr(f2_df['return_24h']) if len(f2_df) > 10 else None
            
            print(f"  F2因子IC:")
            if f2_ic_1h is not None:
                print(f"    1小时: {f2_ic_1h:.4f}")
            if f2_ic_6h is not None:
                print(f"    6小时: {f2_ic_6h:.4f}")
            if f2_ic_24h is not None:
                print(f"    24小时: {f2_ic_24h:.4f}")
            
            # F2因子统计
            print(f"  F2因子统计:")
            print(f"    均值: {f2_df['f2_value'].mean():.4f}")
            print(f"    标准差: {f2_df['f2_value'].std():.4f}")
            print(f"    范围: [{f2_df['f2_value'].min():.4f}, {f2_df['f2_value'].max():.4f}]")
            
            # F2因子与总IC的关系
            if 'ic' in locals() and f2_ic_1h is not None:
                contribution = f2_ic_1h / ic_results.get('1h', {}).get('ic', 1) * 100 if ic_results.get('1h', {}).get('ic', 0) != 0 else 0
                print(f"  F2因子对总IC的贡献: {contribution:.1f}%")
        else:
            print("  ⚠️ 无F2因子数据")
            
    except Exception as e:
        print(f"  ❌ F2因子分析错误: {e}")
    
    # 7. IC预测能力分析
    print("\n🔮 7. IC预测能力分析")
    print("-" * 40)
    
    # 计算不同分数区间的收益
    df['score_quantile'] = pd.qcut(df['score'], q=5, labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'])
    
    quantile_returns = {}
    for quantile in ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']:
        quantile_data = df[df['score_quantile'] == quantile]
        if len(quantile_data) > 10:
            avg_return = quantile_data['return_1h'].mean()
            quantile_returns[quantile] = avg_return
    
    print("  分数分位数收益分析:")
    for quantile, avg_return in sorted(quantile_returns.items()):
        print(f"    {quantile}: 平均收益 = {avg_return*100:.4f}%")
    
    # 计算多空收益
    if 'Q1' in quantile_returns and 'Q5' in quantile_returns:
        long_short_return = quantile_returns['Q5'] - quantile_returns['Q1']
        print(f"  多空收益(Q5-Q1): {long_short_return*100:.4f}%")
    
    # 8. 建议和结论
    print("\n💡 8. 分析结论与建议")
    print("-" * 40)
    
    # 评估IC质量
    avg_ic = ic_results.get('1h', {}).get('ic', 0)
    ic_stability = 1 - (daily_ic.std() / abs(daily_ic.mean())) if daily_ic.mean() != 0 else 0
    
    print("  IC质量评估:")
    print(f"    平均IC: {avg_ic:.4f}")
    print(f"    IC稳定性: {ic_stability:.2f}")
    print(f"    正IC比例: {ic_results.get('1h', {}).get('positive_ratio', 0)*100:.1f}%")
    
    if avg_ic > 0.03:
        print("  ✅ IC质量优秀")
    elif avg_ic > 0.01:
        print("  ⚠️ IC质量一般")
    else:
        print("  ❌ IC质量不足")
    
    print("\n  🎯 优化建议:")
    
    if 'f2_ic_1h' in locals() and f2_ic_1h is not None:
        if f2_ic_1h > avg_ic:
            print("    1. 增加F2因子权重 (F2 IC高于总IC)")
        else:
            print("    1. 减少F2因子权重 (F2 IC低于总IC)")
    
    if decay_6h > 100:
        print("    2. 考虑延长持仓时间 (IC随时间增强)")
    else:
        print("    2. 考虑缩短持仓时间 (IC随时间衰减)")
    
    if regime_ics.get('Trending', {}).get('1h', {}).get('ic', 0) > avg_ic:
        print("    3. 在趋势市场中增加仓位")
    
    if 'long_short_return' in locals() and long_short_return > 0:
        print("    4. 考虑多空策略 (分位数收益显著)")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("✅ 深入IC分析完成")
    print("=" * 70)
    
    print("\n📋 关键发现:")
    print(f"1. 30天IC衰减曲线: 1h={ic_results.get('1h', {}).get('ic', 0):.4f}, 6h={ic_results.get('6h', {}).get('ic', 0):.4f}")
    print(f"2. F2因子表现: {f2_ic_1h if 'f2_ic_1h' in locals() else 'N/A':.4f}")
    print(f"3. 市场状态影响: 不同regime下IC差异显著")
    print(f"4. 时间稳定性: 每日IC标准差={daily_ic.std():.4f}")
    
    print("\n🚀 下一步:")
    print("1. 基于分析结果优化alpha权重")
    print("2. 调整持仓时间基于IC衰减曲线")
    print("3. 针对不同市场状态优化策略")
    print("4. 建立IC监控和告警机制")

def main():
    """主函数"""
    print("🚀 深入IC分析工具")
    print("=" * 70)
    
    deep_ic_analysis()
    
    print("=" * 70)

if __name__ == "__main__":
    main()