#!/usr/bin/env python3
"""
因子IC分析脚本 - 分析V5交易机器人因子的信息系数

IC (Information Coefficient): 因子值与未来收益的相关性
- IC > 0: 因子与未来收益正相关 (好因子)
- IC < 0: 因子与未来收益负相关 (反向因子)
- |IC| > 0.03: 一般认为有效
- |IC| > 0.05: 较好因子
- |IC| > 0.10: 优秀因子

IR (Information Ratio): IC的稳定性
- IR = mean(IC) / std(IC)
- IR > 0.5: 因子稳定有效
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime

DB_PATH = Path('/home/admin/clawd/v5-trading-bot/reports/alpha_history.db')
OUTPUT_PATH = Path('/home/admin/clawd/v5-trading-bot/reports/ic_analysis.json')

# 因子列表
FACTORS = ['f1_mom_5d', 'f2_mom_20d', 'f3_vol_adj_ret_20d', 'f4_volume_expansion', 'f5_rsi_trend_confirm']


def load_alpha_data():
    """从数据库加载alpha历史数据"""
    if not DB_PATH.exists():
        print(f"❌ 数据库不存在: {DB_PATH}")
        return None
    
    conn = sqlite3.connect(str(DB_PATH))
    
    # 加载alpha_snapshots
    df = pd.read_sql_query("""
        SELECT ts, symbol, f1_mom_5d, f2_mom_20d, f3_vol_adj_ret_20d, 
               f4_volume_expansion, f5_rsi_trend_confirm, score
        FROM alpha_snapshots
        ORDER BY ts, symbol
    """, conn)
    
    conn.close()
    
    # 转换时间戳
    df['datetime'] = pd.to_datetime(df['ts'], unit='s')
    
    return df


def calculate_ic(df, factor_col, forward_period=6):
    """
    计算因子的IC值
    
    Args:
        df: DataFrame with factor values
        factor_col: 因子列名
        forward_period: 前瞻期数 (默认6小时)
    """
    # 按币种分组计算未来收益
    df = df.sort_values(['symbol', 'ts'])
    
    # 计算未来收益 (需要价格数据，这里用score变化近似)
    # 实际应该用未来价格，但alpha_history只有score
    # 暂时用score变化作为收益的代理
    df['future_return'] = df.groupby('symbol')['score'].shift(-forward_period) - df['score']
    
    # 计算IC: 因子值与未来收益的相关系数
    ic_values = []
    
    for ts, group in df.groupby('ts'):
        if len(group) < 5:  # 样本太少跳过
            continue
        
        factor_vals = group[factor_col].values
        returns = group['future_return'].values
        
        # 剔除NaN
        mask = ~np.isnan(factor_vals) & ~np.isnan(returns)
        if mask.sum() < 5:
            continue
        
        # 计算Spearman秩相关系数 (更稳健)
        ic = np.corrcoef(
            pd.Series(factor_vals[mask]).rank(),
            pd.Series(returns[mask]).rank()
        )[0, 1]
        
        if not np.isnan(ic):
            ic_values.append(ic)
    
    return ic_values


def analyze_factor(df, factor_col):
    """分析单个因子的IC表现"""
    ic_values = calculate_ic(df, factor_col)
    
    if not ic_values:
        return None
    
    ic_series = pd.Series(ic_values)
    
    return {
        'factor': factor_col,
        'ic_mean': float(ic_series.mean()),
        'ic_std': float(ic_series.std()),
        'ir': float(ic_series.mean() / ic_series.std()) if ic_series.std() > 0 else 0,
        'ic_>0_pct': float((ic_series > 0).mean() * 100),
        '|ic|>0.03_pct': float((ic_series.abs() > 0.03).mean() * 100),
        'sample_count': len(ic_values),
        'ic_series': [float(x) for x in ic_values[-20:]]  # 最近20个值
    }


def main():
    print("=" * 60)
    print("V5 因子IC分析")
    print("=" * 60)
    
    # 加载数据
    print("\n1. 加载alpha历史数据...")
    df = load_alpha_data()
    if df is None or df.empty:
        print("❌ 无数据可分析")
        return
    
    print(f"   加载了 {len(df)} 条记录")
    print(f"   时间范围: {df['datetime'].min()} 至 {df['datetime'].max()}")
    print(f"   币种数量: {df['symbol'].nunique()}")
    
    # 分析各因子
    print("\n2. 计算因子IC值...")
    results = []
    
    for factor in FACTORS:
        print(f"   分析 {factor}...", end=" ")
        result = analyze_factor(df, factor)
        if result:
            results.append(result)
            print(f"✅ IC={result['ic_mean']:.4f}, IR={result['ir']:.2f}")
        else:
            print("❌ 数据不足")
    
    # 生成报告
    print("\n3. 生成分析报告...")
    report = {
        'timestamp': datetime.now().isoformat(),
        'data_summary': {
            'total_records': len(df),
            'symbols': int(df['symbol'].nunique()),
            'time_range': {
                'start': str(df['datetime'].min()),
                'end': str(df['datetime'].max())
            }
        },
        'factor_analysis': results,
        'recommendations': []
    }
    
    # 生成建议
    for r in results:
        if r['ir'] > 0.5 and abs(r['ic_mean']) > 0.03:
            report['recommendations'].append({
                'factor': r['factor'],
                'action': 'increase_weight',
                'reason': f"IC={r['ic_mean']:.4f}, IR={r['ir']:.2f}, 表现优秀"
            })
        elif r['ir'] < 0.2 or abs(r['ic_mean']) < 0.01:
            report['recommendations'].append({
                'factor': r['factor'],
                'action': 'decrease_weight_or_remove',
                'reason': f"IC={r['ic_mean']:.4f}, IR={r['ir']:.2f}, 表现不佳"
            })
        else:
            report['recommendations'].append({
                'factor': r['factor'],
                'action': 'keep',
                'reason': f"IC={r['ic_mean']:.4f}, IR={r['ir']:.2f}, 表现一般"
            })
    
    # 保存报告
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"   报告已保存: {OUTPUT_PATH}")
    
    # 打印摘要
    print("\n" + "=" * 60)
    print("分析摘要")
    print("=" * 60)
    for r in results:
        status = "🟢" if r['ir'] > 0.5 else "🟡" if r['ir'] > 0.2 else "🔴"
        print(f"{status} {r['factor']:20s}: IC={r['ic_mean']:+.4f}, IR={r['ir']:.2f}, |IC|>0.03={r['|ic|>0.03_pct']:.0f}%")
    
    print("\n" + "=" * 60)
    print("权重调整建议")
    print("=" * 60)
    for rec in report['recommendations']:
        action_emoji = "📈" if rec['action'] == 'increase_weight' else "📉" if 'decrease' in rec['action'] else "➡️"
        print(f"{action_emoji} {rec['factor']}: {rec['action']}")
        print(f"   原因: {rec['reason']}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
