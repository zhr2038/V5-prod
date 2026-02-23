#!/usr/bin/env python3
"""
分析市场数据特征
了解为什么策略不产生交易
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sys

def analyze_market_characteristics():
    """分析市场数据特征"""
    
    print("📊 分析市场数据特征")
    print("=" * 60)
    
    db_path = Path("reports/alpha_history.db")
    if not db_path.exists():
        print("❌ 数据库不存在")
        return
    
    conn = sqlite3.connect(str(db_path))
    
    # 获取主要币种
    symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT']
    
    for symbol in symbols:
        print(f"\n🎯 分析 {symbol}:")
        
        # 加载数据
        query = f"""
        SELECT timestamp, open, high, low, close, volume 
        FROM market_data_1h 
        WHERE symbol = ? 
        ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol,))
        
        if len(df) < 100:
            print(f"  ⚠️ 数据不足: {len(df)}行")
            continue
        
        # 计算收益率
        df['returns'] = df['close'].pct_change()
        df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
        
        # 计算统计特征
        stats = {
            "数据行数": len(df),
            "时间范围": f"{datetime.fromtimestamp(df['timestamp'].min())} 到 {datetime.fromtimestamp(df['timestamp'].max())}",
            "天数": (datetime.fromtimestamp(df['timestamp'].max()) - datetime.fromtimestamp(df['timestamp'].min())).days,
            "平均价格": df['close'].mean(),
            "价格标准差": df['close'].std(),
            "平均收益率": df['returns'].mean() * 100,
            "收益率标准差": df['returns'].std() * 100,
            "夏普比率(小时)": df['returns'].mean() / df['returns'].std() if df['returns'].std() > 0 else 0,
            "最大单小时涨幅": df['returns'].max() * 100,
            "最大单小时跌幅": df['returns'].min() * 100,
            "正收益比例": (df['returns'] > 0).mean() * 100,
            "平均交易量": df['volume'].mean(),
        }
        
        # 趋势分析
        price_change = (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0] * 100
        stats["总价格变化"] = price_change
        
        # 波动率分析
        hourly_vol = df['returns'].std() * 100
        daily_vol = hourly_vol * np.sqrt(24)
        stats["小时波动率"] = hourly_vol
        stats["日波动率"] = daily_vol
        
        # 动量特征 (F2相关)
        # 20日动量 (20*24=480小时)
        if len(df) >= 480:
            mom_20d = (df['close'].iloc[-1] - df['close'].iloc[-480]) / df['close'].iloc[-480] * 100
            stats["20日动量"] = mom_20d
        
        # 5日动量 (5*24=120小时)
        if len(df) >= 120:
            mom_5d = (df['close'].iloc[-1] - df['close'].iloc[-120]) / df['close'].iloc[-120] * 100
            stats["5日动量"] = mom_5d
        
        # 显示关键统计
        print(f"  📈 价格变化: {stats['总价格变化']:.2f}%")
        print(f"  📊 平均收益率: {stats['平均收益率']:.4f}%/小时")
        print(f"  📉 波动率: {stats['小时波动率']:.2f}%/小时 ({stats['日波动率']:.2f}%/天)")
        print(f"  📋 正收益比例: {stats['正收益比例']:.1f}%")
        
        if '20日动量' in stats:
            print(f"  🎯 20日动量: {stats['20日动量']:.2f}%")
        if '5日动量' in stats:
            print(f"  🎯 5日动量: {stats['5日动量']:.2f}%")
        
        # 评估策略适用性
        print(f"  🔍 策略适用性评估:")
        
        # F2动量因子需要明显的趋势
        if abs(stats.get('20日动量', 0)) > 5:
            print(f"    ✅ 有明显20日趋势(>{abs(stats['20日动量']):.1f}%)，适合F2因子")
        else:
            print(f"    ⚠️ 20日趋势不明显({stats.get('20日动量', 0):.1f}%)，F2因子可能无效")
        
        # 波动率评估
        if stats['日波动率'] > 2.0:
            print(f"    ✅ 波动率较高({stats['日波动率']:.1f}%)，有机会交易")
        else:
            print(f"    ⚠️ 波动率较低({stats['日波动率']:.1f}%)，交易机会有限")
    
    conn.close()

def compare_with_strategy_requirements():
    """比较市场特征与策略要求"""
    
    print("\n" + "=" * 60)
    print("🎯 市场特征 vs 策略要求")
    print("=" * 60)
    
    print("📋 当前策略要求 (趋势/动量策略):")
    print("  1. 明显的价格趋势 (20日动量 > 5%)")
    print("  2. 足够的波动率 (日波动率 > 2%)")
    print("  3. 持续的趋势方向")
    print("  4. 足够的交易量")
    
    print("\n📊 基于分析的发现:")
    print("  如果市场处于低波动横盘期:")
    print("  - F2动量因子可能无效")
    print("  - 趋势策略难以盈利")
    print("  - 需要调整策略或参数")
    
    print("\n💡 调整建议:")
    print("  1. 考虑均值回归策略")
    print("  2. 降低deadband进一步")
    print("  3. 增加短期因子权重")
    print("  4. 测试不同时间框架")

def generate_recommendations():
    """生成建议"""
    
    print("\n" + "=" * 60)
    print("🚀 基于市场分析的优化建议")
    print("=" * 60)
    
    print("🎯 立即行动:")
    print("  1. 创建测试配置验证市场状态假设")
    print("  2. 运行参数扫描找到有效参数")
    print("  3. 考虑策略适应性调整")
    
    print("\n🔧 具体配置调整建议:")
    print("  A. 更激进的参数:")
    print("     deadband_sideways: 0.02")
    print("     long_top_pct: 0.40")
    print("     pos_mult_risk_off: 0.8")
    
    print("\n  B. 策略逻辑调整:")
    print("     增加短期动量权重(f1_mom_5d)")
    print("     降低长期动量权重(f2_mom_20d)")
    print("     添加均值回归因子")
    
    print("\n  C. 市场状态适应:")
    print("     根据波动率调整仓位")
    print("     根据趋势强度调整参数")
    print("     多策略并行")
    
    print("\n📊 验证计划:")
    print("  1. 创建多个测试配置")
    print("  2. 运行快速回测对比")
    print("  3. 选择最佳参数组合")
    print("  4. 监控实际表现")

def main():
    """主函数"""
    
    print("🚀 市场特征分析 - 诊断无交易问题")
    print("=" * 60)
    
    # 分析市场特征
    analyze_market_characteristics()
    
    # 比较策略要求
    compare_with_strategy_requirements()
    
    # 生成建议
    generate_recommendations()
    
    print("\n✅ 分析完成")
    print("=" * 60)

if __name__ == "__main__":
    main()