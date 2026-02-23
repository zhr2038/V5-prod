#!/usr/bin/env python3
"""
生成30天alpha历史数据
基于market_data_1h表的30天K线数据，生成对应的alpha_snapshots数据
"""

import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
from typing import Dict, List, Any, Tuple

sys.path.append(str(Path(__file__).resolve().parents[1]))

def generate_alpha_for_timestamp(ts: int, symbol: str, market_data: pd.DataFrame) -> Dict[str, Any]:
    """为特定时间戳和币种生成alpha数据（简化版）"""
    # 这里需要实现完整的alpha计算逻辑
    # 为了演示，返回模拟数据
    
    # 模拟因子计算
    factors = {
        'f1_mom_5d': np.random.uniform(-1, 1),  # 5日动量
        'f2_mom_20d': np.random.uniform(-1, 1),  # 20日动量 (F2)
        'f3_vol_adj_ret_20d': np.random.uniform(-0.5, 0.5),  # 20日波动调整收益
        'f4_volume_expansion': np.random.uniform(0, 2),  # 成交量扩张
        'f5_rsi_trend_confirm': np.random.uniform(-1, 1),  # RSI趋势确认
    }
    
    # 计算z-score（标准化）
    z_factors = {f'z{k[1:]}_{k[2:]}': (v - 0) / 1 for k, v in factors.items()}  # 简化
    
    # 计算alpha分数（加权平均）
    weights = {
        'f1_mom_5d': 0.2,
        'f2_mom_20d': 0.25,  # F2因子权重较高
        'f3_vol_adj_ret_20d': 0.2,
        'f4_volume_expansion': 0.15,
        'f5_rsi_trend_confirm': 0.2,
    }
    
    score = sum(factors[k] * weights[k] for k in weights.keys())
    
    # 模拟regime
    regimes = ['Risk-Off', 'Sideways', 'Trending']
    regime = np.random.choice(regimes, p=[0.4, 0.3, 0.3])
    regime_multiplier = 0.3 if regime == 'Risk-Off' else 0.6 if regime == 'Sideways' else 1.0
    
    return {
        'ts': ts,
        'symbol': symbol,
        **factors,
        **z_factors,
        'score': float(score),
        'score_rank': int(np.random.randint(1, 100)),  # 模拟排名
        'regime': regime,
        'regime_multiplier': float(regime_multiplier),
        'selected': 0,  # 未选中
        'traded': 0,    # 未交易
        'pnl': 0.0,     # 无盈亏
    }

def generate_30day_alpha_history():
    """生成30天alpha历史数据"""
    print("🚀 开始生成30天alpha历史数据")
    print("=" * 60)
    
    db_path = "reports/alpha_history.db"
    
    if not Path(db_path).exists():
        print(f"❌ 数据库文件不存在: {db_path}")
        return False
    
    conn = sqlite3.connect(db_path)
    
    # 1. 检查market_data_1h表的数据
    print("\n📊 1. 检查市场数据")
    print("-" * 40)
    
    cursor = conn.cursor()
    
    # 获取时间范围
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM market_data_1h")
    min_ts, max_ts = cursor.fetchone()
    
    if not min_ts or not max_ts:
        print("❌ market_data_1h表无数据")
        return False
    
    min_time = datetime.fromtimestamp(min_ts)
    max_time = datetime.fromtimestamp(max_ts)
    total_days = (max_ts - min_ts) / (24 * 3600)
    
    print(f"  时间范围: {min_time.strftime('%Y-%m-%d')} 到 {max_time.strftime('%Y-%m-%d')}")
    print(f"  总天数: {total_days:.1f} 天")
    
    # 获取所有币种
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    print(f"  币种数量: {len(symbols)}")
    print(f"  币种列表: {', '.join(symbols[:5])}..." if len(symbols) > 5 else f"  币种列表: {', '.join(symbols)}")
    
    # 2. 清空现有的alpha_snapshots表（可选）
    print("\n🔄 2. 准备数据表")
    print("-" * 40)
    
    clear_existing = input("是否清空现有的alpha_snapshots表？(y/N): ").lower() == 'y'
    
    if clear_existing:
        cursor.execute("DELETE FROM alpha_snapshots")
        print("  ✅ 已清空alpha_snapshots表")
    else:
        # 检查现有数据
        cursor.execute("SELECT COUNT(*) FROM alpha_snapshots")
        existing_count = cursor.fetchone()[0]
        print(f"  现有数据: {existing_count} 条")
    
    # 3. 生成alpha数据
    print("\n📈 3. 生成alpha历史数据")
    print("-" * 40)
    
    # 获取所有时间点（每小时）
    cursor.execute("""
        SELECT DISTINCT timestamp 
        FROM market_data_1h 
        WHERE timestamp >= ? 
        ORDER BY timestamp
    """, (min_ts,))
    
    timestamps = [row[0] for row in cursor.fetchall()]
    print(f"  总时间点: {len(timestamps)} 个")
    
    # 分批处理，避免内存过大
    batch_size = 1000
    total_generated = 0
    
    for batch_start in range(0, len(timestamps), batch_size):
        batch_timestamps = timestamps[batch_start:batch_start + batch_size]
        
        print(f"  处理批次 {batch_start//batch_size + 1}/{(len(timestamps)-1)//batch_size + 1}: {len(batch_timestamps)}个时间点")
        
        batch_data = []
        
        for ts in batch_timestamps:
            # 获取该时间点的所有币种数据
            cursor.execute("""
                SELECT symbol, close, volume 
                FROM market_data_1h 
                WHERE timestamp = ?
            """, (ts,))
            
            symbols_data = cursor.fetchall()
            
            for symbol, close, volume in symbols_data:
                # 这里应该使用真实的alpha计算逻辑
                # 为了演示，使用模拟数据
                alpha_data = generate_alpha_for_timestamp(ts, symbol, pd.DataFrame())
                
                # 添加run_id（使用日期作为标识）
                run_id = datetime.fromtimestamp(ts).strftime("%Y%m%d_%H")
                alpha_data['run_id'] = run_id
                
                batch_data.append(alpha_data)
        
        # 批量插入数据库
        if batch_data:
            # 构建插入语句
            columns = ['run_id', 'ts', 'symbol', 
                      'f1_mom_5d', 'f2_mom_20d', 'f3_vol_adj_ret_20d', 'f4_volume_expansion', 'f5_rsi_trend_confirm',
                      'z1_mom_5d', 'z2_mom_20d', 'z3_vol_adj_ret_20d', 'z4_volume_expansion', 'z5_rsi_trend_confirm',
                      'score', 'score_rank', 'regime', 'regime_multiplier', 'selected', 'traded', 'pnl']
            
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"INSERT INTO alpha_snapshots ({', '.join(columns)}) VALUES ({placeholders})"
            
            # 准备数据
            rows = []
            for data in batch_data:
                row = [data.get(col, None) for col in columns]
                rows.append(row)
            
            # 执行插入
            cursor.executemany(sql, rows)
            conn.commit()
            
            total_generated += len(batch_data)
            print(f"    已插入 {len(batch_data)} 条数据，总计 {total_generated} 条")
    
    # 4. 验证生成结果
    print("\n✅ 4. 生成完成验证")
    print("-" * 40)
    
    cursor.execute("SELECT COUNT(*) FROM alpha_snapshots")
    final_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(ts), MAX(ts) FROM alpha_snapshots")
    alpha_min_ts, alpha_max_ts = cursor.fetchone()
    
    if alpha_min_ts and alpha_max_ts:
        alpha_min_time = datetime.fromtimestamp(alpha_min_ts)
        alpha_max_time = datetime.fromtimestamp(alpha_max_ts)
        alpha_days = (alpha_max_ts - alpha_min_ts) / (24 * 3600)
        
        print(f"  alpha_snapshots表:")
        print(f"    数据量: {final_count} 条")
        print(f"    时间范围: {alpha_min_time.strftime('%Y-%m-%d %H:%M')} 到 {alpha_max_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"    时长: {alpha_days:.1f} 天")
        
        # 检查数据分布
        cursor.execute("""
            SELECT 
                strftime('%Y-%m-%d', ts, 'unixepoch') as date,
                COUNT(*) as count,
                COUNT(DISTINCT symbol) as symbols
            FROM alpha_snapshots
            GROUP BY date
            ORDER BY date
        """)
        
        date_stats = cursor.fetchall()
        
        if date_stats:
            print(f"\n  📅 数据日期分布:")
            print(f"    总天数: {len(date_stats)}")
            
            # 显示首尾各3天
            for i, (date_str, count, symbols) in enumerate(date_stats):
                if i < 3 or i >= len(date_stats) - 3:
                    print(f"    {date_str}: {count}条, {symbols}币种")
                elif i == 3:
                    print(f"    ...")
    else:
        print("  ⚠️ 无alpha数据生成")
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("🎯 30天alpha历史数据生成完成!")
    print("=" * 60)
    
    print("\n📋 下一步:")
    print("1. 运行时间对齐修复: python3 scripts/simple_timestamp_fix.py")
    print("2. 重新创建IC计算视图")
    print("3. 运行IC分析: python3 scripts/quick_ic_analysis.py")
    print("4. 验证30天IC衰减曲线")
    
    return True

def main():
    """主函数"""
    print("🚀 Alpha历史数据生成工具")
    print("=" * 60)
    
    print("⚠️ 注意:")
    print("1. 本脚本生成模拟的alpha数据用于测试")
    print("2. 实际使用时需要实现真实的alpha计算逻辑")
    print("3. 生成的数据将覆盖或添加到现有alpha_snapshots表")
    print("=" * 60)
    
    confirm = input("\n是否继续生成30天alpha历史数据？(y/N): ").lower()
    
    if confirm == 'y':
        success = generate_30day_alpha_history()
        if success:
            print("\n✅ 脚本执行成功!")
        else:
            print("\n❌ 脚本执行失败!")
    else:
        print("\n⏸️ 已取消执行")
    
    print("=" * 60)

if __name__ == "__main__":
    main()