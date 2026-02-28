#!/usr/bin/env python3
"""
直接调试Alpha计算
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_alpha_calculation_direct():
    """直接调试Alpha计算"""
    
    print("🔍 直接调试Alpha计算")
    print("=" * 60)
    
    try:
        from src.alpha.alpha_engine import AlphaEngine
        from src.core.models import MarketSeries
        from configs.loader import load_config
        
        # 加载配置
        cfg = load_config("configs/extreme_debug.yaml", env_path=".env")
        
        # 加载真实数据
        print("📊 加载真实BTC数据...")
        
        import sqlite3
        db_path = "reports/alpha_history.db"
        conn = sqlite3.connect(db_path)
        
        symbol = "BTC/USDT"
        query = f"""
        SELECT timestamp, open, high, low, close, volume 
        FROM market_data_1h 
        WHERE symbol = ? 
        ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol,))
        conn.close()
        
        print(f"  BTC/USDT数据: {len(df)}根K线")
        print(f"  时间范围: {pd.to_datetime(df['timestamp'].iloc[0], unit='s')} 到 {pd.to_datetime(df['timestamp'].iloc[-1], unit='s')}")
        print(f"  价格范围: ${df['close'].min():.2f} - ${df['close'].max():.2f}")
        print(f"  最后价格: ${df['close'].iloc[-1]:.2f}")
        
        # 创建MarketSeries
        market_data = {
            symbol: MarketSeries(
                symbol=symbol,
                timeframe="1h",
                ts=df['timestamp'].tolist(),
                open=df['open'].tolist(),
                high=df['high'].tolist(),
                low=df['low'].tolist(),
                close=df['close'].tolist(),
                volume=df['volume'].tolist()
            )
        }
        
        # 创建AlphaEngine
        alpha_engine = AlphaEngine(cfg.alpha)
        
        # 计算Alpha分数
        print(f"\n🎯 计算Alpha分数...")
        alpha_snapshot = alpha_engine.compute_snapshot(market_data)
        
        print(f"  Alpha分数数量: {len(alpha_snapshot.scores)}")
        
        if alpha_snapshot.scores:
            for sym, score in alpha_snapshot.scores.items():
                print(f"  {sym} Alpha分数: {score:.6f}")
                
                # 检查原始因子值
                if sym in alpha_snapshot.raw_factors:
                    raw_factors = alpha_snapshot.raw_factors[sym]
                    print(f"    原始因子:")
                    for factor, value in raw_factors.items():
                        print(f"      {factor}: {value:.6f}")
                
                # 检查z-score因子值
                if sym in alpha_snapshot.z_factors:
                    z_factors = alpha_snapshot.z_factors[sym]
                    print(f"    Z-score因子:")
                    for factor, value in z_factors.items():
                        print(f"      {factor}: {value:.6f}")
        else:
            print(f"  ❌ Alpha分数为空!")
        
        # 手动计算5日动量验证
        print(f"\n🔍 手动计算验证...")
        
        closes = df['close'].tolist()
        if len(closes) >= 120:  # 5天*24小时
            price_now = closes[-1]
            price_5d_ago = closes[-121]  # 5天前(120小时前)
            
            # 使用safe_pct_change
            from src.utils.math import safe_pct_change
            mom_5d = safe_pct_change(price_5d_ago, price_now)
            
            print(f"  手动计算5日动量:")
            print(f"    当前价格: ${price_now:.2f}")
            print(f"    5天前价格: ${price_5d_ago:.2f}")
            print(f"    5日动量: {mom_5d:.6f} ({mom_5d*100:.2f}%)")
            
            # 检查AlphaEngine计算的原始因子
            if symbol in alpha_snapshot.raw_factors:
                raw_f1 = alpha_snapshot.raw_factors[symbol].get('f1', 0)
                print(f"    AlphaEngine F1值: {raw_f1:.6f}")
                
                if abs(raw_f1 - mom_5d) > 0.0001:
                    print(f"    ⚠️ 不匹配! AlphaEngine: {raw_f1:.6f}, 手动计算: {mom_5d:.6f}")
                else:
                    print(f"    ✅ 匹配")
        
        # 检查权重计算
        print(f"\n💰 检查权重计算...")
        
        weights = cfg.alpha.weights
        print(f"  因子权重:")
        print(f"    F1(5日动量): {weights.f1_mom_5d}")
        print(f"    F2(20日动量): {weights.f2_mom_20d}")
        print(f"    F3(波动调整收益): {weights.f3_vol_adj_ret_20d}")
        print(f"    F4(成交量扩张): {weights.f4_volume_expansion}")
        print(f"    F5(RSI趋势确认): {weights.f5_rsi_trend_confirm}")
        
        # 手动计算加权分数
        if symbol in alpha_snapshot.z_factors:
            z_factors = alpha_snapshot.z_factors[symbol]
            
            weighted_score = (
                z_factors.get('f1', 0) * weights.f1_mom_5d +
                z_factors.get('f2', 0) * weights.f2_mom_20d +
                z_factors.get('f3', 0) * weights.f3_vol_adj_ret_20d +
                z_factors.get('f4', 0) * weights.f4_volume_expansion +
                z_factors.get('f5', 0) * weights.f5_rsi_trend_confirm
            )
            
            print(f"  手动加权分数: {weighted_score:.6f}")
            print(f"  AlphaEngine分数: {alpha_snapshot.scores.get(symbol, 0):.6f}")
            
            if abs(weighted_score - alpha_snapshot.scores.get(symbol, 0)) > 0.0001:
                print(f"  ⚠️ 分数计算不匹配!")
            else:
                print(f"  ✅ 分数计算匹配")
        
        print(f"\n💡 Alpha计算调试完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def check_alpha_engine_implementation():
    """检查AlphaEngine实现"""
    
    print("\n" + "=" * 60)
    print("🔍 检查AlphaEngine实现")
    print("=" * 60)
    
    # 读取AlphaEngine的关键部分
    alpha_file = Path("/home/admin/clawd/v5-trading-bot/src/alpha/alpha_engine.py")
    
    if alpha_file.exists():
        with open(alpha_file, 'r') as f:
            content = f.read()
        
        # 查找因子计算部分
        lines = content.split('\n')
        
        print("📋 因子计算逻辑:")
        
        # 查找F1计算
        for i, line in enumerate(lines):
            if 'mom_5d =' in line:
                print(f"  行 {i+1}: {line.strip()}")
                # 显示上下文
                for j in range(max(0, i-2), min(len(lines), i+3)):
                    if j != i:
                        print(f"  行 {j+1}: {lines[j].strip()}")
                break
        
        # 查找safe_pct_change使用
        print(f"\n🔧 safe_pct_change使用:")
        for i, line in enumerate(lines):
            if 'safe_pct_change' in line:
                print(f"  行 {i+1}: {line.strip()}")
    
    else:
        print(f"❌ AlphaEngine文件不存在")

def main():
    """主函数"""
    
    print("🚀 Alpha计算直接调试")
    print("=" * 60)
    
    # 直接调试Alpha计算
    debug_alpha_calculation_direct()
    
    # 检查AlphaEngine实现
    check_alpha_engine_implementation()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 关键发现:")
    print("需要验证Alpha分数计算是否正确")
    print("如果因子计算返回0，Alpha分数将为0")

if __name__ == "__main__":
    main()