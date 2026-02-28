#!/usr/bin/env python3
"""
深度调试：Alpha因子计算
"""

import sys
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_alpha_factors():
    """调试Alpha因子计算"""
    
    print("🔍 深度调试：Alpha因子计算")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        from src.core.models import MarketSeries
        from src.alpha.alpha_engine import AlphaEngine
        
        # 加载配置
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("📋 配置信息:")
        print(f"  Alpha权重: {cfg.alpha.weights}")
        print(f"  Top选择: 前{cfg.alpha.long_top_pct*100:.0f}%")
        
        # 加载真实数据
        print(f"\n📊 加载真实市场数据...")
        
        import sqlite3
        db_path = "reports/alpha_history.db"
        conn = sqlite3.connect(db_path)
        
        symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        market_data = {}
        
        for symbol in symbols:
            query = f"""
            SELECT timestamp, open, high, low, close, volume 
            FROM market_data_1h 
            WHERE symbol = ? 
            ORDER BY timestamp
            """
            
            df = pd.read_sql_query(query, conn, params=(symbol,))
            
            if len(df) >= 100:
                market_data[symbol] = MarketSeries(
                    symbol=symbol,
                    timeframe="1h",
                    ts=df['timestamp'].tolist(),
                    open=df['open'].tolist(),
                    high=df['high'].tolist(),
                    low=df['low'].tolist(),
                    close=df['close'].tolist(),
                    volume=df['volume'].tolist()
                )
                
                print(f"  {symbol}: {len(df)}根K线")
        
        conn.close()
        
        if not market_data:
            print("❌ 无法加载市场数据")
            return
        
        # 创建Alpha引擎
        alpha_engine = AlphaEngine(cfg.alpha)
        
        # 测试不同时间点
        print(f"\n🎯 测试Alpha因子计算...")
        
        test_points = [100, 200, 300, 400, 500]  # 不同时间点
        
        for i, test_idx in enumerate(test_points):
            print(f"\n📊 测试点 {i+1} (K线 {test_idx}):")
            
            # 截取数据
            md_slice = {}
            for symbol, series in market_data.items():
                if len(series.close) >= test_idx:
                    md_slice[symbol] = MarketSeries(
                        symbol=symbol,
                        timeframe=series.timeframe,
                        ts=series.ts[:test_idx],
                        open=series.open[:test_idx],
                        high=series.high[:test_idx],
                        low=series.low[:test_idx],
                        close=series.close[:test_idx],
                        volume=series.volume[:test_idx]
                    )
            
            # 计算Alpha分数
            try:
                alpha_scores = alpha_engine.compute_scores(md_slice)
                
                if alpha_scores:
                    print(f"  ✅ 成功计算Alpha分数")
                    
                    # 显示分数
                    scores_df = pd.DataFrame([
                        {"symbol": s, "score": score}
                        for s, score in alpha_scores.items()
                    ]).sort_values("score", ascending=False)
                    
                    print(f"  📈 Alpha分数排名:")
                    print(scores_df.to_string(index=False))
                    
                    # 分析分数分布
                    print(f"  📊 分数统计:")
                    print(f"    最高分: {scores_df['score'].max():.4f}")
                    print(f"    最低分: {scores_df['score'].min():.4f}")
                    print(f"    平均分: {scores_df['score'].mean():.4f}")
                    print(f"    标准差: {scores_df['score'].std():.4f}")
                    
                    # 检查是否有有效分数
                    if abs(scores_df['score'].max()) < 0.01:
                        print(f"  ⚠️ 分数绝对值过小 (<0.01)，可能无法触发交易")
                    
                else:
                    print(f"  ❌ 未计算Alpha分数")
                    
            except Exception as e:
                print(f"  ❌ Alpha计算错误: {e}")
                import traceback
                traceback.print_exc()
        
        # 调试单个因子
        print(f"\n🔧 调试单个Alpha因子...")
        
        # 检查F2因子(20日动量)
        print(f"  🎯 F2因子(20日动量)分析:")
        
        for symbol, series in market_data.items():
            if len(series.close) >= 480:  # 20天*24小时
                # 计算20日动量
                price_20d_ago = series.close[-480]
                price_now = series.close[-1]
                mom_20d = (price_now - price_20d_ago) / price_20d_ago
                
                print(f"    {symbol}:")
                print(f"      20日前价格: ${price_20d_ago:.2f}")
                print(f"      当前价格: ${price_now:.2f}")
                print(f"      20日动量: {mom_20d*100:.2f}%")
                print(f"      因子预期: {'负分(做空)' if mom_20d < 0 else '正分(做多)'}")
        
        print(f"\n💡 Alpha因子调试完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def debug_factor_weights():
    """调试因子权重"""
    
    print("\n" + "=" * 60)
    print("🎯 调试因子权重")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        weights = cfg.alpha.weights
        print("📋 当前因子权重:")
        print(f"  F1(5日动量): {weights.f1_mom_5d*100:.0f}%")
        print(f"  F2(20日动量): {weights.f2_mom_20d*100:.0f}%")
        print(f"  F3(波动调整收益): {weights.f3_vol_adj_ret_20d*100:.0f}%")
        print(f"  F4(成交量扩张): {weights.f4_volume_expansion*100:.0f}%")
        print(f"  F5(RSI趋势确认): {weights.f5_rsi_trend_confirm*100:.0f}%")
        
        print(f"\n💡 权重调整建议:")
        print(f"  当前市场: 明显下降趋势")
        print(f"  建议调整:")
        print(f"    - 增加F2权重(20日动量)")
        print(f"    - 增加F1权重(5日动量)")
        print(f"    - 考虑添加均值回归因子")
        
    except Exception as e:
        print(f"❌ 权重调试错误: {e}")

def main():
    """主函数"""
    
    print("🚀 Alpha因子深度调试")
    print("=" * 60)
    
    # 调试Alpha因子计算
    debug_alpha_factors()
    
    # 调试因子权重
    debug_factor_weights()
    
    print("\n✅ Alpha因子调试完成")
    print("=" * 60)
    
    print("\n💡 下一步建议:")
    print("1. 如果Alpha分数过小，需要调整因子计算")
    print("2. 如果分数合理但无交易，检查后续逻辑")
    print("3. 考虑因子权重调整以适应市场状态")

if __name__ == "__main__":
    main()