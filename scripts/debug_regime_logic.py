#!/usr/bin/env python3
"""
深度调试：Regime判断逻辑
"""

import sys
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_regime_detection():
    """调试Regime判断逻辑"""
    
    print("🔍 深度调试：Regime判断逻辑")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        from src.core.models import MarketSeries
        from src.regime.regime_engine import RegimeEngine
        
        # 加载配置
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("📋 配置信息:")
        print(f"  ATR阈值: {cfg.regime.atr_threshold}")
        print(f"  极低ATR: {cfg.regime.atr_very_low}")
        print(f"  仓位乘数:")
        print(f"    趋势状态: {cfg.regime.pos_mult_trending}")
        print(f"    横盘状态: {cfg.regime.pos_mult_sideways}")
        print(f"    风险规避: {cfg.regime.pos_mult_risk_off}")
        
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
        
        # 创建Regime引擎
        regime_engine = RegimeEngine(cfg.regime)
        
        # 测试不同时间点
        print(f"\n🎯 测试Regime判断...")
        
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
            
            # 计算Regime
            try:
                regime_result = regime_engine.detect(md_slice)
                
                print(f"  ✅ Regime判断结果:")
                print(f"    状态: {regime_result.state}")
                print(f"    置信度: {regime_result.confidence:.3f}")
                print(f"    仓位乘数: {regime_result.pos_mult}")
                
                # 显示详细指标
                if hasattr(regime_result, 'metrics'):
                    metrics = regime_result.metrics
                    print(f"    📊 详细指标:")
                    print(f"      平均ATR: {metrics.get('avg_atr', 'N/A')}")
                    print(f"      波动率: {metrics.get('volatility', 'N/A')}")
                    print(f"      趋势强度: {metrics.get('trend_strength', 'N/A')}")
                
                # 检查是否Risk-Off
                if regime_result.state == "Risk-Off":
                    print(f"    ⚠️ 处于Risk-Off状态，仓位乘数: {regime_result.pos_mult}")
                    print(f"      可能原因: 波动率过低或市场异常")
                
            except Exception as e:
                print(f"  ❌ Regime判断错误: {e}")
                import traceback
                traceback.print_exc()
        
        # 分析市场波动率
        print(f"\n📈 市场波动率分析...")
        
        for symbol, series in market_data.items():
            if len(series.close) >= 100:
                # 计算ATR
                high = np.array(series.high[-100:])
                low = np.array(series.low[-100:])
                close = np.array(series.close[-100:])
                
                # 计算True Range
                tr1 = high - low
                tr2 = np.abs(high - np.roll(close, 1))
                tr3 = np.abs(low - np.roll(close, 1))
                tr = np.maximum(np.maximum(tr1, tr2), tr3)
                
                atr = np.mean(tr[1:])  # 跳过第一个NaN
                atr_pct = atr / close[-1] * 100
                
                print(f"  {symbol}:")
                print(f"    当前价格: ${close[-1]:.2f}")
                print(f"    ATR(绝对值): ${atr:.2f}")
                print(f"    ATR(百分比): {atr_pct:.3f}%")
                print(f"    阈值比较: {'>' if atr_pct > cfg.regime.atr_threshold*100 else '<'} {cfg.regime.atr_threshold*100}%")
                
                if atr_pct < cfg.regime.atr_threshold * 100:
                    print(f"    ⚠️ ATR低于阈值，可能触发Risk-Off")
        
        print(f"\n💡 Regime判断调试完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def debug_regime_impact():
    """调试Regime对交易的影响"""
    
    print("\n" + "=" * 60)
    print("🎯 调试Regime对交易的影响")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("📋 Regime配置分析:")
        print(f"  ATR阈值: {cfg.regime.atr_threshold} ({cfg.regime.atr_threshold*100:.1f}%)")
        print(f"  极低ATR: {cfg.regime.atr_very_low} ({cfg.regime.atr_very_low*100:.1f}%)")
        
        print(f"\n💡 对交易的影响:")
        print(f"  1. Risk-Off状态:")
        print(f"     仓位乘数: {cfg.regime.pos_mult_risk_off}")
        print(f"     影响: 仓位减少{100*(1-cfg.regime.pos_mult_risk_off):.0f}%")
        
        print(f"  2. 横盘状态:")
        print(f"     仓位乘数: {cfg.regime.pos_mult_sideways}")
        print(f"     影响: 仓位减少{100*(1-cfg.regime.pos_mult_sideways):.0f}%")
        
        print(f"  3. 趋势状态:")
        print(f"     仓位乘数: {cfg.regime.pos_mult_trending}")
        print(f"     影响: 仓位增加{100*(cfg.regime.pos_mult_trending-1):.0f}%")
        
        print(f"\n🎯 基于市场分析的调整建议:")
        print(f"  当前市场: 明显下降趋势，波动率3.2-4.5%")
        print(f"  建议调整:")
        print(f"    - 提高ATR阈值: {cfg.regime.atr_threshold*100:.1f}% → 1.5%")
        print(f"    - 提高Risk-Off仓位: {cfg.regime.pos_mult_risk_off} → 0.6")
        print(f"    - 降低极低ATR阈值")
        
    except Exception as e:
        print(f"❌ 影响分析错误: {e}")

def main():
    """主函数"""
    
    print("🚀 Regime判断深度调试")
    print("=" * 60)
    
    # 调试Regime判断逻辑
    debug_regime_detection()
    
    # 调试Regime影响
    debug_regime_impact()
    
    print("\n✅ Regime调试完成")
    print("=" * 60)
    
    print("\n💡 关键发现和建议:")
    print("1. 检查ATR计算是否准确")
    print("2. 如果市场波动率低于阈值，会进入Risk-Off")
    print("3. Risk-Off状态大幅降低仓位(乘数0.3)")
    print("4. 建议调整阈值或提高Risk-Off仓位")

if __name__ == "__main__":
    main()