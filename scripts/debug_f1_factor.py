#!/usr/bin/env python3
"""
直接调试F1因子(5日动量)计算
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_f1_factor_calculation():
    """调试F1因子计算"""
    
    print("🔍 直接调试F1因子(5日动量)计算")
    print("=" * 60)
    
    try:
        from src.alpha.factors import F1Mom5d
        
        # 创建测试数据
        print("📊 创建测试数据...")
        
        # 创建明显上升趋势数据
        n_bars = 200
        base_price = 1000.0
        
        # 强上升趋势：1%每小时的动量
        time_idx = np.arange(n_bars)
        prices = base_price * np.exp(0.01 * time_idx)  # 1%每小时
        
        # 创建MarketSeries
        from src.core.models import MarketSeries
        from datetime import datetime
        
        test_series = MarketSeries(
            symbol="TEST/USDT",
            timeframe="1h",
            ts=[int(datetime.now().timestamp()) - i*3600 for i in range(n_bars)][::-1],
            open=list(prices * 0.999),
            high=list(prices * 1.002),
            low=list(prices * 0.998),
            close=list(prices),
            volume=list(np.random.lognormal(12, 1, n_bars))
        )
        
        print(f"  测试数据: {n_bars}根K线")
        print(f"  价格范围: ${prices[0]:.2f} - ${prices[-1]:.2f}")
        print(f"  总涨幅: {(prices[-1]/prices[0]-1)*100:.2f}%")
        
        # 创建F1因子
        print(f"\n🎯 创建F1因子...")
        f1_factor = F1Mom5d()
        
        # 测试不同数据长度
        print(f"\n📊 测试F1因子计算...")
        
        test_lengths = [50, 100, 150, 200]
        
        for length in test_lengths:
            print(f"\n  数据长度: {length}根K线")
            
            # 截取数据
            test_data = {
                "TEST/USDT": MarketSeries(
                    symbol=test_series.symbol,
                    timeframe=test_series.timeframe,
                    ts=test_series.ts[:length],
                    open=test_series.open[:length],
                    high=test_series.high[:length],
                    low=test_series.low[:length],
                    close=test_series.close[:length],
                    volume=test_series.volume[:length]
                )
            }
            
            try:
                # 计算F1分数
                scores = f1_factor.compute(test_data)
                
                print(f"    F1分数数量: {len(scores)}")
                
                if scores:
                    for symbol, score in scores.items():
                        print(f"    {symbol}: {score:.6f}")
                        
                        # 分析分数
                        if abs(score) < 0.0001:
                            print(f"    ⚠️ 分数接近0 ({score:.6f})")
                        else:
                            print(f"    ✅ 分数有效 ({score:.6f})")
                            
                        # 计算实际5日动量(5*24=120小时)
                        if length >= 120:
                            price_now = test_series.close[length-1]
                            price_5d_ago = test_series.close[length-121]
                            actual_mom = (price_now - price_5d_ago) / price_5d_ago
                            print(f"    实际5日动量: {actual_mom*100:.2f}%")
                            print(f"    预期F1分数: 应为正数(上升趋势)")
                else:
                    print(f"    ❌ 未计算F1分数")
                    
            except Exception as e:
                print(f"    ❌ F1计算错误: {e}")
        
        # 测试真实数据
        print(f"\n📊 测试真实BTC数据...")
        
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
        
        if len(df) >= 200:
            real_series = MarketSeries(
                symbol=symbol,
                timeframe="1h",
                ts=df['timestamp'].tolist(),
                open=df['open'].tolist(),
                high=df['high'].tolist(),
                low=df['low'].tolist(),
                close=df['close'].tolist(),
                volume=df['volume'].tolist()
            )
            
            test_data = {symbol: real_series}
            
            try:
                scores = f1_factor.compute(test_data)
                print(f"  BTC/USDT F1分数: {scores.get(symbol, 'N/A')}")
                
                # 计算实际5日动量
                if len(df) >= 120:
                    price_now = df['close'].iloc[-1]
                    price_5d_ago = df['close'].iloc[-121]
                    actual_mom = (price_now - price_5d_ago) / price_5d_ago
                    print(f"  实际5日动量: {actual_mom*100:.2f}%")
                    print(f"  价格变化: ${price_5d_ago:.2f} → ${price_now:.2f}")
                    
            except Exception as e:
                print(f"  ❌ 真实数据F1计算错误: {e}")
        
        print(f"\n💡 F1因子调试完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def check_f1_factor_implementation():
    """检查F1因子实现"""
    
    print("\n" + "=" * 60)
    print("🔍 检查F1因子实现")
    print("=" * 60)
    
    f1_file = Path("/home/admin/clawd/v5-trading-bot/src/alpha/factors.py")
    
    if f1_file.exists():
        print(f"📄 文件: {f1_file}")
        
        with open(f1_file, 'r') as f:
            content = f.read()
            
        # 查找F1Mom5d类
        if 'class F1Mom5d' in content:
            print(f"  ✅ 找到F1Mom5d类")
            
            # 显示类定义
            lines = content.split('\n')
            f1_start = None
            for i, line in enumerate(lines):
                if 'class F1Mom5d' in line:
                    f1_start = i
                    print(f"    行 {i+1}: {line.strip()}")
                    # 显示compute方法
                    for j in range(i, min(i+50, len(lines))):
                        if 'def compute' in lines[j]:
                            print(f"    行 {j+1}: {lines[j].strip()}")
                            # 显示方法内容
                            for k in range(j+1, min(j+20, len(lines))):
                                if lines[k].strip() and not lines[k].startswith(' ' * 8):
                                    break
                                print(f"    行 {k+1}: {lines[k].rstrip()}")
                            break
                    break
        else:
            print(f"  ❌ 未找到F1Mom5d类")
    else:
        print(f"❌ factors.py文件不存在")

def main():
    """主函数"""
    
    print("🚀 F1因子直接调试")
    print("=" * 60)
    
    # 调试F1因子计算
    debug_f1_factor_calculation()
    
    # 检查F1因子实现
    check_f1_factor_implementation()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 关键发现:")
    print("如果F1因子计算返回0分数，Alpha分数将为0")
    print("这可能导致策略不生成订单")

if __name__ == "__main__":
    main()