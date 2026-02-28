#!/usr/bin/env python3
"""
测试修复后的Alpha计算
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))

def test_fixed_alpha_calculation():
    """测试修复后的Alpha计算"""
    
    print("🔍 测试修复后的Alpha计算")
    print("=" * 60)
    
    try:
        from src.alpha.alpha_engine import AlphaEngine
        from src.core.models import MarketSeries
        from configs.loader import load_config
        from src.reporting.alpha_evaluation import robust_zscore_cross_section
        
        # 加载配置
        cfg = load_config("configs/fixed_test.yaml", env_path=".env")
        
        # 测试Z-score修复
        print("📊 测试修复的Z-score计算...")
        
        test_cases = [
            {"name": "单币种", "values": {"BTC/USDT": 0.5}},
            {"name": "两币种", "values": {"BTC/USDT": 0.5, "ETH/USDT": 0.8}},
            {"name": "三币种", "values": {"BTC/USDT": 0.5, "ETH/USDT": 0.8, "SOL/USDT": 0.3}},
        ]
        
        for test_case in test_cases:
            print(f"\n  {test_case['name']}:")
            zscores = robust_zscore_cross_section(test_case['values'])
            print(f"    Z-scores: {zscores}")
            
            # 检查是否全为0
            all_zero = all(abs(v) < 1e-12 for v in zscores.values())
            if all_zero:
                print(f"    ⚠️ 所有Z-score为0!")
            else:
                print(f"    ✅ Z-score计算正常")
        
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
        
        # 创建AlphaEngine
        alpha_engine = AlphaEngine(cfg.alpha)
        
        # 计算Alpha分数
        print(f"\n🎯 计算修复后的Alpha分数...")
        alpha_snapshot = alpha_engine.compute_snapshot(market_data)
        
        print(f"  Alpha分数数量: {len(alpha_snapshot.scores)}")
        
        if alpha_snapshot.scores:
            print(f"  Alpha分数详情:")
            for sym, score in alpha_snapshot.scores.items():
                print(f"\n    {sym}: {score:.6f}")
                
                # 检查原始因子
                if sym in alpha_snapshot.raw_factors:
                    raw = alpha_snapshot.raw_factors[sym]
                    print(f"      原始因子:")
                    for factor, value in raw.items():
                        print(f"        {factor}: {value:.6f}")
                
                # 检查Z-score因子
                if sym in alpha_snapshot.z_factors:
                    z = alpha_snapshot.z_factors[sym]
                    print(f"      Z-score因子:")
                    for factor, value in z.items():
                        print(f"        {factor}: {value:.6f}")
                        
                        # 检查Z-score是否为0
                        if abs(value) < 1e-12:
                            print(f"        ⚠️ {factor} Z-score接近0")
        else:
            print(f"  ❌ Alpha分数为空!")
        
        # 检查权重计算
        print(f"\n💰 检查权重计算...")
        
        weights = cfg.alpha.weights
        print(f"  因子权重:")
        for factor, weight in weights.dict().items():
            print(f"    {factor}: {weight:.2f}")
        
        # 手动验证加权计算
        if alpha_snapshot.scores:
            for sym in symbols[:2]:  # 检查前两个币种
                if sym in alpha_snapshot.z_factors:
                    z_factors = alpha_snapshot.z_factors[sym]
                    
                    # 手动计算加权分数
                    manual_score = (
                        z_factors.get('f1', 0) * weights.f1_mom_5d +
                        z_factors.get('f2', 0) * weights.f2_mom_20d +
                        z_factors.get('f3', 0) * weights.f3_vol_adj_ret_20d +
                        z_factors.get('f4', 0) * weights.f4_volume_expansion +
                        z_factors.get('f5', 0) * weights.f5_rsi_trend_confirm
                    )
                    
                    actual_score = alpha_snapshot.scores.get(sym, 0)
                    
                    print(f"\n  {sym}分数验证:")
                    print(f"    手动计算: {manual_score:.6f}")
                    print(f"    AlphaEngine: {actual_score:.6f}")
                    
                    if abs(manual_score - actual_score) < 1e-6:
                        print(f"    ✅ 分数计算正确")
                    else:
                        print(f"    ⚠️ 分数计算不匹配")
        
        print(f"\n💡 修复测试完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"❌ 测试错误: {e}")
        import traceback
        traceback.print_exc()

def test_portfolio_allocation():
    """测试修复后的Portfolio分配"""
    
    print("\n" + "=" * 60)
    print("🎯 测试修复后的Portfolio分配")
    print("=" * 60)
    
    try:
        from src.alpha.alpha_engine import AlphaEngine
        from src.core.models import MarketSeries
        from src.portfolio.portfolio_engine import PortfolioEngine
        from configs.loader import load_config
        
        # 加载配置
        cfg = load_config("configs/fixed_test.yaml", env_path=".env")
        
        # 加载数据
        import sqlite3
        db_path = "reports/alpha_history.db"
        conn = sqlite3.connect(db_path)
        
        symbols = ["BTC/USDT", "ETH/USDT"]
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
        
        conn.close()
        
        # 计算Alpha分数
        alpha_engine = AlphaEngine(cfg.alpha)
        alpha_snapshot = alpha_engine.compute_snapshot(market_data)
        
        if not alpha_snapshot.scores:
            print("❌ 无法计算Alpha分数")
            return
        
        print(f"  Alpha分数:")
        for sym, score in alpha_snapshot.scores.items():
            print(f"    {sym}: {score:.6f}")
        
        # 创建PortfolioEngine
        portfolio_engine = PortfolioEngine(alpha_cfg=cfg.alpha, risk_cfg=cfg.risk)
        
        # 测试分配
        print(f"\n🔧 测试PortfolioEngine.allocate()...")
        
        try:
            portfolio_result = portfolio_engine.allocate(
                scores=alpha_snapshot.scores,
                market_data=market_data,
                regime_mult=1.0,
                audit=None
            )
            
            print(f"  ✅ allocate()执行成功")
            
            # 检查结果
            if hasattr(portfolio_result, 'selected'):
                selected = portfolio_result.selected or []
                print(f"  选择币种: {selected}")
                print(f"  选择数量: {len(selected)}")
            
            if hasattr(portfolio_result, 'target_weights'):
                target_weights = portfolio_result.target_weights or {}
                print(f"  目标权重数量: {len(target_weights)}")
                
                if target_weights:
                    print(f"  目标权重详情:")
                    for symbol, weight in target_weights.items():
                        print(f"    {symbol}: {weight*100:.4f}%")
                else:
                    print(f"  ❌ 目标权重为空!")
            
            # 检查debug信息
            if hasattr(portfolio_result, 'debug_info'):
                debug_info = portfolio_result.debug_info or {}
                print(f"  调试信息: {debug_info}")
                
        except Exception as e:
            print(f"  ❌ allocate()执行错误: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n💡 Portfolio分配测试完成")
        
    except Exception as e:
        print(f"❌ 测试错误: {e}")
        import traceback
        traceback.print_exc()

def main():
    """主函数"""
    
    print("🚀 修复后功能测试")
    print("=" * 60)
    
    # 测试修复后的Alpha计算
    test_fixed_alpha_calculation()
    
    # 测试修复后的Portfolio分配
    test_portfolio_allocation()
    
    print("\n✅ 修复测试完成")
    print("=" * 60)
    
    print("\n💡 下一步:")
    print("如果Alpha计算正常但策略仍无交易，需要检查:")
    print("  1. 订单生成逻辑")
    print("  2. Deadband应用")
    print("  3. 最小交易金额限制")

if __name__ == "__main__":
    main()