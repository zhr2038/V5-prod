#!/usr/bin/env python3
"""
直接调试PortfolioEngine
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_portfolio_allocation():
    """调试PortfolioEngine分配逻辑"""
    
    print("🔍 直接调试PortfolioEngine")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        from src.core.models import MarketSeries
        from src.alpha.alpha_engine import AlphaEngine
        from src.portfolio.portfolio_engine import PortfolioEngine
        
        # 加载配置
        cfg = load_config("configs/extreme_debug.yaml", env_path=".env")
        
        print("📋 使用极端调试配置")
        print(f"  Alpha选择: 前{cfg.alpha.long_top_pct*100:.0f}%")
        print(f"  F1权重: {cfg.alpha.weights.f1_mom_5d}")
        
        # 加载真实数据
        print(f"\n📊 加载真实市场数据...")
        
        import sqlite3
        db_path = "reports/alpha_history.db"
        conn = sqlite3.connect(db_path)
        
        symbols = ["BTC/USDT"]
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
                print(f"    最后价格: ${df['close'].iloc[-1]:.2f}")
        
        conn.close()
        
        if not market_data:
            print("❌ 无法加载市场数据")
            return
        
        # 计算Alpha分数
        print(f"\n🎯 计算Alpha分数...")
        alpha_engine = AlphaEngine(cfg.alpha)
        alpha_snapshot = alpha_engine.compute_snapshot(market_data)
        
        print(f"  Alpha分数数量: {len(alpha_snapshot.scores)}")
        
        if alpha_snapshot.scores:
            print(f"  Alpha分数示例:")
            for symbol, score in alpha_snapshot.scores.items():
                print(f"    {symbol}: {score:.4f}")
        else:
            print(f"  ❌ Alpha分数为空!")
            return
        
        # 创建PortfolioEngine
        print(f"\n⚙️ 创建PortfolioEngine...")
        portfolio_engine = PortfolioEngine(alpha_cfg=cfg.alpha, risk_cfg=cfg.risk)
        
        # 测试分配
        print(f"\n🚀 测试PortfolioEngine.allocate()...")
        
        try:
            portfolio_result = portfolio_engine.allocate(
                scores=alpha_snapshot.scores,
                market_data=market_data,
                regime_mult=1.0,  # 无regime限制
                audit=None
            )
            
            print(f"  ✅ allocate()执行成功")
            
            # 检查结果
            print(f"\n📊 分配结果检查:")
            
            if hasattr(portfolio_result, 'selected'):
                selected = portfolio_result.selected or []
                print(f"  选择币种: {selected}")
                print(f"  选择数量: {len(selected)}")
            else:
                print(f"  ❌ 无selected属性")
            
            if hasattr(portfolio_result, 'target_weights'):
                target_weights = portfolio_result.target_weights or {}
                print(f"  目标权重数量: {len(target_weights)}")
                
                if target_weights:
                    print(f"  目标权重详情:")
                    for symbol, weight in target_weights.items():
                        print(f"    {symbol}: {weight*100:.4f}%")
                else:
                    print(f"  ❌ 目标权重为空!")
                    
                    # 深入调试
                    print(f"\n🔍 深入调试为什么目标权重为空:")
                    
                    # 检查PortfolioEngine内部状态
                    if hasattr(portfolio_engine, 'cfg'):
                        print(f"  PortfolioEngine配置:")
                        print(f"    long_top_pct: {portfolio_engine.cfg.long_top_pct}")
                        print(f"    max_single_weight: {portfolio_engine.risk_cfg.max_single_weight}")
                    
                    # 检查选择逻辑
                    if hasattr(portfolio_result, 'debug_info'):
                        debug_info = portfolio_result.debug_info or {}
                        print(f"  调试信息: {debug_info}")
            
            if hasattr(portfolio_result, 'current_weights'):
                current_weights = portfolio_result.current_weights or {}
                print(f"  当前权重数量: {len(current_weights)}")
            
            # 检查其他属性
            print(f"\n🔧 PortfolioResult属性:")
            for attr in dir(portfolio_result):
                if not attr.startswith('_'):
                    value = getattr(portfolio_result, attr)
                    if value and not callable(value):
                        print(f"  {attr}: {type(value).__name__}")
                        
        except Exception as e:
            print(f"  ❌ allocate()执行错误: {e}")
            import traceback
            traceback.print_exc()
        
        # 测试scale_targets
        print(f"\n🎯 测试scale_targets()...")
        
        if hasattr(portfolio_result, 'target_weights'):
            target_weights = portfolio_result.target_weights or {}
            if target_weights:
                scaled_weights = portfolio_engine.scale_targets(target_weights, 1.0)
                print(f"  缩放后权重数量: {len(scaled_weights)}")
                if scaled_weights:
                    print(f"  缩放后权重:")
                    for symbol, weight in scaled_weights.items():
                        print(f"    {symbol}: {weight*100:.4f}%")
            else:
                print(f"  ⚠️ 无法测试scale_targets，因为target_weights为空")
        
        print(f"\n💡 PortfolioEngine调试完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def check_portfolio_engine_code():
    """检查PortfolioEngine代码"""
    
    print("\n" + "=" * 60)
    print("🔍 检查PortfolioEngine代码")
    print("=" * 60)
    
    portfolio_file = Path("/home/admin/clawd/v5-trading-bot/src/portfolio/portfolio_engine.py")
    
    if portfolio_file.exists():
        print(f"📄 文件: {portfolio_file}")
        
        # 读取关键部分
        with open(portfolio_file, 'r') as f:
            content = f.read()
            
        # 查找allocate方法
        if 'def allocate' in content:
            print(f"  ✅ 找到allocate方法")
            
            # 显示allocate方法签名
            lines = content.split('\n')
            allocate_start = None
            for i, line in enumerate(lines):
                if 'def allocate' in line:
                    allocate_start = i
                    print(f"    行 {i+1}: {line.strip()}")
                    # 显示接下来几行
                    for j in range(i+1, min(i+10, len(lines))):
                        if lines[j].strip() and not lines[j].startswith(' ' * 8):
                            break
                        print(f"    行 {j+1}: {lines[j].rstrip()}")
                    break
        else:
            print(f"  ❌ 未找到allocate方法")
    else:
        print(f"❌ PortfolioEngine文件不存在")

def main():
    """主函数"""
    
    print("🚀 PortfolioEngine直接调试")
    print("=" * 60)
    
    # 调试PortfolioEngine分配逻辑
    debug_portfolio_allocation()
    
    # 检查PortfolioEngine代码
    check_portfolio_engine_code()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 关键发现:")
    print("如果PortfolioEngine.allocate()返回空目标权重，策略将永远不会生成订单")

if __name__ == "__main__":
    main()