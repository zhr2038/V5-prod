#!/usr/bin/env python3
"""
简单策略测试 - 检查基本功能
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

def test_basic_strategy_functionality():
    """测试策略基本功能"""
    
    print("🔧 测试策略基本功能")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        from src.core.pipeline import V5Pipeline
        from src.core.models import MarketSeries
        import numpy as np
        from datetime import datetime
        
        # 加载配置
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("✅ 配置加载成功")
        print(f"  F2权重: {cfg.alpha.weights.f2_mom_20d}")
        print(f"  Deadband: {cfg.rebalance.deadband_sideways}")
        
        # 创建非常明显的趋势数据来测试
        print("\n📈 创建测试数据...")
        
        symbols = ["BTC/USDT", "ETH/USDT"]
        market_data = {}
        
        for symbol in symbols:
            # 创建强上升趋势
            n_bars = 100
            base_price = 1000 if "BTC" in symbol else 100
            
            # 强趋势：1%每小时
            time_idx = np.arange(n_bars)
            prices = base_price * np.exp(0.01 * time_idx)  # 1%每小时
            
            market_data[symbol] = MarketSeries(
                symbol=symbol,
                timeframe="1h",
                ts=[int(datetime.now().timestamp()) - i*3600 for i in range(n_bars)][::-1],
                open=list(prices * 0.999),
                high=list(prices * 1.002),
                low=list(prices * 0.998),
                close=list(prices),
                volume=list(np.random.lognormal(12, 1, n_bars))
            )
        
        print(f"✅ 创建 {len(market_data)} 个币种测试数据")
        
        # 创建pipeline
        pipeline = V5Pipeline(cfg)
        
        # 测试多个时间窗口
        print("\n⚙️ 测试策略逻辑...")
        
        test_windows = [30, 50, 70, 90]  # 不同数据量
        
        for window_size in test_windows:
            print(f"\n📊 测试窗口大小: {window_size}根K线")
            
            # 准备数据
            test_data = {}
            for symbol, series in market_data.items():
                test_data[symbol] = MarketSeries(
                    symbol=symbol,
                    timeframe=series.timeframe,
                    ts=series.ts[:window_size],
                    open=series.open[:window_size],
                    high=series.high[:window_size],
                    low=series.low[:window_size],
                    close=series.close[:window_size],
                    volume=series.volume[:window_size]
                )
            
            # 运行策略
            try:
                result = pipeline.run(
                    market_data_1h=test_data,
                    positions=[],
                    cash_usdt=10000.0,
                    equity_peak_usdt=10000.0
                )
                
                print(f"  Regime状态: {result.regime.state}")
                print(f"  选择币种数: {len(getattr(result.portfolio, 'selected', []) or [])}")
                print(f"  生成订单数: {len(result.orders)}")
                
                if result.orders:
                    print(f"  ✅ 成功生成订单!")
                    for order in result.orders[:2]:  # 显示前2个
                        print(f"    {order.symbol} {order.side} {order.signal_qty:.4f}")
                else:
                    print(f"  ❌ 未生成订单")
                    
                    # 检查中间结果
                    if hasattr(result, 'alpha') and result.alpha:
                        print(f"  Alpha分数:")
                        scores = list(result.alpha.items())
                        scores.sort(key=lambda x: x[1], reverse=True)
                        for symbol, score in scores[:3]:
                            print(f"    {symbol}: {score:.4f}")
                    
            except Exception as e:
                print(f"  ❌ 运行错误: {e}")
                import traceback
                traceback.print_exc()
        
        # 测试成本模型
        print("\n💰 测试成本模型...")
        try:
            from src.backtest.cost_factory import make_cost_model_from_cfg
            
            cost_model = make_cost_model_from_cfg(cfg)
            print(f"  成本模型类型: {type(cost_model).__name__}")
            
            # 测试估计
            test_symbol = "BTC/USDT"
            test_amount = 1000.0
            
            fee = cost_model.estimate_fee(test_symbol, test_amount)
            slippage = cost_model.estimate_slippage(test_symbol, test_amount)
            
            print(f"  测试估计:")
            print(f"    币种: {test_symbol}")
            print(f"    金额: ${test_amount:.2f}")
            print(f"    费用: {fee*10000:.2f}bps")
            print(f"    滑点: {slippage*10000:.2f}bps")
            print(f"    总成本: {(fee+slippage)*10000:.2f}bps")
            
        except Exception as e:
            print(f"  ❌ 成本模型错误: {e}")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 测试错误: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("💡 测试完成")

def check_configuration_issues():
    """检查配置问题"""
    
    print("\n🔍 检查配置问题")
    print("-" * 40)
    
    try:
        from configs.loader import load_config
        
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("📋 关键配置检查:")
        
        # 检查alpha配置
        print(f"  Alpha权重总和: {sum(cfg.alpha.weights.dict().values()):.2f}")
        
        # 检查regime配置
        print(f"  Regime仓位乘数:")
        print(f"    趋势状态: {cfg.regime.pos_mult_trending}")
        print(f"    横盘状态: {cfg.regime.pos_mult_sideways}")
        print(f"    风险规避: {cfg.regime.pos_mult_risk_off}")
        
        # 检查风险配置
        print(f"  风险限制:")
        print(f"    最大单一权重: {cfg.risk.max_single_weight}")
        print(f"    最大总暴露: {cfg.risk.max_gross_exposure}")
        
        # 检查执行配置
        print(f"  执行模式: {cfg.execution.mode}")
        print(f"  是否dry-run: {cfg.execution.dry_run}")
        
        # 检查回测配置
        if hasattr(cfg, 'backtest'):
            print(f"  回测成本模型: {cfg.backtest.cost_model}")
            print(f"  成本数据目录: {cfg.backtest.cost_stats_dir}")
        
    except Exception as e:
        print(f"❌ 配置检查错误: {e}")

def main():
    """主函数"""
    
    print("🚀 策略功能测试")
    print("=" * 60)
    
    # 测试基本功能
    test_basic_strategy_functionality()
    
    # 检查配置
    check_configuration_issues()
    
    print("\n✅ 测试完成")
    print("=" * 60)
    
    print("\n💡 建议:")
    print("1. 如果测试数据能生成订单，说明策略逻辑正常")
    print("2. 如果仍无订单，可能需要调整参数")
    print("3. 考虑测试更敏感的参数配置")
    print("4. 检查真实市场数据的特征")

if __name__ == "__main__":
    main()