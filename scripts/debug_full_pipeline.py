#!/usr/bin/env python3
"""
端到端调试：完整策略执行流程
"""

import sys
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_full_pipeline():
    """调试完整策略执行流程"""
    
    print("🔍 端到端调试：完整策略执行流程")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        from src.core.pipeline import V5Pipeline
        from src.core.models import MarketSeries, Order
        from src.backtest.backtest_engine import BacktestEngine
        from src.backtest.cost_factory import make_cost_model_from_cfg
        
        # 加载配置
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("📋 配置信息:")
        print(f"  策略: F2权重{cfg.alpha.weights.f2_mom_20d*100:.0f}%")
        print(f"  Regime阈值: {cfg.regime.atr_threshold*100:.1f}%")
        print(f"  Risk-Off仓位: {cfg.regime.pos_mult_risk_off}")
        print(f"  Deadband: {cfg.rebalance.deadband_sideways}")
        
        # 加载真实数据
        print(f"\n📊 加载真实市场数据...")
        
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
            
            if len(df) >= 200:
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
                print(f"    价格范围: ${df['close'].min():.2f} - ${df['close'].max():.2f}")
                print(f"    最后价格: ${df['close'].iloc[-1]:.2f}")
        
        conn.close()
        
        if not market_data:
            print("❌ 无法加载市场数据")
            return
        
        # 创建成本模型
        print(f"\n💰 创建成本模型...")
        cost_model, cost_meta = make_cost_model_from_cfg(cfg)
        print(f"  成本模型类型: {type(cost_model).__name__}")
        print(f"  成本模式: {cost_meta.mode}")
        print(f"  来源日期: {cost_meta.source_day}")
        print(f"  全局fills: {cost_meta.global_fills}")
        
        # 创建回测引擎
        print(f"\n⚙️ 创建回测引擎...")
        backtest_engine = BacktestEngine(
            cfg=cfg,
            cost_model=cost_model,
            cost_model_meta=cost_meta.to_dict()
        )
        
        # 运行完整回测
        print(f"\n🚀 运行完整回测...")
        try:
            result = backtest_engine.run(market_data)
            
            print(f"  ✅ 回测完成!")
            print(f"    夏普: {result.sharpe:.3f}")
            print(f"    年化收益: {result.cagr*100:.4f}%")
            print(f"    最大回撤: {result.max_dd*100:.4f}%")
            print(f"    盈亏比: {result.profit_factor:.3f}")
            print(f"    换手率: {result.turnover*100:.4f}%")
            
            # 检查成本假设
            if result.cost_assumption:
                print(f"    成本模式: {result.cost_assumption.get('mode', 'N/A')}")
                print(f"    回退级别: {result.cost_assumption.get('fallback_level', 'N/A')}")
            
            # 检查是否有交易
            if result.sharpe == 0 and result.cagr == 0:
                print(f"\n  ⚠️ 回测结果全为0，可能无交易")
                
        except Exception as e:
            print(f"  ❌ 回测错误: {e}")
            import traceback
            traceback.print_exc()
        
        # 调试单个时间点的策略执行
        print(f"\n🎯 调试单个时间点策略执行...")
        
        # 创建pipeline
        pipeline = V5Pipeline(cfg)
        
        # 测试多个时间点
        test_points = [100, 150, 200]  # 不同数据量
        
        for i, data_length in enumerate(test_points):
            print(f"\n📊 测试点 {i+1} (数据长度: {data_length}):")
            
            # 准备数据
            test_data = {}
            for symbol, series in market_data.items():
                test_data[symbol] = MarketSeries(
                    symbol=symbol,
                    timeframe=series.timeframe,
                    ts=series.ts[:data_length],
                    open=series.open[:data_length],
                    high=series.high[:data_length],
                    low=series.low[:data_length],
                    close=series.close[:data_length],
                    volume=series.volume[:data_length]
                )
            
            # 运行策略
            try:
                pipeline_result = pipeline.run(
                    market_data_1h=test_data,
                    positions=[],  # 空仓位
                    cash_usdt=10000.0,
                    equity_peak_usdt=10000.0
                )
                
                print(f"  ✅ 策略执行成功")
                print(f"    Regime状态: {pipeline_result.regime.state}")
                print(f"    仓位乘数: {pipeline_result.regime.pos_mult}")
                
                # 检查Alpha分数
                if hasattr(pipeline_result, 'alpha') and pipeline_result.alpha:
                    alpha_scores = pipeline_result.alpha
                    print(f"    Alpha分数:")
                    for symbol, score in sorted(alpha_scores.items(), key=lambda x: x[1], reverse=True)[:3]:
                        print(f"      {symbol}: {score:.4f}")
                
                # 检查选择的币种
                if hasattr(pipeline_result.portfolio, 'selected'):
                    selected = pipeline_result.portfolio.selected or []
                    print(f"    选择币种数: {len(selected)}")
                    if selected:
                        print(f"    选择的币种: {selected}")
                
                # 检查生成的订单
                orders = pipeline_result.orders
                print(f"    生成订单数: {len(orders)}")
                
                if orders:
                    print(f"    ✅ 成功生成订单!")
                    for j, order in enumerate(orders[:3]):  # 显示前3个订单
                        print(f"      订单{j+1}: {order.symbol} {order.side}")
                        if hasattr(order, 'qty'):
                            print(f"        数量: {order.qty:.4f}")
                        if hasattr(order, 'notional_usdt'):
                            print(f"        金额: ${order.notional_usdt:.2f}")
                else:
                    print(f"    ❌ 未生成订单")
                    
                    # 深入调试为什么没有订单
                    print(f"    🔍 深入调试:")
                    
                    # 检查portfolio计算
                    if hasattr(pipeline_result, 'portfolio'):
                        portfolio = pipeline_result.portfolio
                        if hasattr(portfolio, 'target_weights'):
                            target_weights = portfolio.target_weights or {}
                            if target_weights:
                                print(f"      目标权重:")
                                for symbol, weight in target_weights.items():
                                    print(f"        {symbol}: {weight*100:.2f}%")
                            else:
                                print(f"      无目标权重")
                        
                        if hasattr(portfolio, 'current_weights'):
                            current_weights = portfolio.current_weights or {}
                            if current_weights:
                                print(f"      当前权重:")
                                for symbol, weight in current_weights.items():
                                    print(f"        {symbol}: {weight*100:.2f}%")
                            else:
                                print(f"      无当前权重(空仓位)")
                    
            except Exception as e:
                print(f"  ❌ 策略执行错误: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"\n💡 端到端调试完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def analyze_possible_issues():
    """分析可能的问题"""
    
    print("\n" + "=" * 60)
    print("🔍 分析可能的问题")
    print("=" * 60)
    
    print("📋 基于调试的可能问题:")
    print("  1. 策略逻辑问题:")
    print("     - 订单生成逻辑有bug")
    print("     - 仓位计算错误")
    print("     - deadband应用问题")
    
    print("  2. 数据问题:")
    print("     - 市场数据特征不适合策略")
    print("     - 数据预处理有问题")
    
    print("  3. 配置问题:")
    print("     - 参数过于保守")
    print("     - 风险限制过严")
    
    print("  4. 集成问题:")
    print("     - 回测引擎与策略集成有问题")
    print("     - 成本模型集成问题")
    
    print(f"\n💡 诊断建议:")
    print("  1. 检查订单生成逻辑")
    print("  2. 检查deadband计算")
    print("  3. 检查仓位权重计算")
    print("  4. 创建最小化测试用例")

def create_minimal_test():
    """创建最小化测试"""
    
    print("\n" + "=" * 60)
    print("🚀 创建最小化测试配置")
    print("=" * 60)
    
    minimal_config = """# 最小化测试配置
symbols:
  - BTC/USDT

timeframe_main: 1h
timeframe_aux: 4h

exchange:
  name: okx
  testnet: false

alpha:
  long_top_pct: 0.50  # 选择前50%
  weights:
    f1_mom_5d: 0.5
    f2_mom_20d: 0.5
    f3_vol_adj_ret_20d: 0.0
    f4_volume_expansion: 0.0
    f5_rsi_trend_confirm: 0.0

regime:
  atr_threshold: 0.005  # 极低阈值
  atr_very_low: 0.002
  pos_mult_trending: 1.0
  pos_mult_sideways: 1.0
  pos_mult_risk_off: 1.0  # 不降低仓位

risk:
  max_single_weight: 1.0  # 允许全仓
  max_gross_exposure: 1.0
  drawdown_trigger: 0.50  # 宽松回撤
  drawdown_delever: 0.50

rebalance:
  interval_minutes: 60
  deadband_sideways: 0.001  # 极低deadband
  deadband_trending: 0.001
  deadband_riskoff: 0.001

execution:
  mode: dry_run
  dry_run: true
  fee_bps: 0  # 零成本
  slippage_bps: 0

backtest:
  start_date: "2026-01-19"
  end_date: "2026-02-17"
  fee_bps: 0
  slippage_bps: 0
  one_bar_delay: true
  walk_forward_folds: 2
  cost_model: fixed
  cost_stats_dir: ""
  fee_quantile: p75
  slippage_quantile: p90
  min_fills_global: 1
  min_fills_bucket: 1
  max_stats_age_days: 365
"""
    
    print("📋 最小化测试要点:")
    print("  1. 只测试1个币种(BTC/USDT)")
    print("  2. 极简Alpha因子(只有动量)")
    print("  3. 极低阈值和deadband")
    print("  4. 零成本")
    print("  5. 宽松风险限制")
    
    # 保存配置
    config_path = Path("configs/minimal_test.yaml")
    config_path.write_text(minimal_config, encoding="utf-8")
    
    print(f"\n✅ 最小化配置已保存到: {config_path}")
    print(f"   使用命令测试: V5_CONFIG={config_path} python3 scripts/run_walk_forward.py")
    
    print(f"\n💡 测试目的:")
    print("  1. 排除复杂因素干扰")
    print("  2. 验证最基本策略逻辑")
    print("  3. 如果仍无交易，说明核心逻辑有问题")

def main():
    """主函数"""
    
    print("🚀 端到端策略调试")
    print("=" * 60)
    
    # 调试完整流程
    debug_full_pipeline()
    
    # 分析可能问题
    analyze_possible_issues()
    
    # 创建最小化测试
    create_minimal_test()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 下一步行动:")
    print("1. 运行最小化测试配置")
    print("2. 如果仍无交易，检查核心策略逻辑")
    print("3. 检查订单生成和仓位计算代码")

if __name__ == "__main__":
    main()