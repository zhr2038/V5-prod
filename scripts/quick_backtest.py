#!/usr/bin/env python3
"""
快速回测验证脚本
测试优化前后的盈利能力
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

def load_market_data():
    """加载市场数据（简化版）"""
    
    print("📊 加载市场数据...")
    
    # 这里应该从数据库或文件加载实际数据
    # 为了快速验证，使用模拟数据
    
    from src.core.models import MarketSeries
    
    # 创建模拟数据（30天，每小时）
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
    n_bars = 30 * 24  # 30天每小时数据
    
    market_data = {}
    
    for symbol in symbols:
        # 模拟价格序列（随机游走）
        np.random.seed(42)  # 可重复
        base_price = 1000 if "BTC" in symbol else 100
        returns = np.random.normal(0.0001, 0.01, n_bars)  # 平均0.01%每小时的回报
        prices = base_price * np.cumprod(1 + returns)
        
        market_data[symbol] = MarketSeries(
            symbol=symbol,
            timeframe="1h",
            ts=[int((datetime.now() - timedelta(hours=i)).timestamp()) for i in range(n_bars)][::-1],
            open=list(prices * 0.999),  # 开盘价略低于收盘价
            high=list(prices * 1.002),  # 最高价
            low=list(prices * 0.998),   # 最低价
            close=list(prices),         # 收盘价
            volume=list(np.random.lognormal(10, 1, n_bars))  # 交易量
        )
    
    print(f"✅ 加载 {len(market_data)} 个币种，每个 {n_bars} 根K线")
    return market_data

def run_simple_backtest(name, f2_weight, cost_model_type="calibrated"):
    """运行简单回测"""
    
    print(f"\n🚀 运行回测: {name}")
    print(f"  F2权重: {f2_weight*100:.0f}%")
    print(f"  成本模型: {cost_model_type}")
    
    try:
        from src.backtest.backtest_engine import BacktestEngine
        from src.core.pipeline import V5Pipeline
        from configs.schema import AppConfig
        from src.backtest.cost_factory import make_cost_model_from_cfg
        
        # 创建配置
        cfg = AppConfig(
            symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"],
            alpha={
                "long_top_pct": 0.20,
                "weights": {
                    "f1_mom_5d": 0.28,
                    "f2_mom_20d": f2_weight,
                    "f3_vol_adj_ret_20d": 0.24,
                    "f4_volume_expansion": 0.14,
                    "f5_rsi_trend_confirm": 0.14
                }
            },
            backtest={
                "fee_bps": 6.0,
                "slippage_bps": 5.0,
                "one_bar_delay": True,
                "cost_model": cost_model_type,
                "cost_stats_dir": "reports/cost_stats_clean",
                "fee_quantile": "p75",
                "slippage_quantile": "p90",
                "min_fills_global": 10,
                "min_fills_bucket": 5,
                "max_stats_age_days": 30
            }
        )
        
        # 创建成本模型
        cost_model = make_cost_model_from_cfg(cfg)
        
        # 创建回测引擎
        bt = BacktestEngine(
            fee_bps=6.0,
            slippage_bps=5.0,
            one_bar_delay=True,
            cost_model=cost_model,
            cost_model_meta={"mode": cost_model_type}
        )
        
        # 加载市场数据
        market_data = load_market_data()
        
        # 运行回测
        result = bt.run(market_data)
        
        print(f"  📈 回测结果:")
        print(f"    夏普比率: {result.sharpe:.3f}")
        print(f"    年化收益: {result.cagr*100:.2f}%")
        print(f"    最大回撤: {result.max_dd*100:.2f}%")
        print(f"    盈亏比: {result.profit_factor:.3f}")
        print(f"    换手率: {result.turnover*100:.2f}%")
        
        return {
            "name": name,
            "sharpe": result.sharpe,
            "cagr": result.cagr,
            "max_dd": result.max_dd,
            "profit_factor": result.profit_factor,
            "turnover": result.turnover,
            "cost_assumption": result.cost_assumption
        }
        
    except Exception as e:
        print(f"  ❌ 回测错误: {e}")
        import traceback
        traceback.print_exc()
        return None

def compare_results(results):
    """对比回测结果"""
    
    print("\n" + "=" * 60)
    print("📊 回测结果对比分析")
    print("=" * 60)
    
    valid_results = [r for r in results if r is not None]
    
    if not valid_results:
        print("❌ 无有效回测结果")
        return None
    
    # 创建对比表格
    comparison = []
    
    for result in valid_results:
        comparison.append({
            "策略": result["name"],
            "夏普比率": f"{result['sharpe']:.3f}",
            "年化收益%": f"{result['cagr']*100:.2f}%",
            "最大回撤%": f"{result['max_dd']*100:.2f}%",
            "盈亏比": f"{result['profit_factor']:.3f}",
            "换手率%": f"{result['turnover']*100:.2f}%",
        })
    
    # 显示对比表格
    df = pd.DataFrame(comparison)
    print(df.to_string(index=False))
    
    # 找出最佳策略
    if len(comparison) > 1:
        print(f"\n🎯 最佳策略分析:")
        
        # 按夏普比率排序
        best_sharpe = max(comparison, key=lambda x: float(x['夏普比率']))
        print(f"  最佳夏普: {best_sharpe['策略']} (夏普: {best_sharpe['夏普比率']})")
        
        # 按年化收益排序
        best_cagr = max(comparison, key=lambda x: float(x['年化收益%'].rstrip('%')))
        print(f"  最佳收益: {best_cagr['策略']} (收益: {best_cagr['年化收益%']})")
        
        # 按风险调整收益排序（夏普×收益）
        def risk_adjusted_score(x):
            sharpe = float(x['夏普比率'])
            ret = float(x['年化收益%'].rstrip('%'))
            return sharpe * ret if sharpe > 0 else -1000
        
        best_risk_adj = max(comparison, key=risk_adjusted_score)
        print(f"  最佳风险调整: {best_risk_adj['策略']} (夏普×收益: {risk_adjusted_score(best_risk_adj):.2f})")
    
    return df

def generate_profitability_report(results_df):
    """生成盈利能力报告"""
    
    print("\n" + "=" * 60)
    print("📋 盈利能力验证报告")
    print("=" * 60)
    
    print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"回测周期: 模拟30天数据")
    
    print(f"\n🎯 关键发现:")
    
    # 分析盈利能力
    profitable_strategies = []
    for _, row in results_df.iterrows():
        annual_return = float(row['年化收益%'].rstrip('%'))
        sharpe = float(row['夏普比率'])
        
        if annual_return > 0:
            profitable_strategies.append((row['策略'], annual_return, sharpe))
    
    if profitable_strategies:
        print(f"  ✅ 盈利策略: {len(profitable_strategies)}/{len(results_df)}")
        for strategy, ret, sharpe in profitable_strategies:
            print(f"    {strategy}: {ret:.2f}% (夏普: {sharpe:.3f})")
    else:
        print(f"  ⚠️ 所有策略在测试期间均未盈利")
    
    # 给出建议
    print(f"\n💡 优化建议:")
    
    if profitable_strategies:
        best_strategy = max(profitable_strategies, key=lambda x: x[1])  # 按收益
        print(f"  1. 推荐策略: {best_strategy[0]} (收益: {best_strategy[1]:.2f}%)")
        print(f"  2. 基于模拟数据，需要实际数据验证")
        print(f"  3. 监控F2权重调整的实际效果")
    else:
        print(f"  1. 检查策略逻辑和市场适应性")
        print(f"  2. 考虑调整因子权重或添加新因子")
        print(f"  3. 验证成本模型准确性")
    
    print(f"\n⚠️ 注意: 这是基于模拟数据的快速回测")
    print(f"💡 下一步: 使用真实历史数据进行完整回测")
    
    print("=" * 60)

def main():
    """主函数"""
    
    print("🚀 快速回测验证盈利能力")
    print("=" * 60)
    print("基于模拟数据的优化前后对比")
    print("=" * 60)
    
    # 定义回测组
    backtest_groups = [
        ("优化前_F2_25%_固定成本", 0.25, "fixed"),
        ("优化后_F2_20%_校准成本", 0.20, "calibrated"),
        ("激进_F2_15%_校准成本", 0.15, "calibrated"),
    ]
    
    results = []
    
    # 运行所有回测
    for name, f2_weight, cost_model in backtest_groups:
        result = run_simple_backtest(name, f2_weight, cost_model)
        results.append(result)
    
    # 对比结果
    results_df = compare_results(results)
    
    # 生成报告
    if results_df is not None:
        generate_profitability_report(results_df)
    
    print("\n✅ 快速回测完成")
    print("=" * 60)
    
    print("\n💡 下一步建议:")
    print("1. 使用真实历史数据进行完整回测")
    print("2. 运行walk-forward验证策略稳定性")
    print("3. 在实际交易中监控优化效果")

if __name__ == "__main__":
    main()