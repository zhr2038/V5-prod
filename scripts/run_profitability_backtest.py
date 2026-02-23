#!/usr/bin/env python3
"""
盈利能力回测验证脚本
对比优化前后的策略表现
"""

import subprocess
import json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys

def create_backtest_config(name, f2_weight, cost_model="calibrated"):
    """创建回测配置"""
    
    config_template = f"""
# 回测配置: {name}
symbols:
  - BTC/USDT
  - ETH/USDT
  - SOL/USDT
  - BNB/USDT
  - ADA/USDT
  - DOGE/USDT
  - XRP/USDT
  - DOT/USDT
  - LINK/USDT
  - AVAX/USDT

timeframe_main: 1h
timeframe_aux: 4h

exchange:
  name: okx
  testnet: false

universe:
  enabled: false  # 回测时禁用动态universe

alpha:
  long_top_pct: 0.20
  weights:
    f1_mom_5d: 0.28
    f2_mom_20d: {f2_weight}
    f3_vol_adj_ret_20d: 0.24
    f4_volume_expansion: 0.14
    f5_rsi_trend_confirm: 0.14

regime:
  atr_threshold: 0.02
  atr_very_low: 0.008
  pos_mult_trending: 1.0
  pos_mult_sideways: 0.7
  pos_mult_risk_off: 0.4

risk:
  max_single_weight: 0.20
  max_gross_exposure: 0.8
  drawdown_trigger: 0.06
  drawdown_delever: 0.50

rebalance:
  interval_minutes: 60
  deadband_sideways: 0.05
  deadband_trending: 0.03
  deadband_riskoff: 0.05

execution:
  mode: backtest
  dry_run: false
  fee_bps: 6
  slippage_bps: 5

backtest:
  start_date: "2026-01-18"  # 30天前
  end_date: "2026-02-17"    # 昨天
  fee_bps: 6
  slippage_bps: 5
  one_bar_delay: true
  walk_forward_folds: 4
  cost_model: {cost_model}
  cost_stats_dir: reports/cost_stats_clean
  fee_quantile: p75
  slippage_quantile: p90
  min_fills_global: 10
  min_fills_bucket: 5
  max_stats_age_days: 30
"""
    
    config_path = Path(f"configs/backtest_{name}.yaml")
    config_path.write_text(config_template, encoding="utf-8")
    
    return config_path

def run_backtest(config_path):
    """运行单个回测"""
    
    print(f"  🚀 运行回测: {config_path.stem}")
    
    try:
        # 运行回测命令
        cmd = [
            "python3", "main.py",
            "--config", str(config_path),
            "--backtest"
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=Path.cwd(),
            timeout=300  # 5分钟超时
        )
        
        if result.returncode == 0:
            print(f"    ✅ 回测成功")
            
            # 查找回测结果文件
            reports_dir = Path("reports")
            backtest_files = list(reports_dir.glob(f"backtest_*_{config_path.stem}*.json"))
            
            if backtest_files:
                latest_file = max(backtest_files, key=lambda x: x.stat().st_mtime)
                return latest_file
            else:
                print(f"    ⚠️ 未找到回测结果文件")
                return None
        else:
            print(f"    ❌ 回测失败: {result.stderr[:200]}")
            return None
            
    except subprocess.TimeoutExpired:
        print(f"    ⏱️ 回测超时")
        return None
    except Exception as e:
        print(f"    ❌ 回测错误: {e}")
        return None

def analyze_backtest_result(result_file):
    """分析回测结果"""
    
    if not result_file or not result_file.exists():
        return None
    
    try:
        with open(result_file, 'r') as f:
            data = json.load(f)
        
        # 提取关键指标
        metrics = data.get('metrics', {})
        trades = data.get('trades', [])
        
        analysis = {
            "total_return_pct": metrics.get('total_return_pct', 0),
            "sharpe": metrics.get('sharpe', 0),
            "max_drawdown_pct": metrics.get('max_drawdown_pct', 0),
            "win_rate": metrics.get('win_rate', 0),
            "profit_factor": metrics.get('profit_factor', 0),
            "total_trades": len(trades),
            "avg_trade_return_pct": metrics.get('avg_trade_return_pct', 0),
            "avg_win_pct": metrics.get('avg_win_pct', 0),
            "avg_loss_pct": metrics.get('avg_loss_pct', 0),
            "total_fees_usdt": metrics.get('total_fees_usdt', 0),
            "total_slippage_usdt": metrics.get('total_slippage_usdt', 0),
            "total_cost_usdt": metrics.get('total_cost_usdt', 0),
        }
        
        # 计算年化收益
        days = 30  # 30天回测
        annual_return = (1 + analysis["total_return_pct"]/100) ** (365/days) - 1
        analysis["annualized_return_pct"] = annual_return * 100
        
        # 计算成本占比
        if analysis["total_return_pct"] != 0:
            total_pnl = analysis["total_return_pct"] / 100  # 转换为小数
            cost_ratio = analysis["total_cost_usdt"] / (total_pnl * 10000) if total_pnl > 0 else 0
            analysis["cost_to_pnl_ratio"] = cost_ratio
        
        return analysis
        
    except Exception as e:
        print(f"    ❌ 分析错误: {e}")
        return None

def compare_backtest_results(results):
    """对比回测结果"""
    
    print("\n" + "=" * 60)
    print("📊 回测结果对比分析")
    print("=" * 60)
    
    if not results:
        print("❌ 无有效回测结果")
        return
    
    # 创建对比表格
    comparison = []
    
    for name, analysis in results.items():
        if analysis:
            comparison.append({
                "策略": name,
                "总收益%": f"{analysis['total_return_pct']:.2f}%",
                "年化收益%": f"{analysis['annualized_return_pct']:.2f}%",
                "夏普比率": f"{analysis['sharpe']:.2f}",
                "最大回撤%": f"{analysis['max_drawdown_pct']:.2f}%",
                "胜率": f"{analysis['win_rate']:.1f}%",
                "盈亏比": f"{analysis['profit_factor']:.2f}",
                "交易次数": analysis['total_trades'],
                "平均交易收益%": f"{analysis['avg_trade_return_pct']:.3f}%",
                "总成本(USDT)": f"${analysis['total_cost_usdt']:.2f}",
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
        
        # 按总收益排序
        best_return = max(comparison, key=lambda x: float(x['总收益%'].rstrip('%')))
        print(f"  最佳收益: {best_return['策略']} (收益: {best_return['总收益%']})")
        
        # 按风险调整收益排序（夏普×收益）
        def risk_adjusted_score(x):
            sharpe = float(x['夏普比率'])
            ret = float(x['总收益%'].rstrip('%'))
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
    print(f"回测周期: 2026-01-18 至 2026-02-17 (30天)")
    
    print(f"\n🎯 关键发现:")
    
    # 分析盈利能力
    profitable_strategies = []
    for _, row in results_df.iterrows():
        total_return = float(row['总收益%'].rstrip('%'))
        sharpe = float(row['夏普比率'])
        
        if total_return > 0:
            profitable_strategies.append((row['策略'], total_return, sharpe))
    
    if profitable_strategies:
        print(f"  ✅ 盈利策略: {len(profitable_strategies)}/{len(results_df)}")
        for strategy, ret, sharpe in profitable_strategies:
            print(f"    {strategy}: {ret:.2f}% (夏普: {sharpe:.2f})")
    else:
        print(f"  ❌ 所有策略在测试期间均未盈利")
    
    # 分析成本影响
    print(f"\n💰 成本影响分析:")
    
    # 这里可以添加成本对比分析
    # 例如：校准模型 vs 固定模型的成本差异
    
    # 给出建议
    print(f"\n💡 优化建议:")
    
    if profitable_strategies:
        best_strategy = max(profitable_strategies, key=lambda x: x[1])  # 按收益
        print(f"  1. 推荐策略: {best_strategy[0]} (收益: {best_strategy[1]:.2f}%)")
        print(f"  2. 监控实际交易验证回测结果")
        print(f"  3. 继续优化参数提升夏普比率")
    else:
        print(f"  1. 检查策略逻辑和市场适应性")
        print(f"  2. 考虑调整因子权重或添加新因子")
        print(f"  3. 验证成本模型准确性")
        print(f"  4. 测试不同市场状态下的表现")
    
    print(f"\n🚀 下一步:")
    print(f"  1. 运行更长时间的回测(90天)")
    print(f"  2. 测试不同参数组合")
    print(f"  3. 验证在实际交易中的表现")
    print(f"  4. 建立持续的回测验证流程")
    
    print("=" * 60)

def main():
    """主函数"""
    
    print("🚀 开始盈利能力回测验证")
    print("=" * 60)
    print("对比优化前后的策略表现")
    print("=" * 60)
    
    # 定义回测组
    backtest_groups = [
        ("优化前_F2_25%_固定成本", 0.25, "fixed"),
        ("优化后_F2_20%_校准成本", 0.20, "calibrated"),
        ("激进_F2_15%_校准成本", 0.15, "calibrated"),
    ]
    
    results = {}
    
    # 运行所有回测
    for name, f2_weight, cost_model in backtest_groups:
        print(f"\n📊 测试组: {name}")
        print(f"  F2权重: {f2_weight*100:.0f}%")
        print(f"  成本模型: {cost_model}")
        
        # 创建配置
        config_path = create_backtest_config(name, f2_weight, cost_model)
        print(f"  ✅ 创建配置: {config_path}")
        
        # 运行回测
        result_file = run_backtest(config_path)
        
        # 分析结果
        if result_file:
            analysis = analyze_backtest_result(result_file)
            if analysis:
                results[name] = analysis
                print(f"  📈 分析完成")
            else:
                print(f"  ⚠️ 分析失败")
        else:
            print(f"  ❌ 回测失败")
    
    # 对比结果
    if results:
        results_df = compare_backtest_results(results)
        
        # 生成报告
        if results_df is not None:
            generate_profitability_report(results_df)
    else:
        print("\n❌ 所有回测均失败，请检查系统")
    
    print("\n✅ 回测验证完成")
    print("=" * 60)

if __name__ == "__main__":
    main()