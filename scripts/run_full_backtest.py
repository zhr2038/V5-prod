#!/usr/bin/env python3
"""
V5完整回测执行脚本
运行优化前后的策略对比回测
"""

import json
import yaml
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys
import subprocess
import time

sys.path.append(str(Path(__file__).resolve().parents[1]))

def check_market_data_availability():
    """检查市场数据可用性"""
    
    print("📊 检查市场数据可用性")
    print("-" * 40)
    
    # 检查数据目录
    data_dirs = [
        "market_data",
        "reports/market_data",
        "data"
    ]
    
    available_data = []
    for data_dir in data_dirs:
        if Path(data_dir).exists():
            # 检查文件
            files = list(Path(data_dir).glob("*.csv")) + list(Path(data_dir).glob("*.parquet")) + list(Path(data_dir).glob("*.db"))
            if files:
                available_data.append((data_dir, len(files)))
    
    if available_data:
        print("✅ 找到市场数据:")
        for dir_path, file_count in available_data:
            print(f"  {dir_path}: {file_count}个文件")
        return True
    else:
        print("❌ 未找到市场数据文件")
        print("💡 需要先收集历史数据")
        return False

def collect_historical_data_if_needed():
    """如果需要，收集历史数据"""
    
    print("\n📥 检查历史数据收集需求")
    print("-" * 40)
    
    # 检查是否有数据收集脚本
    data_collector_scripts = [
        "scripts/collect_market_data.py",
        "scripts/fetch_historical_data.py",
        "scripts/data_collector.py"
    ]
    
    for script_path in data_collector_scripts:
        if Path(script_path).exists():
            print(f"✅ 找到数据收集脚本: {script_path}")
            
            # 询问是否运行数据收集
            print("\n💡 需要历史数据来回测")
            print("建议收集1-2个月的1小时K线数据")
            print("立即收集数据？(y/N)")
            
            response = input().lower()
            if response == 'y':
                print(f"🚀 运行数据收集: {script_path}")
                try:
                    result = subprocess.run(
                        ["python3", script_path, "--days", "60", "--timeframe", "1h"],
                        capture_output=True,
                        text=True,
                        timeout=300  # 5分钟超时
                    )
                    if result.returncode == 0:
                        print("✅ 数据收集成功")
                        return True
                    else:
                        print(f"❌ 数据收集失败: {result.stderr[:200]}")
                        return False
                except subprocess.TimeoutExpired:
                    print("⏱️ 数据收集超时")
                    return False
                except Exception as e:
                    print(f"❌ 数据收集错误: {e}")
                    return False
            else:
                print("⏸️ 跳过数据收集，使用现有数据")
                return True
    
    print("❌ 未找到数据收集脚本")
    print("💡 需要手动准备历史数据")
    return False

def create_backtest_config_for_group(group_name, alpha_weights, cost_model):
    """为测试组创建回测配置"""
    
    print(f"\n🔧 创建回测配置: {group_name}")
    
    # 加载基础配置
    base_config_path = "configs/full_backtest_optimized.yaml"
    with open(base_config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 更新alpha权重
    config['alpha']['weights'] = alpha_weights
    
    # 更新成本模型
    config['backtest']['cost_model'] = cost_model
    if cost_model == "fixed":
        config['backtest']['cost_stats_dir'] = None
    
    # 设置回测组标识
    config['backtest']['test_group'] = group_name
    config['backtest']['output_dir'] = f"reports/full_backtest_results/{group_name}"
    
    # 保存配置
    config_path = f"configs/backtest_{group_name}.yaml"
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print(f"  ✅ 创建配置: {config_path}")
    return config_path

def run_single_backtest(config_path):
    """运行单个回测"""
    
    group_name = Path(config_path).stem.replace("backtest_", "")
    print(f"\n🚀 运行回测: {group_name}")
    
    output_dir = f"reports/full_backtest_results/{group_name}"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 记录开始时间
    start_time = datetime.now()
    
    try:
        # 尝试使用现有的回测功能
        from src.backtest.backtest_engine import BacktestEngine
        from src.backtest.cost_factory import make_cost_model_from_cfg
        from configs.loader import load_config
        from src.core.pipeline import V5Pipeline
        
        print(f"  📊 加载配置...")
        cfg = load_config(config_path)
        
        print(f"  💰 创建成本模型...")
        cost_model = make_cost_model_from_cfg(cfg)
        
        print(f"  🔧 创建回测引擎...")
        bt = BacktestEngine(
            fee_bps=cfg.backtest.fee_bps,
            slippage_bps=cfg.backtest.slippage_bps,
            one_bar_delay=cfg.backtest.one_bar_delay,
            cost_model=cost_model,
            cost_model_meta={"mode": cfg.backtest.cost_model}
        )
        
        print(f"  📈 加载市场数据...")
        # 这里需要实际加载市场数据
        # 暂时使用模拟数据
        from src.core.models import MarketSeries
        
        symbols = cfg.symbols
        market_data = {}
        
        # 创建模拟数据（简化）
        n_bars = 30 * 24  # 30天每小时数据
        for symbol in symbols[:4]:  # 只测试前4个币种以加快速度
            np.random.seed(42)
            base_price = 1000 if "BTC" in symbol else 100
            returns = np.random.normal(0.0005, 0.02, n_bars)  # 平均0.05%每小时的回报
            prices = base_price * np.cumprod(1 + returns)
            
            market_data[symbol] = MarketSeries(
                symbol=symbol,
                timeframe="1h",
                ts=[int((datetime.now() - timedelta(hours=i)).timestamp()) for i in range(n_bars)][::-1],
                open=list(prices * 0.999),
                high=list(prices * 1.002),
                low=list(prices * 0.998),
                close=list(prices),
                volume=list(np.random.lognormal(10, 1, n_bars))
            )
        
        print(f"  ⚙️ 运行回测...")
        pipeline = V5Pipeline(cfg)
        result = bt.run(market_data, pipeline=pipeline)
        
        # 计算运行时间
        run_time = (datetime.now() - start_time).total_seconds()
        
        print(f"  ✅ 回测完成 ({run_time:.1f}秒)")
        print(f"    夏普比率: {result.sharpe:.3f}")
        print(f"    年化收益: {result.cagr*100:.2f}%")
        print(f"    最大回撤: {result.max_dd*100:.2f}%")
        print(f"    盈亏比: {result.profit_factor:.3f}")
        print(f"    换手率: {result.turnover*100:.2f}%")
        
        # 保存结果
        result_dict = {
            "group_name": group_name,
            "timestamp": datetime.now().isoformat(),
            "run_time_seconds": run_time,
            "sharpe": result.sharpe,
            "cagr": result.cagr,
            "max_dd": result.max_dd,
            "profit_factor": result.profit_factor,
            "turnover": result.turnover,
            "cost_assumption": result.cost_assumption,
            "market_data_symbols": len(market_data),
            "market_data_bars": n_bars
        }
        
        result_file = Path(output_dir) / "backtest_result.json"
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, indent=2)
        
        return result_dict
        
    except ImportError as e:
        print(f"  ❌ 导入错误: {e}")
        print(f"  💡 回测功能可能需要完善")
        return None
    except Exception as e:
        print(f"  ❌ 回测错误: {e}")
        import traceback
        traceback.print_exc()
        return None

def compare_backtest_results(results):
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
            "策略组": result["group_name"],
            "夏普比率": f"{result['sharpe']:.3f}",
            "年化收益%": f"{result['cagr']*100:.2f}%",
            "最大回撤%": f"{result['max_dd']*100:.2f}%",
            "盈亏比": f"{result['profit_factor']:.3f}",
            "换手率%": f"{result['turnover']*100:.2f}%",
            "运行时间(秒)": f"{result['run_time_seconds']:.1f}",
        })
    
    # 显示对比表格
    df = pd.DataFrame(comparison)
    print(df.to_string(index=False))
    
    # 找出最佳策略
    if len(comparison) > 1:
        print(f"\n🎯 最佳策略分析:")
        
        # 按夏普比率排序
        best_sharpe = max(comparison, key=lambda x: float(x['夏普比率']))
        print(f"  最佳夏普: {best_sharpe['策略组']} (夏普: {best_sharpe['夏普比率']})")
        
        # 按年化收益排序
        best_cagr = max(comparison, key=lambda x: float(x['年化收益%'].rstrip('%')))
        print(f"  最佳收益: {best_cagr['策略组']} (收益: {best_cagr['年化收益%']})")
        
        # 按风险调整收益排序
        def risk_adjusted_score(x):
            sharpe = float(x['夏普比率'])
            ret = float(x['年化收益%'].rstrip('%'))
            return sharpe * ret if sharpe > 0 else -1000
        
        best_risk_adj = max(comparison, key=risk_adjusted_score)
        print(f"  最佳风险调整: {best_risk_adj['策略组']} (夏普×收益: {risk_adjusted_score(best_risk_adj):.2f})")
    
    return df

def generate_backtest_report(results_df):
    """生成回测报告"""
    
    print("\n" + "=" * 60)
    print("📋 V5完整回测验证报告")
    print("=" * 60)
    
    print(f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"回测周期: 模拟30天数据 (1小时K线)")
    print(f"测试币种: BTC/USDT, ETH/USDT, SOL/USDT, BNB/USDT")
    
    if results_df is not None:
        print(f"\n📊 回测结果摘要:")
        print(f"  测试组数量: {len(results_df)}")
        
        # 分析盈利能力
        profitable_groups = []
        for _, row in results_df.iterrows():
            annual_return = float(row['年化收益%'].rstrip('%'))
            sharpe = float(row['夏普比率'])
            
            if annual_return > 0:
                profitable_groups.append((row['策略组'], annual_return, sharpe))
        
        if profitable_groups:
            print(f"  ✅ 盈利策略组: {len(profitable_groups)}/{len(results_df)}")
            for group, ret, sharpe in profitable_groups:
                print(f"    {group}: {ret:.2f}% (夏普: {sharpe:.3f})")
        else:
            print(f"  ⚠️ 所有策略组在测试期间均未盈利")
        
        # F2优化效果分析
        print(f"\n🎯 F2优化效果分析:")
        f2_groups = [row for _, row in results_df.iterrows() if "F2" in row['策略组']]
        
        if len(f2_groups) >= 2:
            # 找到F2权重不同的组
            f2_weights = {}
            for row in f2_groups:
                if "25%" in row['策略组']:
                    f2_weights[25] = row
                elif "20%" in row['策略组']:
                    f2_weights[20] = row
                elif "15%" in row['策略组']:
                    f2_weights[15] = row
            
            if len(f2_weights) >= 2:
                print(f"  F2权重对比:")
                for weight, row in sorted(f2_weights.items()):
                    ret = float(row['年化收益%'].rstrip('%'))
                    sharpe = float(row['夏普比率'])
                    print(f"    F2 {weight}%: {ret:.2f}% (夏普: {sharpe:.3f})")
    
    print(f"\n⚠️ 注意: 这是基于模拟数据的回测")
    print(f"💡 下一步建议:")
    print(f"  1. 收集真实历史数据进行回测")
    print(f"  2. 运行Walk-forward验证策略稳定性")
    print(f"  3. 基于回测结果进一步优化参数")
    print(f"  4. 在实际交易中验证优化效果")
    
    print("=" * 60)

def main():
    """主函数"""
    
    print("🚀 V5完整回测验证 - 选项C")
    print("=" * 60)
    print("运行优化前后的策略对比回测")
    print("=" * 60)
    
    # 1. 检查数据可用性
    if not check_market_data_availability():
        if not collect_historical_data_if_needed():
            print("\n❌ 无法进行回测：缺少市场数据")
            print("💡 请先收集历史数据或准备数据文件")
            return
    
    # 2. 定义测试组
    test_groups = [
        {
            "name": "优化前_F2_25%_固定成本",
            "alpha_weights": {
                "f1_mom_5d": 0.30,
                "f2_mom_20d": 0.25,
                "f3_vol_adj_ret_20d": 0.25,
                "f4_volume_expansion": 0.10,
                "f5_rsi_trend_confirm": 0.10
            },
            "cost_model": "fixed"
        },
        {
            "name": "优化后_F2_20%_校准成本",
            "alpha_weights": {
                "f1_mom_5d": 0.28,
                "f2_mom_20d": 0.20,
                "f3_vol_adj_ret_20d": 0.24,
                "f4_volume_expansion": 0.14,
                "f5_rsi_trend_confirm": 0.14
            },
            "cost_model": "calibrated"
        },
        {
            "name": "激进_F2_15%_校准成本",
            "alpha_weights": {
                "f1_mom_5d": 0.29,
                "f2_mom_20d": 0.15,
                "f3_vol_adj_ret_20d": 0.26,
                "f4_volume_expansion": 0.15,
                "f5_rsi_trend_confirm": 0.15
            },
            "cost_model": "calibrated"
        }
    ]
    
    # 3. 运行所有回测
    results = []
    
    for group in test_groups:
        # 创建配置
        config_path = create_backtest_config_for_group(
            group["name"],
            group["alpha_weights"],
            group["cost_model"]
        )
        
        # 运行回测
        result = run_single_backtest(config_path)
        results.append(result)
    
    # 4. 对比结果
    results_df = compare_backtest_results(results)
    
    # 5. 生成报告
    if results_df is not None:
        generate_backtest_report(results_df)
    
    print("\n✅ V5完整回测完成")
    print("=" * 60)
    
    print("\n💡 总结:")
    print("基于模拟数据的回测已完成，可以查看各策略组表现")
    print("建议下一步收集真实历史数据进行更准确的回测")

if __name__ == "__main__":
    main()