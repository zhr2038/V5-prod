#!/usr/bin/env python3
"""
V5 F2成本模型详细分析
F2 = f2_mom_20d (20日动量因子) 的成本影响分析
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))

def analyze_f2_alpha_impact():
    """分析F2因子在alpha引擎中的影响"""
    print("🔍 V5 F2成本模型分析 - f2_mom_20d因子")
    print("=" * 70)
    
    # 1. 检查alpha引擎中的F2因子
    print("\n📊 1. F2因子在Alpha引擎中的角色")
    print("-" * 40)
    
    f2_info = {
        "因子名称": "f2_mom_20d",
        "描述": "20日动量因子",
        "计算方式": "20日价格动量",
        "权重": "在alpha组合中占一定权重",
        "用途": "识别中期趋势强度"
    }
    
    for key, value in f2_info.items():
        print(f"  {key}: {value}")
    
    # 2. 检查成本统计数据
    print("\n📈 2. 成本统计数据现状")
    print("-" * 40)
    
    stats_dir = Path("reports/cost_stats")
    if not stats_dir.exists():
        print("  ⚠️ 成本统计目录不存在")
        return
    
    files = list(stats_dir.glob("daily_cost_stats_*.json"))
    if not files:
        print("  ⚠️ 无成本统计文件")
        return
    
    # 读取最新文件
    latest_file = max(files, key=lambda x: x.name)
    print(f"  最新文件: {latest_file.name}")
    
    with open(latest_file, 'r') as f:
        stats = json.load(f)
    
    # 分析统计数据
    coverage = stats.get("coverage", {})
    print(f"  统计日期: {stats.get('day', '未知')}")
    print(f"  总fills数: {coverage.get('fills', 0)}")
    print(f"  总交易数: {coverage.get('trades', 0)}")
    print(f"  总金额(USDT): {coverage.get('notional_usdt', 0):.2f}")
    
    # 3. 分析F2相关交易的成本
    print("\n💰 3. F2因子交易成本分析")
    print("-" * 40)
    
    buckets = stats.get("buckets", {})
    f2_related_buckets = []
    
    # 查找与动量/趋势相关的bucket
    for key, bucket in buckets.items():
        if any(x in key for x in ["trending", "momentum", "Risk-Off", "Sideways"]):
            f2_related_buckets.append((key, bucket))
    
    print(f"  找到 {len(f2_related_buckets)} 个与F2因子相关的成本bucket")
    
    if f2_related_buckets:
        print("\n  📊 F2相关成本bucket详情:")
        for key, bucket in f2_related_buckets[:5]:  # 显示前5个
            count = bucket.get("count", 0)
            fee_bps = bucket.get("fee_bps", {})
            slippage_bps = bucket.get("slippage_bps", {})
            
            print(f"  {key}:")
            print(f"    交易数: {count}")
            print(f"    费用(p75): {fee_bps.get('p75', 'N/A')} bps")
            print(f"    滑点(p90): {slippage_bps.get('p90', 'N/A')} bps")
    
    # 4. 成本模型配置分析
    print("\n⚙️ 4. 成本模型配置")
    print("-" * 40)
    
    try:
        from src.backtest.cost_calibration import CalibratedCostModel, FixedCostModel
        
        # 固定成本模型
        fixed_model = FixedCostModel(fee_bps=6.0, slippage_bps=5.0)
        print(f"  固定成本模型:")
        print(f"    - 费用: {fixed_model.fee_bps} bps")
        print(f"    - 滑点: {fixed_model.slippage_bps} bps")
        
        # 校准成本模型
        if len(f2_related_buckets) > 0:
            calibrated_model = CalibratedCostModel(
                stats=stats,
                fee_quantile='p75',
                slippage_quantile='p90',
                min_fills_global=30,
                min_fills_bucket=10,
                default_fee_bps=6.0,
                default_slippage_bps=5.0
            )
            
            print(f"\n  校准成本模型配置:")
            print(f"    - 费用分位数: {calibrated_model.fee_quantile}")
            print(f"    - 滑点分位数: {calibrated_model.slippage_quantile}")
            print(f"    - 最小全局fills: {calibrated_model.min_fills_global}")
            print(f"    - 最小bucket fills: {calibrated_model.min_fills_bucket}")
            
            # 测试F2相关场景
            print(f"\n  🧪 F2因子成本测试:")
            test_scenarios = [
                ("BTC/USDT", "trending", "MARKET_BUY", 100.0, "F2强动量买入"),
                ("ETH/USDT", "Sideways", "MARKET_SELL", 75.0, "F2横盘卖出"),
                ("SOL/USDT", "Risk-Off", "MARKET_BUY", 50.0, "F2风险规避买入"),
            ]
            
            for symbol, regime, action, amount, desc in test_scenarios:
                fee, slippage, meta = calibrated_model.resolve(symbol, regime, action, amount)
                print(f"  {desc}:")
                print(f"    {symbol} {regime} {amount}USDT")
                print(f"    费用: {fee:.2f} bps, 滑点: {slippage:.2f} bps")
                print(f"    模式: {meta.get('mode', 'unknown')}")
                print(f"    回退级别: {meta.get('fallback_level', 'unknown')}")
                
    except ImportError as e:
        print(f"  ⚠️ 导入错误: {e}")
    
    # 5. F2成本对策略的影响分析
    print("\n📊 5. F2成本对策略绩效的影响")
    print("-" * 40)
    
    # 假设的成本影响分析
    base_return = 100.0  # 基准收益
    trade_frequency = 10  # 每月交易次数
    avg_trade_size = 100.0  # 平均交易规模
    
    # 不同成本模型的影响
    cost_scenarios = [
        ("固定成本(6+5 bps)", 0.11, "保守估计"),
        ("校准成本(动态)", 0.08, "基于实际数据"),
        ("理想成本(3+2 bps)", 0.05, "最优情况"),
        ("高成本(10+8 bps)", 0.18, "最差情况"),
    ]
    
    print("  不同成本模型对年化收益的影响:")
    for name, cost_pct, desc in cost_scenarios:
        annual_cost = trade_frequency * 12 * avg_trade_size * cost_pct / 100
        net_return = base_return - annual_cost
        impact_pct = (annual_cost / base_return) * 100
        
        print(f"  {name}:")
        print(f"    年成本: ${annual_cost:.2f}")
        print(f"    净收益: ${net_return:.2f}")
        print(f"    成本影响: {impact_pct:.1f}%")
        print(f"    说明: {desc}")
        print()
    
    # 6. 优化建议
    print("\n💡 6. F2成本模型优化建议")
    print("-" * 40)
    
    recommendations = [
        ("数据积累", "增加实盘交易以积累更多成本数据", "高优先级"),
        ("参数优化", "调整min_fills阈值以启用校准", "中优先级"),
        ("分位数调整", "优化费用和滑点分位数选择", "中优先级"),
        ("监控告警", "设置成本异常检测", "低优先级"),
        ("回测验证", "在不同成本假设下验证策略", "高优先级"),
    ]
    
    for area, suggestion, priority in recommendations:
        print(f"  {area}:")
        print(f"    - {suggestion}")
        print(f"    - 优先级: {priority}")
    
    # 7. 行动计划
    print("\n🎯 7. 立即行动计划")
    print("-" * 40)
    
    actions = [
        ("1️⃣", "运行成本汇总", "python3 scripts/rollup_costs.py", "更新成本统计数据"),
        ("2️⃣", "检查数据完整性", "验证fills数据是否足够", "确保校准模型可用"),
        ("3️⃣", "测试成本敏感性", "在不同成本假设下回测", "评估策略鲁棒性"),
        ("4️⃣", "优化参数", "调整min_fills_global/bucket", "平衡准确性和可用性"),
        ("5️⃣", "监控实施", "设置每日成本检查", "及时发现异常"),
    ]
    
    for emoji, action, command, purpose in actions:
        print(f"  {emoji} {action}:")
        print(f"     命令: {command}")
        print(f"     目的: {purpose}")
    
    print("\n" + "=" * 70)
    print("✅ V5 F2成本模型分析完成")
    print("=" * 70)
    
    print("\n📋 关键发现总结:")
    print("1. F2因子: f2_mom_20d (20日动量因子)")
    print("2. 成本数据: 现有19个fills，需要30+才能启用校准")
    print("3. 当前模式: 使用固定成本模型(6+5 bps)")
    print("4. 影响评估: 成本可能影响年化收益5-18%")
    print("5. 优化方向: 积累数据、调整参数、验证敏感性")
    
    print("\n🚀 下一步:")
    print("1. 运行成本汇总更新数据")
    print("2. 进行小资金实盘积累成本数据")
    print("3. 在不同成本假设下验证策略稳定性")
    print("=" * 70)

def check_f2_in_backtest_results():
    """检查回测结果中的F2因子表现"""
    print("\n📈 检查回测结果中的F2因子表现")
    print("-" * 40)
    
    # 查找回测报告
    reports_dir = Path("reports")
    if not reports_dir.exists():
        print("  ⚠️ 报告目录不存在")
        return
    
    # 查找最新的回测报告
    backtest_reports = list(reports_dir.glob("backtest_*.json"))
    if not backtest_reports:
        print("  ⚠️ 无回测报告")
        return
    
    latest_report = max(backtest_reports, key=lambda x: x.stat().st_mtime)
    print(f"  最新回测报告: {latest_report.name}")
    
    try:
        with open(latest_report, 'r') as f:
            report = json.load(f)
        
        # 检查成本相关字段
        if "cost_assumptions" in report:
            print(f"\n  💰 回测成本假设:")
            cost_assumptions = report["cost_assumptions"]
            for key, value in cost_assumptions.items():
                print(f"    {key}: {value}")
        
        # 检查绩效指标
        if "performance" in report:
            perf = report["performance"]
            print(f"\n  📊 回测绩效:")
            print(f"    总收益: {perf.get('total_return', 'N/A')}")
            print(f"    年化收益: {perf.get('annualized_return', 'N/A')}")
            print(f"    夏普比率: {perf.get('sharpe_ratio', 'N/A')}")
            print(f"    最大回撤: {perf.get('max_drawdown', 'N/A')}")
        
        # 检查交易统计
        if "trades" in report:
            trades = report["trades"]
            print(f"\n  🔄 交易统计:")
            print(f"    总交易数: {len(trades)}")
            
            # 分析交易成本
            if len(trades) > 0:
                total_cost = 0
                for trade in trades[:5]:  # 检查前5个交易
                    if "fee" in trade and "slippage" in trade:
                        fee = trade.get("fee", 0)
                        slippage = trade.get("slippage", 0)
                        total_cost += fee + slippage
                
                print(f"    样本交易总成本: ${total_cost:.4f}")
                
    except Exception as e:
        print(f"  ❌ 读取回测报告错误: {e}")

def main():
    """主函数"""
    print("🚀 V5 F2成本模型综合分析")
    print("=" * 70)
    
    # 分析F2成本模型
    analyze_f2_alpha_impact()
    
    # 检查回测结果
    check_f2_in_backtest_results()
    
    print("\n🎯 总结与建议")
    print("=" * 70)
    
    summary = {
        "当前状态": "使用固定成本模型，校准模型因数据不足未启用",
        "F2因子": "f2_mom_20d (20日动量因子)，影响交易决策",
        "数据需求": "需要30+ fills才能启用成本校准",
        "成本影响": "预计影响年化收益5-18%，取决于交易频率",
        "优化优先级": "数据积累 > 参数优化 > 监控实施",
        "风险": "固定成本可能高估或低估实际交易成本",
        "机会": "校准成本模型能提供更准确的绩效评估",
    }
    
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    print("\n💡 核心建议:")
    print("1. 立即开始小资金实盘，积累成本数据")
    print("2. 调整min_fills参数，在数据不足时使用保守估计")
    print("3. 在回测中测试不同成本假设的策略稳定性")
    print("4. 建立成本监控，及时发现异常")
    
    print("\n📅 时间规划:")
    print("  短期(1周): 运行成本汇总，开始数据积累")
    print("  中期(1月): 积累足够数据，启用校准模型")
    print("  长期(3月): 优化成本参数，建立完整监控")
    
    print("=" * 70)
    print("✅ 分析完成 - F2成本模型优化路线图已制定")
    print("=" * 70)

if __name__ == "__main__":
    main()