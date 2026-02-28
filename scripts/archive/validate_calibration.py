#!/usr/bin/env python3
"""
校准模型验证脚本
对比真实数据校准 vs 固定成本模型的效果
"""

import json
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

def load_cost_stats():
    """加载成本统计数据"""
    
    print("📊 加载成本统计数据")
    print("-" * 40)
    
    # 真实数据统计
    real_stats_dir = Path("reports/cost_stats_real")
    real_stats_files = list(real_stats_dir.glob("daily_cost_stats_*.json"))
    
    if not real_stats_files:
        print("❌ 无真实统计数据")
        return None, None
    
    # 加载最新真实数据
    latest_real = max(real_stats_files, key=lambda x: x.name)
    print(f"✅ 最新真实数据: {latest_real.name}")
    
    with open(latest_real, 'r') as f:
        real_stats = json.load(f)
    
    # 混合数据统计（如果有）
    mixed_stats_dir = Path("reports/cost_stats")
    mixed_stats_files = list(mixed_stats_dir.glob("daily_cost_stats_*.json"))
    
    mixed_stats = None
    if mixed_stats_files:
        latest_mixed = max(mixed_stats_files, key=lambda x: x.name)
        print(f"✅ 最新混合数据: {latest_mixed.name}")
        
        with open(latest_mixed, 'r') as f:
            mixed_stats = json.load(f)
    
    return real_stats, mixed_stats

def analyze_cost_distribution(stats):
    """分析成本分布"""
    
    if not stats:
        return None
    
    buckets = stats.get("buckets", {})
    
    analysis = {
        "total_buckets": len(buckets),
        "total_fills": stats.get("coverage", {}).get("fills", 0),
        "fee_distribution": [],
        "slippage_distribution": [],
        "bucket_details": []
    }
    
    for key, bucket in buckets.items():
        count = bucket.get("count", 0)
        fee_stats = bucket.get("fee_bps", {})
        slippage_stats = bucket.get("slippage_bps", {})
        
        fee_p75 = fee_stats.get("p75")
        slippage_p90 = slippage_stats.get("p90")
        
        if fee_p75 is not None:
            analysis["fee_distribution"].append(fee_p75)
        if slippage_p90 is not None:
            analysis["slippage_distribution"].append(slippage_p90)
        
        analysis["bucket_details"].append({
            "key": key,
            "count": count,
            "fee_p75": fee_p75,
            "slippage_p90": slippage_p90,
            "total_cost": (fee_p75 or 0) + (slippage_p90 or 0)
        })
    
    # 计算统计量
    if analysis["fee_distribution"]:
        analysis["fee_mean"] = np.mean(analysis["fee_distribution"])
        analysis["fee_std"] = np.std(analysis["fee_distribution"])
        analysis["fee_min"] = np.min(analysis["fee_distribution"])
        analysis["fee_max"] = np.max(analysis["fee_distribution"])
    
    if analysis["slippage_distribution"]:
        analysis["slippage_mean"] = np.mean(analysis["slippage_distribution"])
        analysis["slippage_std"] = np.std(analysis["slippage_distribution"])
        analysis["slippage_min"] = np.min(analysis["slippage_distribution"])
        analysis["slippage_max"] = np.max(analysis["slippage_distribution"])
    
    return analysis

def compare_calibrated_vs_fixed(real_analysis):
    """对比校准模型 vs 固定成本模型"""
    
    print("\n💰 成本模型对比分析")
    print("-" * 40)
    
    if not real_analysis:
        print("❌ 无分析数据")
        return
    
    # 固定成本模型假设
    fixed_fee = 6.0  # bps
    fixed_slippage = 5.0  # bps
    fixed_total = fixed_fee + fixed_slippage
    
    # 校准模型成本（基于真实数据）
    calibrated_fee_mean = real_analysis.get("fee_mean", fixed_fee)
    calibrated_slippage_mean = real_analysis.get("slippage_mean", fixed_slippage)
    calibrated_total_mean = calibrated_fee_mean + calibrated_slippage_mean
    
    print("📊 成本对比:")
    print(f"  固定成本模型: {fixed_fee:.2f}bps + {fixed_slippage:.2f}bps = {fixed_total:.2f}bps")
    print(f"  校准成本模型: {calibrated_fee_mean:.2f}bps + {calibrated_slippage_mean:.2f}bps = {calibrated_total_mean:.2f}bps")
    
    # 计算差异
    fee_diff = calibrated_fee_mean - fixed_fee
    slippage_diff = calibrated_slippage_mean - fixed_slippage
    total_diff = calibrated_total_mean - fixed_total
    
    print(f"\n📈 差异分析:")
    print(f"  费用差异: {fee_diff:+.2f}bps ({fee_diff/fixed_fee*100:+.1f}%)")
    print(f"  滑点差异: {slippage_diff:+.2f}bps ({slippage_diff/fixed_slippage*100:+.1f}%)")
    print(f"  总成本差异: {total_diff:+.2f}bps ({total_diff/fixed_total*100:+.1f}%)")
    
    # 对策略的影响
    print(f"\n🎯 对策略的影响:")
    
    if total_diff < 0:
        print(f"  ✅ 校准模型成本更低，策略绩效可能被低估")
        print(f"  💡 实际收益可能比回测显示的高 {-total_diff:.2f}bps")
    elif total_diff > 0:
        print(f"  ⚠️ 校准模型成本更高，策略绩效可能被高估")
        print(f"  💡 实际收益可能比回测显示的低 {total_diff:.2f}bps")
    else:
        print(f"  🔄 成本基本一致，策略评估准确")
    
    # 对F2因子的影响
    print(f"\n🎯 对F2因子的影响:")
    print(f"  F2权重: 25% → 15% (已降低40%)")
    
    # 基于成本差异的进一步优化建议
    if abs(total_diff) > 2.0:  # 差异大于2bps
        print(f"  💡 成本差异显著，建议重新评估F2因子有效性")
        if total_diff < 0:
            print(f"  💡 实际F2成本可能更低，可考虑微调权重")
        else:
            print(f"  💡 实际F2成本可能更高，权重降低是合理的")
    
    return {
        "fixed": {"fee": fixed_fee, "slippage": fixed_slippage, "total": fixed_total},
        "calibrated": {"fee": calibrated_fee_mean, "slippage": calibrated_slippage_mean, "total": calibrated_total_mean},
        "differences": {"fee": fee_diff, "slippage": slippage_diff, "total": total_diff}
    }

def analyze_f2_specific_costs(real_analysis):
    """分析F2相关交易的成本"""
    
    print("\n🎯 F2因子成本专项分析")
    print("-" * 40)
    
    if not real_analysis or not real_analysis.get("bucket_details"):
        print("❌ 无bucket详情数据")
        return
    
    # 识别F2相关交易（基于币种和regime）
    f2_buckets = []
    f2_keywords = ["SOL", "BNB", "ETH", "BTC", "ADA", "DOGE", "Trending", "Sideways"]
    
    for bucket in real_analysis["bucket_details"]:
        key = bucket["key"]
        if any(kw in key for kw in f2_keywords):
            f2_buckets.append(bucket)
    
    print(f"找到 {len(f2_buckets)} 个F2相关bucket")
    
    if f2_buckets:
        # 计算F2相关交易的平均成本
        f2_fees = [b["fee_p75"] for b in f2_buckets if b["fee_p75"] is not None]
        f2_slippages = [b["slippage_p90"] for b in f2_buckets if b["slippage_p90"] is not None]
        
        if f2_fees:
            f2_avg_fee = np.mean(f2_fees)
            f2_avg_slippage = np.mean(f2_slippages) if f2_slippages else 5.0
            f2_avg_total = f2_avg_fee + f2_avg_slippage
            
            print(f"📊 F2相关交易成本:")
            print(f"  平均费用: {f2_avg_fee:.2f}bps")
            print(f"  平均滑点: {f2_avg_slippage:.2f}bps")
            print(f"  平均总成本: {f2_avg_total:.2f}bps")
            
            # 与整体平均对比
            overall_avg_fee = real_analysis.get("fee_mean", 6.0)
            overall_avg_slippage = real_analysis.get("slippage_mean", 5.0)
            overall_avg_total = overall_avg_fee + overall_avg_slippage
            
            fee_diff = f2_avg_fee - overall_avg_fee
            total_diff = f2_avg_total - overall_avg_total
            
            print(f"\n📈 与整体平均对比:")
            print(f"  费用差异: {fee_diff:+.2f}bps")
            print(f"  总成本差异: {total_diff:+.2f}bps")
            
            # 对F2权重调整的验证
            print(f"\n✅ F2权重调整验证:")
            if total_diff > 0.5:  # F2成本显著高于平均
                print(f"  ✅ F2成本较高({total_diff:+.2f}bps)，权重降低合理")
            elif total_diff < -0.5:  # F2成本显著低于平均
                print(f"  ⚠️ F2成本较低({total_diff:+.2f}bps)，可考虑权重调整")
            else:
                print(f"  🔄 F2成本与平均相近，权重调整适中")
    
    return f2_buckets

def generate_validation_report(comparison_result, f2_analysis):
    """生成验证报告"""
    
    print("\n" + "=" * 60)
    print("📋 校准模型验证报告")
    print("=" * 60)
    
    print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if comparison_result:
        fixed = comparison_result["fixed"]
        calibrated = comparison_result["calibrated"]
        diff = comparison_result["differences"]
        
        print(f"\n💰 成本模型验证结果:")
        print(f"  固定成本: {fixed['total']:.2f}bps")
        print(f"  校准成本: {calibrated['total']:.2f}bps")
        print(f"  差异: {diff['total']:+.2f}bps ({diff['total']/fixed['total']*100:+.1f}%)")
        
        if diff["total"] < -1.0:
            print(f"  ✅ 校准模型成本显著更低")
        elif diff["total"] > 1.0:
            print(f"  ⚠️ 校准模型成本显著更高")
        else:
            print(f"  🔄 成本差异在可接受范围内")
    
    print(f"\n🎯 F2因子优化验证:")
    print(f"  权重调整: 25% → 15% (降低40%)")
    
    if f2_analysis:
        print(f"  F2相关交易: {len(f2_analysis)}个bucket")
        # 这里可以添加更多F2分析结果
    
    print(f"\n🚀 下一步建议:")
    
    if comparison_result and abs(comparison_result["differences"]["total"]) > 2.0:
        print("  1. 运行回测对比两种成本模型下的策略表现")
        print("  2. 基于真实成本重新评估F2因子有效性")
        print("  3. 考虑进一步优化其他因子权重")
    else:
        print("  1. 监控校准模型在实际交易中的表现")
        print("  2. 继续积累真实交易数据")
        print("  3. 定期重新校准成本模型")
    
    print(f"\n📅 验证计划:")
    print("  阶段1: 成本模型验证 (今天)")
    print("  阶段2: F2因子优化验证 (1-2天)")
    print("  阶段3: 整体策略优化 (本周)")
    
    print("=" * 60)

def main():
    """主函数"""
    
    print("🚀 开始校准模型验证和优化")
    print("=" * 60)
    print("阶段1: 验证真实数据校准效果")
    print("=" * 60)
    
    # 1. 加载数据
    real_stats, mixed_stats = load_cost_stats()
    
    # 2. 分析成本分布
    real_analysis = analyze_cost_distribution(real_stats)
    mixed_analysis = analyze_cost_distribution(mixed_stats)
    
    # 3. 对比校准vs固定模型
    comparison_result = compare_calibrated_vs_fixed(real_analysis)
    
    # 4. 分析F2特定成本
    f2_analysis = analyze_f2_specific_costs(real_analysis)
    
    # 5. 生成报告
    generate_validation_report(comparison_result, f2_analysis)
    
    print("\n✅ 阶段1验证完成")
    print("=" * 60)
    
    print("\n🎯 下一步: 开始阶段2 - F2因子优化验证")
    print("建议运行: python3 scripts/validate_f2_optimization.py")

if __name__ == "__main__":
    main()