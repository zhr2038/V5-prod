#!/usr/bin/env python3
"""
参数扫描评估脚本
用于评估不同 temperature、调仓频率、banding 参数的效果
"""

from __future__ import annotations

import os
import json
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class ParameterSet:
    """参数组合"""
    temperature: float  # softmax temperature
    rebalance_hours: int  # 调仓频率（小时）
    deadband_new_position_mult: float  # 新建仓deadband倍数
    weights: Dict[str, float]  # 因子权重
    
    def to_dict(self) -> Dict:
        return {
            "temperature": self.temperature,
            "rebalance_hours": self.rebalance_hours,
            "deadband_new_position_mult": self.deadband_new_position_mult,
            "weights": self.weights
        }


@dataclass
class PerformanceMetrics:
    """性能指标"""
    total_return_pct: float
    turnover_ratio: float  # 年化换手率
    cost_ratio: float  # 成本占比
    sharpe: float
    max_drawdown_pct: float
    effective_n: float  # 有效持仓数 1/∑w²
    ic_1h: float  # 1小时IC
    ic_6h: float  # 6小时IC
    ic_24h: float  # 24小时IC
    
    def to_dict(self) -> Dict:
        return {
            "total_return_pct": self.total_return_pct,
            "turnover_ratio": self.turnover_ratio,
            "cost_ratio": self.cost_ratio,
            "sharpe": self.sharpe,
            "max_drawdown_pct": self.max_drawdown_pct,
            "effective_n": self.effective_n,
            "ic_1h": self.ic_1h,
            "ic_6h": self.ic_6h,
            "ic_24h": self.ic_24h
        }


def calculate_effective_n(weights: Dict[str, float]) -> float:
    """计算有效持仓数 1/∑w²"""
    if not weights:
        return 0.0
    sum_sq = sum(w * w for w in weights.values())
    return 1.0 / sum_sq if sum_sq > 0 else 0.0


def estimate_turnover_reduction(current_hours: int, new_hours: int, 
                               current_turnover: float) -> float:
    """估计调仓频率改变后的换手率"""
    # 简化估计：换手率与调仓频率成正比
    return current_turnover * (new_hours / current_hours)


def estimate_cost_reduction(turnover_ratio: float, cost_per_turnover: float = 0.001) -> float:
    """估计成本（假设成本与换手率成正比）"""
    return turnover_ratio * cost_per_turnover


def generate_parameter_sets() -> List[ParameterSet]:
    """生成要测试的参数组合"""
    
    # 基础权重（你的建议）
    base_weights = {
        "f1_mom_5d": 0.25,
        "f2_mom_20d": 0.10,
        "f3_vol_adj_ret_20d": 0.35,
        "f4_volume_expansion": 0.10,
        "f5_rsi_trend_confirm": 0.20
    }
    
    parameter_sets = []
    
    # temperature 扫描
    temperatures = [0.5, 0.7, 0.9, 1.1, 1.3]
    
    # 调仓频率
    rebalance_hours_options = [1, 4, 6, 12]
    
    # banding 倍数
    deadband_mults = [1.0, 1.5, 2.0, 2.5]
    
    # 生成组合（简化：只测试关键组合）
    # 组合1：当前配置（基准）
    parameter_sets.append(ParameterSet(
        temperature=0.5,
        rebalance_hours=1,
        deadband_new_position_mult=1.0,
        weights=base_weights
    ))
    
    # 组合2：你的推荐配置
    parameter_sets.append(ParameterSet(
        temperature=0.9,
        rebalance_hours=6,
        deadband_new_position_mult=2.0,
        weights=base_weights
    ))
    
    # 组合3：更保守
    parameter_sets.append(ParameterSet(
        temperature=1.1,
        rebalance_hours=12,
        deadband_new_position_mult=2.5,
        weights=base_weights
    ))
    
    # 组合4：更激进（低换手但集中）
    parameter_sets.append(ParameterSet(
        temperature=0.7,
        rebalance_hours=4,
        deadband_new_position_mult=1.5,
        weights=base_weights
    ))
    
    return parameter_sets


def simulate_performance(params: ParameterSet, 
                        baseline_metrics: PerformanceMetrics) -> PerformanceMetrics:
    """模拟参数组合的性能（简化版）"""
    
    # 基于基准指标进行估计
    
    # 1. 计算有效持仓数变化
    effective_n = calculate_effective_n(params.weights)
    
    # 2. 估计换手率变化
    # temperature 影响：更高的 temperature → 更分散 → 换手可能降低
    temp_factor = 1.0 + (params.temperature - 0.5) * 0.2  # 每增加0.5 temperature，换手降低10%
    
    # 调仓频率影响
    freq_factor = params.rebalance_hours / 1.0  # 相对于1小时
    
    # banding 影响
    banding_factor = 1.0 / (params.deadband_new_position_mult ** 0.5)  # 保守估计
    
    estimated_turnover = baseline_metrics.turnover_ratio * temp_factor * freq_factor * banding_factor
    
    # 3. 估计成本变化
    estimated_cost = estimate_cost_reduction(estimated_turnover)
    
    # 4. 估计收益变化（简化：假设与信号暴露和换手相关）
    # 更高的 effective_n 可能降低集中风险但也降低alpha暴露
    diversity_factor = 1.0 - (effective_n - 3.0) * 0.05 if effective_n > 3.0 else 1.0
    
    # 成本对净收益的影响
    cost_impact = 1.0 - (estimated_cost / baseline_metrics.cost_ratio) * 0.5
    
    estimated_return = baseline_metrics.total_return_pct * diversity_factor * cost_impact
    
    # 5. IC 衰减估计（更高的temperature可能平滑信号）
    ic_smoothing = 1.0 - (params.temperature - 0.5) * 0.1
    ic_1h = baseline_metrics.ic_1h * ic_smoothing
    ic_6h = baseline_metrics.ic_6h * (1.0 + (params.rebalance_hours - 1) * 0.05)  # 更匹配的horizon可能提升IC
    ic_24h = baseline_metrics.ic_24h
    
    return PerformanceMetrics(
        total_return_pct=estimated_return,
        turnover_ratio=estimated_turnover,
        cost_ratio=estimated_cost,
        sharpe=baseline_metrics.sharpe * (estimated_return / max(baseline_metrics.total_return_pct, 0.01)),
        max_drawdown_pct=baseline_metrics.max_drawdown_pct * (1.0 - (effective_n - 2.0) * 0.05),
        effective_n=effective_n,
        ic_1h=ic_1h,
        ic_6h=ic_6h,
        ic_24h=ic_24h
    )


def main():
    print("🔬 参数优化扫描评估")
    print("=" * 60)
    
    # 基准性能（基于你的报告）
    baseline = PerformanceMetrics(
        total_return_pct=100.0,  # 假设
        turnover_ratio=21.5,  # 2150% 年化换手
        cost_ratio=0.4,  # 成本吃掉40%
        sharpe=1.5,  # 假设
        max_drawdown_pct=15.0,  # 假设
        effective_n=2.5,  # 假设
        ic_1h=0.042,
        ic_6h=0.025,  # 估计
        ic_24h=0.012
    )
    
    print("📊 基准性能:")
    print(f"  年化换手: {baseline.turnover_ratio:.1f}%")
    print(f"  成本占比: {baseline.cost_ratio:.1%}")
    print(f"  IC(1h): {baseline.ic_1h:.3f}, IC(6h): {baseline.ic_6h:.3f}, IC(24h): {baseline.ic_24h:.3f}")
    print(f"  有效持仓数: {baseline.effective_n:.1f}")
    
    # 生成参数组合
    param_sets = generate_parameter_sets()
    
    print(f"\n🔍 测试 {len(param_sets)} 个参数组合:")
    print("=" * 60)
    
    results = []
    for i, params in enumerate(param_sets):
        print(f"\n组合 {i+1}:")
        print(f"  temperature: {params.temperature}")
        print(f"  调仓频率: {params.rebalance_hours}h")
        print(f"  新建仓deadband倍数: {params.deadband_new_position_mult}")
        print(f"  有效持仓数: {calculate_effective_n(params.weights):.2f}")
        
        # 模拟性能
        metrics = simulate_performance(params, baseline)
        
        # 计算净收益（考虑成本）
        net_return = metrics.total_return_pct * (1 - metrics.cost_ratio)
        baseline_net = baseline.total_return_pct * (1 - baseline.cost_ratio)
        improvement = (net_return / baseline_net - 1) * 100 if baseline_net > 0 else 0
        
        print(f"  估计换手: {metrics.turnover_ratio:.1f}% (降低: {(1 - metrics.turnover_ratio/baseline.turnover_ratio)*100:.0f}%)")
        print(f"  估计成本占比: {metrics.cost_ratio:.1%}")
        print(f"  估计净收益改善: {improvement:.1f}%")
        
        results.append({
            "params": params.to_dict(),
            "metrics": metrics.to_dict(),
            "net_return": net_return,
            "improvement_pct": improvement
        })
    
    # 排序并推荐
    results.sort(key=lambda x: x["improvement_pct"], reverse=True)
    
    print("\n" + "=" * 60)
    print("🏆 推荐参数组合 (按净收益改善排序):")
    print("=" * 60)
    
    for i, result in enumerate(results[:3]):
        params = result["params"]
        metrics = result["metrics"]
        
        print(f"\n#{i+1} (改善: {result['improvement_pct']:.1f}%):")
        print(f"  temperature: {params['temperature']}")
        print(f"  调仓频率: {params['rebalance_hours']}h")
        print(f"  新建仓deadband倍数: {params['deadband_new_position_mult']}")
        print(f"  有效持仓数: {metrics['effective_n']:.2f}")
        print(f"  估计换手: {metrics['turnover_ratio']:.1f}%")
        print(f"  估计成本占比: {metrics['cost_ratio']:.1%}")
    
    print("\n" + "=" * 60)
    print("📋 立即执行建议:")
    print("1. ✅ temperature: 0.5 → 0.9 (已修改)")
    print("2. ✅ 因子权重调整 (已修改)")
    print("3. ✅ banding: 新建仓deadband加倍 (已实现)")
    print("4. ⏳ 调仓频率: 1h → 6h (需修改运行方式)")
    print("5. 📊 运行真实数据收集IC/换手/成本指标")
    print("=" * 60)
    
    # 保存结果
    os.makedirs("reports/parameter_sweep", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"reports/parameter_sweep/sweep_{timestamp}.json"
    
    with open(output_path, "w") as f:
        json.dump({
            "baseline": baseline.to_dict(),
            "results": results,
            "timestamp": timestamp
        }, f, indent=2)
    
    print(f"\n📁 结果已保存: {output_path}")


if __name__ == "__main__":
    main()