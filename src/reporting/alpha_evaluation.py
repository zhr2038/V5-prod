"""
Alpha 有效性评估闭环（信号层）
实现：IC曲线、分位数收益表、衰减分析、成本归因
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
from scipy import stats

from src.utils.math import zscore_cross_section

log = logging.getLogger(__name__)


@dataclass
class AlphaEvalConfig:
    """Alpha评估配置"""
    # 持有期（小时）
    holding_periods: List[int] = None  # 默认 [1, 4, 12, 24, 72] (1h,4h,12h,1d,3d)
    # 分位数数量
    n_quantiles: int = 5
    # 成本假设（bps）
    fee_bps: float = 6.0
    slippage_bps: float = 5.0
    # 稳健标准化
    winsorize_pct: float = 0.05  # 缩尾5%
    use_robust_zscore: bool = True  # 使用median+MAD替代mean+std


@dataclass
class AlphaEvalResult:
    """Alpha评估结果"""
    # IC分析
    ic_by_horizon: Dict[int, Dict[str, float]]  # horizon -> {mean, std, ir, count}
    # 分位数分析
    quantile_returns: Dict[int, Dict[int, Dict[str, float]]]  # horizon -> quantile -> {mean_return, win_rate, vol}
    # 衰减分析
    decay_curve: List[Tuple[int, float, float]]  # (horizon, ic_mean, ic_std)
    # 因子贡献
    factor_contributions: Dict[str, Dict[str, float]]  # factor -> {ic_mean, ic_ir, weight}
    # 成本敏感性
    cost_sensitivity: Dict[str, float]  # 毛收益、净收益、换手率等


def robust_zscore_cross_section(values: Dict[str, float], winsorize_pct: float = 0.05) -> Dict[str, float]:
    """稳健的截面z-score：缩尾 + median + MAD"""
    if not values:
        return {}
    
    keys = list(values.keys())
    xs = np.array([float(values[k]) for k in keys], dtype=float)
    
    # 1. 缩尾处理
    if winsorize_pct > 0:
        lower = np.percentile(xs, winsorize_pct * 100)
        upper = np.percentile(xs, (1 - winsorize_pct) * 100)
        xs = np.clip(xs, lower, upper)
    
    # 2. 使用median和MAD（Median Absolute Deviation）
    med = np.median(xs)
    mad = np.median(np.abs(xs - med))
    
    # 3. 标准化：MAD -> 标准差近似 (MAD * 1.4826 ≈ std for normal)
    if mad < 1e-12:
        clipped_values = {k: float(x) for k, x in zip(keys, xs)}
        return zscore_cross_section(clipped_values)
    
    zs = (xs - med) / (mad * 1.4826)
    return {k: float(z) for k, z in zip(keys, zs)}


def compute_quote_volume(volume: List[float], close: List[float]) -> float:
    """计算quote volume（USDT价值）"""
    if len(volume) != len(close):
        return 0.0
    # 简单实现：volume * close 的加权平均
    return float(np.sum(np.array(volume) * np.array(close)))


def calculate_forward_returns(
    market_data: Dict[str, Dict], 
    current_ts: int, 
    horizon_hours: int
) -> Dict[str, float]:
    """计算未来horizon小时的收益"""
    # 简化实现：需要时间序列对齐
    # 实际实现需要根据时间戳对齐数据
    returns = {}
    for sym, data in market_data.items():
        # 这里需要根据时间戳找到未来价格
        # 暂时返回0，需要完善数据接口
        returns[sym] = 0.0
    return returns


def evaluate_alpha_snapshot(
    alpha_scores: Dict[str, float],
    forward_returns: Dict[str, float],
    horizon_hours: int
) -> Dict[str, Any]:
    """评估单个时间点的alpha预测能力"""
    common_symbols = set(alpha_scores.keys()) & set(forward_returns.keys())
    if len(common_symbols) < 5:
        return {"ic": 0.0, "count": 0}
    
    # 提取数据
    scores = []
    rets = []
    for sym in common_symbols:
        scores.append(alpha_scores[sym])
        rets.append(forward_returns[sym])
    
    scores_np = np.array(scores)
    rets_np = np.array(rets)
    
    # 计算Rank IC（Spearman相关系数）
    if len(scores_np) > 2:
        ic, pvalue = stats.spearmanr(scores_np, rets_np)
        ic = float(ic) if not np.isnan(ic) else 0.0
    else:
        ic = 0.0
        pvalue = 1.0
    
    # 分位数分析
    n_quantiles = 5
    quantile_results = []
    if len(scores_np) >= n_quantiles:
        # 按score排序
        sorted_indices = np.argsort(scores_np)
        q_size = len(sorted_indices) // n_quantiles
        
        for q in range(n_quantiles):
            start = q * q_size
            end = (q + 1) * q_size if q < n_quantiles - 1 else len(sorted_indices)
            if end > start:
                q_rets = rets_np[sorted_indices[start:end]]
                quantile_results.append({
                    "quantile": q + 1,
                    "mean_return": float(np.mean(q_rets)),
                    "win_rate": float(np.mean(q_rets > 0)),
                    "vol": float(np.std(q_rets)) if len(q_rets) > 1 else 0.0,
                    "count": len(q_rets)
                })
    
    return {
        "horizon_hours": horizon_hours,
        "ic": ic,
        "ic_pvalue": float(pvalue),
        "count": len(common_symbols),
        "quantiles": quantile_results,
        "score_stats": {
            "mean": float(np.mean(scores_np)),
            "std": float(np.std(scores_np)) if len(scores_np) > 1 else 0.0,
            "min": float(np.min(scores_np)),
            "max": float(np.max(scores_np))
        },
        "return_stats": {
            "mean": float(np.mean(rets_np)),
            "std": float(np.std(rets_np)) if len(rets_np) > 1 else 0.0,
            "min": float(np.min(rets_np)),
            "max": float(np.max(rets_np))
        }
    }


def run_alpha_evaluation_historical(
    historical_snapshots: List[Dict[str, Any]],  # 每个时间点的alpha snapshot和未来收益
    config: Optional[AlphaEvalConfig] = None
) -> AlphaEvalResult:
    """运行历史alpha评估"""
    config = config or AlphaEvalConfig()
    
    # 按持有期分组
    ic_by_horizon = {}
    quantile_returns = {}
    
    for horizon in config.holding_periods or [1, 4, 12, 24, 72]:
        horizon_ics = []
        horizon_quantiles = {q: [] for q in range(config.n_quantiles)}
        
        for snap in historical_snapshots:
            if f"fwd_ret_{horizon}h" in snap:
                eval_result = evaluate_alpha_snapshot(
                    snap["alpha_scores"],
                    snap[f"fwd_ret_{horizon}h"],
                    horizon
                )
                horizon_ics.append(eval_result["ic"])
                
                # 累积分位数结果
                for q_data in eval_result.get("quantiles", []):
                    q = q_data["quantile"] - 1
                    horizon_quantiles[q].append(q_data["mean_return"])
        
        # 计算IC统计
        if horizon_ics:
            ic_array = np.array(horizon_ics)
            ic_mean = float(np.mean(ic_array))
            ic_std = float(np.std(ic_array)) if len(ic_array) > 1 else 0.0
            ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0
            
            ic_by_horizon[horizon] = {
                "mean": ic_mean,
                "std": ic_std,
                "ir": ic_ir,
                "count": len(horizon_ics)
            }
        
        # 计算分位数平均收益
        quantile_returns[horizon] = {}
        for q in range(config.n_quantiles):
            if horizon_quantiles[q]:
                q_rets = np.array(horizon_quantiles[q])
                quantile_returns[horizon][q] = {
                    "mean_return": float(np.mean(q_rets)),
                    "win_rate": float(np.mean(q_rets > 0)),
                    "vol": float(np.std(q_rets)) if len(q_rets) > 1 else 0.0,
                    "count": len(q_rets)
                }
    
    # 衰减曲线
    decay_curve = []
    for horizon in sorted(ic_by_horizon.keys()):
        decay_curve.append((
            horizon,
            ic_by_horizon[horizon]["mean"],
            ic_by_horizon[horizon]["std"]
        ))
    
    # 因子贡献（简化：需要因子级别的IC）
    factor_contributions = {}
    
    # 成本敏感性（简化）
    cost_sensitivity = {
        "estimated_turnover_pct": 0.0,
        "cost_bps_per_trade": config.fee_bps + config.slippage_bps,
        "breakeven_ic": 0.0  # 需要计算
    }
    
    return AlphaEvalResult(
        ic_by_horizon=ic_by_horizon,
        quantile_returns=quantile_returns,
        decay_curve=decay_curve,
        factor_contributions=factor_contributions,
        cost_sensitivity=cost_sensitivity
    )


def save_alpha_evaluation_report(
    result: AlphaEvalResult,
    output_path: str,
    config: AlphaEvalConfig
) -> None:
    """保存评估报告"""
    report = {
        "config": {
            "holding_periods": config.holding_periods,
            "n_quantiles": config.n_quantiles,
            "fee_bps": config.fee_bps,
            "slippage_bps": config.slippage_bps,
            "winsorize_pct": config.winsorize_pct,
            "use_robust_zscore": config.use_robust_zscore
        },
        "ic_analysis": result.ic_by_horizon,
        "quantile_analysis": result.quantile_returns,
        "decay_curve": result.decay_curve,
        "factor_contributions": result.factor_contributions,
        "cost_sensitivity": result.cost_sensitivity,
        "summary": {
            "best_horizon": max(result.ic_by_horizon.items(), key=lambda x: x[1]["ir"])[0] if result.ic_by_horizon else 0,
            "avg_ic": float(np.mean([v["mean"] for v in result.ic_by_horizon.values()])) if result.ic_by_horizon else 0.0,
            "monotonic": check_monotonicity(result.quantile_returns)
        }
    }
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    log.info(f"Alpha evaluation report saved to {output_path}")


def check_monotonicity(quantile_returns: Dict[int, Dict[int, Dict[str, float]]]) -> bool:
    """检查分位数收益是否单调递增"""
    if not quantile_returns:
        return False
    
    # 检查第一个持有期
    first_horizon = min(quantile_returns.keys())
    q_data = quantile_returns[first_horizon]
    
    if len(q_data) < 2:
        return False
    
    # 提取收益
    returns = [q_data[q]["mean_return"] for q in sorted(q_data.keys())]
    
    # 检查单调性：高score分位数应该有更高收益
    for i in range(1, len(returns)):
        if returns[i] < returns[i-1]:
            return False
    
    return True


def generate_alpha_evaluation_summary(result: AlphaEvalResult) -> str:
    """生成评估摘要（文本格式）"""
    lines = []
    lines.append("=" * 60)
    lines.append("ALPHA EVALUATION SUMMARY")
    lines.append("=" * 60)
    
    # IC分析
    lines.append("\n[IC Analysis by Holding Period]")
    lines.append("Horizon(h) | IC Mean | IC Std | IC IR | Count")
    lines.append("-" * 50)
    for horizon in sorted(result.ic_by_horizon.keys()):
        stats = result.ic_by_horizon[horizon]
        lines.append(f"{horizon:10d} | {stats['mean']:7.4f} | {stats['std']:6.4f} | {stats['ir']:5.2f} | {stats['count']:5d}")
    
    # 衰减曲线
    lines.append("\n[Decay Curve]")
    for horizon, ic_mean, ic_std in result.decay_curve:
        lines.append(f"  {horizon:3d}h: IC = {ic_mean:.4f} ± {ic_std:.4f}")
    
    # 分位数分析
    if result.quantile_returns:
        lines.append("\n[Quantile Returns (1h horizon)]")
        first_horizon = min(result.quantile_returns.keys())
        q_data = result.quantile_returns[first_horizon]
        lines.append("Quantile | Mean Return | Win Rate | Vol")
        lines.append("-" * 45)
        for q in sorted(q_data.keys()):
            stats = q_data[q]
            lines.append(f"Q{q+1:8d} | {stats['mean_return']:11.4%} | {stats['win_rate']:8.2%} | {stats['vol']:.4f}")
    
    # 成本敏感性
    lines.append("\n[Cost Sensitivity]")
    for k, v in result.cost_sensitivity.items():
        lines.append(f"  {k}: {v}")
    
    lines.append("\n" + "=" * 60)
    return "\n".join(lines)
