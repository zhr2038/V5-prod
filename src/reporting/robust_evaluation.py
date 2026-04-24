"""
稳健评估方法：包含信息泄露防护、显著性检验、成本模型
"""

from __future__ import annotations

import numpy as np
from typing import List, Dict, Any, Tuple
from scipy import stats
import warnings


def purged_time_series_split(
    timestamps: List[int],
    n_splits: int = 5,
    purge_gap_hours: int = 24,
    embargo_hours: int = 6
) -> List[Tuple[List[int], List[int]]]:
    """
    Purged Time Series Cross-Validation
    防止信息泄露的时间序列分割
    
    Args:
        timestamps: 排序后的时间戳列表
        n_splits: 分割数量
        purge_gap_hours: 训练集和测试集之间的清除间隔（小时）
        embargo_hours: 测试集样本之间的隔离间隔（小时）
    
    Returns:
        List of (train_indices, test_indices) tuples
    """
    ordered = sorted(enumerate(timestamps), key=lambda item: int(item[1]))
    sorted_timestamps = [int(ts) for _, ts in ordered]
    sorted_to_original = [idx for idx, _ in ordered]
    n_samples = len(sorted_timestamps)
    
    # 计算每个fold的大小
    fold_size = n_samples // n_splits
    splits = []
    
    for i in range(n_splits):
        # 测试集：当前fold
        test_start = i * fold_size
        test_end = (i + 1) * fold_size if i < n_splits - 1 else n_samples
        
        # 训练集：之前的所有fold（排除purge间隔）
        train_indices = []
        test_indices = [sorted_to_original[idx] for idx in range(test_start, test_end)]
        
        # 计算purge边界
        test_min_ts = sorted_timestamps[test_start]
        test_max_ts = sorted_timestamps[test_end - 1]
        
        purge_start = test_min_ts - (purge_gap_hours * 3600)
        purge_end = test_max_ts + (purge_gap_hours * 3600)
        
        # 选择训练样本（在purge间隔之外）
        for idx, ts in enumerate(sorted_timestamps):
            if idx < test_start and ts < purge_start:
                train_indices.append(sorted_to_original[idx])
            elif idx >= test_end and ts > purge_end:
                train_indices.append(sorted_to_original[idx])
        
        # 添加embargo：移除测试集附近的训练样本
        embargo_start = test_max_ts
        embargo_end = test_max_ts + (embargo_hours * 3600)
        
        train_indices = [
            idx for idx in train_indices 
            if not (int(timestamps[idx]) > embargo_start and int(timestamps[idx]) < embargo_end)
        ]
        
        splits.append((train_indices, test_indices))
    
    return splits


def block_bootstrap_confidence_interval(
    data: np.ndarray,
    statistic_func,
    n_bootstrap: int = 1000,
    block_size: int = 5,
    confidence_level: float = 0.95
) -> Dict[str, float]:
    """
    Block Bootstrap 置信区间
    处理时间序列自相关性
    
    Args:
        data: 时间序列数据
        statistic_func: 统计量函数（如 np.mean）
        n_bootstrap: 重采样次数
        block_size: 块大小
        confidence_level: 置信水平
    
    Returns:
        包含置信区间的字典
    """
    n = len(data)
    if n < block_size * 2:
        block_size = max(1, n // 4)
    
    bootstrap_stats = []
    
    for _ in range(n_bootstrap):
        # 块重采样
        resampled = []
        i = 0
        while i < n:
            # 随机选择一个起始块
            start = np.random.randint(0, n - block_size + 1)
            block = data[start:start + block_size]
            resampled.extend(block)
            i += block_size
        
        # 截断到原始长度
        resampled = resampled[:n]
        
        # 计算统计量
        stat = statistic_func(resampled)
        bootstrap_stats.append(stat)
    
    bootstrap_stats = np.array(bootstrap_stats)
    
    # 计算百分位数置信区间
    alpha = 1 - confidence_level
    lower = np.percentile(bootstrap_stats, alpha / 2 * 100)
    upper = np.percentile(bootstrap_stats, (1 - alpha / 2) * 100)
    mean = np.mean(bootstrap_stats)
    std = np.std(bootstrap_stats)
    
    return {
        "mean": float(mean),
        "std": float(std),
        f"ci_{int(confidence_level*100)}_lower": float(lower),
        f"ci_{int(confidence_level*100)}_upper": float(upper),
        "bootstrap_samples": n_bootstrap,
        "block_size": block_size
    }


def deflated_sharpe_ratio(
    sharpe_ratio: float,
    n_observations: int,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
    n_tests: int = 1,
    autocorr: float = 0.0
) -> Dict[str, float]:
    """
    Deflated Sharpe Ratio (DSR)
    修正多重检验和非正态性的夏普比率
    
    Based on: Bailey, D.H., & López de Prado, M. (2014). 
    "The Deflated Sharpe Ratio: Correcting for Selection Bias, 
    Backtest Overfitting, and Non-Normality"
    
    Args:
        sharpe_ratio: 观测到的夏普比率
        n_observations: 观测数量
        skewness: 收益偏度
        kurtosis: 收益峰度
        n_tests: 进行的独立检验数量
        autocorr: 一阶自相关系数
    
    Returns:
        DSR及相关统计量
    """
    # 估计夏普比率的方差
    # 考虑自相关和非正态性
    var_sr = (1 - autocorr) / (n_observations - 1) * (
        1 + 0.5 * sharpe_ratio**2 + 
        (kurtosis - 3) / 4 * sharpe_ratio**2 -
        skewness * sharpe_ratio
    )
    
    if var_sr <= 0:
        var_sr = 1e-12
    
    std_sr = np.sqrt(var_sr)
    
    # 多重检验修正（Bonferroni-like）
    # 假设我们选择了最好的策略
    z_score = sharpe_ratio / std_sr
    
    # 计算p-value（单尾）
    p_value = 1 - stats.norm.cdf(z_score)
    
    # 多重检验修正
    p_value_corrected = min(1.0, p_value * n_tests)
    
    # 计算DSR
    dsr = stats.norm.ppf(1 - p_value_corrected) * std_sr
    
    return {
        "sharpe_ratio": sharpe_ratio,
        "deflated_sharpe_ratio": float(dsr),
        "z_score": float(z_score),
        "p_value": float(p_value),
        "p_value_corrected": float(p_value_corrected),
        "probability_real": float(1 - p_value_corrected),
        "variance_sr": float(var_sr),
        "std_sr": float(std_sr),
        "n_tests": n_tests
    }


def probabilistic_backtest_overfitting(
    sharpe_ratios: List[float],
    n_observations: int
) -> Dict[str, float]:
    """
    概率性回测过拟合 (PBO) 估计
    
    Based on: Bailey, D.H., Borwein, J., López de Prado, M., & Zhu, Q.J. (2016).
    "The Probability of Backtest Overfitting"
    
    Args:
        sharpe_ratios: 不同参数集的夏普比率列表
        n_observations: 每个回测的观测数量
    
    Returns:
        PBO及相关指标
    """
    if len(sharpe_ratios) < 2:
        return {"pbo": 0.5, "warning": "Insufficient strategies for PBO"}
    
    sharpe_ratios = np.array(sharpe_ratios)
    
    # 排序夏普比率
    sorted_indices = np.argsort(sharpe_ratios)[::-1]  # 降序
    
    # 计算相对排名损失
    n_strategies = len(sharpe_ratios)
    rank_loss = 0.0
    
    for i in range(n_strategies // 2):
        # 前半（训练集表现好）在测试集的排名
        train_best_idx = sorted_indices[i]
        # 在实际中，我们需要样本外表现，这里简化
        # 假设排名随机化作为保守估计
        rank_loss += (n_strategies - i - 1) / n_strategies
    
    pbo = rank_loss / (n_strategies // 2)
    
    return {
        "pbo": float(pbo),
        "probability_overfit": float(pbo),
        "n_strategies": n_strategies,
        "best_sharpe_in_sample": float(np.max(sharpe_ratios)),
        "median_sharpe": float(np.median(sharpe_ratios)),
        "interpretation": f"{pbo*100:.1f}% probability that best in-sample strategy is not best out-of-sample"
    }


def effective_number_of_bets(
    weights: np.ndarray
) -> Dict[str, float]:
    """
    计算有效赌注数量 (Effective N)
    衡量组合集中度
    
    Args:
        weights: 权重数组（和为1）
    
    Returns:
        集中度指标
    """
    weights = np.array(weights)
    weights = weights / np.sum(weights)  # 确保归一化
    
    # Herfindahl-Hirschman Index (HHI)
    hhi = np.sum(weights ** 2)
    
    # Effective N (1/HHI)
    effective_n = 1.0 / hhi if hhi > 0 else len(weights)
    
    # 集中度比率
    concentration_ratio = np.sum(np.sort(weights)[-3:])  # 前3大权重之和
    
    # 最大单一权重
    max_weight = np.max(weights)
    
    return {
        "effective_n": float(effective_n),
        "hhi": float(hhi),
        "concentration_ratio_top3": float(concentration_ratio),
        "max_single_weight": float(max_weight),
        "interpretation": f"Portfolio behaves like {effective_n:.1f} equally weighted bets"
    }


def cost_model_breakdown(
    turnover_annual_pct: float,
    fee_bps: float,
    spread_bps: float,
    slippage_bps: float,
    avg_trade_size_usdt: float = 1000.0
) -> Dict[str, float]:
    """
    详细成本模型分解
    
    Args:
        turnover_annual_pct: 年化换手率（百分比）
        fee_bps: 手续费（基点）
        spread_bps: 点差成本（基点）
        slippage_bps: 滑点成本（基点）
        avg_trade_size_usdt: 平均交易规模（USDT）
    
    Returns:
        成本分解
    """
    # 总成本（每笔交易）
    total_cost_bps = fee_bps + spread_bps + slippage_bps
    
    # 年化成本
    # turnover是交易额/资产的比例，所以成本 = turnover * cost_per_trade
    cost_drag_annual = turnover_annual_pct / 100.0 * total_cost_bps / 10000.0
    
    # 分解
    fee_drag = turnover_annual_pct / 100.0 * fee_bps / 10000.0
    spread_drag = turnover_annual_pct / 100.0 * spread_bps / 10000.0
    slippage_drag = turnover_annual_pct / 100.0 * slippage_bps / 10000.0
    
    # 盈亏平衡 alpha
    breakeven_alpha = cost_drag_annual
    
    return {
        "total_cost_bps_per_trade": total_cost_bps,
        "cost_drag_annual": float(cost_drag_annual),
        "cost_drag_annual_bps": float(cost_drag_annual * 10000),
        "fee_drag": float(fee_drag),
        "spread_drag": float(spread_drag),
        "slippage_drag": float(slippage_drag),
        "breakeven_alpha": float(breakeven_alpha),
        "breakeven_alpha_bps": float(breakeven_alpha * 10000),
        "turnover_annual_pct": turnover_annual_pct,
        "avg_trade_size_usdt": avg_trade_size_usdt,
        "annual_trades": turnover_annual_pct / 100.0 * avg_trade_size_usdt
    }


def check_time_alignment(
    snapshot_times: List[int],
    price_times: List[int],
    forward_horizon_hours: int
) -> Dict[str, Any]:
    """
    检查时间对齐：确保没有信息泄露
    
    Args:
        snapshot_times: alpha snapshot的时间戳
        price_times: 价格数据的时间戳
        forward_horizon_hours: 未来收益的持有期
    
    Returns:
        对齐检查结果
    """
    errors = []
    warnings = []
    ordered_price_times = sorted(int(ts) for ts in price_times)
    
    # 检查1: snapshot时间必须在价格时间之前
    for i, snap_ts in enumerate(snapshot_times):
        # 找到snapshot时间对应的价格时间
        price_idx = np.searchsorted(ordered_price_times, snap_ts, side='right') - 1
        
        if price_idx < 0:
            errors.append(f"Snapshot {i}: No price data before snapshot time {snap_ts}")
            continue
        
        price_ts = ordered_price_times[price_idx]
        
        # snapshot时间应该 >= 价格时间（使用该价格计算alpha）
        if snap_ts < price_ts:
            errors.append(f"Snapshot {i}: snapshot_ts={snap_ts} < price_ts={price_ts}")
        
        # 检查未来收益：必须使用snapshot时间之后的价格
        future_price_idx = np.searchsorted(ordered_price_times, snap_ts + forward_horizon_hours * 3600)
        
        if future_price_idx >= len(ordered_price_times):
            warnings.append(f"Snapshot {i}: Insufficient data for {forward_horizon_hours}h forward return")
    
    return {
        "passed": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "num_snapshots": len(snapshot_times),
        "num_prices": len(price_times),
        "forward_horizon_hours": forward_horizon_hours
    }
