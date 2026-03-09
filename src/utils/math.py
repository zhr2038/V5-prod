from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

import numpy as np


def zscore_cross_section(values: Dict[str, float], eps: float = 1e-12) -> Dict[str, float]:
    """
    计算横截面z-score标准化
    
    Args:
        values: 原始值字典 {symbol: value}
        eps: 最小标准差阈值，避免除以零
        
    Returns:
        标准化后的z-score字典
    """
    if not values:
        return {}
    keys = list(values.keys())
    xs = np.array([float(values[k]) for k in keys], dtype=float)
    mu = float(np.mean(xs))
    sd = float(np.std(xs))
    if sd < eps:
        return {k: 0.0 for k in keys}
    zs = (xs - mu) / sd
    return {k: float(z) for k, z in zip(keys, zs)}


def clamp(x: float, lo: float, hi: float) -> float:
    """
    将数值限制在指定范围内
    
    Args:
        x: 输入值
        lo: 下限
        hi: 上限
        
    Returns:
        限制后的值
    """
    return float(max(lo, min(hi, x)))


def safe_pct_change(a: float, b: float) -> float:
    """Return b/a - 1, guarding zeros."""
    a = float(a)
    b = float(b)
    if a == 0:
        return 0.0
    return b / a - 1.0
