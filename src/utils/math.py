from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

import numpy as np


def zscore_cross_section(values: Dict[str, float], eps: float = 1e-12) -> Dict[str, float]:
    """
    横截面标准化。当std=0时，使用排名而不是返回全0。
    返回：zscore字典，均值为0，标准差为1（或近似）
    """
    if not values:
        return {}
    
    keys = list(values.keys())
    xs = np.array([float(values[k]) for k in keys], dtype=float)
    mu = float(np.mean(xs))
    sd = float(np.std(xs))
    
    if sd < eps:
        # 当std=0时，使用排名来创建差异
        # 如果有多个相同的值，给微小差异
        if len(xs) == 1:
            # 只有一个值，返回0（中性）
            return {keys[0]: 0.0}
        else:
            # 多个相同值，给微小随机差异或基于索引的微小差异
            # 使用微小差异避免除0，同时保持顺序
            tiny_diffs = np.arange(len(xs)) * 1e-12
            xs_with_diff = xs + tiny_diffs
            mu = float(np.mean(xs_with_diff))
            sd = float(np.std(xs_with_diff))
            if sd < eps:
                sd = 1.0  # 保底
            zs = (xs_with_diff - mu) / sd
            return {k: float(z) for k, z in zip(keys, zs)}
    
    zs = (xs - mu) / sd
    return {k: float(z) for k, z in zip(keys, zs)}


def clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def safe_pct_change(a: float, b: float) -> float:
    """Return b/a - 1, guarding zeros."""
    a = float(a)
    b = float(b)
    if a == 0:
        return 0.0
    return b / a - 1.0
