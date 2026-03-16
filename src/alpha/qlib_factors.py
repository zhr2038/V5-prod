
"""
Qlib Alpha158 风格因子（轻量版）

面向 V5 的可迁移实现：
- CORR: Corr(close, log(volume+1), window)
- CORD: Corr(close/Ref(close,1), log(volume/Ref(volume,1)+1), window)
- RSQR: 线性回归拟合优度
- RANK: 当前价格在窗口中的分位
- IMAX/IMIN/IMXD: 距离近端高低点的位置指标（Aroon风格）
"""

from typing import Dict, List

import numpy as np
import pandas as pd


def _safe_series(xs: List[float]) -> pd.Series:
    arr = np.asarray(list(xs or []), dtype=float)
    if arr.size == 0:
        arr = np.asarray([0.0], dtype=float)
    s = pd.Series(arr)
    s = s.replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(0.0)
    return s


def _rsquare_roll(x: np.ndarray) -> float:
    """R-square of simple linear regression on window x."""
    try:
        n = len(x)
        if n < 3:
            return 0.0
        y = np.asarray(x, dtype=float)
        if not np.isfinite(y).all():
            return 0.0
        if np.std(y) < 1e-12:
            return 0.0
        t = np.arange(n, dtype=float)
        slope, intercept = np.polyfit(t, y, 1)
        y_hat = slope * t + intercept
        ss_res = float(np.sum((y - y_hat) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        if ss_tot <= 1e-12:
            return 0.0
        return float(max(0.0, min(1.0, 1.0 - ss_res / ss_tot)))
    except Exception:
        return 0.0


def _rank_last_roll(x: np.ndarray) -> float:
    """Percentile rank of latest point in window."""
    try:
        n = len(x)
        if n < 2:
            return 0.5
        y = np.asarray(x, dtype=float)
        last = float(y[-1])
        return float(np.mean(y <= last))
    except Exception:
        return 0.5


def _age_since_max_roll(x: np.ndarray) -> float:
    """Distance (normalized) from latest point to most recent max in window."""
    try:
        y = np.asarray(x, dtype=float)
        n = len(y)
        if n < 2:
            return 0.0
        idx = int(np.argmax(y))
        age = (n - 1) - idx
        return float(age / max(1, n - 1))
    except Exception:
        return 0.0


def _age_since_min_roll(x: np.ndarray) -> float:
    """Distance (normalized) from latest point to most recent min in window."""
    try:
        y = np.asarray(x, dtype=float)
        n = len(y)
        if n < 2:
            return 0.0
        idx = int(np.argmin(y))
        age = (n - 1) - idx
        return float(age / max(1, n - 1))
    except Exception:
        return 0.0


def compute_alpha158_style_factors(
    close: List[float],
    high: List[float],
    low: List[float],
    volume: List[float],
    *,
    corr_window: int = 10,
    rank_window: int = 20,
    aroon_window: int = 14,
) -> Dict[str, float]:
    """Compute selected Alpha158-style factors from OHLCV arrays."""
    c = _safe_series(close)
    h = _safe_series(high if high else close)
    l = _safe_series(low if low else close)
    v = _safe_series(volume)

    corr_w = max(3, int(corr_window))
    rank_w = max(3, int(rank_window))
    aroon_w = max(3, int(aroon_window))

    # CORR(close, log(volume+1))
    lv = np.log(v + 1.0)
    corr = c.rolling(corr_w).corr(lv).iloc[-1]

    # CORD(c/Ref(c,1), log(v/Ref(v,1)+1))
    c_ret = c / c.shift(1)
    v_chg = np.log((v / v.shift(1)).replace([np.inf, -np.inf], np.nan) + 1.0)
    cord = c_ret.rolling(corr_w).corr(v_chg).iloc[-1]

    # RSQR
    rsqr = c.rolling(corr_w).apply(_rsquare_roll, raw=True).iloc[-1]

    # RANK
    rankv = c.rolling(rank_w).apply(_rank_last_roll, raw=True).iloc[-1]

    # IMAX/IMIN/IMXD
    imax = h.rolling(aroon_w).apply(_age_since_max_roll, raw=True).iloc[-1]
    imin = l.rolling(aroon_w).apply(_age_since_min_roll, raw=True).iloc[-1]
    imxd = float(imax - imin) if np.isfinite(imax) and np.isfinite(imin) else 0.0

    def _safe(vx: float, default: float = 0.0) -> float:
        try:
            x = float(vx)
            if not np.isfinite(x):
                return float(default)
            return x
        except Exception:
            return float(default)

    return {
        "f6_corr_pv_10": _safe(corr, 0.0),
        "f7_cord_10": _safe(cord, 0.0),
        "f8_rsqr_10": _safe(rsqr, 0.0),
        "f9_rank_20": _safe(rankv, 0.5),
        "f10_imax_14": _safe(imax, 0.0),
        "f11_imin_14": _safe(imin, 0.0),
        "f12_imxd_14": _safe(imxd, 0.0),
    }
