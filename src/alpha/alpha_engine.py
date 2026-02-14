from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np

from src.core.models import MarketSeries
from src.utils.math import safe_pct_change, zscore_cross_section
from configs.schema import AlphaConfig


def _rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    deltas = np.diff(np.array(closes[-(period + 1) :], dtype=float))
    gains = np.clip(deltas, 0, None)
    losses = -np.clip(deltas, None, 0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


@dataclass
class AlphaSnapshot:
    raw_factors: Dict[str, Dict[str, float]]  # symbol -> factor -> value
    z_factors: Dict[str, Dict[str, float]]
    scores: Dict[str, float]


class AlphaEngine:
    def __init__(self, cfg: AlphaConfig):
        self.cfg = cfg

    def compute_scores(self, market_data: Dict[str, MarketSeries]) -> Dict[str, float]:
        snap = self.compute_snapshot(market_data)
        return snap.scores

    def compute_snapshot(self, market_data: Dict[str, MarketSeries]) -> AlphaSnapshot:
        # Compute raw factors
        f1: Dict[str, float] = {}
        f2: Dict[str, float] = {}
        f3: Dict[str, float] = {}
        f4: Dict[str, float] = {}
        f5: Dict[str, float] = {}

        for sym, s in (market_data or {}).items():
            c = list(s.close)
            v = list(s.volume)
            if len(c) < 25:
                continue

            # 5d momentum and 20d momentum: on 1h data, treat 24 bars/day
            # For this scaffold: assume 1h bars and approximate.
            mom_5d = safe_pct_change(c[-1 - 24 * 5], c[-1]) if len(c) > 24 * 5 else safe_pct_change(c[0], c[-1])
            mom_20d = safe_pct_change(c[-1 - 24 * 20], c[-1]) if len(c) > 24 * 20 else safe_pct_change(c[0], c[-1])

            # 20d vol-adjusted return: mom_20d / vol_20d
            rets = np.diff(np.array(c[-(24 * 20 + 1) :], dtype=float)) / np.array(c[-(24 * 20 + 1) : -1], dtype=float)
            vol = float(np.std(rets)) if len(rets) > 10 else 0.0
            vol_adj = mom_20d / (vol + 1e-12)

            # volume expansion: last 24h volume vs prev 7d average daily volume
            vol_1d = float(np.sum(v[-24:])) if len(v) >= 24 else float(np.sum(v))
            daily = []
            if len(v) >= 24 * 8:
                for k in range(1, 8):
                    daily.append(float(np.sum(v[-24 * (k + 1) : -24 * k])))
            avg_7d = float(np.mean(daily)) if daily else vol_1d
            vol_exp = (vol_1d / (avg_7d + 1e-12)) - 1.0

            # RSI trend confirm: RSI - 50 (positive is bullish)
            rsi = _rsi(c, 14)
            rsi_trend = (rsi - 50.0) / 50.0

            f1[sym] = float(mom_5d)
            f2[sym] = float(mom_20d)
            f3[sym] = float(vol_adj)
            f4[sym] = float(vol_exp)
            f5[sym] = float(rsi_trend)

        z1 = zscore_cross_section(f1)
        z2 = zscore_cross_section(f2)
        z3 = zscore_cross_section(f3)
        z4 = zscore_cross_section(f4)
        z5 = zscore_cross_section(f5)

        raw_factors: Dict[str, Dict[str, float]] = {}
        z_factors: Dict[str, Dict[str, float]] = {}
        scores: Dict[str, float] = {}

        w = self.cfg.weights
        for sym in z1.keys():
            raw_factors[sym] = {
                "f1_mom_5d": f1.get(sym, 0.0),
                "f2_mom_20d": f2.get(sym, 0.0),
                "f3_vol_adj_ret_20d": f3.get(sym, 0.0),
                "f4_volume_expansion": f4.get(sym, 0.0),
                "f5_rsi_trend_confirm": f5.get(sym, 0.0),
            }
            z_factors[sym] = {
                "f1_mom_5d": z1.get(sym, 0.0),
                "f2_mom_20d": z2.get(sym, 0.0),
                "f3_vol_adj_ret_20d": z3.get(sym, 0.0),
                "f4_volume_expansion": z4.get(sym, 0.0),
                "f5_rsi_trend_confirm": z5.get(sym, 0.0),
            }
            score = (
                w.f1_mom_5d * z1.get(sym, 0.0)
                + w.f2_mom_20d * z2.get(sym, 0.0)
                + w.f3_vol_adj_ret_20d * z3.get(sym, 0.0)
                + w.f4_volume_expansion * z4.get(sym, 0.0)
                + w.f5_rsi_trend_confirm * z5.get(sym, 0.0)
            )
            scores[sym] = float(score)

        return AlphaSnapshot(raw_factors=raw_factors, z_factors=z_factors, scores=scores)
