from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

import numpy as np

from configs.schema import AlphaConfig, RiskConfig
from src.core.models import MarketSeries
from src.utils.math import clamp


@dataclass
class PortfolioSnapshot:
    target_weights: Dict[str, float]
    selected: List[str]
    volatilities: Dict[str, float]
    notes: str = ""


class PortfolioEngine:
    def __init__(self, alpha_cfg: AlphaConfig, risk_cfg: RiskConfig):
        self.alpha_cfg = alpha_cfg
        self.risk_cfg = risk_cfg

    def allocate(
        self,
        scores: Dict[str, float],
        market_data: Dict[str, MarketSeries],
        regime_mult: float,
        audit: Optional[Any] = None,
    ) -> PortfolioSnapshot:
        if not scores:
            return PortfolioSnapshot(target_weights={}, selected=[], volatilities={}, notes="no_scores")

        # Select top pct by score
        items = sorted(scores.items(), key=lambda kv: float(kv[1]), reverse=True)
        k = max(1, int(np.ceil(len(items) * float(self.alpha_cfg.long_top_pct))))
        selected = [s for s, _ in items[:k]]

        # Compute vol for inverse-vol weights (20d realized vol on 1h bars approx)
        vols: Dict[str, float] = {}
        inv: Dict[str, float] = {}
        for sym in selected:
            s = market_data.get(sym)
            if not s or len(s.close) < 2:
                vols[sym] = 1.0
                inv[sym] = 1.0
                continue
            c = np.array(s.close, dtype=float)
            rets = np.diff(c) / c[:-1]
            # window ~ 20d on 1h bars
            w = min(len(rets), 24 * 20)
            rv = float(np.std(rets[-w:])) if w > 10 else float(np.std(rets))
            rv = max(rv, 1e-6)
            vols[sym] = rv
            inv[sym] = 1.0 / rv

        inv_sum = float(sum(inv.values())) or 1.0
        base_w = {sym: float(inv[sym]) / inv_sum for sym in selected}

        # Confidence weighting by normalized score within selected (0..1)
        sel_scores = np.array([scores[s] for s in selected], dtype=float)
        mn = float(np.min(sel_scores))
        mx = float(np.max(sel_scores))
        denom = (mx - mn) if (mx - mn) != 0 else 1.0
        conf = {s: float((scores[s] - mn) / denom) for s in selected}
        
        # 诊断：记录为什么conf可能为0
        portfolio_debug = {
            "inv_vol_norm": base_w,
            "confidence_raw": conf,
            "score_stats": {
                "min": mn,
                "max": mx,
                "std": float(np.std(sel_scores)) if len(sel_scores) > 1 else 0.0,
                "count": len(sel_scores)
            }
        }
        
        # 如果所有confidence都是0，fallback到等权
        all_conf_zero = all(abs(c) < 1e-12 for c in conf.values())
        if all_conf_zero and len(selected) > 0:
            # Fallback: 等权分配
            fallback_weight = 1.0 / len(selected)
            conf = {s: fallback_weight for s in selected}
            portfolio_debug["fallback_reason"] = "all_confidence_zero"
            portfolio_debug["fallback_weight"] = fallback_weight

        raw = {s: base_w[s] * conf[s] for s in selected}
        raw_sum = float(sum(raw.values())) or 1.0
        w2 = {s: raw[s] / raw_sum for s in selected}
        
        # 记录zero_reason_by_symbol
        zero_reason_by_symbol = {}
        for s in selected:
            if abs(w2[s]) < 1e-12:
                if abs(base_w[s]) < 1e-12:
                    zero_reason_by_symbol[s] = "inv_vol_zero"
                elif abs(conf[s]) < 1e-12:
                    zero_reason_by_symbol[s] = "confidence_zero"
                else:
                    zero_reason_by_symbol[s] = "normalization_zero"
        
        portfolio_debug["zero_reason_by_symbol"] = zero_reason_by_symbol
        portfolio_debug["weight_pre_clip"] = w2

        # Apply regime multiplier to overall gross exposure, then cap max_single_weight
        gross = float(self.risk_cfg.max_gross_exposure) * float(regime_mult)
        gross = clamp(gross, 0.0, float(self.risk_cfg.max_gross_exposure))

        capped = {s: min(w2[s] * gross, float(self.risk_cfg.max_single_weight)) for s in selected}
        
        # 将portfolio_debug添加到audit
        if audit is not None and hasattr(audit, 'portfolio_debug'):
            audit.portfolio_debug = portfolio_debug
            # 记录portfolio_rejects
            if zero_reason_by_symbol:
                for reason in zero_reason_by_symbol.values():
                    if hasattr(audit, 'reject'):
                        audit.reject(f"portfolio_{reason}")
        
        return PortfolioSnapshot(target_weights=capped, selected=selected, volatilities=vols)

    def scale_targets(self, targets: Dict[str, float], mult: float) -> Dict[str, float]:
        """Scale portfolio target weights by exposure multiplier, keeping caps."""
        m = float(mult)
        if m >= 1.0:
            return dict(targets or {})
        out: Dict[str, float] = {}
        for sym, w in (targets or {}).items():
            w2 = float(w) * m
            out[sym] = min(w2, float(self.risk_cfg.max_single_weight))
        return out
