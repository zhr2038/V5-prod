from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import json

import numpy as np

from configs.schema import AlphaConfig, RiskConfig
from src.core.models import MarketSeries
from src.utils.math import clamp


@dataclass
class PortfolioSnapshot:
    """投资组合快照"""
    target_weights: Dict[str, float]
    selected: List[str]
    volatilities: Dict[str, float]
    notes: str = ""


class PortfolioEngine:
    """投资组合引擎
    
    根据Alpha评分分配目标权重
    """
    
    def __init__(self, alpha_cfg: AlphaConfig, risk_cfg: RiskConfig):
        """初始化投资组合引擎
        
        Args:
            alpha_cfg: Alpha配置
            risk_cfg: 风险配置
        """
        self.alpha_cfg = alpha_cfg
        self.risk_cfg = risk_cfg

    def _load_fused_signals(self) -> Optional[Dict[str, float]]:
        """Load fused signals from strategy_signals.json if available"""
        try:
            from pathlib import Path
            from datetime import datetime
            import json
            
            strategy_file = Path(f"reports/runs/{datetime.now().strftime('%Y%m%d_%H')}/strategy_signals.json")
            if not strategy_file.exists():
                # Try to find latest run
                runs_dir = Path("reports/runs")
                if runs_dir.exists():
                    run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()], reverse=True)
                    if run_dirs:
                        strategy_file = run_dirs[0] / "strategy_signals.json"
            
            if strategy_file.exists():
                with open(strategy_file) as f:
                    data = json.load(f)
                    fused = data.get("fused", {})
                    if fused:
                        # Convert to score format (buy=positive, sell=negative)
                        scores = {}
                        for sym, sig in fused.items():
                            direction = sig.get("direction", "hold")
                            score = sig.get("score", 0)
                            if direction == "buy":
                                scores[sym] = float(score)
                            elif direction == "sell":
                                scores[sym] = -float(score)
                            else:
                                scores[sym] = 0.0
                        return scores if scores else None
        except Exception:
            pass
        return None

    def _get_dynamic_max_positions(self) -> Optional[int]:
        """Return effective max positions cap.

        Priority:
        1) risk.max_positions_override (hard override)
        2) auto-risk level mapping from reports/auto_risk_eval.json
        """
        try:
            ov = getattr(self.risk_cfg, "max_positions_override", None)
            if ov is not None:
                ov_i = int(ov)
                if ov_i > 0:
                    return ov_i
        except Exception:
            pass

        try:
            p = Path("reports/auto_risk_eval.json")
            if not p.exists():
                return None
            obj = json.loads(p.read_text(encoding="utf-8"))
            lvl = str(obj.get("current_level", "")).upper()
            cap_map = {
                "PROTECT": 1,
                "DEFENSE": 3,
                "NEUTRAL": 5,
                "ATTACK": 8,
            }
            return cap_map.get(lvl)
        except Exception:
            return None

    def allocate(
        self,
        scores: Dict[str, float],
        market_data: Dict[str, MarketSeries],
        regime_mult: float,
        audit: Optional[Any] = None,
    ) -> PortfolioSnapshot:
        """分配目标权重
        
        Args:
            scores: Alpha评分 {symbol: score}
            market_data: 市场数据
            regime_mult: 市场状态乘数
            audit: 审计对象
            
        Returns:
            投资组合快照
        """
        # Try to use fused signals from multi-strategy if available
        fused_scores = self._load_fused_signals()
        if fused_scores:
            # Use fused signals for selection, but keep original scores for weight calculation
            # This ensures we select symbols that multi-strategy wants to trade
            selection_scores = fused_scores
            if audit:
                audit.add_note(f"Using fused signals for selection: {list(fused_scores.keys())[:5]}")
        else:
            selection_scores = scores
        
        if not selection_scores:
            return PortfolioSnapshot(target_weights={}, selected=[], volatilities={}, notes="no_scores")

        # Select top pct by score (using fused signals if available)
        items = sorted(selection_scores.items(), key=lambda kv: float(kv[1]), reverse=True)
        k = max(1, int(np.ceil(len(items) * float(self.alpha_cfg.long_top_pct))))
        selected = [s for s, score in items[:k] if score >= float(getattr(self.alpha_cfg, "min_score_threshold", 0.0))]

        # Enforce dynamic risk-level position cap (PROTECT/DEFENSE/NEUTRAL/ATTACK)
        max_pos = self._get_dynamic_max_positions()
        if max_pos is not None and max_pos >= 0 and len(selected) > max_pos:
            selected = selected[:max_pos]
            if audit:
                audit.add_note(f"AutoRisk position cap applied: max_positions={max_pos}")

        # For weight calculation, use original scores (or fused if no original)
        weights_scores = scores if scores else fused_scores
        
        # Handle case where selected symbols may not be in weights_scores
        # (e.g., fused signals include symbols not in alpha scores)
        valid_selected = [s for s in selected if s in weights_scores]
        if len(valid_selected) != len(selected) and audit:
            skipped = [s for s in selected if s not in weights_scores]
            audit.add_note(f"Skipping symbols not in scores: {skipped}")
        selected = valid_selected
        
        if not selected:
            return PortfolioSnapshot(target_weights={}, selected=[], volatilities={}, notes="no_valid_selection")
        
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

        # Confidence weighting: softmax with temperature (更平滑的映射)
        sel_scores = np.array([weights_scores[s] for s in selected], dtype=float)
        
        # 方法1: softmax with temperature (避免0权重)
        # temperature 参数优化：0.5→0.9 降低集中度，减少换手
        # 目标 Effective N (1/∑w²) 在 3-6 之间
        temperature = 0.9  # 温度参数，越小越集中，越大越分散
        exp_scores = np.exp(sel_scores / temperature)
        softmax_probs = exp_scores / np.sum(exp_scores)
        conf = {s: float(softmax_probs[i]) for i, s in enumerate(selected)}
        
        # 方法2: 带下限的min-max (保留原逻辑作为备选)
        # mn = float(np.min(sel_scores))
        # mx = float(np.max(sel_scores))
        # denom = (mx - mn) if (mx - mn) != 0 else 1.0
        # conf = {s: max(0.2, float((scores[s] - mn) / denom)) for s in selected}  # 20%下限
        
        # 诊断：记录为什么conf可能为0
        portfolio_debug = {
            "inv_vol_norm": base_w,
            "confidence_raw": conf,
            "score_stats": {
                "min": float(np.min(sel_scores)) if len(sel_scores) > 0 else 0.0,
                "max": float(np.max(sel_scores)) if len(sel_scores) > 0 else 0.0,
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
