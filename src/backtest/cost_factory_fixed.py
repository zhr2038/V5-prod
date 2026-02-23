from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from configs.schema import AppConfig

from .cost_calibration import CalibratedCostModel, FixedCostModel, load_latest_cost_stats


@dataclass
class CostModelMeta:
    mode: str  # calibrated|default
    source_day: Optional[str]
    fee_quantile: str
    slippage_quantile: str
    min_fills_global: int
    min_fills_bucket: int
    max_stats_age_days: int
    stats_path: Optional[str]
    global_fills: Optional[int]
    reason: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "source_day": self.source_day,
            "fee_quantile": self.fee_quantile,
            "slippage_quantile": self.slippage_quantile,
            "min_fills_global": self.min_fills_global,
            "min_fills_bucket": self.min_fills_bucket,
            "max_stats_age_days": self.max_stats_age_days,
            "stats_path": self.stats_path,
            "global_fills": self.global_fills,
            "reason": self.reason,
        }


def make_cost_model_from_cfg(cfg: AppConfig):
    '''创建成本模型，返回(model, meta)元组'''
    bt = cfg.backtest
    default_model = FixedCostModel(fee_bps=float(bt.fee_bps), slippage_bps=float(bt.slippage_bps))

    if str(bt.cost_model).lower() != "calibrated":
        return default_model, CostModelMeta(
            mode="default",
            source_day=None,
            fee_quantile=str(bt.fee_quantile),
            slippage_quantile=str(bt.slippage_quantile),
            min_fills_global=int(bt.min_fills_global),
            min_fills_bucket=int(bt.min_fills_bucket),
            max_stats_age_days=int(bt.max_stats_age_days),
            stats_path=None,
            global_fills=None,
            reason="cost_model_disabled",
        )

    stats, stats_path = load_latest_cost_stats(str(bt.cost_stats_dir), max_age_days=int(bt.max_stats_age_days))
    if not stats:
        return default_model, CostModelMeta(
            mode="default",
            source_day=None,
            fee_quantile=str(bt.fee_quantile),
            slippage_quantile=str(bt.slippage_quantile),
            min_fills_global=int(bt.min_fills_global),
            min_fills_bucket=int(bt.min_fills_bucket),
            max_stats_age_days=int(bt.max_stats_age_days),
            stats_path=stats_path,
            global_fills=None,
            reason="no_stats_found_or_too_old",
        )

    global_fills = None
    try:
        global_fills = int((stats.get("coverage") or {}).get("fills") or 0)
    except Exception:
        global_fills = None

    model = CalibratedCostModel(
        stats=stats,
        fee_quantile=str(bt.fee_quantile),
        slippage_quantile=str(bt.slippage_quantile),
        min_fills_global=int(bt.min_fills_global),
        min_fills_bucket=int(bt.min_fills_bucket),
        default_fee_bps=float(bt.fee_bps),
        default_slippage_bps=float(bt.slippage_bps),
    )

    # even if global fills insufficient, resolve() will fallback; meta explains source
    mode = "calibrated" if (global_fills is not None and global_fills >= int(bt.min_fills_global)) else "default"
    reason = None if mode == "calibrated" else "global_fills_insufficient"

    return model, CostModelMeta(
        mode=mode,
        source_day=stats.get("day"),
        fee_quantile=str(bt.fee_quantile),
        slippage_quantile=str(bt.slippage_quantile),
        min_fills_global=int(bt.min_fills_global),
        min_fills_bucket=int(bt.min_fills_bucket),
        max_stats_age_days=int(bt.max_stats_age_days),
        stats_path=stats_path,
        global_fills=global_fills,
        reason=reason,
    )


def make_cost_model_simple(cfg: AppConfig):
    '''简化版本：只返回模型，不返回元数据'''
    model, _ = make_cost_model_from_cfg(cfg)
    return model
