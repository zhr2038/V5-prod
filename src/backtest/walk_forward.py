from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
from collections import Counter
import numpy as np

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.backtest.backtest_engine import BacktestEngine, BacktestResult
from src.backtest.cost_factory import make_cost_model_from_cfg


@dataclass
class WalkForwardFold:
    """WalkForwardFold类"""
    train_range: Tuple[int, int]
    test_range: Tuple[int, int]
    result: BacktestResult


def build_walk_forward_report(folds: List[WalkForwardFold], cost_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Serialize folds + cost assumption into a report dict (schema_version=2)."""
    report: Dict[str, Any] = {
        "schema_version": 2,
        "cost_assumption_meta": cost_meta or {},
        "cost_assumption_aggregate": {"fallback_level_counts": {}},
        "folds": [],
    }

    agg = Counter()
    for f in folds:
        ca = (f.result.cost_assumption or {})
        fc = ca.get("fallback_level_counts") or {}
        try:
            agg.update({str(k): int(v) for k, v in fc.items()})
        except Exception:
            pass

        report["folds"].append(
            {
                "train_range": list(f.train_range),
                "test_range": list(f.test_range),
                "result": f.result.__dict__,
                "cost_assumption": ca,
            }
        )

    report["cost_assumption_aggregate"]["fallback_level_counts"] = dict(agg)
    return report


def build_folds(n: int, folds: int = 4) -> List[Tuple[Tuple[int, int], Tuple[int, int]]]:
    """Build folds"""
    folds = int(folds)
    if folds <= 0:
        return []
    step = n // folds
    out = []
    for i in range(folds):
        test_start = i * step
        test_end = n if i == folds - 1 else (i + 1) * step
        train_start = 0
        train_end = test_start
        out.append(((train_start, train_end), (test_start, test_end)))
    return out


def run_walk_forward(
    market_data: Dict[str, MarketSeries],
    folds: int = 4,
    cfg: Optional[AppConfig] = None,
    *,
    data_provider=None,
) -> List[WalkForwardFold]:
    """Run walk forward"""
    syms = list(market_data.keys())
    if not syms:
        return []
    n = min(len(market_data[s].close) for s in syms)
    out: List[WalkForwardFold] = []
    if cfg is None:
        cfg = AppConfig(symbols=syms)
    cost_model, meta = make_cost_model_from_cfg(cfg)
    bt = BacktestEngine(
        fee_bps=float(cfg.backtest.fee_bps),
        slippage_bps=float(cfg.backtest.slippage_bps),
        one_bar_delay=bool(cfg.backtest.one_bar_delay),
        cost_model=cost_model,
        cost_model_meta=meta.to_dict(),
    )
    for tr, te in build_folds(n, folds=folds):
        # For now: run on test slice only (train slice reserved for future calibration)
        s0, s1 = te
        if (s1 - s0) < 80:
            continue
        sub = {s: MarketSeries(symbol=s, timeframe=market_data[s].timeframe,
                               ts=market_data[s].ts[s0:s1], open=market_data[s].open[s0:s1],
                               high=market_data[s].high[s0:s1], low=market_data[s].low[s0:s1],
                               close=market_data[s].close[s0:s1], volume=market_data[s].volume[s0:s1]) for s in syms}
        res = bt.run(sub, cfg=cfg, data_provider=data_provider)
        out.append(WalkForwardFold(train_range=tr, test_range=te, result=res))
    return out


def build_portfolio_analysis_record(report: Dict[str, Any]) -> Dict[str, Any]:
    folds = list(report.get("folds") or [])
    sharpes = [float(((fold.get("result") or {}).get("sharpe") or 0.0)) for fold in folds]
    cagrs = [float(((fold.get("result") or {}).get("cagr") or 0.0)) for fold in folds]
    max_dds = [float(((fold.get("result") or {}).get("max_dd") or 0.0)) for fold in folds]
    profit_factors = [float(((fold.get("result") or {}).get("profit_factor") or 0.0)) for fold in folds]
    turnovers = [float(((fold.get("result") or {}).get("turnover") or 0.0)) for fold in folds]

    def _summary(vals: List[float]) -> Dict[str, float]:
        if not vals:
            return {"count": 0, "mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0}
        arr = np.asarray(vals, dtype=float)
        return {
            "count": int(len(arr)),
            "mean": float(np.mean(arr)),
            "std": float(np.std(arr)),
            "min": float(np.min(arr)),
            "max": float(np.max(arr)),
        }

    return {
        "status": "completed",
        "schema_version": int(report.get("schema_version") or 0),
        "fold_count": int(len(folds)),
        "metrics": {
            "sharpe": _summary(sharpes),
            "cagr": _summary(cagrs),
            "max_dd": _summary(max_dds),
            "profit_factor": _summary(profit_factors),
            "turnover": _summary(turnovers),
        },
        "cost_assumption_meta": report.get("cost_assumption_meta") or {},
        "cost_assumption_aggregate": report.get("cost_assumption_aggregate") or {},
    }
