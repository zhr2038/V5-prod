from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter
import os

import numpy as np

from configs.schema import AppConfig
from src.backtest.backtest_engine import BacktestEngine, BacktestResult
from src.backtest.cost_factory import make_cost_model_from_cfg
from src.core.models import MarketSeries


@dataclass
class WalkForwardFold:
    train_range: Tuple[int, int]
    test_range: Tuple[int, int]
    result: BacktestResult


def _slice_market_data(
    market_data: Dict[str, MarketSeries],
    *,
    start: int,
    end: int,
) -> Dict[str, MarketSeries]:
    syms = list(market_data.keys())
    return {
        s: MarketSeries(
            symbol=s,
            timeframe=market_data[s].timeframe,
            ts=market_data[s].ts[start:end],
            open=market_data[s].open[start:end],
            high=market_data[s].high[start:end],
            low=market_data[s].low[start:end],
            close=market_data[s].close[start:end],
            volume=market_data[s].volume[start:end],
        )
        for s in syms
    }


def _run_single_walk_forward_fold(
    *,
    fold_idx: int,
    train_range: Tuple[int, int],
    test_range: Tuple[int, int],
    market_data: Dict[str, MarketSeries],
    cfg: AppConfig,
    data_provider=None,
    suppress_output: bool = False,
) -> tuple[int, WalkForwardFold]:
    cost_model, meta = make_cost_model_from_cfg(cfg)
    bt = BacktestEngine(
        fee_bps=float(cfg.backtest.fee_bps),
        slippage_bps=float(cfg.backtest.slippage_bps),
        one_bar_delay=bool(cfg.backtest.one_bar_delay),
        cost_model=cost_model,
        cost_model_meta=meta.to_dict(),
    )
    if suppress_output:
        with open(os.devnull, "w", encoding="utf-8") as sink:
            with redirect_stdout(sink), redirect_stderr(sink):
                res = bt.run(market_data, cfg=cfg, data_provider=data_provider)
    else:
        res = bt.run(market_data, cfg=cfg, data_provider=data_provider)
    return fold_idx, WalkForwardFold(train_range=train_range, test_range=test_range, result=res)


def build_walk_forward_report(folds: List[WalkForwardFold], cost_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
    parallel_workers: int = 1,
) -> List[WalkForwardFold]:
    syms = list(market_data.keys())
    if not syms:
        return []
    n = min(len(market_data[s].close) for s in syms)
    out: List[WalkForwardFold] = []
    if cfg is None:
        cfg = AppConfig(symbols=syms)

    fold_specs: list[tuple[int, Tuple[int, int], Tuple[int, int], Dict[str, MarketSeries]]] = []
    for idx, (tr, te) in enumerate(build_folds(n, folds=folds)):
        s0, s1 = te
        if (s1 - s0) < 80:
            continue
        fold_specs.append((idx, tr, te, _slice_market_data(market_data, start=s0, end=s1)))

    if not fold_specs:
        return out

    effective_workers = max(1, min(int(parallel_workers or 1), len(fold_specs)))
    if effective_workers <= 1 or data_provider is not None:
        for fold_idx, tr, te, fold_market_data in fold_specs:
            _, fold = _run_single_walk_forward_fold(
                fold_idx=fold_idx,
                train_range=tr,
                test_range=te,
                market_data=fold_market_data,
                cfg=cfg,
                data_provider=data_provider,
            )
            out.append(fold)
        return out

    ordered: dict[int, WalkForwardFold] = {}
    with ProcessPoolExecutor(max_workers=effective_workers) as executor:
        future_map = {
            executor.submit(
                _run_single_walk_forward_fold,
                fold_idx=fold_idx,
                train_range=tr,
                test_range=te,
                market_data=fold_market_data,
                cfg=cfg,
                data_provider=data_provider,
                suppress_output=True,
            ): fold_idx
            for fold_idx, tr, te, fold_market_data in fold_specs
        }
        for future in as_completed(future_map):
            fold_idx, fold = future.result()
            ordered[int(fold_idx)] = fold

    out.extend(ordered[idx] for idx, _, _, _ in fold_specs)
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
