from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.backtest.backtest_engine import BacktestEngine, BacktestResult
from src.backtest.cost_factory import make_cost_model_from_cfg


@dataclass
class WalkForwardFold:
    train_range: Tuple[int, int]
    test_range: Tuple[int, int]
    result: BacktestResult


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


def run_walk_forward(market_data: Dict[str, MarketSeries], folds: int = 4, cfg: Optional[AppConfig] = None) -> List[WalkForwardFold]:
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
        res = bt.run(sub)
        out.append(WalkForwardFold(train_range=tr, test_range=te, result=res))
    return out
