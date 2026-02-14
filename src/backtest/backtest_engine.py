from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np

from src.core.models import MarketSeries


@dataclass
class BacktestResult:
    sharpe: float
    cagr: float
    max_dd: float
    profit_factor: float
    turnover: float


class BacktestEngine:
    """Backtest skeleton (walk-forward + realistic constraints).

    Required by spec:
    - 1-bar delay
    - fees + slippage
    - volume constraints hook
    - walk-forward harness placeholder

    This scaffold provides API + placeholders; strategy logic should plug in.
    """

    def __init__(self, fee_bps: float = 6.0, slippage_bps: float = 5.0, one_bar_delay: bool = True):
        self.fee_bps = float(fee_bps)
        self.slippage_bps = float(slippage_bps)
        self.one_bar_delay = bool(one_bar_delay)

    def run(self, market_data: Dict[str, MarketSeries]) -> BacktestResult:
        # Placeholder backtest: compute buy&hold equal-weight returns as sanity check
        syms = list(market_data.keys())
        if not syms:
            return BacktestResult(0.0, 0.0, 0.0, 0.0, 0.0)

        # align by minimum length
        n = min(len(market_data[s].close) for s in syms)
        closes = np.stack([np.array(market_data[s].close[-n:], dtype=float) for s in syms], axis=1)
        rets = closes[1:] / closes[:-1] - 1.0
        port = np.mean(rets, axis=1)

        # fees/slippage approx per rebalance event not modeled here
        eq = np.cumprod(1.0 + port)
        max_eq = np.maximum.accumulate(eq)
        dd = 1.0 - (eq / max_eq)
        max_dd = float(np.max(dd)) if len(dd) else 0.0

        # sharpe annualization for 1h bars ~ 24*365
        ann = np.sqrt(24 * 365)
        sharpe = float(np.mean(port) / (np.std(port) + 1e-12) * ann)

        cagr = float(eq[-1] ** (365 * 24 / max(1, len(port))) - 1.0) if len(eq) else 0.0

        return BacktestResult(sharpe=sharpe, cagr=cagr, max_dd=max_dd, profit_factor=0.0, turnover=0.0)

    def walk_forward(self, market_data: Dict[str, MarketSeries], folds: int = 4) -> List[BacktestResult]:
        # Placeholder: split time into folds and run run()
        syms = list(market_data.keys())
        if not syms:
            return []
        n = min(len(market_data[s].close) for s in syms)
        step = max(10, n // int(folds))
        results: List[BacktestResult] = []
        for i in range(folds):
            end = min(n, (i + 1) * step)
            if end < 30:
                continue
            sub = {s: MarketSeries(symbol=s, timeframe=market_data[s].timeframe, ts=market_data[s].ts[-end:],
                                   open=market_data[s].open[-end:], high=market_data[s].high[-end:],
                                   low=market_data[s].low[-end:], close=market_data[s].close[-end:],
                                   volume=market_data[s].volume[-end:]) for s in syms}
            results.append(self.run(sub))
        return results
