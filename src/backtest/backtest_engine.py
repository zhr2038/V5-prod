from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from collections import Counter

import numpy as np

from src.core.models import MarketSeries


@dataclass
class BacktestResult:
    sharpe: float
    cagr: float
    max_dd: float
    profit_factor: float
    turnover: float
    cost_assumption: Optional[Dict[str, Any]] = None


class BacktestEngine:
    """Backtest skeleton that reuses V5Pipeline semantics.

    Required by spec:
    - 1-bar delay
    - fees + slippage
    - volume constraints hook
    - walk-forward harness

    This scaffold implements a minimal simulator; it is not yet production-grade.
    """

    def __init__(
        self,
        fee_bps: float = 6.0,
        slippage_bps: float = 5.0,
        one_bar_delay: bool = True,
        cost_model=None,
        cost_model_meta: Optional[Dict[str, Any]] = None,
    ):
        self.fee_bps = float(fee_bps)
        self.slippage_bps = float(slippage_bps)
        self.one_bar_delay = bool(one_bar_delay)
        self.cost_model = cost_model
        self.cost_model_meta = cost_model_meta or {}
        self._fallback_counts = Counter()

    def _cost_assumption(self) -> Dict[str, Any]:
        ca = dict(self.cost_model_meta or {})
        ca["fallback_level_counts"] = dict(self._fallback_counts)
        return ca

    def run(self, market_data: Dict[str, MarketSeries], pipeline=None) -> BacktestResult:
        syms = list(market_data.keys())
        if not syms:
            return BacktestResult(0.0, 0.0, 0.0, 0.0, 0.0, cost_assumption=self._cost_assumption())

        from src.execution.position_store import Position
        from src.core.pipeline import V5Pipeline
        from configs.schema import AppConfig

        if pipeline is None:
            pipeline = V5Pipeline(AppConfig(symbols=syms))

        # align by min length
        n = min(len(market_data[s].close) for s in syms)
        if n < 80:
            return BacktestResult(0.0, 0.0, 0.0, 0.0, 0.0, cost_assumption=self._cost_assumption())

        cash = 1.0
        equity_curve = []
        peak = 1.0
        turnovers = []
        gains = 0.0
        losses = 0.0

        positions: Dict[str, Position] = {}

        for i in range(60, n - 2):
            # 1-bar delay: signals computed on bar i, executed at bar i+1 close
            md_slice = {s: MarketSeries(symbol=s, timeframe=market_data[s].timeframe,
                                        ts=market_data[s].ts[: i + 1],
                                        open=market_data[s].open[: i + 1],
                                        high=market_data[s].high[: i + 1],
                                        low=market_data[s].low[: i + 1],
                                        close=market_data[s].close[: i + 1],
                                        volume=market_data[s].volume[: i + 1]) for s in syms}

            # Backtest semantics: pass *cash* into pipeline; pipeline computes equity = cash + positions MTM.
            out = pipeline.run(md_slice, positions=list(positions.values()), cash_usdt=float(cash), equity_peak_usdt=float(peak))
            regime_state = str(out.regime.state.value if hasattr(out.regime.state, 'value') else out.regime.state)

            exec_px = {s: float(market_data[s].close[i + 1]) for s in syms}

            # apply orders
            traded_notional = 0.0
            for o in out.orders:
                px = float(exec_px.get(o.symbol, o.signal_price) or 0.0)
                if px <= 0:
                    continue

                # fees+slippage (calibrated if cost_model is provided)
                fee_bps = self.fee_bps
                slp_bps = self.slippage_bps
                if self.cost_model is not None:
                    try:
                        res = self.cost_model.resolve(o.symbol, regime_state, "fill", float(o.notional_usdt))
                        meta = {}
                        if isinstance(res, tuple) and len(res) == 3:
                            fee_bps, slp_bps, meta = res
                        else:
                            fee_bps, slp_bps = res
                        lvl = (meta or {}).get("fallback_level")
                        if lvl:
                            self._fallback_counts[str(lvl)] += 1
                        else:
                            self._fallback_counts["UNKNOWN"] += 1
                    except Exception:
                        fee_bps, slp_bps = self.fee_bps, self.slippage_bps
                        self._fallback_counts["ERROR"] += 1

                cost = (float(fee_bps) + float(slp_bps)) / 10_000.0
                notional = float(o.notional_usdt)

                if o.side == "buy":
                    # cash outflow is full notional; received base qty net of costs
                    if cash < notional:
                        continue
                    cash -= notional
                    qty = (notional / px) * (1.0 - cost)
                    traded_notional += abs(notional)

                    p = positions.get(o.symbol)
                    if p is None:
                        positions[o.symbol] = Position(
                            symbol=o.symbol,
                            qty=qty,
                            avg_px=px,
                            entry_ts="0",
                            highest_px=px,
                            last_update_ts="0",
                            last_mark_px=px,
                            unrealized_pnl_pct=0.0,
                        )
                    else:
                        new_qty = p.qty + qty
                        avg = (p.avg_px * p.qty + px * qty) / new_qty if new_qty else px
                        positions[o.symbol] = Position(
                            symbol=o.symbol,
                            qty=new_qty,
                            avg_px=avg,
                            entry_ts=p.entry_ts,
                            highest_px=max(p.highest_px, px),
                            last_update_ts="0",
                            last_mark_px=px,
                            unrealized_pnl_pct=0.0,
                        )

                else:
                    p = positions.get(o.symbol)
                    if p is None:
                        continue

                    traded_notional += abs(notional)

                    # Realize PnL (simplification: treat as full close)
                    pnl = (px - p.avg_px) * p.qty
                    if pnl >= 0:
                        gains += pnl
                    else:
                        losses += -pnl

                    # cash inflow net of costs
                    cash += notional * (1.0 - cost)
                    positions.pop(o.symbol, None)

            turnovers.append(traded_notional)

            # Mark-to-market equity after execution at bar i+1 close
            eq_now = float(cash)
            for p in positions.values():
                mp = float(exec_px.get(p.symbol, 0.0) or 0.0)
                if mp > 0:
                    eq_now += float(p.qty) * mp

            equity_curve.append(eq_now)
            peak = max(peak, eq_now)

        eq = np.array(equity_curve, dtype=float)
        if len(eq) < 5:
            return BacktestResult(0.0, 0.0, 0.0, 0.0, 0.0, cost_assumption=self._cost_assumption())

        rets = eq[1:] / eq[:-1] - 1.0
        max_eq = np.maximum.accumulate(eq)
        dd = 1.0 - (eq / max_eq)
        max_dd = float(np.max(dd))

        ann = np.sqrt(24 * 365)
        sharpe = float(np.mean(rets) / (np.std(rets) + 1e-12) * ann)
        cagr = float(eq[-1] ** (365 * 24 / max(1, len(rets))) - 1.0)
        pf = float(gains / (losses + 1e-12))
        turnover = float(np.mean(np.array(turnovers, dtype=float)))

        ca = dict(self.cost_model_meta or {})
        ca["fallback_level_counts"] = dict(self._fallback_counts)
        return BacktestResult(sharpe=sharpe, cagr=cagr, max_dd=max_dd, profit_factor=pf, turnover=turnover, cost_assumption=ca)

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
