from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from configs.schema import AppConfig
from src.alpha.alpha_engine import AlphaEngine, AlphaSnapshot
from src.core.models import MarketSeries, Order
from src.execution.position_store import Position
from src.portfolio.portfolio_engine import PortfolioEngine, PortfolioSnapshot
from src.regime.regime_engine import RegimeEngine, RegimeResult
from src.risk.exit_policy import ExitPolicy, ExitConfig
from src.risk.risk_engine import RiskEngine
from src.core.models import PositionState


@dataclass
class PipelineOutput:
    alpha: AlphaSnapshot
    regime: RegimeResult
    portfolio: PortfolioSnapshot
    orders: List[Order]


class V5Pipeline:
    """Shared Alpha->Regime->Portfolio->Risk->Exit pipeline.

    Designed so live(dry-run) and backtest can share the same semantics.
    """

    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.alpha_engine = AlphaEngine(cfg.alpha)
        self.regime_engine = RegimeEngine(cfg.regime)
        self.portfolio_engine = PortfolioEngine(alpha_cfg=cfg.alpha, risk_cfg=cfg.risk)
        self.risk_engine = RiskEngine(cfg.risk)
        self.exit_policy = ExitPolicy(ExitConfig())

    def run(self, market_data_1h: Dict[str, MarketSeries], positions: List[Position], equity_usdt: float = 100.0) -> PipelineOutput:
        alpha = self.alpha_engine.compute_snapshot(market_data_1h)
        btc = market_data_1h.get("BTC/USDT") or next(iter(market_data_1h.values()))
        regime = self.regime_engine.detect(btc)
        portfolio = self.portfolio_engine.allocate(scores=alpha.scores, market_data=market_data_1h, regime_mult=regime.multiplier)

        ps = PositionState(
            equity_usdt=float(equity_usdt),
            equity_peak_usdt=float(equity_usdt),
            positions={},
            entry_prices={p.symbol: p.avg_px for p in positions},
            highest_prices={p.symbol: p.highest_px for p in positions},
            days_held={},
        )
        rd = self.risk_engine.apply(ps)
        target = {s: float(w) * float(rd.delever_mult) for s, w in (portfolio.target_weights or {}).items()}

        prices = {s: float(market_data_1h[s].close[-1]) for s in market_data_1h.keys() if market_data_1h[s].close}

        # exits
        exit_orders = self.exit_policy.evaluate(positions=positions, market_data=market_data_1h, regime_state=str(regime.state.value if hasattr(regime.state, 'value') else regime.state))

        # rebalance (currently assumes current weights unknown => only opens; backtest fills handle positions)
        rebalance_orders: List[Order] = []
        for sym, tw in target.items():
            px = float(prices.get(sym, 0.0) or 0.0)
            if px <= 0:
                continue
            # if already held, treat as rebalance; else open
            held = next((p for p in positions if p.symbol == sym and p.qty > 0), None)
            side = "buy"
            intent = "OPEN_LONG" if held is None else "REBALANCE"
            notional = float(tw) * float(equity_usdt)
            if notional <= 0:
                continue
            rebalance_orders.append(Order(symbol=sym, side=side, intent=intent, notional_usdt=notional, signal_price=px, meta={"target_w": tw}))

        orders = exit_orders + rebalance_orders
        return PipelineOutput(alpha=alpha, regime=regime, portfolio=portfolio, orders=orders)
