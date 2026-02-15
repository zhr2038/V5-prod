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
from src.reporting.decision_audit import DecisionAudit


@dataclass
class PipelineOutput:
    alpha: AlphaSnapshot
    regime: RegimeResult
    portfolio: PortfolioSnapshot
    orders: List[Order]


class V5Pipeline:
    """Shared Alpha->Regime->Portfolio->Risk->Exit pipeline.

    Adds Commit-B semantics:
    - mark-to-market at cycle start (highest_px/mark/pnl/update_ts)
    - equity = cash + Σ(qty*mark)
    - portfolio drawdown scaling via RiskEngine

    Designed so live(dry-run) and backtest can share the same semantics.
    """

    def __init__(self, cfg: AppConfig, clock=None):
        self.cfg = cfg
        from src.core.clock import SystemClock

        self.clock = clock or SystemClock()
        self.alpha_engine = AlphaEngine(cfg.alpha)
        self.regime_engine = RegimeEngine(cfg.regime)
        self.portfolio_engine = PortfolioEngine(alpha_cfg=cfg.alpha, risk_cfg=cfg.risk)
        self.risk_engine = RiskEngine(cfg.risk)
        self.exit_policy = ExitPolicy(ExitConfig(), clock=self.clock)

    def mark_to_market(self, store, market_data_1h: Dict[str, MarketSeries]) -> None:
        now_ts = self.clock.now().isoformat().replace("+00:00", "Z")
        for p in store.list():
            s = market_data_1h.get(p.symbol)
            if not s or not s.close:
                continue
            mark = float(s.close[-1])
            hi = float(s.high[-1]) if s.high else mark
            store.mark_position(symbol=p.symbol, now_ts=now_ts, mark_px=mark, high_px=hi)

    def compute_equity(self, cash_usdt: float, positions: List[Position], market_data_1h: Dict[str, MarketSeries]) -> float:
        eq = float(cash_usdt)
        for p in positions:
            s = market_data_1h.get(p.symbol)
            if not s or not s.close:
                continue
            eq += float(p.qty) * float(s.close[-1])
        return float(eq)

    def run(
        self,
        market_data_1h: Dict[str, MarketSeries],
        positions: List[Position],
        cash_usdt: float,
        equity_peak_usdt: float,
        run_logger=None,
        audit: Optional[DecisionAudit] = None,
    ) -> PipelineOutput:
        # mark first
        store = None
        if positions and hasattr(positions[0], 'symbol'):
            pass
        # caller can pass store via run_logger hook if desired; for now, marking is done by main.

        # 1. Alpha计算后审计
        alpha = self.alpha_engine.compute_snapshot(market_data_1h)
        if audit:
            # 记录top scores
            sorted_scores = sorted(alpha.scores.items(), key=lambda x: x[1], reverse=True)
            audit.top_scores = [{"symbol": sym, "score": score} for sym, score in sorted_scores[:10]]
            audit.counts["scored"] = len(alpha.scores)
        
        # 2. Regime检测后审计
        btc = market_data_1h.get("BTC/USDT") or next(iter(market_data_1h.values()))
        regime = self.regime_engine.detect(btc)
        if audit:
            audit.regime = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
            audit.regime_multiplier = regime.multiplier
        
        equity = self.compute_equity(cash_usdt=cash_usdt, positions=positions, market_data_1h=market_data_1h)

        # Risk: drawdown-based exposure multiplier
        from src.portfolio.portfolio_state import PortfolioState

        pst = PortfolioState(cash_usdt=float(cash_usdt), equity_usdt=float(equity), peak_equity_usdt=float(equity_peak_usdt))
        pst.update_equity(equity)
        dd_mult = self.risk_engine.exposure_multiplier(pst.drawdown_pct)
        
        # 3. DD multiplier审计
        if audit and dd_mult < 1.0:
            audit.reject("dd_throttle")
            audit.add_note(f"DD multiplier: {dd_mult} (drawdown: {pst.drawdown_pct:.2%})")

        # 4. Portfolio分配后审计
        portfolio = self.portfolio_engine.allocate(
            scores=alpha.scores, 
            market_data=market_data_1h, 
            regime_mult=regime.multiplier,
            audit=audit
        )
        target0 = dict(portfolio.target_weights or {})
        if audit:
            audit.targets_pre_risk = target0
            audit.counts["targets_pre_risk"] = len(target0)
            audit.counts["selected"] = len(portfolio.selected)
            # 从portfolio_debug获取更多信息
            if hasattr(audit, 'portfolio_debug') and audit.portfolio_debug:
                audit.portfolio_debug = audit.portfolio_debug
        
        # 5. 风险缩放后审计
        target = self.portfolio_engine.scale_targets(target0, dd_mult)
        if audit:
            audit.targets_post_risk = target
        
        prices = {s: float(market_data_1h[s].close[-1]) for s in market_data_1h.keys() if market_data_1h[s].close}

        # 6. Exit orders审计
        exit_orders = self.exit_policy.evaluate(
            positions=positions,
            market_data=market_data_1h,
            regime_state=str(regime.state.value if hasattr(regime.state, 'value') else regime.state),
        )
        if audit:
            audit.counts["orders_exit"] = len(exit_orders)

        # 7. Rebalance orders生成（deadband + 拒绝原因审计）
        rebalance_orders: List[Order] = []
        router_decisions = []

        # deadband: adapt by regime
        rstate = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
        if rstate == "Trending":
            deadband = float(self.cfg.rebalance.deadband_trending)
        elif rstate in ("Risk-Off", "Risk_Off", "RiskOff"):
            deadband = float(self.cfg.rebalance.deadband_riskoff)
        else:
            deadband = float(self.cfg.rebalance.deadband_sideways)
        if audit:
            audit.rebalance_deadband_pct = deadband

        # current weights
        current_w: Dict[str, float] = {}
        if equity > 0:
            for p in positions:
                pxp = float(prices.get(p.symbol, 0.0) or 0.0)
                if pxp <= 0:
                    continue
                current_w[p.symbol] = float(p.qty) * pxp / float(equity)

        for sym, tw in target.items():
            # deadband check on weight drift
            cw = float(current_w.get(sym, 0.0))
            drift = float(tw) - cw
            if audit:
                audit.rebalance_drift_by_symbol[sym] = drift
            if abs(drift) <= deadband:
                if audit:
                    audit.rebalance_skipped_deadband_count += 1
                    audit.rebalance_skipped_deadband_by_symbol[sym] = abs(drift)
                    audit.reject("deadband_skip")
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "deadband",
                        "drift": drift,
                        "deadband": deadband,
                    })
                continue

            px = float(prices.get(sym, 0.0) or 0.0)
            if px <= 0:
                if audit:
                    audit.reject("no_closed_bar")
                continue
            
            held = next((p for p in positions if p.symbol == sym and p.qty > 0), None)
            side = "buy"
            intent = "OPEN_LONG" if held is None else "REBALANCE"
            notional = float(tw) * float(equity)
            
            if notional <= 0:
                continue
            
            # 模拟router决策（这里简化，实际应该调用router）
            # 检查min_notional（假设最小交易额为10 USDT）
            min_notional = 10.0
            if notional < min_notional:
                if audit:
                    audit.reject("min_notional")
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "min_notional",
                        "notional": notional,
                        "min_notional": min_notional
                    })
                continue
            
            # 检查cash是否足够
            if notional > cash_usdt:
                if audit:
                    audit.reject("insufficient_cash")
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip", 
                        "reason": "insufficient_cash",
                        "notional": notional,
                        "cash_available": cash_usdt
                    })
                continue
            
            # 如果通过所有检查，生成订单
            meta = {"target_w": tw, "dd_mult": dd_mult}
            if audit:
                meta.update(
                    {
                        "regime": audit.regime,
                        "window_start_ts": audit.window_start_ts,
                        "window_end_ts": audit.window_end_ts,
                        "deadband_pct": audit.rebalance_deadband_pct,
                        "drift": drift,
                    }
                )

            rebalance_orders.append(
                Order(
                    symbol=sym,
                    side=side,
                    intent=intent,
                    notional_usdt=notional,
                    signal_price=px,
                    meta=meta,
                )
            )
            
            if audit:
                router_decisions.append({
                    "symbol": sym,
                    "action": "create",
                    "reason": "ok",
                    "notional": notional
                })
        
        if audit:
            audit.router_decisions = router_decisions
            audit.counts["orders_rebalance"] = len(rebalance_orders)

        if run_logger is not None:
            try:
                now_ts = self.clock.now().isoformat().replace("+00:00", "Z")
                run_logger.log_equity({
                    "ts": now_ts,
                    "cash": float(cash_usdt),
                    "equity": float(equity),
                    "peak": float(pst.peak_equity_usdt),
                    "dd": float(pst.drawdown_pct),
                    "exposure_mult": float(dd_mult),
                })
                for p in positions:
                    run_logger.log_position({
                        "ts": now_ts,
                        "symbol": p.symbol,
                        "qty": float(p.qty),
                        "avg_px": float(p.avg_px),
                        "mark_px": float(getattr(p, 'last_mark_px', 0.0) or prices.get(p.symbol, 0.0)),
                        "highest_px": float(getattr(p, 'highest_px', 0.0)),
                        "unrealized_pnl_pct": float(getattr(p, 'unrealized_pnl_pct', 0.0)),
                    })
            except Exception:
                pass

        orders = exit_orders + rebalance_orders
        return PipelineOutput(alpha=alpha, regime=regime, portfolio=portfolio, orders=orders)
