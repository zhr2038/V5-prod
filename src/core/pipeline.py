from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

import json
from pathlib import Path


def _effective_deadband(base: float, cfg: AppConfig, audit: Optional[DecisionAudit]) -> float:
    """F3.1: widen deadband when daily budget exceeded (monitor-driven, controlled).

    Budget is computed in main() from persisted daily state; pipeline consumes audit.budget.
    """
    db = float(base)
    try:
        if not cfg.budget.action_enabled:
            return db
        b = (audit.budget or {}) if audit else {}
        if not b or not bool(b.get("exceeded")):
            return db
        mult = float(cfg.budget.deadband_multiplier_exceeded)
        cap = float(cfg.budget.deadband_cap)
        return float(min(db * mult, cap))
    except Exception:
        return db


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


def _load_borrow_prevention_rules(path: str) -> Dict[str, Any]:
    try:
        p = Path(path)
        if not p.exists():
            return {}
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _is_high_risk_symbol(sym: str, *, rules: Dict[str, Any]) -> bool:
    s = str(sym)
    hr = rules.get("high_risk_symbols") or []
    if isinstance(hr, list) and s in [str(x) for x in hr]:
        return True
    return False


def _min_price_usdt(*, rules: Dict[str, Any]) -> Optional[float]:
    try:
        th = rules.get("rules") or []
        for r in th:
            if isinstance(r, dict) and "thresholds" in r:
                t = r.get("thresholds") or {}
                if isinstance(t, dict) and "min_price_usdt" in t:
                    return float(t.get("min_price_usdt"))
    except Exception:
        pass
    return None


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
        
        # 2. Regime检测后审计（显式处理空行情，避免 StopIteration）
        if not market_data_1h:
            if audit:
                audit.reject("no_market_data")
                audit.add_note("market_data_1h is empty; cannot run pipeline")
            raise ValueError("market_data_1h is empty")

        btc = market_data_1h.get("BTC/USDT")
        if btc is None:
            btc = next(iter(market_data_1h.values()))
        regime = self.regime_engine.detect(btc)
        if audit:
            audit.regime = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
            audit.regime_multiplier = regime.multiplier
        
        # Compute *raw* equity (for reporting / performance).
        equity_raw = self.compute_equity(cash_usdt=cash_usdt, positions=positions, market_data_1h=market_data_1h)
        cash_raw = float(cash_usdt)

        # Live small-budget safety: cap sizing equity if configured.
        # IMPORTANT: this cap is for *order sizing only*; it must not pollute reporting.
        equity = float(equity_raw)
        cash_usdt = float(cash_raw)

        cap_eq = getattr(self.cfg.budget, "live_equity_cap_usdt", None)
        if cap_eq is not None:
            try:
                cap_eq_f = float(cap_eq)
                if cap_eq_f >= 0:
                    equity = min(float(equity), cap_eq_f)
                    cash_usdt = min(float(cash_usdt), cap_eq_f)
            except Exception:
                pass

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
            # capture detailed exit reasons for explainability
            xs = []
            for o in exit_orders:
                meta = o.meta or {}
                xs.append(
                    {
                        "symbol": o.symbol,
                        "side": o.side,
                        "intent": o.intent,
                        "reason": meta.get("reason"),
                        "last": meta.get("last") or o.signal_price,
                        "stop": meta.get("stop"),
                        "highest": meta.get("highest"),
                        "atr": meta.get("atr"),
                        "atr_mult": meta.get("atr_mult"),
                        "atr_n": meta.get("atr_n"),
                    }
                )
            audit.exit_signals = xs

        # 7. Rebalance orders生成（deadband + 拒绝原因审计）
        rebalance_orders: List[Order] = []
        router_decisions = []

        # deadband: adapt by regime
        rstate = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
        if rstate == "Trending":
            deadband_base = float(self.cfg.rebalance.deadband_trending)
        elif rstate in ("Risk-Off", "Risk_Off", "RiskOff"):
            deadband_base = float(self.cfg.rebalance.deadband_riskoff)
        else:
            deadband_base = float(self.cfg.rebalance.deadband_sideways)

        deadband = _effective_deadband(deadband_base, self.cfg, audit)
        if audit:
            audit.rebalance_deadband_pct = deadband
            # record budget action (F3.1)
            b = audit.budget or {}
            if b.get("exceeded") and self.cfg.budget.action_enabled:
                audit.budget_action = {
                    "enabled": True,
                    "trigger": b.get("reason") or "unknown",
                    "deadband_base": deadband_base,
                    "deadband_multiplier": float(self.cfg.budget.deadband_multiplier_exceeded),
                    "deadband_cap": float(self.cfg.budget.deadband_cap),
                    "deadband_effective": deadband,
                    "min_trade_notional_multiplier": 1.0,
                    "min_trade_notional_effective": None,
                    "suppressed_orders_count": 0,
                    "suppressed_reasons": [],
                }
            else:
                audit.budget_action = {"enabled": False}

        # current weights
        current_w: Dict[str, float] = {}
        if equity > 0:
            for p in positions:
                pxp = float(prices.get(p.symbol, 0.0) or 0.0)
                if pxp <= 0:
                    continue
                current_w[p.symbol] = float(p.qty) * pxp / float(equity)

        cash_remaining = float(cash_usdt)

        # Rebalance should also handle symbols currently held but removed from target universe.
        # Iterate union(current positions, target weights). For symbols not in target, desired weight = 0.
        symbols_all = sorted(set(current_w.keys()) | set(target.keys()))

        for sym in symbols_all:
            tw = float(target.get(sym, 0.0))
            # deadband check on weight drift with banding: new position vs existing
            cw = float(current_w.get(sym, 0.0))
            drift = float(tw) - cw
            
            # Banding 逻辑：新建仓阈值 > 维持仓阈值
            # 判断是否是新建仓（当前权重接近0）
            is_new_position = cw < 0.001  # 当前持仓小于0.1%
            
            # 调整 deadband：新建仓需要更大的信号强度
            effective_deadband = deadband
            if is_new_position:
                # 新建仓阈值加倍（更保守）
                effective_deadband = deadband * 2.0
                if audit:
                    audit.add_note(f"Banding: {sym} is new position, deadband {deadband}→{effective_deadband:.3f}")
            
            if audit:
                audit.rebalance_drift_by_symbol[sym] = drift
                audit.rebalance_effective_deadband_by_symbol[sym] = effective_deadband
            
            if abs(drift) <= effective_deadband:
                if audit:
                    audit.rebalance_skipped_deadband_count += 1
                    audit.rebalance_skipped_deadband_by_symbol[sym] = abs(drift)
                    audit.reject("deadband_skip")
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "deadband",
                        "drift": drift,
                        "deadband": effective_deadband,
                        "is_new_position": is_new_position,
                    })
                continue

            px = float(prices.get(sym, 0.0) or 0.0)
            if px <= 0:
                if audit:
                    audit.reject("no_closed_bar")
                continue
            
            held = next((p for p in positions if p.symbol == sym and p.qty > 0), None)

            # If symbol is removed from target universe but currently held, generate a sell to reduce drift.
            if sym not in target and held is not None:
                side = "sell"
                intent = "REBALANCE"
                notional = abs(float(cw)) * float(equity)
                if notional <= 0:
                    continue
            else:
                side = "buy"
                intent = "OPEN_LONG" if held is None else "REBALANCE"
                notional = float(tw) * float(equity)
                if notional <= 0:
                    continue
            
            # Router check: min_notional (base + F3.2 stage-2)
            min_notional = float(self.cfg.budget.min_trade_notional_base)
            if audit and (audit.budget or {}).get("exceeded") and self.cfg.budget.action_enabled:
                try:
                    from src.core.budget_action import effective_min_trade_notional

                    eff, patch = effective_min_trade_notional(self.cfg, audit)
                    min_notional = float(eff)
                    # merge patch into budget_action
                    ba = audit.budget_action or {}
                    ba.update(patch)
                    audit.budget_action = ba
                except Exception:
                    pass

            # Borrow-prevention filter (live): skip opening high-risk low-price meme coins.
            # - allow sells to exit/clean up positions
            if side == "buy" and bool(getattr(self.cfg.execution, "borrow_prevention", False)):
                rules = _load_borrow_prevention_rules(str(getattr(self.cfg.execution, "high_risk_blacklist_path", "configs/borrow_prevention_rules.json")))
                mp = _min_price_usdt(rules=rules)
                if _is_high_risk_symbol(sym, rules=rules):
                    if audit:
                        audit.reject("high_risk_symbol")
                        router_decisions.append({"symbol": sym, "action": "skip", "reason": "high_risk_symbol"})
                    continue
                if mp is not None and float(px) < float(mp):
                    if audit:
                        audit.reject("min_price")
                        router_decisions.append({"symbol": sym, "action": "skip", "reason": f"min_price<{mp}", "px": px})
                    continue

            # Min-notional filter: apply to buys; allow sells (especially for removed symbols) to reduce drift.
            if side == "buy" and notional < float(min_notional):
                if audit:
                    audit.reject("min_notional")
                    router_decisions.append(
                        {
                            "symbol": sym,
                            "action": "skip",
                            "reason": "min_notional",
                            "notional": notional,
                            "min_notional": float(min_notional),
                        }
                    )
                    # budget_action suppression stats
                    try:
                        ba = audit.budget_action or {}
                        if ba.get("enabled"):
                            ba.setdefault("suppressed_reasons", [])
                            if "min_notional" not in ba["suppressed_reasons"]:
                                ba["suppressed_reasons"].append("min_notional")
                            ba["suppressed_orders_count"] = int(ba.get("suppressed_orders_count") or 0) + 1
                            sbs = ba.get("suppressed_by_symbol") or {}
                            sbs[sym] = float(notional)
                            ba["suppressed_by_symbol"] = sbs
                            audit.budget_action = ba
                    except Exception:
                        pass
                continue
            
            # 检查cash是否足够（按批次累计扣减，避免多单同时通过导致超额下单）
            if notional > cash_remaining:
                if audit:
                    audit.reject("insufficient_cash")
                    router_decisions.append(
                        {
                            "symbol": sym,
                            "action": "skip",
                            "reason": "insufficient_cash",
                            "notional": notional,
                            "cash_available": cash_remaining,
                            "cash_initial": float(cash_usdt),
                        }
                    )
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

            # Update batch cash budget.
            if side == "buy":
                cash_remaining -= float(notional)
            else:
                cash_remaining += float(notional)

            if audit:
                router_decisions.append(
                    {
                        "symbol": sym,
                        "action": "create",
                        "reason": "ok",
                        "side": side,
                        "notional": notional,
                        "cash_after": cash_remaining,
                    }
                )
        
        if audit:
            audit.router_decisions = router_decisions
            audit.counts["orders_rebalance"] = len(rebalance_orders)
            # fill budget_action suppression stats (F3.1)
            try:
                ba = audit.budget_action or {}
                if ba.get("enabled"):
                    ba["suppressed_orders_count"] = int(audit.rebalance_skipped_deadband_count)
                    ba["suppressed_reasons"] = ["deadband"] if audit.rebalance_skipped_deadband_count > 0 else []
                    audit.budget_action = ba
            except Exception:
                pass

        if run_logger is not None:
            try:
                now_ts = self.clock.now().isoformat().replace("+00:00", "Z")
                run_logger.log_equity({
                    "ts": now_ts,
                    # Reporting (raw) vs sizing (capped)
                    "cash": float(cash_raw),
                    "equity": float(equity_raw),
                    "cash_sizing": float(cash_usdt),
                    "equity_sizing": float(equity),
                    "equity_cap_usdt": float(getattr(self.cfg.budget, "live_equity_cap_usdt", 0.0) or 0.0) if getattr(self.cfg.budget, "live_equity_cap_usdt", None) is not None else None,
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
