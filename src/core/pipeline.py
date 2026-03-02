from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

import json
from pathlib import Path

# 定义报告目录
REPORTS_DIR = Path(__file__).parent.parent.parent / 'reports'


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
from src.execution.position_builder import PositionBuilder  # Phase 2: 分批建仓
from src.execution.multi_level_stop_loss import MultiLevelStopLoss, StopLossConfig  # Phase 2: 动态止损
from src.portfolio.portfolio_engine import PortfolioEngine, PortfolioSnapshot

# RegimeEngine选择：Ensemble（推荐）或传统MA
try:
    from src.regime.ensemble_regime_engine import EnsembleRegimeEngine
    ENSEMBLE_AVAILABLE = True
except ImportError:
    ENSEMBLE_AVAILABLE = False
from src.regime.regime_engine import RegimeEngine, RegimeResult

from src.risk.exit_policy import ExitPolicy, ExitConfig
from src.risk.risk_engine import RiskEngine
from src.risk.fixed_stop_loss import FixedStopLossManager, FixedStopLossConfig
from src.risk.profit_taking import ProfitTakingManager  # 程序化利润管理
from src.risk.auto_risk_guard import AutoRiskGuard, get_auto_risk_guard  # 自动风险档位
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
    """PipelineOutput类"""
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
        
        # RegimeEngine选择：Ensemble（HMM+情绪）或传统MA
        if ENSEMBLE_AVAILABLE and getattr(cfg.regime, 'use_ensemble', False):
            print("[Pipeline] 使用EnsembleRegimeEngine (HMM+资金费率+RSS)")
            self.regime_engine = EnsembleRegimeEngine(cfg.regime)
        else:
            print("[Pipeline] 使用传统RegimeEngine (MA+ATR)")
            self.regime_engine = RegimeEngine(cfg.regime, 
                                              use_hmm=getattr(cfg.regime, 'use_hmm', False))
        
        self.portfolio_engine = PortfolioEngine(alpha_cfg=cfg.alpha, risk_cfg=cfg.risk)
        self.risk_engine = RiskEngine(cfg.risk)
        self.exit_policy = ExitPolicy(ExitConfig(), clock=self.clock)
        
        # Phase 2: 初始化分批建仓和动态止损管理器
        self.position_builder = PositionBuilder(
            stages=[0.3, 0.3, 0.4],
            price_drop_threshold=0.02,
            trend_confirmation_bars=2
        )
        self.stop_loss_manager = MultiLevelStopLoss(
            config=StopLossConfig(
                tight_pct=0.03,
                normal_pct=0.05,
                loose_pct=0.08
            )
        )
        
        # 固定比例止损（买入后立即生效的硬性止损）
        self.fixed_stop_loss = FixedStopLossManager(
            config=FixedStopLossConfig(
                enabled=True,
                base_stop_pct=0.05  # 5%硬性止损
            )
        )
        
        # 程序化利润管理
        self.profit_taking = ProfitTakingManager()
        
        # 自动风险档位守卫
        self.auto_risk_guard = get_auto_risk_guard()
        
        # Phase 3: 初始化ML数据收集器
        from src.execution.ml_data_collector import MLDataCollector
        self.data_collector = MLDataCollector()

    def mark_to_market(self, store, market_data_1h: Dict[str, MarketSeries]) -> None:
        """按市值计价更新持仓
        
        Args:
            store: 持仓存储
            market_data_1h: 市场数据
        """
        now_ts = self.clock.now().isoformat().replace("+00:00", "Z")
        for p in store.list():
            s = market_data_1h.get(p.symbol)
            if not s or not s.close:
                continue
            mark = float(s.close[-1])
            hi = float(s.high[-1]) if s.high else mark
            store.mark_position(symbol=p.symbol, now_ts=now_ts, mark_px=mark, high_px=hi)

    def compute_equity(self, cash_usdt: float, positions: List[Position], market_data_1h: Dict[str, MarketSeries]) -> float:
        """计算总权益
        
        Args:
            cash_usdt: 现金余额
            positions: 持仓列表
            market_data_1h: 市场数据
            
        Returns:
            总权益 (现金 + 持仓市值)
        """
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
        """运行完整的交易流水线
        
        Pipeline流程:
        1. 市场状态检测 (Regime)
        2. Alpha因子计算
        3. 投资组合分配
        4. 风控检查
        5. 退出策略评估
        6. 订单生成
        
        Args:
            market_data_1h: 1小时K线数据
            positions: 当前持仓
            cash_usdt: 现金余额
            equity_peak_usdt: 权益峰值
            run_logger: 运行日志记录器
            audit: 决策审计对象
            
        Returns:
            流水线输出 (Alpha, Regime, Portfolio, Orders)
        """
        # mark first
        store = None
        # 严谨的类型检查：确保positions是列表且元素有symbol属性
        if positions is not None and isinstance(positions, (list, tuple)) and len(positions) > 0:
            first_pos = positions[0]
            if hasattr(first_pos, 'symbol'):
                pass  # 正常情况
            elif isinstance(first_pos, dict) and 'symbol' in first_pos:
                pass  # dict格式也接受
            else:
                # 类型不匹配，记录警告
                if run_logger:
                    run_logger.warning(f"[Pipeline] positions格式异常: {type(first_pos)}")
        # caller can pass store via run_logger hook if desired; for now, marking is done by main.

        # 1) Regime detection (needed early if we want regime-aware alpha weights)
        # Regime检测后审计（显式处理空行情，避免 StopIteration）
        if not market_data_1h:
            if audit:
                audit.reject("no_market_data")
                audit.add_note("market_data_1h is empty; cannot run pipeline")
            raise ValueError("market_data_1h is empty")

        btc = market_data_1h.get("BTC/USDT")
        if btc is None:
            btc = next(iter(market_data_1h.values()))
        regime = self.regime_engine.detect(btc)
        
        # 2) Alpha计算（用于短线覆盖判断）
        alpha = self.alpha_engine.compute_snapshot(market_data_1h)
        
        # 3) 短线交易增强：Risk-Off 机会覆盖 (已禁用 - HMM标签已修复)
        # 当Alpha评分很高时，覆盖Risk-Off状态，允许短线交易
        # 注意：此功能已禁用，因为HMM模型已修复(TrendingUp标签)
        # 如果市场确实是Risk-Off(TrendingDown)，不应该强行买入
        """
        if regime.state.value == "Risk-Off":
            try:
                from src.regime.short_term_override import check_short_term_opportunity
                override = check_short_term_opportunity(
                    alpha_scores=alpha.scores
                )
                if override.should_override:
                    from configs.schema import RegimeState
                    from dataclasses import replace
                    old_state = regime.state
                    regime = replace(regime, state=RegimeState.SIDEWAYS, multiplier=override.new_multiplier)
                    if audit:
                        audit.add_note(f"[ShortTermOverride] {old_state.value} → Sideways: {override.reason}")
                        audit.regime_override = {
                            'from': 'Risk-Off',
                            'to': 'Sideways',
                            'reason': override.reason,
                            'confidence': override.confidence,
                            'new_multiplier': override.new_multiplier
                        }
            except Exception as e:
                if audit:
                    audit.add_note(f"[ShortTermOverride] error: {e}")
        """
        
        if audit:
            audit.regime = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
            audit.regime_multiplier = regime.multiplier
            # 保存Ensemble详情（如果可用）
            if hasattr(regime, 'votes') and regime.votes:
                audit.regime_details = {
                    'method': 'EnsembleRegimeEngine',
                    'votes': regime.votes,
                    'final_score': getattr(regime, 'final_score', 0),
                    'hmm_weight': getattr(self.cfg.regime, 'hmm_weight', 0),
                    'funding_weight': getattr(self.cfg.regime, 'funding_weight', 0),
                    'rss_weight': getattr(self.cfg.regime, 'rss_weight', 0),
                }

        # Optional: override alpha weights by regime (research/shadow only)
        if bool(getattr(self.cfg.alpha, 'dynamic_weights_by_regime_enabled', False)) and getattr(self.cfg.alpha, 'dynamic_weights_by_regime_path', None):
            try:
                import json
                from pathlib import Path
                from configs.schema import AlphaWeights

                p = Path(str(getattr(self.cfg.alpha, 'dynamic_weights_by_regime_path')))
                obj = json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}
                reg_key = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
                w = ((obj.get('regimes') or {}).get(reg_key) or {}).get('weights')
                if isinstance(w, dict) and w:
                    self.alpha_engine.cfg.weights = AlphaWeights(**w)
                    if audit:
                        audit.add_note(f"alpha weights overridden by regime={reg_key} from {p}")
            except Exception:
                pass

        # 2) Alpha计算后审计 (alpha已在前面计算)
        if audit:
            sorted_scores = sorted(alpha.scores.items(), key=lambda x: x[1], reverse=True)
            
            # Add strategy signal audit if multi-strategy is used
            if hasattr(self.alpha_engine, 'use_multi_strategy') and self.alpha_engine.use_multi_strategy:
                try:
                    import json as _json
                    from datetime import datetime
                    from pathlib import Path
                    # Load strategy signals from audit file
                    strategy_audit_file = Path(f"reports/runs/{datetime.now().strftime('%Y%m%d_%H')}/strategy_signals.json")
                    if strategy_audit_file.exists():
                        with open(strategy_audit_file, 'r') as f:
                            strategy_data = _json.load(f)
                        audit.strategy_signals = strategy_data.get('strategies', [])
                        # Add note about multi-strategy
                        total_signals = sum(s.get('total_signals', 0) for s in audit.strategy_signals)
                        audit.add_note(f"Multi-strategy: {len(audit.strategy_signals)} strategies, {total_signals} total signals")
                        for s in audit.strategy_signals:
                            audit.add_note(f"  {s['strategy']}: {s['total_signals']} signals ({s['buy_signals']} buy, {s['sell_signals']} sell)")
                except Exception as e:
                    audit.add_note(f"Strategy signal audit error: {str(e)[:50]}")
            
            audit.top_scores = [{"symbol": sym, "score": score} for sym, score in sorted_scores[:10]]
            audit.counts["scored"] = len(alpha.scores)

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
        # IMPORTANT: drawdown must be computed on *raw* equity (accounting truth), not capped sizing equity.
        # Otherwise small-budget equity caps (e.g. 20U) will create a fake massive drawdown and permanently throttle.
        # ALSO: track scale_basis for proper drawdown calculation when budget changes.
        from src.portfolio.portfolio_state import PortfolioState
        from src.execution.account_store import AccountStore

        # 获取资金规模基准（优先从数据库读取历史记录）
        cap_eq = getattr(self.cfg.budget, "live_equity_cap_usdt", None)
        
        # 读取数据库中的历史 scale_basis
        acc_store = AccountStore(path=str(REPORTS_DIR / 'positions.sqlite'))
        acc_state = acc_store.get()
        old_scale_basis = float(acc_state.scale_basis_usdt or 0)
        
        # 如果数据库没有记录，使用当前 budget_cap 或 peak
        if old_scale_basis <= 0:
            old_scale_basis = float(cap_eq) if cap_eq else float(equity_peak_usdt)
        
        # 新的 scale_basis 来自配置
        scale_basis = float(cap_eq) if cap_eq else old_scale_basis
        
        # 检测资金规模变化
        if scale_basis > 0 and old_scale_basis > 0:
            scale_ratio = scale_basis / old_scale_basis
            if scale_ratio < 0.5 or scale_ratio > 2.0:
                # 资金规模变化超过2倍，按比例调整峰值
                new_peak = float(equity_peak_usdt) * scale_ratio
                if audit:
                    audit.add_note(f"Scale basis changed: {old_scale_basis:.2f} -> {scale_basis:.2f}, "
                                 f"peak adjusted: {equity_peak_usdt:.2f} -> {new_peak:.2f}")
                equity_peak_usdt = new_peak
                
                # 更新数据库中的 scale_basis
                acc_store.update_scale_basis(scale_basis, propagate_to_peak=False)
            elif abs(scale_ratio - 1.0) < 0.01:
                # scale_basis 没有变化，确保数据库记录正确
                if acc_state.scale_basis_usdt != scale_basis:
                    acc_store.update_scale_basis(scale_basis, propagate_to_peak=False)

        pst = PortfolioState(
            cash_usdt=float(cash_raw),
            equity_usdt=float(equity_raw),
            peak_equity_usdt=float(equity_peak_usdt),
            scale_basis_usdt=scale_basis,
        )
        pst.update_equity(equity_raw)
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

        # 4.4 确保已有持仓都注册到止损/利润管理（避免重启后状态丢失）
        for p in positions:
            if float(p.qty) <= 0:
                continue
            px = float(prices.get(p.symbol, 0.0) or 0.0)
            if px <= 0:
                continue
            entry_ref = float(p.avg_px) if float(getattr(p, 'avg_px', 0.0) or 0.0) > 0 else px
            if p.symbol not in self.fixed_stop_loss.entry_prices:
                self.fixed_stop_loss.register_position(p.symbol, entry_ref)
            if p.symbol not in self.profit_taking.positions:
                self.profit_taking.register_position(p.symbol, entry_ref, current_price=px)

        # 4.5 Profit-first exit priority (profit-taking > fixed stop > rank exit)
        profit_orders = []
        fixed_stop_orders = []
        profit_symbols = set()  # Track symbols already handled by profit-taking
        
        for p in positions:
            s = market_data_1h.get(p.symbol)
            if not s or not s.close:
                continue
            current_price = float(s.close[-1])
            
            # 1st priority: 程序化利润管理（利润回撤锁盈）
            action, value, reason = self.profit_taking.evaluate(p.symbol, current_price)
            
            if action == 'sell_all' and float(p.qty) > 0:
                profit_orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * current_price,
                        signal_price=current_price,
                        meta={
                            "reason": f"profit_taking_{reason}",
                            "action": action,
                            "value": value,
                        },
                    )
                )
                profit_symbols.add(p.symbol)
                if audit:
                    audit.add_note(f"Profit taking: {p.symbol} {reason}")
                continue  # Skip other exit checks for this symbol
            
            # 2nd priority: 固定比例止损
            should_stop, stop_price, loss_pct = self.fixed_stop_loss.should_stop_loss(
                p.symbol, current_price
            )
            
            if should_stop and float(p.qty) > 0:
                fixed_stop_orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * current_price,
                        signal_price=current_price,
                        meta={
                            "reason": "fixed_stop_loss",
                            "entry_price": self.fixed_stop_loss.entry_prices.get(p.symbol, p.avg_px),
                            "stop_price": stop_price,
                            "loss_pct": loss_pct,
                        },
                    )
                )
                profit_symbols.add(p.symbol)  # Also skip rank exit
                if audit:
                    audit.add_note(f"Fixed stop loss: {p.symbol} loss {loss_pct*100:.1f}%")
                continue  # Skip rank exit for this symbol
        
        # 3rd priority: 排名退出（只在未被利润/止损处理时）
        ranking_exit_orders = []
        if hasattr(alpha, 'scores') and alpha.scores:
            sorted_scores = sorted(alpha.scores.items(), key=lambda x: x[1], reverse=True)
            symbol_ranks = {sym: idx+1 for idx, (sym, _) in enumerate(sorted_scores)}
            
            for p in positions:
                if p.qty <= 0 or p.symbol in profit_symbols:
                    continue  # Skip if already handled by profit-taking or stop loss
                
                current_rank = symbol_ranks.get(p.symbol, 999)
                should_exit, reason = self.profit_taking.should_exit_by_rank(
                    p.symbol, current_rank, max_rank=3
                )
                if should_exit:
                    s = market_data_1h.get(p.symbol)
                    if s and s.close:
                        current_price = float(s.close[-1])
                        ranking_exit_orders.append(
                            Order(
                                symbol=p.symbol,
                                side="sell",
                                intent="CLOSE_LONG",
                                notional_usdt=float(p.qty) * current_price,
                                signal_price=current_price,
                                meta={
                                    "reason": f"rank_exit_{reason}",
                                    "current_rank": current_rank,
                                },
                            )
                        )
                        if audit:
                            audit.add_note(f"Rank exit: {p.symbol} rank {current_rank}, {reason}")
        
        # 4. Policy-based exits (if not already handled)
        exit_orders = self.exit_policy.evaluate(
            positions=positions,
            market_data=market_data_1h,
            regime_state=str(regime.state.value if hasattr(regime.state, 'value') else regime.state),
        )
        # Filter out symbols already handled by profit/stop
        exit_orders = [o for o in exit_orders if o.symbol not in profit_symbols]
        
        # Merge all exit orders: profit first, then stop, then rank, then policy
        exit_orders = profit_orders + fixed_stop_orders + ranking_exit_orders + exit_orders

        # Deduplicate: keep only one exit per symbol per round, priority: sell_all > fixed_stop > atr > partial
        if exit_orders:
            prio_map = {
                'profit_taking_stop_loss_hit': 100,
                'fixed_stop_loss': 90,
                'atr_trailing': 80,
                'regime_exit': 70,
                'profit_partial': 60,
            }
            best = {}
            for o in exit_orders:
                reason = str((o.meta or {}).get('reason', ''))
                prio = 10
                for k, v in prio_map.items():
                    if reason.startswith(k):
                        prio = v
                        break
                cur = best.get(o.symbol)
                if cur is None or prio > cur[0]:
                    best[o.symbol] = (prio, o)
            exit_orders = [v[1] for v in best.values()]
        
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
        invalid_price_warnings: List[Dict] = []  # 记录价格无效的告警

        # Risk-Off 下是否进入 close-only：
        # 仅当策略明确将 risk_off 仓位倍数设为 0 时，才强制禁止 rebalance buy。
        # 这样可支持“Risk-Off 试探仓”（例如 pos_mult_risk_off=0.2）。
        regime_state_str = str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
        risk_off_mult = float(getattr(self.cfg.regime, 'pos_mult_risk_off', 0.0))
        is_risk_off_close_only = (
            regime_state_str in ("Risk-Off", "Risk_Off", "RiskOff") and risk_off_mult <= 0.0
        )
        if is_risk_off_close_only and audit:
            audit.add_note("Risk-Off close-only: rebalance buy suppressed (pos_mult_risk_off<=0)")

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

        # current weights (with dust filtering)
        current_w: Dict[str, float] = {}
        # Small-account-safe dust thresholds:
        # - value threshold is primary
        # - qty threshold only applies to tiny-value positions (to avoid wiping valid low-price holdings)
        DUST_QTY_THRESHOLD = float(getattr(self.cfg.execution, 'dust_qty_threshold', 1e-6) or 1e-6)
        DUST_VALUE_THRESHOLD = float(getattr(self.cfg.execution, 'dust_value_threshold', 0.5) or 0.5)

        if equity > 0:
            for p in positions:
                pxp = float(prices.get(p.symbol, 0.0) or 0.0)
                if pxp <= 0:
                    continue

                qty = float(p.qty or 0.0)
                position_value = qty * pxp

                # Treat as dust only when value is truly tiny.
                # qty gate is secondary and only meaningful in tiny-value zone.
                is_dust = (position_value < DUST_VALUE_THRESHOLD) and (qty < DUST_QTY_THRESHOLD or position_value < DUST_VALUE_THRESHOLD)
                if is_dust:
                    if audit and qty > 0:
                        audit.add_note(
                            f"Dust filter: {p.symbol} qty={qty:.8f} value=${position_value:.4f} "
                            f"(qty_th={DUST_QTY_THRESHOLD}, val_th={DUST_VALUE_THRESHOLD}) treated as 0"
                        )
                    continue

                current_w[p.symbol] = position_value / float(equity)

        cash_remaining = float(cash_usdt)

        # Rebalance should also handle symbols currently held but removed from target universe.
        # Iterate union(current positions, target weights). For symbols not in target, desired weight = 0.
        held_symbols = {p.symbol for p in positions if float(getattr(p, 'qty', 0.0) or 0.0) > 0}
        symbols_all = sorted(set(current_w.keys()) | set(target.keys()) | held_symbols)

        # Optional hard rule: force-close held symbols that are no longer in current scored universe.
        scored_symbols = set(alpha.scores.keys()) if alpha and getattr(alpha, 'scores', None) else set()
        force_close_unscored = bool(getattr(self.cfg.execution, 'force_close_unscored_positions', False))

        # Optional hard rule: require fused strategy signals for any buy order (disable alpha fallback buys).
        require_fused_buy = bool(getattr(self.cfg.execution, 'require_fused_signals_for_buy', False))
        fused_buy_symbols = set()
        if require_fused_buy:
            try:
                import json as _json
                from pathlib import Path as _Path
                from datetime import datetime as _dt

                cand_files = []
                same_hour = _Path(f"reports/runs/{_dt.now().strftime('%Y%m%d_%H')}/strategy_signals.json")
                if same_hour.exists():
                    cand_files.append(same_hour)

                runs_dir = _Path("reports/runs")
                if runs_dir.exists():
                    run_dirs = sorted(
                        [d for d in runs_dir.iterdir() if d.is_dir() and (d / "strategy_signals.json").exists()],
                        key=lambda x: x.stat().st_mtime,
                        reverse=True,
                    )
                    for d in run_dirs[:5]:
                        fp = d / "strategy_signals.json"
                        if fp not in cand_files:
                            cand_files.append(fp)

                for fp in cand_files:
                    obj = _json.loads(fp.read_text(encoding='utf-8'))
                    fused = obj.get('fused')
                    if isinstance(fused, dict) and fused:
                        for fsym, sig in fused.items():
                            if str((sig or {}).get('direction', '')).lower() == 'buy':
                                fused_buy_symbols.add(str(fsym))
                        break
            except Exception:
                fused_buy_symbols = set()

            if audit:
                audit.add_note(f"require_fused_signals_for_buy enabled: fused_buy_symbols={len(fused_buy_symbols)}")

        for sym in symbols_all:
            tw = float(target.get(sym, 0.0))
            # deadband check on weight drift with banding: new position vs existing
            cw = float(current_w.get(sym, 0.0))
            drift = float(tw) - cw

            held = next((p for p in positions if p.symbol == sym and float(getattr(p, 'qty', 0.0) or 0.0) > 0), None)

            # User hard rule: if symbol is held but absent from scoring list, force CLOSE_LONG.
            if force_close_unscored and held is not None and scored_symbols and sym not in scored_symbols:
                px_fs = float(prices.get(sym, 0.0) or 0.0)
                if px_fs <= 0:
                    px_fs = float(getattr(held, 'last_mark_px', 0.0) or 0.0)
                if px_fs > 0:
                    notional_fs = max(0.0, float(getattr(held, 'qty', 0.0) or 0.0) * px_fs)
                    if notional_fs > 0:
                        rebalance_orders.append(
                            Order(
                                symbol=sym,
                                side='sell',
                                intent='CLOSE_LONG',
                                notional_usdt=notional_fs,
                                signal_price=px_fs,
                                meta={'reason': 'force_close_unscored'},
                            )
                        )
                        if audit:
                            audit.add_note(f"Force close unscored: {sym} qty={float(getattr(held,'qty',0.0)):.8f}")
                            router_decisions.append({'symbol': sym, 'action': 'close', 'reason': 'force_close_unscored'})
                        continue
            
            # Banding 逻辑：新建仓阈值 > 维持仓阈值
            # 判断是否是新建仓（当前权重接近0）
            eps = float(getattr(self.cfg.rebalance, "new_position_weight_eps", 0.001) or 0.001)
            is_new_position = cw < eps

            # 调整 deadband：新建仓需要更大的信号强度；清仓（tw≈0）允许更小 deadband 以加速清理
            effective_deadband = deadband
            if is_new_position:
                mult = float(getattr(self.cfg.rebalance, "new_position_deadband_multiplier", 2.0) or 2.0)
                effective_deadband = deadband * mult
                if audit:
                    audit.add_note(f"Banding: {sym} is new position, deadband {deadband}→{effective_deadband:.3f}")

            # If target weight is ~0 (close-only), shrink deadband (but keep sells allowed) to avoid stuck dust positions.
            try:
                tw_eps = float(getattr(self.cfg.rebalance, "close_only_weight_eps", 0.001) or 0.001)
                if abs(float(tw)) <= tw_eps and abs(float(cw)) > tw_eps:
                    # 清仓模式：死区大幅降低，确保能卖出
                    cm = float(getattr(self.cfg.rebalance, "close_only_deadband_multiplier", 0.1) or 0.1)  # 0.5->0.1
                    effective_deadband = min(float(effective_deadband), float(deadband) * float(cm))
                    if audit:
                        audit.add_note(f"Close-only: {sym} tw≈0, deadband {deadband}→{effective_deadband:.3f} (force exit)")
            except Exception:
                pass
            
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
                # 记录价格无效告警
                invalid_price_warnings.append({
                    "symbol": sym,
                    "timestamp": datetime.utcnow().isoformat(),
                    "reason": "price_invalid_or_missing"
                })
                continue
            
            # P0 FIX: Risk-Off close-only 模式：跳过所有买入型的 rebalance
            if is_risk_off_close_only and drift > 0:
                if audit:
                    audit.reject("risk_off_close_only")
                    router_decisions.append({
                        "symbol": sym,
                        "action": "skip",
                        "reason": "risk_off_close_only",
                        "drift": drift,
                    })
                continue

            # If symbol is removed from target universe but currently held, generate a sell to reduce drift.
            # P0 FIX: 统一逻辑：根据 drift 符号决定买卖方向
            if drift < 0:
                # 需要减仓/清仓
                side = "sell"
                intent = "REBALANCE"
                # P0 FIX: notional 用 delta 计算
                notional = abs(float(drift)) * float(equity)
                if notional <= 0:
                    continue
            else:
                # drift > 0，需要加仓
                side = "buy"
                intent = "OPEN_LONG" if held is None else "REBALANCE"
                # P0 FIX: notional 用 delta 计算
                notional = abs(float(drift)) * float(equity)
                if notional <= 0:
                    continue
            
            # Require fused signals for buys (no alpha-fallback buys).
            if side == "buy" and require_fused_buy:
                if not fused_buy_symbols:
                    if audit:
                        audit.reject("require_fused_missing")
                        router_decisions.append(
                            {
                                "symbol": sym,
                                "action": "skip",
                                "reason": "require_fused_missing",
                            }
                        )
                    continue
                if sym not in fused_buy_symbols:
                    if audit:
                        audit.reject("require_fused_symbol")
                        router_decisions.append(
                            {
                                "symbol": sym,
                                "action": "skip",
                                "reason": "require_fused_symbol",
                            }
                        )
                    continue

            # Hard budget buy-block: when equity hits configured cap, force sell-only.
            # Prefer live equity from main(audit.budget.current_equity_usdt) to avoid local-state drift bypass.
            if side == "buy" and bool(getattr(self.cfg.budget, "hard_buy_block_on_cap", False)):
                try:
                    cap_raw = getattr(self.cfg.budget, "live_equity_cap_usdt", None)
                    cap_ratio = float(getattr(self.cfg.budget, "hard_buy_block_cap_ratio", 1.0) or 1.0)
                    if cap_raw is not None and float(cap_raw) > 0:
                        hard_cap = float(cap_raw) * cap_ratio

                        equity_ref = float(equity_raw)
                        try:
                            if audit and isinstance(getattr(audit, "budget", None), dict):
                                eq_live = audit.budget.get("current_equity_usdt")
                                if eq_live is not None:
                                    equity_ref = float(eq_live)
                        except Exception:
                            pass

                        if float(equity_ref) >= float(hard_cap):
                            if audit:
                                audit.reject("budget_hard_buy_block")
                                router_decisions.append(
                                    {
                                        "symbol": sym,
                                        "action": "skip",
                                        "reason": "budget_hard_buy_block",
                                        "equity_ref": float(equity_ref),
                                        "hard_cap": float(hard_cap),
                                    }
                                )
                            continue
                except Exception:
                    pass

            # Anti-chase for existing positions: avoid adding too high above own entry and avoid oversized add-ons.
            if side == "buy" and held is not None and bool(getattr(self.cfg.execution, "anti_chase_enabled", False)):
                try:
                    entry_px = float(getattr(held, "avg_px", 0.0) or 0.0)
                    held_qty = float(getattr(held, "qty", 0.0) or 0.0)
                    held_value = held_qty * float(px)
                    premium = (float(px) / entry_px - 1.0) if entry_px > 0 else 0.0
                    max_premium = float(getattr(self.cfg.execution, "anti_chase_max_entry_premium_pct", 0.015) or 0.015)
                    max_add_ratio = float(getattr(self.cfg.execution, "anti_chase_max_add_notional_ratio", 0.25) or 0.25)

                    if entry_px > 0 and premium > max_premium:
                        if audit:
                            audit.reject("anti_chase_premium")
                            router_decisions.append(
                                {
                                    "symbol": sym,
                                    "action": "skip",
                                    "reason": "anti_chase_premium",
                                    "entry_px": float(entry_px),
                                    "px": float(px),
                                    "premium": float(premium),
                                    "max_premium": float(max_premium),
                                }
                            )
                        continue

                    if held_value > 0 and float(notional) > float(held_value) * float(max_add_ratio):
                        if audit:
                            audit.reject("anti_chase_add_size")
                            router_decisions.append(
                                {
                                    "symbol": sym,
                                    "action": "skip",
                                    "reason": "anti_chase_add_size",
                                    "notional": float(notional),
                                    "held_value": float(held_value),
                                    "max_add_ratio": float(max_add_ratio),
                                }
                            )
                        continue
                except Exception:
                    pass

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

            # Exchange min-order filter (symbol-specific): avoid placing orders that the exchange will reject.
            # Uses OKX instrument minSz (base qty) to estimate a minimum USDT notional.
            if side == "buy" and bool(getattr(self.cfg.budget, "exchange_min_notional_enabled", True)):
                try:
                    from src.data.okx_instruments import OKXSpotInstrumentsCache

                    spec = OKXSpotInstrumentsCache().get_spec(symbol_to_inst_id(sym))
                    if spec is not None:
                        min_sz = float(spec.min_sz or 0.0)
                        # Estimate min notional requirement from base minSz.
                        min_notional_ex = float(min_sz) * float(px)
                        slack = float(getattr(self.cfg.budget, "exchange_min_notional_slack_multiplier", 1.05) or 1.05)
                        if min_notional_ex > 0 and float(notional) < float(min_notional_ex) * slack:
                            if audit:
                                audit.reject("exchange_min_notional")
                                router_decisions.append(
                                    {
                                        "symbol": sym,
                                        "action": "skip",
                                        "reason": "exchange_min_notional",
                                        "notional": float(notional),
                                        "min_notional_ex": float(min_notional_ex),
                                        "min_sz": float(min_sz),
                                        "px": float(px),
                                        "slack": float(slack),
                                    }
                                )
                            continue
                except Exception:
                    pass

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

            # 买入订单：注册止损和利润管理
            if side == "buy":
                self.fixed_stop_loss.register_position(sym, px)
                self.profit_taking.register_position(sym, px)  # 注册利润管理
                if audit:
                    stop_pct = self.fixed_stop_loss.config.get_stop_pct(sym)
                    audit.add_note(f"Fixed stop registered: {sym} @ {px:.4f}, stop @ {px*(1-stop_pct):.4f}")

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

        # Phase 3: ML数据收集
        # 收集特征快照用于训练ML模型
        try:
            current_ts = int(self.clock.now().timestamp() * 1000)
            for sym in symbols_all:
                if sym in market_data_1h:
                    px = float(prices.get(sym, 0))
                    if px > 0:
                        self.data_collector.collect_features(
                            timestamp=current_ts,
                            symbol=sym,
                            market_data={
                                'close': list(market_data_1h[sym].close) if hasattr(market_data_1h[sym], 'close') else [px],
                                'high': list(market_data_1h[sym].high) if hasattr(market_data_1h[sym], 'high') else [px],
                                'low': list(market_data_1h[sym].low) if hasattr(market_data_1h[sym], 'low') else [px],
                                'volume': list(market_data_1h[sym].volume) if hasattr(market_data_1h[sym], 'volume') else [0],
                            },
                            regime=str(regime.state.value if hasattr(regime.state, 'value') else regime.state)
                        )
            
            # 回填6小时前的标签
            filled_count = self.data_collector.fill_labels(current_ts)
            if audit and filled_count > 0:
                audit.add_note(f"ML data: filled {filled_count} labels")
        except Exception as e:
            # 数据收集失败不应影响交易
            if audit:
                audit.add_note(f"ML data collection skipped: {str(e)[:50]}")

        orders = exit_orders + rebalance_orders
        
        # 输出价格无效警告（如果有）
        if invalid_price_warnings and run_logger:
            run_logger.warning(f"[Pipeline] {len(invalid_price_warnings)} symbols have invalid prices: " + 
                             ", ".join([w['symbol'] for w in invalid_price_warnings[:5]]))
        
        return PipelineOutput(alpha=alpha, regime=regime, portfolio=portfolio, orders=orders)
