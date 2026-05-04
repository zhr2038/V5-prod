from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.fill_store import derive_runtime_auto_risk_eval_path, derive_runtime_auto_risk_guard_path, derive_runtime_named_json_path
from src.execution.position_store import Position
from src.execution.same_symbol_reentry_guard import record_same_symbol_exit_memory
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(sym: str, close: float) -> MarketSeries:
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(60)]
    close_arr = [close for _ in range(60)]
    vol = [1000.0 for _ in range(60)]
    return MarketSeries(
        symbol=sym,
        timeframe="1h",
        ts=ts,
        open=close_arr,
        high=close_arr,
        low=close_arr,
        close=close_arr,
        volume=vol,
    )


def _regime(state=RegimeState.TRENDING) -> RegimeResult:
    return RegimeResult(
        state=state,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.0 if state != RegimeState.RISK_OFF else 0.0,
    )


def _write_auto_risk_level(order_store_path: str, level: str, *, ts: str | None = None) -> None:
    path = derive_runtime_auto_risk_eval_path(order_store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"current_level": level}
    if ts is not None:
        payload["ts"] = ts
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_auto_risk_guard_level(order_store_path: str, level: str, *, last_update: str | None = None) -> None:
    path = derive_runtime_auto_risk_guard_path(order_store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"current_level": level}
    if last_update is not None:
        payload["last_update"] = last_update
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _strategy_payload(signals: dict[str, tuple[str, float, dict | None]]):
    trend_signals = []
    alpha6_signals = []
    for symbol, payload in signals.items():
        trend_side, trend_score, alpha6_meta = payload
        if trend_side:
            trend_signals.append(
                {
                    "symbol": symbol,
                    "side": trend_side,
                    "score": trend_score,
                    "confidence": 0.8,
                    "metadata": {"adx": 35.0},
                }
            )
        if alpha6_meta is not None:
            alpha6_signals.append(
                {
                    "symbol": symbol,
                    "side": str(alpha6_meta.get("side", "buy")),
                    "score": float(alpha6_meta.get("score", 0.0)),
                    "metadata": {
                        "z_factors": {
                            "f4_volume_expansion": float(alpha6_meta.get("f4", 0.0)),
                            "f5_rsi_trend_confirm": float(alpha6_meta.get("f5", 0.0)),
                        }
                    },
                }
            )
    strategies = []
    if trend_signals:
        strategies.append(
            {
                "strategy": "TrendFollowing",
                "type": "trend",
                "allocation": 0.5,
                "total_signals": len(trend_signals),
                "buy_signals": sum(1 for s in trend_signals if s["side"] == "buy"),
                "sell_signals": sum(1 for s in trend_signals if s["side"] == "sell"),
                "signals": trend_signals,
            }
        )
    if alpha6_signals:
        strategies.append(
            {
                "strategy": "Alpha6Factor",
                "type": "alpha_6factor",
                "allocation": 0.5,
                "total_signals": len(alpha6_signals),
                "buy_signals": sum(1 for s in alpha6_signals if s["side"] == "buy"),
                "sell_signals": sum(1 for s in alpha6_signals if s["side"] == "sell"),
                "signals": alpha6_signals,
            }
        )
    return {"strategies": strategies}


class _DummyNegexp:
    def __init__(self, *, blocked=None, stats=None):
        self._blocked = blocked or {}
        self._stats = stats or {}

    def is_blocked(self, symbol):
        return self._blocked.get(symbol)

    def get_symbol_stats(self, symbol):
        return self._stats.get(symbol, {})

    def set_scope(self, **kwargs):
        return None

    def refresh(self, force=False):
        return {"symbols": self._blocked, "stats": self._stats}


def _build_pipe(cfg: AppConfig, tmp_path: Path, strategy_payload: dict) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path / "reports"
    pipe = V5Pipeline(cfg)
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.portfolio_engine._load_fused_signals = lambda: {}
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    pipe.alpha_engine.get_latest_strategy_signal_payload = lambda: strategy_payload
    pipe.alpha_engine.strategy_signals_path = lambda: tmp_path / "reports" / "runs" / "test" / "strategy_signals.json"
    pipe.negative_expectancy_cooldown = _DummyNegexp()
    return pipe


def _base_cfg(tmp_path: Path) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.market_impulse_probe_enabled = True
    cfg.execution.market_impulse_probe_only_in_protect = True
    cfg.execution.market_impulse_probe_min_trend_buy_count = 3
    cfg.execution.market_impulse_probe_require_btc_trend_buy = True
    cfg.execution.market_impulse_probe_min_btc_trend_score = 0.60
    cfg.execution.market_impulse_probe_min_symbol_trend_score = 0.60
    cfg.execution.market_impulse_probe_target_w = 0.06
    cfg.execution.market_impulse_probe_max_symbols = 1
    cfg.execution.market_impulse_probe_time_stop_hours = 4
    cfg.execution.market_impulse_probe_dynamic_sizing_enabled = True
    cfg.execution.market_impulse_probe_min_executable_buffer = 1.05
    cfg.execution.market_impulse_probe_max_target_w = 0.10
    cfg.execution.min_trade_value_usdt = 0.0
    cfg.budget.min_trade_notional_base = 5.0
    return cfg


def _market_data() -> dict[str, MarketSeries]:
    return {
        "BTC/USDT": _series("BTC/USDT", 70000.0),
        "ETH/USDT": _series("ETH/USDT", 3500.0),
        "SOL/USDT": _series("SOL/USDT", 150.0),
        "BNB/USDT": _series("BNB/USDT", 650.0),
    }


def _market_data_with_btc(close: float) -> dict[str, MarketSeries]:
    data = _market_data()
    data["BTC/USDT"] = _series("BTC/USDT", close)
    return data


def _empty_portfolio():
    return SimpleNamespace(target_weights={}, selected=[], entry_candidates=[], volatilities={}, notes="")


def _write_reentry_memory(
    cfg: AppConfig,
    pipe: V5Pipeline,
    *,
    symbol: str = "BTC/USDT",
    hours_ago: float = 2.0,
    reason: str = "protect_profit_lock_trailing",
    exit_px: float = 70000.0,
    highest_px_before_exit: float = 70200.0,
    net_bps: float = 223.0,
) -> None:
    record_same_symbol_exit_memory(
        path=derive_runtime_named_json_path(cfg.execution.order_store_path, "same_symbol_reentry_exit_memory"),
        symbol=symbol,
        exit_ts_ms=int(pipe.clock.now().timestamp() * 1000) - int(hours_ago * 3600 * 1000),
        exit_px=exit_px,
        exit_reason=reason,
        highest_px_before_exit=highest_px_before_exit,
        net_bps=net_bps,
    )


def _impulse_payload():
    return _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
            "BNB/USDT": ("sell", 0.20, None),
        }
    )


def test_market_impulse_probe_opens_small_probe_in_protect(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _impulse_payload()
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-open")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.symbol == "BTC/USDT"
    assert order.intent == "OPEN_LONG"
    assert order.notional_usdt == 6.0
    assert order.meta["market_impulse_probe"] is True
    assert audit.counts["market_impulse_probe_open_count"] == 1
    assert audit.market_impulse_selection_mode == "priority"
    shadow = audit.market_impulse_shadow_selection
    assert shadow["active"] is True
    assert shadow["selected_live"] == "BTC/USDT"
    assert shadow["selected_by_priority"] == "BTC/USDT"


def test_market_impulse_shadow_trend_score_selects_highest_without_changing_live_priority(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 1.00, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-shadow-trend")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert out.orders[0].symbol == "BTC/USDT"
    shadow = audit.market_impulse_shadow_selection
    assert shadow["selected_live"] == "BTC/USDT"
    assert shadow["selected_by_priority"] == "BTC/USDT"
    assert shadow["selected_by_trend_score"] == "ETH/USDT"
    assert shadow["live_missed_eth_by_trend_score"] is True


def test_market_impulse_shadow_alpha6_confirmed_prefers_confirmed_buy(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.95, {"side": "sell", "score": 0.80, "f4": 0.20, "f5": 0.60}),
            "SOL/USDT": ("buy", 0.78, {"side": "buy", "score": 0.55, "f4": 0.10, "f5": 0.50}),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-shadow-alpha6")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert out.orders[0].symbol == "BTC/USDT"
    shadow = audit.market_impulse_shadow_selection
    assert shadow["selected_by_alpha6_confirmed"] == "SOL/USDT"
    sol = next(item for item in shadow["candidates"] if item["symbol"] == "SOL/USDT")
    assert sol["alpha6_confirmed"] is True


def test_market_impulse_shadow_expected_net_selects_observed_best(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    pipe.negative_expectancy_cooldown = _DummyNegexp(
        stats={
            "BTC/USDT": {"net_expectancy_bps": 5.0},
            "ETH/USDT": {"net_expectancy_bps": 22.0},
            "SOL/USDT": {"net_expectancy_bps": 12.0},
        }
    )
    audit = DecisionAudit(run_id="market-impulse-shadow-expected-net")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert out.orders[0].symbol == "BTC/USDT"
    shadow = audit.market_impulse_shadow_selection
    assert shadow["selected_by_expected_net_shadow"] == "ETH/USDT"
    eth = next(item for item in shadow["candidates"] if item["symbol"] == "ETH/USDT")
    assert eth["expected_net_bps"] == 22.0


def test_same_symbol_reentry_blocks_market_impulse_after_profit_lock(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _impulse_payload())
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    _write_reentry_memory(
        cfg,
        pipe,
        hours_ago=2.0,
        reason="protect_profit_lock_trailing",
        exit_px=70000.0,
        highest_px_before_exit=70200.0,
    )
    audit = DecisionAudit(run_id="same-symbol-profit-lock-block")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    decision = next(d for d in audit.router_decisions if d.get("reason") == "same_symbol_reentry_cooldown")
    assert decision["symbol"] == "BTC/USDT"
    assert decision["last_exit_reason"] == "protect_profit_lock_trailing"
    assert decision["last_exit_px"] == 70000.0
    assert decision["highest_px_before_exit"] == 70200.0
    assert decision["required_cooldown_hours"] == 6.0
    assert decision["breakout_exception_met"] is False
    assert audit.counts["same_symbol_reentry_cooldown_count"] == 1
    assert audit.counts["market_impulse_probe_open_count"] == 0


def test_same_symbol_reentry_allows_breakout_above_last_high(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _impulse_payload())
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    _write_reentry_memory(
        cfg,
        pipe,
        hours_ago=2.0,
        reason="protect_profit_lock_trailing",
        exit_px=70000.0,
        highest_px_before_exit=70200.0,
    )
    audit = DecisionAudit(run_id="same-symbol-breakout-bypass")

    out = pipe.run(
        market_data_1h=_market_data_with_btc(70350.0),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].symbol == "BTC/USDT"
    assert out.orders[0].meta["market_impulse_probe"] is True
    assert audit.counts["same_symbol_reentry_breakout_bypass_count"] == 1
    assert not any(d.get("reason") == "same_symbol_reentry_cooldown" for d in audit.router_decisions)


def test_same_symbol_reentry_blocks_probe_after_probe_stop_loss(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _impulse_payload())
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    _write_reentry_memory(
        cfg,
        pipe,
        hours_ago=2.0,
        reason="probe_stop_loss",
        exit_px=70000.0,
        highest_px_before_exit=70200.0,
        net_bps=-70.0,
    )
    audit = DecisionAudit(run_id="same-symbol-probe-stop-block")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    decision = next(d for d in audit.router_decisions if d.get("reason") == "same_symbol_reentry_cooldown")
    assert decision["last_exit_reason"] == "probe_stop_loss"
    assert decision["required_cooldown_hours"] == 8.0
    assert audit.counts["same_symbol_reentry_cooldown_count"] == 1


def test_same_symbol_reentry_does_not_block_other_symbol_memory(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _impulse_payload())
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    _write_reentry_memory(
        cfg,
        pipe,
        symbol="ETH/USDT",
        hours_ago=2.0,
        reason="protect_profit_lock_trailing",
        exit_px=3500.0,
        highest_px_before_exit=3510.0,
    )
    audit = DecisionAudit(run_id="same-symbol-other-symbol")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].symbol == "BTC/USDT"
    assert audit.counts["same_symbol_reentry_cooldown_count"] == 0


def test_same_symbol_reentry_allows_after_cooldown_expiry(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _impulse_payload())
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    _write_reentry_memory(
        cfg,
        pipe,
        hours_ago=7.0,
        reason="protect_profit_lock_trailing",
        exit_px=70000.0,
        highest_px_before_exit=70200.0,
    )
    audit = DecisionAudit(run_id="same-symbol-expired")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].symbol == "BTC/USDT"
    assert audit.counts["same_symbol_reentry_cooldown_count"] == 0


def test_market_impulse_probe_requires_three_trend_buys(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("sell", 0.30, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-min-count")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert audit.counts["market_impulse_probe_candidate_count"] == 2


def test_market_impulse_probe_requires_btc_trend_buy(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("sell", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
            "BNB/USDT": ("buy", 0.74, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-btc-required")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert not out.orders


def test_market_impulse_probe_respects_active_cooldown(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    pipe.negative_expectancy_cooldown = _DummyNegexp(blocked={"BTC/USDT": {"remain_seconds": 3600}})
    audit = DecisionAudit(run_id="market-impulse-cooldown")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert audit.counts["market_impulse_probe_blocked_count"] >= 1


def test_market_impulse_probe_can_bypass_single_fast_fail_cycle(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = True
    cfg.execution.negative_expectancy_fast_fail_open_block_min_closed_cycles = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    pipe.negative_expectancy_cooldown = _DummyNegexp(
        stats={"BTC/USDT": {"fast_fail_closed_cycles": 1, "fast_fail_net_expectancy_bps": -70.0}}
    )
    audit = DecisionAudit(run_id="market-impulse-fast-fail-bypass")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].meta["market_impulse_probe_bypassed_negative_expectancy_reason"] == "negative_expectancy_fast_fail_open_block"


def test_market_impulse_probe_not_active_outside_protect(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "DEFENSE")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-non-protect")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert not out.orders


def test_market_impulse_probe_not_active_in_risk_off(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-risk-off")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(RegimeState.RISK_OFF),
    )

    assert not out.orders


def test_market_impulse_probe_time_stop_generates_exit(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.probe_exit_enabled = False
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("sell", 0.20, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    state_path = derive_runtime_named_json_path(cfg.execution.order_store_path, "market_impulse_probe_state")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "BTC/USDT": {
                    "entry_ts_ms": _ms(1_700_000_000 + 55 * 3600),
                    "cooldown_until_ms": _ms(1_700_000_000 + 70 * 3600),
                    "time_stop_hours": 4,
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    audit = DecisionAudit(run_id="market-impulse-time-stop")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[
            Position(
                symbol="BTC/USDT",
                qty=0.001,
                avg_px=70000.0,
                entry_ts="2023-11-16T07:00:00Z",
                highest_px=70000.0,
                last_update_ts="2023-11-16T12:00:00Z",
                last_mark_px=70000.0,
                unrealized_pnl_pct=0.0,
                tags_json="{}",
            )
        ],
        cash_usdt=20.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any((order.meta or {}).get("reason") == "market_impulse_probe_time_stop" for order in out.orders)
    assert audit.counts["market_impulse_probe_time_stop_count"] == 1


def test_market_impulse_probe_dynamically_sizes_to_executable_notional(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.min_trade_notional_base = 9.0
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-dynamic-sizing")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=106.96,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.notional_usdt > 10.0
    assert (order.meta or {}).get("market_impulse_probe_target_w", 0.0) > 0.06
    assert (order.meta or {}).get("market_impulse_probe_target_w", 0.0) <= 0.10


def test_market_impulse_probe_skips_when_min_executable_target_exceeds_cap(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.min_trade_notional_base = 9.0
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    payload = _strategy_payload(
        {
            "BTC/USDT": ("buy", 0.90, None),
            "ETH/USDT": ("buy", 0.82, None),
            "SOL/USDT": ("buy", 0.78, None),
        }
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _empty_portfolio()
    audit = DecisionAudit(run_id="market-impulse-unexecutable")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=90.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "market_impulse_probe_unexecutable_notional" for d in audit.router_decisions)
    assert audit.counts["market_impulse_probe_unexecutable_notional_count"] == 1


def test_regular_non_probe_order_keeps_original_min_notional_behavior(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.market_impulse_probe_enabled = False
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.min_trade_notional_base = 9.0
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(
        cfg,
        tmp_path,
        _strategy_payload({"BTC/USDT": ("buy", 0.90, {"side": "buy", "score": 0.60, "f4": 0.10, "f5": 0.50})}),
    )
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 0.07},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="regular-min-notional")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "min_notional" for d in audit.router_decisions)
    assert not any(d.get("reason") == "market_impulse_probe_unexecutable_notional" for d in audit.router_decisions)
