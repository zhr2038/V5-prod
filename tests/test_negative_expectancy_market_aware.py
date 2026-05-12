from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries, Order
from src.core.pipeline import V5Pipeline
from src.execution.fill_store import derive_runtime_auto_risk_eval_path
from src.execution.position_store import Position
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


def _write_auto_risk_level(order_store_path: str, level: str) -> None:
    path = derive_runtime_auto_risk_eval_path(order_store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"current_level": level}, ensure_ascii=False), encoding="utf-8")


def _strategy_payload(*, market_impulse: bool, include_alpha6_buy: bool = True):
    trend_signals = [
        {"symbol": "BTC/USDT", "side": "buy", "score": 0.90, "confidence": 0.8, "metadata": {"adx": 35.0}},
        {"symbol": "ETH/USDT", "side": "buy", "score": 0.82, "confidence": 0.8, "metadata": {"adx": 35.0}},
    ]
    if market_impulse:
        trend_signals.append(
            {"symbol": "SOL/USDT", "side": "buy", "score": 0.78, "confidence": 0.8, "metadata": {"adx": 35.0}}
        )

    strategies = [
        {
            "strategy": "TrendFollowing",
            "type": "trend",
            "allocation": 0.5,
            "total_signals": len(trend_signals),
            "buy_signals": len(trend_signals),
            "sell_signals": 0,
            "signals": trend_signals,
        }
    ]
    if include_alpha6_buy:
        strategies.append(
            {
                "strategy": "Alpha6Factor",
                "type": "alpha_6factor",
                "allocation": 0.5,
                "total_signals": 1,
                "buy_signals": 1,
                "sell_signals": 0,
                "signals": [
                    {
                        "symbol": "BTC/USDT",
                        "side": "buy",
                        "score": 0.60,
                        "metadata": {
                            "z_factors": {
                                "f4_volume_expansion": 0.10,
                                "f5_rsi_trend_confirm": 0.50,
                            }
                        },
                    }
                ],
            }
        )
    return {"strategies": strategies}


def _strategy_payload_for_symbol(symbol: str, *, score: float = 0.60):
    return {
        "strategies": [
            {
                "strategy": "TrendFollowing",
                "type": "trend",
                "allocation": 0.5,
                "total_signals": 1,
                "buy_signals": 1,
                "sell_signals": 0,
                "signals": [
                    {
                        "symbol": symbol,
                        "side": "buy",
                        "score": 0.90,
                        "confidence": 0.8,
                        "metadata": {"adx": 35.0},
                    }
                ],
            },
            {
                "strategy": "Alpha6Factor",
                "type": "alpha_6factor",
                "allocation": 0.5,
                "total_signals": 1,
                "buy_signals": 1,
                "sell_signals": 0,
                "signals": [
                    {
                        "symbol": symbol,
                        "side": "buy",
                        "score": score,
                        "confidence": 0.8,
                        "metadata": {
                            "raw_factors": {
                                "f4_volume_expansion": 1.246,
                                "f5_rsi_trend_confirm": 0.695,
                            },
                            "z_factors": {
                                "f4_volume_expansion": 1.246,
                                "f5_rsi_trend_confirm": 0.695,
                            },
                        },
                    }
                ],
            },
        ]
    }


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


def _base_cfg(tmp_path: Path) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = True
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = True
    cfg.execution.negative_expectancy_fast_fail_open_block_min_closed_cycles = 1
    cfg.execution.negative_expectancy_fast_fail_open_block_floor_bps = 5.0
    cfg.execution.negative_expectancy_fast_fail_market_aware = True
    cfg.execution.negative_expectancy_fast_fail_bypass_when_market_impulse = True
    cfg.execution.negative_expectancy_fast_fail_bypass_max_cycles = 1
    cfg.execution.negative_expectancy_fast_fail_bypass_min_net_bps = -80.0
    cfg.budget.min_trade_notional_base = 5.0
    return cfg


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


def _market_data() -> dict[str, MarketSeries]:
    return {
        "BTC/USDT": _series("BTC/USDT", 70000.0),
        "ETH/USDT": _series("ETH/USDT", 3500.0),
        "SOL/USDT": _series("SOL/USDT", 150.0),
        "BNB/USDT": _series("BNB/USDT", 650.0),
    }


def _selected_btc_portfolio():
    return SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )


def _selected_bnb_portfolio():
    return SimpleNamespace(
        target_weights={"BNB/USDT": 0.15},
        selected=["BNB/USDT"],
        entry_candidates=["BNB/USDT"],
        volatilities={},
        notes="",
    )


def _selected_symbol_portfolio(symbol: str):
    return SimpleNamespace(
        target_weights={symbol: 0.15},
        selected=[symbol],
        entry_candidates=[symbol],
        volatilities={},
        notes="",
    )


def test_fast_fail_rank_guard_hard_blocks_without_market_impulse(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload(market_impulse=False))
    audit = DecisionAudit(run_id="negexp-hard-block")
    alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0, "ETH/USDT": 0.8})

    adjusted = pipe._apply_negative_expectancy_rank_guard(
        alpha,
        {"symbols": {}, "stats": {"BTC/USDT": {"closed_cycles": 1, "fast_fail_closed_cycles": 1, "net_expectancy_bps": -70.0, "fast_fail_net_expectancy_bps": -70.0}}},
        positions=[],
        current_auto_risk_level="PROTECT",
        regime_state_str="Trending",
        audit=audit,
    )

    assert adjusted.scores["BTC/USDT"] < adjusted.scores["ETH/USDT"]
    assert audit.counts["negative_expectancy_fast_fail_hard_block_count"] == 1
    assert audit.counts.get("negative_expectancy_fast_fail_softened_count", 0) == 0


def test_fast_fail_rank_guard_softens_with_market_impulse(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload(market_impulse=True))
    audit = DecisionAudit(run_id="negexp-soften")
    alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0, "ETH/USDT": 0.8})

    adjusted = pipe._apply_negative_expectancy_rank_guard(
        alpha,
        {"symbols": {}, "stats": {"BTC/USDT": {"closed_cycles": 1, "fast_fail_closed_cycles": 1, "net_expectancy_bps": -70.0, "fast_fail_net_expectancy_bps": -70.0}}},
        positions=[],
        current_auto_risk_level="PROTECT",
        regime_state_str="Trending",
        audit=audit,
    )

    assert adjusted.scores["BTC/USDT"] == 1.0
    assert audit.counts["negative_expectancy_fast_fail_softened_count"] == 1
    assert audit.counts.get("negative_expectancy_fast_fail_hard_block_count", 0) == 0
    assert any("negative_expectancy_fast_fail_softened_by_market_impulse" in note for note in (audit.notes or []))


def test_active_cooldown_is_not_bypassed_even_with_market_impulse(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.negative_expectancy_cooldown_enabled = True
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload(market_impulse=True))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _selected_btc_portfolio()
    pipe.negative_expectancy_cooldown = _DummyNegexp(
        blocked={"BTC/USDT": {"remain_seconds": 3600}},
        stats={"BTC/USDT": {"closed_cycles": 1, "fast_fail_closed_cycles": 1, "net_expectancy_bps": -70.0, "fast_fail_net_expectancy_bps": -70.0}},
    )
    audit = DecisionAudit(run_id="negexp-active-cooldown")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "negative_expectancy_cooldown" for d in audit.router_decisions)
    assert audit.counts.get("negative_expectancy_fast_fail_softened_count", 0) == 0


def test_two_fast_fail_cycles_are_not_bypassed(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload(market_impulse=True))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _selected_btc_portfolio()
    pipe.negative_expectancy_cooldown = _DummyNegexp(
        stats={"BTC/USDT": {"closed_cycles": 2, "fast_fail_closed_cycles": 2, "net_expectancy_bps": -70.0, "fast_fail_net_expectancy_bps": -70.0}},
    )
    audit = DecisionAudit(run_id="negexp-two-cycles")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "negative_expectancy_fast_fail_open_block" for d in audit.router_decisions)
    assert audit.counts["negative_expectancy_fast_fail_hard_block_count"] == 1


def test_market_impulse_single_fast_fail_is_softened_in_open_path(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload(market_impulse=True))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _selected_btc_portfolio()
    pipe.negative_expectancy_cooldown = _DummyNegexp(
        stats={"BTC/USDT": {"closed_cycles": 1, "fast_fail_closed_cycles": 1, "net_expectancy_bps": -70.0, "fast_fail_net_expectancy_bps": -70.0}},
    )
    audit = DecisionAudit(run_id="negexp-soft-open-path")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].symbol == "BTC/USDT"
    assert not any(d.get("reason") == "negative_expectancy_fast_fail_open_block" for d in audit.router_decisions)
    assert audit.counts["negative_expectancy_fast_fail_softened_count"] == 1


def _run_short_cycle_guard_case(tmp_path: Path, *, current_level: str, stats: dict):
    cfg = _base_cfg(tmp_path)
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.protect_negative_expectancy_short_cycle_guard_enabled = True
    cfg.execution.protect_negative_expectancy_short_cycle_min_cycles = 2
    cfg.execution.protect_negative_expectancy_short_cycle_floor_bps = -80.0
    cfg.execution.protect_negative_expectancy_short_cycle_apply_to_normal_entry = True
    cfg.execution.protect_negative_expectancy_short_cycle_apply_to_probe = False
    cfg.execution.protect_alt_short_cycle_guard_enabled = False
    _write_auto_risk_level(cfg.execution.order_store_path, current_level)
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload(market_impulse=False))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _selected_bnb_portfolio()
    pipe.negative_expectancy_cooldown = _DummyNegexp(stats={"BNB/USDT": stats})
    audit = DecisionAudit(run_id=f"short-cycle-{current_level}")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BNB/USDT": 1.0}),
        precomputed_regime=_regime(),
    )
    return out, audit


def test_protect_short_cycle_negative_expectancy_blocks_normal_open(tmp_path: Path) -> None:
    out, audit = _run_short_cycle_guard_case(
        tmp_path,
        current_level="PROTECT",
        stats={"closed_cycles": 2, "net_expectancy_bps": -126.25},
    )

    assert not out.orders
    decision = next(
        d for d in audit.router_decisions if d.get("reason") == "protect_negative_expectancy_short_cycle_block"
    )
    assert decision["symbol"] == "BNB/USDT"
    assert decision["closed_cycles"] == 2
    assert decision["net_expectancy_bps"] == -126.25
    assert decision["floor_bps"] == -80.0
    assert audit.counts["protect_negative_expectancy_short_cycle_block_count"] == 1


def test_short_cycle_guard_is_inactive_outside_protect(tmp_path: Path) -> None:
    out, audit = _run_short_cycle_guard_case(
        tmp_path,
        current_level="NORMAL",
        stats={"closed_cycles": 2, "net_expectancy_bps": -126.25},
    )

    assert any(order.symbol == "BNB/USDT" and order.intent == "OPEN_LONG" for order in out.orders)
    assert not any(
        d.get("reason") == "protect_negative_expectancy_short_cycle_block" for d in audit.router_decisions
    )


def test_short_cycle_guard_does_not_apply_to_probe_by_default(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.protect_negative_expectancy_short_cycle_guard_enabled = True
    cfg.execution.protect_negative_expectancy_short_cycle_apply_to_probe = False
    cfg.execution.protect_alt_short_cycle_guard_enabled = False
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload(market_impulse=True))

    assert (
        pipe._protect_negative_expectancy_short_cycle_block_context(
            symbol="BTC/USDT",
            stat={"closed_cycles": 2, "net_expectancy_bps": -126.25},
            current_auto_risk_level="PROTECT",
            is_probe=True,
        )
        is None
    )


def test_short_cycle_guard_requires_min_cycles(tmp_path: Path) -> None:
    out, audit = _run_short_cycle_guard_case(
        tmp_path,
        current_level="PROTECT",
        stats={"closed_cycles": 1, "net_expectancy_bps": -126.25},
    )

    assert not any(
        d.get("reason") == "protect_negative_expectancy_short_cycle_block" for d in audit.router_decisions
    )
    assert not audit.counts.get("protect_negative_expectancy_short_cycle_block_count", 0)
    assert out.orders or any(str(d.get("reason", "")).startswith("protect_entry_") for d in audit.router_decisions)


def test_short_cycle_guard_requires_floor_breach(tmp_path: Path) -> None:
    out, audit = _run_short_cycle_guard_case(
        tmp_path,
        current_level="PROTECT",
        stats={"closed_cycles": 2, "net_expectancy_bps": -30.0},
    )

    assert not any(
        d.get("reason") == "protect_negative_expectancy_short_cycle_block" for d in audit.router_decisions
    )
    assert not audit.counts.get("protect_negative_expectancy_short_cycle_block_count", 0)
    assert out.orders or any(str(d.get("reason", "")).startswith("protect_entry_") for d in audit.router_decisions)


def _run_alt_short_cycle_guard_case(
    tmp_path: Path,
    *,
    symbol: str = "BNB/USDT",
    current_level: str = "PROTECT",
    stats: dict,
    final_score=1.0,
):
    cfg = _base_cfg(tmp_path)
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.protect_negative_expectancy_short_cycle_guard_enabled = False
    cfg.execution.protect_negative_expectancy_short_cycle_min_cycles = 2
    cfg.execution.protect_negative_expectancy_short_cycle_floor_bps = -80.0
    cfg.execution.protect_alt_short_cycle_guard_enabled = True
    cfg.execution.protect_alt_short_cycle_symbols = ["BNB/USDT", "ETH/USDT"]
    cfg.execution.protect_alt_short_cycle_min_cycles = 2
    cfg.execution.protect_alt_short_cycle_net_floor_bps = -20.0
    cfg.execution.protect_alt_short_cycle_fast_fail_floor_bps = -30.0
    cfg.execution.protect_alt_short_cycle_apply_to_normal_entry = True
    cfg.execution.protect_alt_short_cycle_apply_to_probe = False
    cfg.execution.protect_entry_confirm_rounds = 1
    cfg.execution.cost_aware_entry_enabled = False
    cfg.execution.cost_aware_score_per_bps = 0.0030
    cfg.execution.cost_aware_min_score_floor = 0.18
    _write_auto_risk_level(cfg.execution.order_store_path, current_level)
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload_for_symbol(symbol, score=0.60))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _selected_symbol_portfolio(symbol)
    pipe.negative_expectancy_cooldown = _DummyNegexp(stats={symbol: stats})
    audit = DecisionAudit(run_id=f"alt-short-cycle-{symbol.replace('/', '-')}")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(
            raw_factors={},
            z_factors={},
            scores={} if final_score is None else {symbol: float(final_score)},
        ),
        precomputed_regime=_regime(),
    )
    return out, audit


def test_protect_alt_short_cycle_blocks_bnb_fast_fail_negative_expectancy(tmp_path: Path) -> None:
    out, audit = _run_alt_short_cycle_guard_case(
        tmp_path,
        stats={
            "closed_cycles": 2,
            "net_expectancy_bps": -17.15,
            "fast_fail_net_expectancy_bps": -36.55,
        },
    )

    assert not out.orders
    decision = next(
        d for d in audit.router_decisions if d.get("reason") == "protect_alt_short_cycle_negative_expectancy"
    )
    assert decision["symbol"] == "BNB/USDT"
    assert decision["closed_cycles"] == 2
    assert decision["net_expectancy_bps"] == -17.15
    assert decision["fast_fail_net_expectancy_bps"] == -36.55
    assert decision["net_floor_bps"] == -20.0
    assert decision["fast_fail_floor_bps"] == -30.0
    assert audit.counts["protect_alt_short_cycle_negative_expectancy_count"] == 1


def test_protect_alt_short_cycle_requires_min_cycles(tmp_path: Path) -> None:
    out, audit = _run_alt_short_cycle_guard_case(
        tmp_path,
        stats={
            "closed_cycles": 1,
            "net_expectancy_bps": -100.0,
            "fast_fail_net_expectancy_bps": -100.0,
        },
    )

    assert any(order.symbol == "BNB/USDT" and order.intent == "OPEN_LONG" for order in out.orders)
    assert not any(
        d.get("reason") == "protect_alt_short_cycle_negative_expectancy" for d in audit.router_decisions
    )


def test_protect_alt_short_cycle_allows_positive_expectancy_bnb(tmp_path: Path) -> None:
    out, audit = _run_alt_short_cycle_guard_case(
        tmp_path,
        stats={
            "closed_cycles": 2,
            "net_expectancy_bps": 10.0,
            "fast_fail_net_expectancy_bps": -5.0,
        },
    )

    assert any(order.symbol == "BNB/USDT" and order.intent == "OPEN_LONG" for order in out.orders)
    assert not any(
        d.get("reason") == "protect_alt_short_cycle_negative_expectancy" for d in audit.router_decisions
    )


def test_buy_order_meta_includes_expected_edge_score_proxy(tmp_path: Path) -> None:
    out, _audit = _run_alt_short_cycle_guard_case(
        tmp_path,
        final_score=0.59,
        stats={
            "closed_cycles": 2,
            "net_expectancy_bps": 10.0,
            "fast_fail_net_expectancy_bps": -5.0,
        },
    )

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.intent == "OPEN_LONG")
    assert order.meta["final_score"] == 0.59
    assert order.meta["alpha6_score"] == 0.60
    assert order.meta["trend_score"] == 0.90
    assert order.meta["expected_edge_source"] == "final_score_proxy"
    assert order.meta["expected_edge_bps"] == pytest.approx((0.59 - 0.18) / 0.0030)
    assert order.meta["alpha6_expected_edge_bps_proxy"] == pytest.approx((0.60 - 0.18) / 0.0030)


def test_buy_order_meta_keeps_expected_edge_not_observable_when_final_score_missing(tmp_path: Path) -> None:
    out, _audit = _run_alt_short_cycle_guard_case(
        tmp_path,
        final_score=None,
        stats={
            "closed_cycles": 2,
            "net_expectancy_bps": 10.0,
            "fast_fail_net_expectancy_bps": -5.0,
        },
    )

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.intent == "OPEN_LONG")
    assert order.meta["final_score"] == "not_observable"
    assert order.meta["expected_edge_bps"] == "not_observable"
    assert order.meta["expected_edge_source"] == "not_observable"
    assert order.meta["alpha6_expected_edge_bps_proxy"] == pytest.approx((0.60 - 0.18) / 0.0030)


def test_protect_alt_short_cycle_ignores_symbols_outside_config(tmp_path: Path) -> None:
    out, audit = _run_alt_short_cycle_guard_case(
        tmp_path,
        symbol="SOL/USDT",
        stats={
            "closed_cycles": 2,
            "net_expectancy_bps": -120.0,
            "fast_fail_net_expectancy_bps": -120.0,
        },
    )

    assert any(order.symbol == "SOL/USDT" and order.intent == "OPEN_LONG" for order in out.orders)
    assert not any(
        d.get("reason") == "protect_alt_short_cycle_negative_expectancy" for d in audit.router_decisions
    )


def test_protect_alt_short_cycle_does_not_intercept_exit(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.protect_alt_short_cycle_guard_enabled = True
    cfg.execution.protect_alt_short_cycle_symbols = ["BNB/USDT", "ETH/USDT"]
    cfg.execution.protect_alt_short_cycle_min_cycles = 2
    cfg.execution.protect_alt_short_cycle_net_floor_bps = -20.0
    cfg.execution.protect_alt_short_cycle_fast_fail_floor_bps = -30.0
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload_for_symbol("BNB/USDT", score=0.60))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    pipe.exit_policy.evaluate = lambda positions, market_data, regime_state: [
        Order(
            symbol="BNB/USDT",
            side="sell",
            intent="CLOSE_LONG",
            notional_usdt=50.0,
            signal_price=650.0,
            meta={"reason": "atr_trailing"},
        )
    ]
    pipe.negative_expectancy_cooldown = _DummyNegexp(
        stats={
            "BNB/USDT": {
                "closed_cycles": 2,
                "net_expectancy_bps": -120.0,
                "fast_fail_net_expectancy_bps": -120.0,
            }
        }
    )
    audit = DecisionAudit(run_id="alt-short-cycle-exit")

    out = pipe.run(
        market_data_1h=_market_data(),
        positions=[
            Position(
                symbol="BNB/USDT",
                qty=0.1,
                avg_px=650.0,
                entry_ts="2026-05-11T00:00:00Z",
                highest_px=660.0,
                last_update_ts="2026-05-11T01:00:00Z",
                last_mark_px=650.0,
                unrealized_pnl_pct=0.0,
            )
        ],
        cash_usdt=100.0,
        equity_peak_usdt=120.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BNB/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any(order.symbol == "BNB/USDT" and order.intent == "CLOSE_LONG" for order in out.orders)
    close_order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.intent == "CLOSE_LONG")
    assert "expected_edge_bps" not in close_order.meta
    assert not any(
        d.get("reason") == "protect_alt_short_cycle_negative_expectancy" for d in audit.router_decisions
    )
