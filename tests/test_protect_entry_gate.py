from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries, Order
from src.core.pipeline import V5Pipeline
from src.execution.fill_store import derive_runtime_auto_risk_eval_path, derive_runtime_auto_risk_guard_path
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


def _regime() -> RegimeResult:
    return RegimeResult(
        state=RegimeState.TRENDING,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.0,
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


def _strategy_payload(*, trend_signal=None, alpha6_signal=None):
    strategies = []
    if trend_signal is not None:
        strategies.append(
            {
                "strategy": "TrendFollowing",
                "type": "trend",
                "allocation": 0.5,
                "total_signals": 1,
                "buy_signals": 1 if str(trend_signal.get("side", "")).lower() == "buy" else 0,
                "sell_signals": 1 if str(trend_signal.get("side", "")).lower() == "sell" else 0,
                "signals": [trend_signal],
            }
        )
    if alpha6_signal is not None:
        strategies.append(
            {
                "strategy": "Alpha6Factor",
                "type": "alpha_6factor",
                "allocation": 0.5,
                "total_signals": 1,
                "buy_signals": 1 if str(alpha6_signal.get("side", "")).lower() == "buy" else 0,
                "sell_signals": 1 if str(alpha6_signal.get("side", "")).lower() == "sell" else 0,
                "signals": [alpha6_signal],
            }
        )
    return {"strategies": strategies}


def _build_pipe(cfg: AppConfig, tmp_path: Path, strategy_payload: dict) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path
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
    pipe.profit_taking.state_file = tmp_path / "profit_taking_state.json"
    pipe.profit_taking.positions = {}
    return pipe


def _base_cfg(tmp_path: Path) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    return cfg


def test_protect_trend_only_buy_is_skipped(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.92,
            "confidence": 0.8,
            "metadata": {"adx": 35.0},
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
    audit = DecisionAudit(run_id="protect-trend-only")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(
        d.get("reason") == "protect_entry_trend_only"
        and d.get("symbol") == "BTC/USDT"
        and d.get("trend_score") == 0.92
        for d in audit.router_decisions
    )


def test_protect_gate_falls_back_to_auto_risk_guard_when_eval_snapshot_missing(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_guard_level(cfg.execution.order_store_path, "PROTECT")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.92,
            "confidence": 0.8,
            "metadata": {"adx": 35.0},
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
    audit = DecisionAudit(run_id="protect-guard-fallback")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "protect_entry_trend_only" for d in audit.router_decisions)


def test_protect_gate_prefers_newer_auto_risk_guard_over_stale_eval_snapshot(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT", ts="2026-04-19T13:00:00")
    _write_auto_risk_guard_level(cfg.execution.order_store_path, "DEFENSE", last_update="2026-04-19T14:05:00")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.92,
            "confidence": 0.8,
            "metadata": {"adx": 35.0},
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
    audit = DecisionAudit(run_id="protect-guard-newer-than-eval")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].intent == "OPEN_LONG"
    assert not any(d.get("reason") == "protect_entry_trend_only" for d in audit.router_decisions)


def test_protect_gate_prefers_latest_eval_history_ts_when_eval_history_is_unsorted(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    eval_path = derive_runtime_auto_risk_eval_path(cfg.execution.order_store_path)
    eval_path.parent.mkdir(parents=True, exist_ok=True)
    eval_path.write_text(
        json.dumps(
            {
                "current_level": "PROTECT",
                "history": [
                    {"ts": "2026-04-19T15:05:00", "to": "PROTECT"},
                    {"ts": "2026-04-19T13:00:00", "to": "DEFENSE"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_auto_risk_guard_level(cfg.execution.order_store_path, "DEFENSE", last_update="2026-04-19T14:05:00")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.92,
            "confidence": 0.8,
            "metadata": {"adx": 35.0},
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
    audit = DecisionAudit(run_id="protect-eval-history-newer-than-guard")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "protect_entry_trend_only" for d in audit.router_decisions)


def test_protect_gate_accepts_legacy_guard_level_when_eval_missing(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    path = derive_runtime_auto_risk_guard_path(cfg.execution.order_store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"level": "PROTECT", "last_update": "2026-04-19T14:05:00"}), encoding="utf-8")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.92,
            "confidence": 0.8,
            "metadata": {"adx": 35.0},
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
    audit = DecisionAudit(run_id="protect-legacy-guard")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "protect_entry_trend_only" for d in audit.router_decisions)


def test_protect_trend_plus_alpha6_buy_can_pass(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.91,
            "confidence": 0.8,
            "metadata": {"adx": 32.0},
        },
        alpha6_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.45,
            "raw_score": 0.45,
            "confidence": 0.7,
            "metadata": {
                "raw_factors": {
                    "f4_volume_expansion": 0.10,
                    "f5_rsi_trend_confirm": 0.40,
                },
                "z_factors": {
                    "f4_volume_expansion": 0.20,
                    "f5_rsi_trend_confirm": 0.40,
                },
            },
        },
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="protect-alpha6-pass")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].symbol == "BTC/USDT"
    assert out.orders[0].intent == "OPEN_LONG"
    assert not any(str(d.get("reason", "")).startswith("protect_entry_") for d in audit.router_decisions)


def test_protect_alpha6_buy_with_too_weak_rsi_confirm_is_skipped(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.88,
            "confidence": 0.8,
            "metadata": {"adx": 31.0},
        },
        alpha6_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.45,
            "raw_score": 0.45,
            "confidence": 0.7,
            "metadata": {
                "raw_factors": {
                    "f4_volume_expansion": 0.10,
                    "f5_rsi_trend_confirm": 0.20,
                },
                "z_factors": {
                    "f4_volume_expansion": 0.20,
                    "f5_rsi_trend_confirm": 0.20,
                },
            },
        },
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="protect-rsi-negative")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(
        d.get("reason") == "protect_entry_rsi_confirm_too_weak"
        and d.get("symbol") == "BTC/USDT"
        and float(d.get("f5_rsi_trend_confirm")) < 0.30
        for d in audit.router_decisions
    )
    assert audit.counts["protect_entry_rsi_confirm_too_weak_count"] == 1


def test_protect_blocks_candidate_with_negative_volume_confirm_even_when_score_and_rsi_pass(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    payload = _strategy_payload(
        alpha6_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.46,
            "raw_score": 0.46,
            "confidence": 0.7,
            "metadata": {
                "raw_factors": {
                    "f4_volume_expansion": -0.05,
                    "f5_rsi_trend_confirm": 0.40,
                },
                "z_factors": {
                    "f4_volume_expansion": -0.10,
                    "f5_rsi_trend_confirm": 0.40,
                },
            },
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
    audit = DecisionAudit(run_id="protect-volume-negative")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(
        d.get("reason") == "protect_entry_volume_confirm_negative"
        and d.get("symbol") == "BTC/USDT"
        and float(d.get("f4_volume_expansion")) < 0.0
        for d in audit.router_decisions
    )
    assert audit.counts["protect_entry_volume_confirm_negative_count"] == 1


def test_protect_blocks_btc_candidate_with_low_score_and_negative_volume_confirm(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    payload = _strategy_payload(
        alpha6_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.386,
            "raw_score": 0.386,
            "confidence": 0.7,
            "metadata": {
                "raw_factors": {
                    "f4_volume_expansion": -0.134,
                    "f5_rsi_trend_confirm": 0.532,
                },
                "z_factors": {
                    "f4_volume_expansion": -0.268,
                    "f5_rsi_trend_confirm": 0.532,
                },
            },
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
    audit = DecisionAudit(run_id="protect-btc-quality")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(
        d.get("reason") == "protect_entry_alpha6_score_too_low"
        and d.get("symbol") == "BTC/USDT"
        and d.get("alpha6_score") == 0.386
        for d in audit.router_decisions
    )
    assert audit.counts["protect_entry_alpha6_score_too_low_count"] == 1


def test_protect_blocks_bnb_candidate_with_negative_volume_confirm(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BNB/USDT",
            "side": "buy",
            "score": 0.93,
            "confidence": 0.8,
            "metadata": {"adx": 30.0},
        },
        alpha6_signal={
            "symbol": "BNB/USDT",
            "side": "buy",
            "score": 0.279,
            "raw_score": 0.279,
            "confidence": 0.7,
            "metadata": {
                "raw_factors": {
                    "f4_volume_expansion": -0.527,
                    "f5_rsi_trend_confirm": 0.408,
                },
                "z_factors": {
                    "f4_volume_expansion": -1.054,
                    "f5_rsi_trend_confirm": 0.408,
                },
            },
        },
    )
    pipe = _build_pipe(cfg, tmp_path, payload)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BNB/USDT": 1.0},
        selected=["BNB/USDT"],
        entry_candidates=["BNB/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="protect-bnb-quality")

    out = pipe.run(
        market_data_1h={"BNB/USDT": _series("BNB/USDT", 600.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BNB/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(
        d.get("reason") == "protect_entry_alpha6_score_too_low"
        and d.get("symbol") == "BNB/USDT"
        for d in audit.router_decisions
    )
    assert audit.counts["protect_entry_alpha6_score_too_low_count"] == 1


def test_protect_candidate_with_strong_alpha6_rsi_and_volume_confirm_can_pass(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    payload = _strategy_payload(
        alpha6_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.45,
            "raw_score": 0.45,
            "confidence": 0.7,
            "metadata": {
                "raw_factors": {
                    "f4_volume_expansion": 0.10,
                    "f5_rsi_trend_confirm": 0.40,
                },
                "z_factors": {
                    "f4_volume_expansion": 0.20,
                    "f5_rsi_trend_confirm": 0.40,
                },
            },
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
    audit = DecisionAudit(run_id="protect-strong-pass")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].intent == "OPEN_LONG"
    assert not any(str(d.get("reason", "")).startswith("protect_entry_") for d in audit.router_decisions)


def test_non_protect_keeps_legacy_trend_only_open_behavior(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "NEUTRAL")

    payload = _strategy_payload(
        trend_signal={
            "symbol": "BTC/USDT",
            "side": "buy",
            "score": 0.92,
            "confidence": 0.8,
            "metadata": {"adx": 35.0},
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
    audit = DecisionAudit(run_id="neutral-trend-only")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].intent == "OPEN_LONG"
    assert not any(str(d.get("reason", "")).startswith("protect_entry_") for d in audit.router_decisions)


def test_exit_sell_is_not_affected_by_protect_entry_gate(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    pipe = _build_pipe(cfg, tmp_path, _strategy_payload())
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    pipe.exit_policy.evaluate = lambda positions, market_data, regime_state: [
        Order(
            symbol="BTC/USDT",
            side="sell",
            intent="CLOSE_LONG",
            notional_usdt=50.0,
            signal_price=50000.0,
            meta={"reason": "regime_exit"},
        )
    ]
    audit = DecisionAudit(run_id="protect-exit-sell")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[
            Position(
                symbol="BTC/USDT",
                qty=0.001,
                avg_px=50000.0,
                entry_ts="2026-04-18T00:00:00Z",
                highest_px=51000.0,
                last_update_ts="2026-04-18T00:00:00Z",
                last_mark_px=50000.0,
                unrealized_pnl_pct=0.0,
            )
        ],
        cash_usdt=50.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any(order.side == "sell" and order.intent == "CLOSE_LONG" for order in out.orders)
    assert any(
        d.get("reason") == "exit_signal_priority" and d.get("symbol") == "BTC/USDT"
        for d in audit.router_decisions
    )
