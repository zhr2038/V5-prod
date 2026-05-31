from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.clock import FixedClock
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.execution_engine import ExecutionEngine
from src.execution.fill_store import derive_runtime_auto_risk_eval_path, derive_runtime_named_json_path
from src.execution.position_store import Position, PositionStore
from src.execution.same_symbol_reentry_guard import record_same_symbol_exit_memory
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


NOW = datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc)


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(symbol: str, close: float, *, high: float | None = None) -> MarketSeries:
    closes = [close for _ in range(60)]
    highs = [high if high is not None else close for _ in range(60)]
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(60)]
    return MarketSeries(
        symbol=symbol,
        timeframe="1h",
        ts=ts,
        open=list(closes),
        high=highs,
        low=list(closes),
        close=list(closes),
        volume=[1000.0 for _ in closes],
    )


def _regime(state: RegimeState = RegimeState.TRENDING) -> RegimeResult:
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
    path.write_text(json.dumps({"current_level": level}), encoding="utf-8")


def _alpha6_payload(
    symbol: str = "BTC/USDT",
    *,
    score: float = 0.60,
    f4: float = 0.50,
    f5: float = 0.35,
    factor_contribution: dict | None = None,
):
    metadata = {
        "raw_factors": {
            "f4_volume_expansion": f4,
            "f5_rsi_trend_confirm": f5,
        }
    }
    if factor_contribution is not None:
        metadata["factor_contribution"] = factor_contribution
    return {
        "strategies": [
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
                        "metadata": metadata,
                    }
                ],
            }
        ]
    }


def _strategy_payload(*, trend_signal: dict | None = None, alpha6_signal: dict | None = None) -> dict:
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


def _base_cfg(tmp_path: Path, symbols: list[str] | None = None) -> AppConfig:
    cfg = AppConfig(symbols=symbols or ["BTC/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.execution.slippage_db_path = str((tmp_path / "reports" / "slippage.sqlite").resolve())
    cfg.execution.market_impulse_probe_enabled = False
    cfg.execution.btc_leadership_probe_enabled = False
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.cost_aware_entry_enabled = False
    cfg.execution.fee_bps = 0.0
    cfg.execution.slippage_bps = 0.0
    cfg.budget.exchange_min_notional_enabled = False
    cfg.budget.min_trade_notional_base = 1.0
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.rebalance.deadband_trending = 0.0
    cfg.rebalance.deadband_sideways = 0.0
    cfg.rebalance.deadband_riskoff = 0.0
    return cfg


def _build_pipe(cfg: AppConfig, tmp_path: Path, strategy_payload: dict | None = None) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path
    pipe = V5Pipeline(cfg, clock=FixedClock(NOW))
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.portfolio_engine._load_fused_signals = lambda: {}
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    pipe.alpha_engine.get_latest_strategy_signal_payload = lambda: strategy_payload or {"strategies": []}
    pipe.alpha_engine.strategy_signals_path = lambda: tmp_path / "reports" / "runs" / "test" / "strategy_signals.json"
    pipe.profit_taking.state_file = tmp_path / "profit_taking_state.json"
    pipe.profit_taking.positions = {}
    return pipe


def _swing_position(symbol: str = "BTC/USDT", *, entry_ts: str = "2026-05-08T02:00:00Z") -> Position:
    tags = {
        "swing_hold_position": True,
        "swing_entry_ts": entry_ts,
        "swing_min_hold_hours": 24.0,
        "entry_reason": "normal_entry",
        "alpha6_score": 0.60,
        "alpha6_side": "buy",
        "f4_volume_expansion": 0.50,
        "f5_rsi_trend_confirm": 0.35,
        "current_level": "NORMAL",
        "entry_px": 100.0,
    }
    return Position(
        symbol=symbol,
        qty=1.0,
        avg_px=100.0,
        entry_ts=entry_ts,
        highest_px=100.0,
        last_update_ts=entry_ts,
        last_mark_px=100.0,
        unrealized_pnl_pct=0.0,
        tags_json=json.dumps(tags, ensure_ascii=False),
    )


def _write_same_symbol_exit_memory(
    cfg: AppConfig,
    *,
    symbol: str = "SOL/USDT",
    hours_ago: float = 1.0,
    exit_px: float = 102.0,
    highest_px_before_exit: float = 103.0,
    reason: str = "protect_profit_lock_trailing",
) -> None:
    record_same_symbol_exit_memory(
        path=derive_runtime_named_json_path(cfg.execution.order_store_path, "same_symbol_reentry_exit_memory"),
        symbol=symbol,
        exit_ts_ms=int(NOW.timestamp() * 1000) - int(float(hours_ago) * 3600 * 1000),
        exit_px=exit_px,
        exit_reason=reason,
        highest_px_before_exit=highest_px_before_exit,
        net_bps=128.0,
    )


def test_normal_alpha6_entry_marks_swing_hold_in_order_and_position_state(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "NORMAL")
    pipe = _build_pipe(cfg, tmp_path, _alpha6_payload())
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="swing-entry")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 100.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.intent == "OPEN_LONG"
    assert order.meta["swing_hold_position"] is True
    assert order.meta["entry_reason"] == "normal_entry"
    assert audit.counts["swing_hold_position_count"] == 1

    store = PositionStore(path=str((tmp_path / "reports" / "positions.sqlite").resolve()))
    ExecutionEngine(cfg.execution, position_store=store).execute(out.orders)
    position = store.get("BTC/USDT")
    assert position is not None
    tags = json.loads(position.tags_json)
    assert tags["swing_hold_position"] is True
    assert tags["swing_min_hold_hours"] == 24.0
    assert tags["alpha6_score"] == 0.60
    assert tags["f5_rsi_trend_confirm"] == 0.35


def test_f3_dominant_weak_confirmation_does_not_mark_swing_hold(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.swing_min_f4_volume = 0.0
    cfg.execution.swing_min_f5_rsi = 0.0
    _write_auto_risk_level(cfg.execution.order_store_path, "NORMAL")
    pipe = _build_pipe(
        cfg,
        tmp_path,
        _alpha6_payload(
            f4=0.10,
            f5=0.20,
            factor_contribution={"dominant_factor": "f3_vol_adj_ret", "contribution_pct": 0.70},
        ),
    )
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="f3-dominant-swing-block")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 100.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.intent == "OPEN_LONG"
    assert order.meta["swing_hold_position"] is False
    assert order.meta["swing_f3_dominant_blocked"] is True
    assert order.meta["dominant_factor"] == "f3_vol_adj_ret"
    assert order.meta["contribution_pct"] == pytest.approx(0.70)
    assert order.meta["dominant_factor_contribution_pct"] == pytest.approx(0.70)
    assert order.meta["f4"] == pytest.approx(0.10)
    assert order.meta["f5"] == pytest.approx(0.20)
    assert order.meta["swing_hold_block_reason"] == "swing_f3_dominant_not_qualified"
    assert audit.counts["swing_hold_position_count"] == 0
    decision = next(d for d in audit.router_decisions if d.get("action") == "create" and d.get("symbol") == "BTC/USDT")
    assert decision["swing_hold_position"] is False
    assert decision["swing_f3_dominant_blocked"] is True
    assert decision["dominant_factor"] == "f3_vol_adj_ret"
    assert decision["dominant_factor_contribution_pct"] == pytest.approx(0.70)
    assert decision["f4_volume_expansion"] == pytest.approx(0.10)
    assert decision["f5_rsi_trend_confirm"] == pytest.approx(0.20)
    assert decision["swing_hold_block_reason"] == "swing_f3_dominant_not_qualified"


def test_f3_dominant_strong_f4_f5_allows_swing_hold(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.swing_min_f4_volume = 0.0
    cfg.execution.swing_min_f5_rsi = 0.0
    _write_auto_risk_level(cfg.execution.order_store_path, "NORMAL")
    pipe = _build_pipe(
        cfg,
        tmp_path,
        _alpha6_payload(
            f4=0.80,
            f5=0.60,
            factor_contribution={"dominant_factor": "f3_vol_adj_ret", "contribution_pct": 0.70},
        ),
    )
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="f3-dominant-swing-allowed")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 100.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].meta["swing_hold_position"] is True
    assert "swing_f3_dominant_blocked" not in out.orders[0].meta
    assert audit.counts["swing_hold_position_count"] == 1
    decision = next(d for d in audit.router_decisions if d.get("action") == "create" and d.get("symbol") == "BTC/USDT")
    assert decision["entry_reason"] == "normal_entry"
    assert decision["swing_hold_position"] is True
    assert decision["swing_f3_dominant_blocked"] is False
    assert decision["dominant_factor"] == "f3_vol_adj_ret"
    assert decision["dominant_factor_contribution_pct"] == pytest.approx(0.70)
    assert decision["f4_volume_expansion"] == pytest.approx(0.80)
    assert decision["f5_rsi_trend_confirm"] == pytest.approx(0.60)


def test_non_f3_dominant_does_not_affect_swing_hold(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.swing_min_f4_volume = 0.0
    cfg.execution.swing_min_f5_rsi = 0.0
    _write_auto_risk_level(cfg.execution.order_store_path, "NORMAL")
    pipe = _build_pipe(
        cfg,
        tmp_path,
        _alpha6_payload(
            f4=0.10,
            f5=0.20,
            factor_contribution={"dominant_factor": "f4_volume_expansion", "contribution_pct": 0.70},
        ),
    )
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="non-f3-dominant-swing")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 100.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].meta["swing_hold_position"] is True
    assert "swing_f3_dominant_blocked" not in out.orders[0].meta
    assert audit.counts["swing_hold_position_count"] == 1
    decision = next(d for d in audit.router_decisions if d.get("action") == "create" and d.get("symbol") == "BTC/USDT")
    assert decision["entry_reason"] == "normal_entry"
    assert decision["swing_hold_position"] is True
    assert decision["swing_f3_dominant_blocked"] is False
    assert decision["dominant_factor"] == "f4_volume_expansion"
    assert decision["dominant_factor_contribution_pct"] == pytest.approx(0.70)


def test_same_symbol_reentry_blocks_normal_swing_entry_after_profit_lock(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT"])
    cfg.execution.same_symbol_reentry_apply_to_normal_entry = True
    cfg.execution.protect_entry_confirm_rounds = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    _write_same_symbol_exit_memory(
        cfg,
        symbol="SOL/USDT",
        hours_ago=1.0,
        exit_px=102.0,
        highest_px_before_exit=103.0,
    )
    pipe = _build_pipe(cfg, tmp_path, _alpha6_payload("SOL/USDT"))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 1.0},
        selected=["SOL/USDT"],
        entry_candidates=["SOL/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="same-symbol-normal-swing-block")

    out = pipe.run(
        market_data_1h={"SOL/USDT": _series("SOL/USDT", 102.1, high=102.2)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    decision = next(d for d in audit.router_decisions if d.get("reason") == "same_symbol_reentry_cooldown")
    assert decision["symbol"] == "SOL/USDT"
    assert decision["action"] == "skip"
    assert decision["last_exit_reason"] == "protect_profit_lock_trailing"
    assert decision["last_exit_px"] == 102.0
    assert decision["highest_px_before_exit"] == 103.0
    assert decision["required_cooldown_hours"] == 6.0
    assert decision["breakout_exception_met"] is False
    assert audit.counts["same_symbol_reentry_cooldown_count"] == 1
    assert not any(d.get("action") == "create" and d.get("symbol") == "SOL/USDT" for d in audit.router_decisions)


def test_final_same_symbol_reentry_guard_blocks_late_memory_before_order_create(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT"])
    cfg.execution.same_symbol_reentry_apply_to_normal_entry = True
    cfg.execution.protect_entry_confirm_rounds = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _alpha6_payload("SOL/USDT"))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 1.0},
        selected=["SOL/USDT"],
        entry_candidates=["SOL/USDT"],
        volatilities={},
        notes="",
    )

    def delayed_memory_guard(**kwargs):
        record_same_symbol_exit_memory(
            path=derive_runtime_named_json_path(cfg.execution.order_store_path, "same_symbol_reentry_exit_memory"),
            symbol="SOL/USDT",
            exit_ts_ms=int(NOW.timestamp() * 1000) - 3600 * 1000,
            exit_px=102.0,
            exit_reason="protect_profit_lock_trailing",
            highest_px_before_exit=103.0,
            net_bps=128.0,
            memory_status="pending_router_exit",
            source="test_late_memory",
        )
        return {"active": False, "blocked": False, "breakout_exception_met": False}

    pipe._evaluate_same_symbol_reentry_guard = delayed_memory_guard
    audit = DecisionAudit(run_id="same-symbol-final-guard-block")

    out = pipe.run(
        market_data_1h={"SOL/USDT": _series("SOL/USDT", 102.1, high=102.2)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    decision = next(d for d in audit.router_decisions if d.get("reason") == "same_symbol_reentry_cooldown")
    assert decision["symbol"] == "SOL/USDT"
    assert decision["action"] == "skip"
    assert decision["guard_stage"] == "final_before_order_create"
    assert decision["last_exit_reason"] == "protect_profit_lock_trailing"
    assert not any(d.get("action") == "create" and d.get("symbol") == "SOL/USDT" for d in audit.router_decisions)


def test_same_symbol_reentry_allows_normal_swing_entry_after_cooldown(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT"])
    cfg.execution.protect_entry_confirm_rounds = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    _write_same_symbol_exit_memory(
        cfg,
        symbol="SOL/USDT",
        hours_ago=6.1,
        exit_px=102.0,
        highest_px_before_exit=103.0,
    )
    pipe = _build_pipe(cfg, tmp_path, _alpha6_payload("SOL/USDT"))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 1.0},
        selected=["SOL/USDT"],
        entry_candidates=["SOL/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="same-symbol-normal-swing-expired")

    out = pipe.run(
        market_data_1h={"SOL/USDT": _series("SOL/USDT", 102.1, high=102.2)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].symbol == "SOL/USDT"
    assert out.orders[0].intent == "OPEN_LONG"
    assert out.orders[0].meta["swing_hold_position"] is True
    assert audit.counts["same_symbol_reentry_cooldown_count"] == 0


def test_same_symbol_reentry_breakout_bypass_audited_for_normal_swing_entry(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT"])
    cfg.execution.protect_entry_confirm_rounds = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    _write_same_symbol_exit_memory(
        cfg,
        symbol="SOL/USDT",
        hours_ago=1.0,
        exit_px=102.0,
        highest_px_before_exit=103.0,
    )
    pipe = _build_pipe(cfg, tmp_path, _alpha6_payload("SOL/USDT"))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 1.0},
        selected=["SOL/USDT"],
        entry_candidates=["SOL/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="same-symbol-normal-swing-breakout")

    out = pipe.run(
        market_data_1h={"SOL/USDT": _series("SOL/USDT", 103.5, high=103.6)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].symbol == "SOL/USDT"
    decision = next(d for d in audit.router_decisions if d.get("reason") == "same_symbol_reentry_breakout_bypass")
    assert decision["action"] == "allow"
    assert decision["symbol"] == "SOL/USDT"
    assert decision["breakout_exception_met"] is True
    assert audit.counts["same_symbol_reentry_breakout_bypass_count"] == 1


def test_protect_profit_lock_exit_writes_pending_reentry_memory_before_fill(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT"])
    cfg.execution.protect_entry_confirm_rounds = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    exit_pipe = _build_pipe(cfg, tmp_path)
    exit_pipe._load_current_auto_risk_level = lambda: "PROTECT"
    exit_pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 0.50},
        selected=["SOL/USDT"],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    exit_position = _swing_position("SOL/USDT")
    exit_position.highest_px = 101.70
    exit_audit = DecisionAudit(run_id="same-symbol-pending-memory-exit")

    exit_out = exit_pipe.run(
        market_data_1h={"SOL/USDT": _series("SOL/USDT", 101.10, high=101.70)},
        positions=[exit_position],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=exit_audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any((order.meta or {}).get("reason") == "protect_profit_lock_trailing" for order in exit_out.orders)
    memory_path = derive_runtime_named_json_path(cfg.execution.order_store_path, "same_symbol_reentry_exit_memory")
    memory = json.loads(memory_path.read_text(encoding="utf-8"))
    rec = memory["symbols"]["SOL/USDT"]
    assert rec["exit_reason"] == "protect_profit_lock_trailing"
    assert rec["memory_status"] == "pending_router_exit"
    assert rec["source"] == "router_decision"
    assert rec["exit_px"] == 101.10
    assert rec["highest_px_before_exit"] == 101.70

    entry_pipe = _build_pipe(cfg, tmp_path, _alpha6_payload("SOL/USDT"))
    entry_pipe._load_current_auto_risk_level = lambda: "PROTECT"
    entry_pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 1.0},
        selected=["SOL/USDT"],
        entry_candidates=["SOL/USDT"],
        volatilities={},
        notes="",
    )
    entry_audit = DecisionAudit(run_id="same-symbol-pending-memory-block")

    entry_out = entry_pipe.run(
        market_data_1h={"SOL/USDT": _series("SOL/USDT", 101.2, high=101.3)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=entry_audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not entry_out.orders
    decision = next(d for d in entry_audit.router_decisions if d.get("reason") == "same_symbol_reentry_cooldown")
    assert decision["symbol"] == "SOL/USDT"
    assert decision["action"] == "skip"
    assert decision["last_exit_reason"] == "protect_profit_lock_trailing"
    assert decision["required_cooldown_hours"] == 6.0
    assert not any(
        d.get("action") == "create"
        and d.get("symbol") == "SOL/USDT"
        and d.get("side") == "buy"
        for d in entry_audit.router_decisions
    )


def test_same_symbol_filled_memory_overrides_pending_without_duplicate(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT"])
    memory_path = derive_runtime_named_json_path(cfg.execution.order_store_path, "same_symbol_reentry_exit_memory")
    ts_ms = int(NOW.timestamp() * 1000)

    record_same_symbol_exit_memory(
        path=memory_path,
        symbol="SOL/USDT",
        exit_ts_ms=ts_ms,
        exit_px=101.10,
        exit_reason="protect_profit_lock_trailing",
        highest_px_before_exit=101.70,
        net_bps=110.0,
        memory_status="pending_router_exit",
        source="router_decision",
    )
    record_same_symbol_exit_memory(
        path=memory_path,
        symbol="SOL/USDT",
        exit_ts_ms=ts_ms,
        exit_px=101.30,
        exit_reason="protect_profit_lock_trailing",
        highest_px_before_exit=101.80,
        net_bps=128.0,
        memory_status="filled",
        source="fill_reconciler",
    )
    record_same_symbol_exit_memory(
        path=memory_path,
        symbol="SOL/USDT",
        exit_ts_ms=ts_ms,
        exit_px=101.10,
        exit_reason="protect_profit_lock_trailing",
        highest_px_before_exit=101.70,
        net_bps=110.0,
        memory_status="pending_router_exit",
        source="router_decision",
    )

    state = json.loads(memory_path.read_text(encoding="utf-8"))
    assert list(state["symbols"].keys()) == ["SOL/USDT"]
    rec = state["symbols"]["SOL/USDT"]
    assert rec["memory_status"] == "filled"
    assert rec["source"] == "fill_reconciler"
    assert rec["exit_px"] == 101.30
    assert rec["highest_px_before_exit"] == 101.80
    assert rec["net_bps"] == 128.0


def test_swing_guard_allows_zero_target_close_exception_before_min_hold(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "NORMAL")
    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="swing-zero-target")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 100.0)},
        positions=[_swing_position()],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    sell_orders = [order for order in out.orders if order.side == "sell"]
    assert sell_orders
    assert not any(d.get("reason") == "swing_min_hold_guard" for d in audit.router_decisions)
    assert audit.counts["swing_min_hold_guard_count"] == 0


def test_swing_post_min_hold_keeps_current_when_replacement_is_blocked(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT", "BTC/USDT"])
    cfg.execution.protect_entry_confirm_rounds = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(
        cfg,
        tmp_path,
        _strategy_payload(
            trend_signal={
                "symbol": "BTC/USDT",
                "side": "buy",
                "score": 0.95,
                "metadata": {"adx": 35.0},
            },
            alpha6_signal={
                "symbol": "BTC/USDT",
                "side": "sell",
                "score": 0.90,
                "metadata": {
                    "raw_factors": {
                        "f4_volume_expansion": -1.0,
                        "f5_rsi_trend_confirm": 0.10,
                    }
                },
            },
        ),
    )
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="swing-post-min-replacement-blocked")

    out = pipe.run(
        market_data_1h={
            "SOL/USDT": _series("SOL/USDT", 101.0),
            "BTC/USDT": _series("BTC/USDT", 50000.0),
        },
        positions=[_swing_position("SOL/USDT", entry_ts="2026-05-07T07:00:00Z")],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 0.20, "BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not any(order.symbol == "SOL/USDT" and order.side == "sell" for order in out.orders)
    assert not any(order.symbol == "BTC/USDT" and order.side == "buy" for order in out.orders)
    decision = next(
        d for d in audit.router_decisions if d.get("reason") == "swing_hold_current_replacement_blocked"
    )
    assert decision["symbol"] == "SOL/USDT"
    assert decision["action"] == "skip"
    assert decision["replacement_symbol"] == "BTC/USDT"
    assert decision["replacement_block_reason"] == "protect_entry_no_alpha6_confirmation"
    assert decision["hold_hours"] == 25.0
    assert decision["net_bps"] > 0.0
    assert audit.counts["swing_hold_current_replacement_blocked_count"] == 1


def test_swing_post_min_hold_allows_close_when_replacement_passes(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT", "BTC/USDT"])
    cfg.execution.protect_entry_confirm_rounds = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(
        cfg,
        tmp_path,
        _strategy_payload(
            trend_signal={
                "symbol": "BTC/USDT",
                "side": "buy",
                "score": 0.95,
                "metadata": {"adx": 35.0},
            },
            alpha6_signal={
                "symbol": "BTC/USDT",
                "side": "buy",
                "score": 0.60,
                "metadata": {
                    "raw_factors": {
                        "f4_volume_expansion": 0.50,
                        "f5_rsi_trend_confirm": 0.35,
                    }
                },
            },
        ),
    )
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="swing-post-min-replacement-passes")

    out = pipe.run(
        market_data_1h={
            "SOL/USDT": _series("SOL/USDT", 101.0),
            "BTC/USDT": _series("BTC/USDT", 50000.0),
        },
        positions=[_swing_position("SOL/USDT", entry_ts="2026-05-07T07:00:00Z")],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 0.20, "BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any(order.symbol == "SOL/USDT" and order.side == "sell" for order in out.orders)
    assert any(order.symbol == "BTC/USDT" and order.side == "buy" for order in out.orders)
    assert not any(d.get("reason") == "swing_hold_current_replacement_blocked" for d in audit.router_decisions)


def test_swing_post_min_hold_allows_close_when_current_is_weak_and_losing(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["SOL/USDT", "BTC/USDT"])
    cfg.execution.protect_entry_confirm_rounds = 1
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(
        cfg,
        tmp_path,
        _strategy_payload(
            trend_signal={
                "symbol": "BTC/USDT",
                "side": "buy",
                "score": 0.95,
                "metadata": {"adx": 35.0},
            },
            alpha6_signal={
                "symbol": "BTC/USDT",
                "side": "sell",
                "score": 0.90,
                "metadata": {
                    "raw_factors": {
                        "f4_volume_expansion": -1.0,
                        "f5_rsi_trend_confirm": 0.10,
                    }
                },
            },
        ),
    )
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="swing-post-min-current-weak")

    out = pipe.run(
        market_data_1h={
            "SOL/USDT": _series("SOL/USDT", 99.0),
            "BTC/USDT": _series("BTC/USDT", 50000.0),
        },
        positions=[_swing_position("SOL/USDT", entry_ts="2026-05-07T07:00:00Z")],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"SOL/USDT": 0.05, "BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any(order.symbol == "SOL/USDT" and order.side == "sell" for order in out.orders)
    assert not any(d.get("reason") == "swing_hold_current_replacement_blocked" for d in audit.router_decisions)


def test_swing_guard_blocks_rank_exit_before_min_hold(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path, ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"])
    cfg.execution.rank_exit_max_rank = 3
    cfg.execution.rank_exit_confirm_rounds = 1
    cfg.execution.min_hold_minutes_before_rank_exit = 0
    _write_auto_risk_level(cfg.execution.order_store_path, "NORMAL")
    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    pipe.profit_taking.should_exit_by_rank = lambda *args, **kwargs: (True, "rank_4_exceeds_3_streak_1")
    audit = DecisionAudit(run_id="swing-rank-exit")

    out = pipe.run(
        market_data_1h={"BNB/USDT": _series("BNB/USDT", 100.0)},
        positions=[_swing_position("BNB/USDT")],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(
            raw_factors={},
            z_factors={},
            scores={"BTC/USDT": 1.0, "ETH/USDT": 0.9, "SOL/USDT": 0.8, "BNB/USDT": 0.1},
        ),
        precomputed_regime=_regime(),
    )

    assert not [
        order
        for order in out.orders
        if str((order.meta or {}).get("reason", "")).startswith("rank_exit")
    ]
    decision = next(
        d
        for d in audit.router_decisions
        if d.get("reason") == "swing_min_hold_guard"
        and str(d.get("blocked_exit_reason", "")).startswith("rank_exit")
    )
    assert decision["symbol"] == "BNB/USDT"
    assert decision["rank"] == 4
    assert decision["would_exit_shadow"] is True
    assert decision["blocked_policy_reason"] == "swing_min_hold_soft_exit_blocked"


def test_stop_loss_and_profit_lock_exits_are_not_blocked_by_swing_guard(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    stop_pipe = _build_pipe(cfg, tmp_path)
    stop_pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (True, 95.0, -0.05)
    stop_pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 0.50},
        selected=["BTC/USDT"],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    stop_audit = DecisionAudit(run_id="swing-stop-loss")
    stop_out = stop_pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 95.0)},
        positions=[_swing_position()],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=stop_audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )
    assert any("stop_loss" in str((order.meta or {}).get("reason", "")) for order in stop_out.orders)
    assert stop_audit.counts["swing_min_hold_guard_count"] == 0
    assert not any(d.get("reason") == "swing_hold_current_replacement_blocked" for d in stop_audit.router_decisions)

    lock_pipe = _build_pipe(cfg, tmp_path)
    lock_pipe._load_current_auto_risk_level = lambda: "PROTECT"
    lock_pipe.portfolio_engine.allocate = stop_pipe.portfolio_engine.allocate
    lock_audit = DecisionAudit(run_id="swing-profit-lock")
    lock_position = _swing_position()
    lock_position.highest_px = 101.70
    lock_out = lock_pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 101.10, high=101.70)},
        positions=[lock_position],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=lock_audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )
    assert any((order.meta or {}).get("reason") == "protect_profit_lock_trailing" for order in lock_out.orders)
    assert lock_audit.counts["swing_min_hold_guard_count"] == 0
    assert not any(d.get("reason") == "swing_hold_current_replacement_blocked" for d in lock_audit.router_decisions)


def test_probe_position_is_not_blocked_by_swing_guard(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.probe_ignore_normal_zero_target_close = False
    _write_auto_risk_level(cfg.execution.order_store_path, "NORMAL")
    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    probe_tags = {
        "entry_reason": "market_impulse_probe",
        "entry_ts": "2026-05-08T02:00:00Z",
        "entry_px": 100.0,
        "probe_type": "market_impulse_probe",
        "market_impulse_probe": True,
        "target_w": 0.06,
    }
    position = _swing_position()
    position.tags_json = json.dumps(probe_tags, ensure_ascii=False)
    audit = DecisionAudit(run_id="swing-probe-unaffected")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 100.0)},
        positions=[position],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any((order.meta or {}).get("turnover_cap_bypass_reason") == "zero_target_close" for order in out.orders)
    assert audit.counts["swing_min_hold_guard_count"] == 0
