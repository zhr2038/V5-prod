from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.clock import FixedClock
from src.core.models import MarketSeries, Order
from src.core.pipeline import V5Pipeline
from src.execution.execution_engine import ExecutionEngine
from src.execution.fill_store import derive_runtime_named_json_path
from src.execution.position_store import Position, PositionStore
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


NOW = datetime(2026, 4, 25, 8, 0, tzinfo=timezone.utc)


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(close: float, *, high: float | None = None) -> MarketSeries:
    closes = [100.0 for _ in range(30)] + [close]
    highs = [100.0 for _ in range(30)] + [high if high is not None else close]
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(len(closes))]
    return MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=ts,
        open=list(closes),
        high=highs,
        low=list(closes),
        close=list(closes),
        volume=[1000.0 for _ in closes],
    )


def _regime() -> RegimeResult:
    return RegimeResult(
        state=RegimeState.TRENDING,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.0,
    )


def _probe_position(
    *,
    entry_ts: str = "2026-04-25T07:00:00Z",
    highest_px: float = 100.0,
    probe: bool = True,
    probe_type: str = "btc_leadership_probe",
) -> Position:
    tags = {}
    if probe:
        tags = {
            "entry_reason": probe_type,
            "entry_ts": entry_ts,
            "entry_px": 100.0,
            "probe_type": probe_type,
            "target_w": 0.08,
            "highest_net_bps": 0.0,
            probe_type: True,
        }
    return Position(
        symbol="BTC/USDT",
        qty=1.0,
        avg_px=100.0,
        entry_ts=entry_ts,
        highest_px=highest_px,
        last_update_ts=entry_ts,
        last_mark_px=100.0,
        unrealized_pnl_pct=0.0,
        tags_json=json.dumps(tags, ensure_ascii=False),
    )


def _cfg(tmp_path: Path) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.execution.slippage_db_path = str((tmp_path / "reports" / "slippage.sqlite").resolve())
    cfg.execution.btc_leadership_probe_enabled = False
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.fee_bps = 0.0
    cfg.execution.slippage_bps = 0.0
    cfg.execution.probe_time_stop_hours = 8
    cfg.alpha.use_fused_score_for_weighting = False
    return cfg


def _build_pipe(cfg: AppConfig, tmp_path: Path) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path
    pipe = V5Pipeline(cfg, clock=FixedClock(NOW))
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.portfolio_engine._load_fused_signals = lambda: {}
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 0.50},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    pipe.alpha_engine.get_latest_strategy_signal_payload = lambda: {"strategies": []}
    pipe.alpha_engine.strategy_signals_path = lambda: tmp_path / "reports" / "runs" / "test" / "strategy_signals.json"
    return pipe


def _run_exit_case(tmp_path: Path, position: Position, series: MarketSeries):
    cfg = _cfg(tmp_path)
    pipe = _build_pipe(cfg, tmp_path)
    audit = DecisionAudit(run_id="probe-exit")
    out = pipe.run(
        market_data_1h={"BTC/USDT": series},
        positions=[position],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )
    return out, audit


def _single_exit(out) -> Order:
    exits = [order for order in out.orders if order.side == "sell" and order.intent == "CLOSE_LONG"]
    assert len(exits) == 1
    return exits[0]


def _single_probe_exit_decision(audit: DecisionAudit) -> dict:
    decisions = [
        decision
        for decision in (audit.router_decisions or [])
        if decision.get("probe_exit_policy_active") is True
    ]
    assert len(decisions) == 1
    return decisions[0]


def _write_market_impulse_probe_state(
    cfg: AppConfig,
    *,
    symbol: str = "BTC/USDT",
    entry_ts: str = "2026-04-25T03:00:00Z",
    time_stop_hours: int = 4,
    target_w: float = 0.06,
) -> None:
    entry_dt = datetime.fromisoformat(entry_ts.replace("Z", "+00:00"))
    state_path = derive_runtime_named_json_path(cfg.execution.order_store_path, "market_impulse_probe_state")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                symbol: {
                    "symbol": symbol,
                    "entry_ts_ms": int(entry_dt.timestamp() * 1000),
                    "entry_ts": entry_ts,
                    "cooldown_until_ms": int(datetime(2026, 4, 25, 16, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    "time_stop_hours": time_stop_hours,
                    "target_w": target_w,
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_probe_take_profit_at_80_net_bps(tmp_path: Path) -> None:
    out, audit = _run_exit_case(tmp_path, _probe_position(), _series(100.80))

    order = _single_exit(out)
    assert order.meta["reason"] == "probe_take_profit"
    assert order.meta["exit_reason"] == "probe_take_profit"
    assert order.meta["probe_exit_policy_active"] is True
    assert order.meta["bypass_turnover_cap_for_exit"] is True
    decision = _single_probe_exit_decision(audit)
    assert decision["exit_reason"] == "probe_take_profit"
    assert decision["probe_type"] == "btc_leadership_probe"
    assert round(decision["net_bps"], 6) == 80.0
    assert audit.counts["probe_take_profit_count"] == 1


def test_probe_stop_loss_at_minus_50_net_bps(tmp_path: Path) -> None:
    out, audit = _run_exit_case(tmp_path, _probe_position(), _series(99.50))

    order = _single_exit(out)
    assert order.meta["reason"] == "probe_stop_loss"
    assert order.meta["probe_exit_policy_active"] is True
    decision = _single_probe_exit_decision(audit)
    assert decision["exit_reason"] == "probe_stop_loss"
    assert round(decision["net_bps"], 6) == -50.0
    assert audit.counts["probe_stop_loss_count"] == 1


def test_probe_trailing_after_60_bps_retraces_25_bps(tmp_path: Path) -> None:
    position = _probe_position(highest_px=100.60)
    out, audit = _run_exit_case(tmp_path, position, _series(100.35, high=100.60))

    order = _single_exit(out)
    assert order.meta["reason"] == "probe_trailing_stop"
    assert round(order.meta["high_net_bps"], 6) == 60.0
    assert round(order.meta["highest_net_bps"], 6) == 60.0
    decision = _single_probe_exit_decision(audit)
    assert decision["exit_reason"] == "probe_trailing_stop"
    assert round(decision["highest_net_bps"], 6) == 60.0
    assert audit.counts["probe_trailing_stop_count"] == 1


def test_probe_time_stop_after_8h_below_10_net_bps(tmp_path: Path) -> None:
    position = _probe_position(entry_ts="2026-04-24T23:30:00Z")
    out, audit = _run_exit_case(tmp_path, position, _series(100.05))

    order = _single_exit(out)
    assert order.meta["reason"] == "probe_time_stop"
    assert order.meta["probe_time_stop_hours"] == 8.0
    assert order.meta["hold_hours"] >= 8.0
    decision = _single_probe_exit_decision(audit)
    assert decision["exit_reason"] == "probe_time_stop"
    assert decision["hold_hours"] >= 8.0
    assert audit.counts["probe_time_stop_count"] == 1


def test_probe_time_stop_does_not_fire_before_8h(tmp_path: Path) -> None:
    position = _probe_position(entry_ts="2026-04-25T03:30:00Z")
    out, audit = _run_exit_case(tmp_path, position, _series(100.05))

    assert not [order for order in out.orders if order.side == "sell" and order.intent == "CLOSE_LONG"]
    assert audit.counts["probe_time_stop_count"] == 0


def test_market_impulse_probe_uses_probe_exit_policy(tmp_path: Path) -> None:
    out, audit = _run_exit_case(
        tmp_path,
        _probe_position(probe_type="market_impulse_probe"),
        _series(100.80),
    )

    order = _single_exit(out)
    assert order.meta["reason"] == "probe_take_profit"
    assert order.meta["probe_type"] == "market_impulse_probe"
    assert audit.counts["probe_take_profit_count"] == 1


def test_market_impulse_probe_metadata_skips_legacy_4h_time_stop(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.execution.market_impulse_probe_time_stop_hours = 4
    cfg.execution.probe_time_stop_hours = 8
    pipe = _build_pipe(cfg, tmp_path)
    state_path = derive_runtime_named_json_path(cfg.execution.order_store_path, "market_impulse_probe_state")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "BTC/USDT": {
                    "entry_ts_ms": int(datetime(2026, 4, 25, 3, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    "cooldown_until_ms": int(datetime(2026, 4, 25, 16, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    "time_stop_hours": 4,
                }
            }
        ),
        encoding="utf-8",
    )
    audit = DecisionAudit(run_id="probe-exit-legacy-time-stop-skip")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series(100.05)},
        positions=[_probe_position(entry_ts="2026-04-25T03:00:00Z", probe_type="market_impulse_probe")],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not [order for order in out.orders if order.side == "sell" and order.intent == "CLOSE_LONG"]
    assert audit.counts["market_impulse_probe_time_stop_count"] == 0
    assert audit.counts["probe_time_stop_count"] == 0


def test_state_only_market_impulse_probe_skips_legacy_4h_time_stop_when_policy_enabled(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.execution.market_impulse_probe_time_stop_hours = 4
    cfg.execution.probe_time_stop_hours = 8
    _write_market_impulse_probe_state(cfg, entry_ts="2026-04-25T03:00:00Z", time_stop_hours=4)
    pipe = _build_pipe(cfg, tmp_path)
    audit = DecisionAudit(run_id="probe-exit-state-only-legacy-skip")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series(100.05)},
        positions=[_probe_position(entry_ts="2026-04-25T03:00:00Z", probe=False, probe_type="market_impulse_probe")],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not [order for order in out.orders if order.side == "sell" and order.intent == "CLOSE_LONG"]
    assert audit.counts["market_impulse_probe_time_stop_count"] == 0
    assert audit.counts["probe_time_stop_count"] == 0


def test_state_only_market_impulse_probe_uses_probe_8h_time_stop_when_policy_enabled(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.execution.market_impulse_probe_time_stop_hours = 4
    cfg.execution.probe_time_stop_hours = 8
    _write_market_impulse_probe_state(cfg, entry_ts="2026-04-24T23:00:00Z", time_stop_hours=4)
    pipe = _build_pipe(cfg, tmp_path)
    audit = DecisionAudit(run_id="probe-exit-state-only-probe-time-stop")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series(100.05)},
        positions=[_probe_position(entry_ts="2026-04-24T23:00:00Z", probe=False, probe_type="market_impulse_probe")],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    order = _single_exit(out)
    assert order.meta["reason"] == "probe_time_stop"
    assert order.meta["probe_type"] == "market_impulse_probe"
    assert order.meta["probe_time_stop_hours"] == 8.0
    assert audit.counts["probe_time_stop_count"] == 1
    assert audit.counts["market_impulse_probe_time_stop_count"] == 0


def test_probe_exit_disabled_allows_legacy_market_impulse_time_stop_fallback(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    cfg.execution.probe_exit_enabled = False
    cfg.execution.market_impulse_probe_time_stop_hours = 4
    _write_market_impulse_probe_state(cfg, entry_ts="2026-04-25T03:00:00Z", time_stop_hours=4)
    pipe = _build_pipe(cfg, tmp_path)
    audit = DecisionAudit(run_id="probe-exit-disabled-legacy-fallback")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series(100.05)},
        positions=[_probe_position(entry_ts="2026-04-25T03:00:00Z", probe=False, probe_type="market_impulse_probe")],
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    order = _single_exit(out)
    assert order.meta["reason"] == "market_impulse_probe_time_stop"
    assert audit.counts["market_impulse_probe_time_stop_count"] == 1
    assert audit.counts["probe_time_stop_count"] == 0


def test_non_probe_position_is_not_affected(tmp_path: Path) -> None:
    out, audit = _run_exit_case(tmp_path, _probe_position(probe=False), _series(100.80))

    assert not [order for order in out.orders if order.side == "sell" and order.intent == "CLOSE_LONG"]
    assert audit.counts["probe_take_profit_count"] == 0
    assert audit.counts["probe_stop_loss_count"] == 0
    assert audit.counts["probe_trailing_stop_count"] == 0
    assert audit.counts["probe_time_stop_count"] == 0


def test_probe_open_records_position_and_profit_state(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    store = PositionStore(path=str((tmp_path / "reports" / "positions.sqlite").resolve()))
    engine = ExecutionEngine(cfg.execution, position_store=store, run_id="probe-open-record")
    order = Order(
        symbol="BTC/USDT",
        side="buy",
        intent="OPEN_LONG",
        notional_usdt=80.0,
        signal_price=100.0,
        meta={
            "btc_leadership_probe": True,
            "entry_reason": "btc_leadership_probe",
            "probe_type": "btc_leadership_probe",
            "target_w": 0.08,
        },
    )

    engine.execute([order])

    pos = store.get("BTC/USDT")
    assert pos is not None
    tags = json.loads(pos.tags_json)
    assert tags["entry_reason"] == "btc_leadership_probe"
    assert tags["probe_type"] == "btc_leadership_probe"
    assert tags["entry_px"] == 100.0
    assert tags["target_w"] == 0.08
    assert tags["highest_net_bps"] == 0.0

    state_path = derive_runtime_named_json_path(cfg.execution.order_store_path, "profit_taking_state")
    state = json.loads(state_path.read_text(encoding="utf-8"))["BTC/USDT"]
    assert state["entry_reason"] == "btc_leadership_probe"
    assert state["probe_type"] == "btc_leadership_probe"
    assert state["entry_px"] == 100.0
    assert state["target_w"] == 0.08
    assert state["highest_net_bps"] == 0.0
