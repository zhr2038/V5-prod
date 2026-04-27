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
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.fill_store import derive_position_store_path, derive_runtime_named_json_path
from src.execution.highest_px_tracker import derive_tracker_state_path
from src.execution.position_store import Position
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


NOW = datetime(2026, 4, 26, 22, 0, tzinfo=timezone.utc)
ENTRY_TS = "2026-04-26T16:00:00+00:00"
ENTRY_MS = int(datetime(2026, 4, 26, 16, 0, tzinfo=timezone.utc).timestamp() * 1000)
COOLDOWN_MS = int(datetime(2026, 4, 27, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
BTC_PX = 78_021.7


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(close: float) -> MarketSeries:
    closes = [close for _ in range(40)]
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(len(closes))]
    return MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=ts,
        open=list(closes),
        high=list(closes),
        low=list(closes),
        close=list(closes),
        volume=[1000.0 for _ in closes],
    )


def _regime() -> RegimeResult:
    return RegimeResult(
        state=RegimeState.TRENDING,
        atr_pct=0.01,
        ma20=BTC_PX,
        ma60=BTC_PX,
        multiplier=1.0,
    )


def _cfg(tmp_path: Path) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.execution.slippage_db_path = str((tmp_path / "reports" / "slippage.sqlite").resolve())
    cfg.execution.btc_leadership_probe_enabled = False
    cfg.execution.market_impulse_probe_enabled = False
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.dust_value_threshold = 0.5
    cfg.execution.dust_usdt_ignore = 1.0
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.exchange_min_notional_enabled = False
    cfg.budget.min_trade_notional_base = 1.0
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.rebalance.deadband_trending = 0.0
    cfg.rebalance.deadband_sideways = 0.0
    cfg.rebalance.deadband_riskoff = 0.0
    cfg.rebalance.new_position_deadband_multiplier = 1.0
    return cfg


def _state_paths(cfg: AppConfig) -> dict[str, Path]:
    order_store_path = Path(cfg.execution.order_store_path)
    return {
        "profit": derive_runtime_named_json_path(order_store_path, "profit_taking_state"),
        "stop": derive_runtime_named_json_path(order_store_path, "stop_loss_state"),
        "fixed": derive_runtime_named_json_path(order_store_path, "fixed_stop_loss_state"),
        "market_probe": derive_runtime_named_json_path(order_store_path, "market_impulse_probe_state"),
        "highest": derive_tracker_state_path(derive_position_store_path(order_store_path)),
    }


def _write_active_probe_state(cfg: AppConfig) -> dict[str, Path]:
    paths = _state_paths(cfg)
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    paths["profit"].write_text(
        json.dumps(
            {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "entry_price": BTC_PX,
                    "entry_px": BTC_PX,
                    "entry_time": ENTRY_TS,
                    "entry_ts": ENTRY_TS,
                    "highest_price": 79_488.0,
                    "profit_high": 0.0,
                    "current_stop": BTC_PX * 0.95,
                    "current_action": "hold",
                    "partial_sold": False,
                    "triggered_actions": [],
                    "entry_reason": "market_impulse_probe",
                    "probe_type": "market_impulse_probe",
                    "target_w": 0.08,
                }
            }
        ),
        encoding="utf-8",
    )
    paths["stop"].write_text(
        json.dumps(
            {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "entry_price": BTC_PX,
                    "entry_time": ENTRY_TS,
                    "highest_price": 79_488.0,
                    "current_stop_price": BTC_PX * 0.95,
                    "current_stop_type": "initial_normal",
                    "is_breakeven": False,
                    "is_trailing": False,
                    "profit_high_watermark": 0.0,
                }
            }
        ),
        encoding="utf-8",
    )
    paths["fixed"].write_text(
        json.dumps({"BTC/USDT": {"entry_price": BTC_PX, "entry_time": ENTRY_TS}}),
        encoding="utf-8",
    )
    paths["highest"].write_text(
        json.dumps(
            {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "highest_px": 79_488.0,
                    "entry_px": BTC_PX,
                    "updated_at": ENTRY_TS,
                    "source": "trade",
                }
            }
        ),
        encoding="utf-8",
    )
    paths["market_probe"].write_text(
        json.dumps(
            {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "entry_ts_ms": ENTRY_MS,
                    "entry_ts": ENTRY_TS,
                    "cooldown_until_ms": COOLDOWN_MS,
                    "cooldown_until": "2026-04-27T00:00:00Z",
                    "time_stop_hours": 4,
                    "target_w": 0.08,
                    "source_cl_ord_id": "probe-open",
                }
            }
        ),
        encoding="utf-8",
    )
    return paths


def _build_pipe(cfg: AppConfig, tmp_path: Path) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path
    pipe = V5Pipeline(cfg, clock=FixedClock(NOW))
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    pipe.portfolio_engine._load_fused_signals = lambda: {}
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    pipe.alpha_engine.get_latest_strategy_signal_payload = lambda: {"strategies": []}
    pipe.alpha_engine.strategy_signals_path = lambda: tmp_path / "reports" / "runs" / "test" / "strategy_signals.json"
    return pipe


def _position(value_usdt: float, *, px: float = BTC_PX) -> Position:
    tags = {
        "entry_reason": "market_impulse_probe",
        "entry_ts": ENTRY_TS,
        "entry_px": BTC_PX,
        "probe_type": "market_impulse_probe",
        "target_w": 0.08,
        "market_impulse_probe": True,
    }
    return Position(
        symbol="BTC/USDT",
        qty=float(value_usdt) / float(px),
        avg_px=BTC_PX,
        entry_ts=ENTRY_TS,
        highest_px=79_488.0,
        last_update_ts=ENTRY_TS,
        last_mark_px=px,
        unrealized_pnl_pct=0.0,
        tags_json=json.dumps(tags, ensure_ascii=False),
    )


def test_dust_probe_close_cleanup_clears_active_state_and_preserves_cooldown(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    paths = _write_active_probe_state(cfg)
    pipe = _build_pipe(cfg, tmp_path)
    audit = DecisionAudit(run_id="probe-cleanup")

    active_positions, decisions, dust_symbols = pipe.cleanup_stale_position_state_for_dust_positions(
        [_position(0.00042)],
        prices={"BTC/USDT": BTC_PX},
        audit=audit,
    )

    assert active_positions == []
    assert dust_symbols == {"BTC/USDT"}
    assert audit.counts["stale_position_state_detected_count"] == 1
    assert audit.counts["position_state_cleared_after_close_count"] == 1
    decision = decisions[0]
    assert decision["position_state_cleared_after_close"] is True
    assert abs(float(decision["remaining_value_usdt"]) - 0.00042) < 1e-12
    assert decision["dust_threshold_usdt"] >= 1.0
    assert set(decision["cleared_state_keys"]) == {
        "profit_taking_state",
        "stop_loss_state",
        "fixed_stop_loss_state",
        "highest_px_state",
        "market_impulse_probe_state_active",
    }

    for key in ("profit", "stop", "fixed", "highest"):
        payload = json.loads(paths[key].read_text(encoding="utf-8"))
        assert "BTC/USDT" not in payload

    market_probe_state = json.loads(paths["market_probe"].read_text(encoding="utf-8"))["BTC/USDT"]
    assert market_probe_state["cooldown_until_ms"] == COOLDOWN_MS
    assert market_probe_state["active_position"] is False
    assert "entry_ts_ms" not in market_probe_state
    assert "time_stop_hours" not in market_probe_state


def test_cleaned_dust_position_does_not_emit_probe_time_stop_next_round(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    _write_active_probe_state(cfg)
    pipe = _build_pipe(cfg, tmp_path)
    audit = DecisionAudit(run_id="probe-cleanup-next-run")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series(BTC_PX)},
        positions=[_position(0.00042)],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={}),
        precomputed_regime=_regime(),
    )

    reasons = [str((order.meta or {}).get("reason") or "") for order in out.orders]
    assert "market_impulse_probe_time_stop" not in reasons
    assert "probe_time_stop" not in reasons
    assert not any(d.get("reason") == "dust_residual_no_close_order" for d in audit.router_decisions)
    assert audit.counts["market_impulse_probe_time_stop_count"] == 0
    assert audit.counts["position_state_cleared_after_close_count"] == 1


def test_non_dust_true_position_state_is_not_cleared(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    paths = _write_active_probe_state(cfg)
    pipe = _build_pipe(cfg, tmp_path)
    audit = DecisionAudit(run_id="probe-cleanup-real-position")

    active_positions, decisions, dust_symbols = pipe.cleanup_stale_position_state_for_dust_positions(
        [_position(8.0)],
        prices={"BTC/USDT": BTC_PX},
        audit=audit,
    )

    assert len(active_positions) == 1
    assert decisions == []
    assert dust_symbols == set()
    assert audit.counts["stale_position_state_detected_count"] == 0
    assert "BTC/USDT" in json.loads(paths["profit"].read_text(encoding="utf-8"))
    assert "BTC/USDT" in json.loads(paths["stop"].read_text(encoding="utf-8"))
    assert "BTC/USDT" in json.loads(paths["fixed"].read_text(encoding="utf-8"))
    assert "BTC/USDT" in json.loads(paths["highest"].read_text(encoding="utf-8"))
    assert "entry_ts_ms" in json.loads(paths["market_probe"].read_text(encoding="utf-8"))["BTC/USDT"]
