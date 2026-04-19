from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
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


def _regime(state: RegimeState, multiplier: float) -> RegimeResult:
    return RegimeResult(
        state=state,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=multiplier,
    )


def _build_pipe(cfg: AppConfig, tmp_path: Path) -> V5Pipeline:
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
    return pipe


def test_risk_off_zero_target_counts_as_risk_off_not_deadband(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BNB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.regime.pos_mult_risk_off = 0.0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BNB/USDT": 0.0},
        selected=[],
        entry_candidates=["BNB/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="risk-off-zero-target")

    out = pipe.run(
        market_data_1h={"BNB/USDT": _series("BNB/USDT", 600.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BNB/USDT": 1.0}),
        precomputed_regime=_regime(RegimeState.RISK_OFF, 0.0),
    )

    assert out.orders == []
    assert audit.counts["selected"] == 0
    assert audit.counts["risk_off_suppressed_count"] == 1
    assert audit.counts["target_zero_after_regime_count"] == 1
    assert audit.rebalance_skipped_deadband_count == 0
    assert any(
        d.get("reason") == "target_zero_no_order"
        and d.get("target_zero_reason") == "risk_off_pos_mult_zero"
        for d in audit.router_decisions
    )
    assert not any(d.get("reason") == "deadband" for d in audit.router_decisions)


def test_target_zero_without_position_does_not_use_deadband(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 0.0},
        selected=[],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="target-zero-no-position")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(RegimeState.TRENDING, 1.0),
    )

    assert out.orders == []
    assert audit.rebalance_skipped_deadband_count == 0
    assert any(d.get("reason") == "target_zero_no_order" for d in audit.router_decisions)
    assert not any(d.get("reason") == "deadband" for d in audit.router_decisions)


def test_nonzero_target_small_drift_still_counts_as_deadband(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.rebalance.deadband_trending = 0.02

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 0.05},
        selected=["BTC/USDT"],
        entry_candidates=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="true-deadband")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[
            Position(
                symbol="BTC/USDT",
                qty=0.00008,  # about 4 USDT on 100 equity => current_w ~= 0.04
                avg_px=50000.0,
                entry_ts="2026-04-19T00:00:00Z",
                highest_px=51000.0,
                last_update_ts="2026-04-19T00:00:00Z",
                last_mark_px=50000.0,
                unrealized_pnl_pct=0.0,
            )
        ],
        cash_usdt=96.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(RegimeState.TRENDING, 1.0),
    )

    assert out.orders == []
    assert audit.rebalance_skipped_deadband_count == 1
    assert any(d.get("reason") == "deadband" for d in audit.router_decisions)
