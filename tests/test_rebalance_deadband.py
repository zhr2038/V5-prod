from __future__ import annotations

from types import SimpleNamespace

import pytest

from configs.schema import AppConfig, RegimeState
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.position_store import Position
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(sym: str, close: float) -> MarketSeries:
    # minimal MarketSeries with enough bars for alpha (>=25)
    ts = [_ms(1700000000 + i * 3600) for i in range(30)]
    close_arr = [close for _ in range(30)]
    vol = [1000.0 for _ in range(30)]
    return MarketSeries(symbol=sym, timeframe="1h", ts=ts, open=close_arr, high=close_arr, low=close_arr, close=close_arr, volume=vol)


def _sideways_regime() -> RegimeResult:
    return RegimeResult(
        state=RegimeState.SIDEWAYS,
        atr_pct=0.01,
        ma20=100.0,
        ma60=100.0,
        multiplier=1.0,
    )


def _build_pipe(cfg: AppConfig, tmp_path) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path
    pipe = V5Pipeline(cfg)
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.profit_taking.evaluate = lambda symbol, current_price: ("hold", 0.0, "")
    pipe.profit_taking.should_exit_by_rank = lambda *args, **kwargs: (False, "")
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    pipe.profit_taking.state_file = tmp_path / "profit_taking_state.json"
    pipe.profit_taking.positions = {}
    return pipe


def test_deadband_skips_small_drift():
    cfg = AppConfig()
    cfg.rebalance.deadband_sideways = 0.05
    cfg.budget.min_trade_notional_base = 0.0
    cfg.budget.exchange_min_notional_enabled = False

    pipe = V5Pipeline(cfg)

    md = {"SOL/USDT": _series("SOL/USDT", 100.0), "BTC/USDT": _series("BTC/USDT", 100.0)}

    # current weight ~0.23, target 0.25 => drift 0.02 <= 0.05 -> skip
    positions = [
        Position(
            symbol="SOL/USDT",
            qty=0.23,
            avg_px=100.0,
            entry_ts="t",
            highest_px=100.0,
            last_update_ts="t",
            last_mark_px=100.0,
            unrealized_pnl_pct=0.0,
        )
    ]
    audit = DecisionAudit(run_id="t")

    # disable exits for this unit test
    pipe.exit_policy.evaluate = lambda **kwargs: []

    # force target by monkeypatching portfolio_engine to return desired target
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 0.25}, selected=["SOL/USDT"], volatilities={}, notes=""
    )

    out = pipe.run(market_data_1h=md, positions=positions, cash_usdt=77.0, equity_peak_usdt=100.0, audit=audit)

    assert len(out.orders) == 0
    assert audit.rebalance_deadband_pct == pytest.approx(0.05)
    assert audit.rebalance_skipped_deadband_count == 1
    assert "SOL/USDT" in audit.rebalance_skipped_deadband_by_symbol


def test_deadband_allows_large_drift():
    cfg = AppConfig()
    cfg.rebalance.deadband_sideways = 0.05
    cfg.budget.min_trade_notional_base = 0.0
    cfg.budget.exchange_min_notional_enabled = False

    pipe = V5Pipeline(cfg)

    md = {"SOL/USDT": _series("SOL/USDT", 100.0), "BTC/USDT": _series("BTC/USDT", 100.0)}

    # current weight 0.10, target 0.25 => drift 0.15 > 0.05 -> allow
    positions = [
        Position(
            symbol="SOL/USDT",
            qty=0.10,
            avg_px=100.0,
            entry_ts="t",
            highest_px=100.0,
            last_update_ts="t",
            last_mark_px=100.0,
            unrealized_pnl_pct=0.0,
        )
    ]
    audit = DecisionAudit(run_id="t")

    # disable exits for this unit test
    pipe.exit_policy.evaluate = lambda **kwargs: []

    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 0.25}, selected=["SOL/USDT"], volatilities={}, notes=""
    )

    out = pipe.run(market_data_1h=md, positions=positions, cash_usdt=90.0, equity_peak_usdt=100.0, audit=audit)

    # should create one rebalance/open order
    assert len(out.orders) == 1
    assert out.orders[0].symbol == "SOL/USDT"
    assert audit.rebalance_skipped_deadband_count == 0


def test_close_only_weight_eps_zero_does_not_force_sell_small_positive_target(tmp_path):
    cfg = AppConfig(symbols=["SOL/USDT", "BTC/USDT"])
    cfg.rebalance.deadband_sideways = 0.03
    cfg.rebalance.deadband_trending = 0.03
    cfg.rebalance.deadband_riskoff = 0.03
    cfg.rebalance.close_only_weight_eps = 0.0
    cfg.rebalance.close_only_deadband_multiplier = 0.5
    cfg.budget.min_trade_notional_base = 0.0
    cfg.budget.exchange_min_notional_enabled = False
    cfg.alpha.use_fused_score_for_weighting = False

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 0.0005},
        selected=["SOL/USDT"],
        volatilities={},
        notes="",
    )

    md = {"SOL/USDT": _series("SOL/USDT", 1.0), "BTC/USDT": _series("BTC/USDT", 100.0)}
    positions = [
        Position(
            symbol="SOL/USDT",
            qty=20.0,
            avg_px=1.0,
            entry_ts="t",
            highest_px=1.0,
            last_update_ts="t",
            last_mark_px=1.0,
            unrealized_pnl_pct=0.0,
        )
    ]
    audit = DecisionAudit(run_id="close-only-eps-zero")

    out = pipe.run(
        market_data_1h=md,
        positions=positions,
        cash_usdt=980.0,
        equity_peak_usdt=1000.0,
        audit=audit,
        precomputed_regime=_sideways_regime(),
    )

    assert out.orders == []
    assert audit.rebalance_effective_deadband_by_symbol["SOL/USDT"] == pytest.approx(0.03)
    assert not any("Close-only: SOL/USDT" in str(note) for note in audit.notes)


def test_new_position_weight_eps_zero_does_not_expand_deadband_for_tiny_existing_weight(tmp_path):
    cfg = AppConfig(symbols=["SOL/USDT", "BTC/USDT"])
    cfg.rebalance.deadband_sideways = 0.03
    cfg.rebalance.deadband_trending = 0.03
    cfg.rebalance.deadband_riskoff = 0.03
    cfg.rebalance.new_position_weight_eps = 0.0
    cfg.rebalance.new_position_deadband_multiplier = 2.0
    cfg.budget.min_trade_notional_base = 0.0
    cfg.budget.exchange_min_notional_enabled = False
    cfg.alpha.use_fused_score_for_weighting = False

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"SOL/USDT": 0.0405},
        selected=["SOL/USDT"],
        volatilities={},
        notes="",
    )

    md = {"SOL/USDT": _series("SOL/USDT", 1.0), "BTC/USDT": _series("BTC/USDT", 100.0)}
    positions = [
        Position(
            symbol="SOL/USDT",
            qty=0.5,
            avg_px=1.0,
            entry_ts="t",
            highest_px=1.0,
            last_update_ts="t",
            last_mark_px=1.0,
            unrealized_pnl_pct=0.0,
        )
    ]
    audit = DecisionAudit(run_id="new-position-eps-zero")

    out = pipe.run(
        market_data_1h=md,
        positions=positions,
        cash_usdt=99.5,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_regime=_sideways_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].symbol == "SOL/USDT"
    assert out.orders[0].side == "buy"
    assert audit.rebalance_effective_deadband_by_symbol["SOL/USDT"] == pytest.approx(0.03)
    assert not any("Banding: SOL/USDT is new position" in str(note) for note in audit.notes)
