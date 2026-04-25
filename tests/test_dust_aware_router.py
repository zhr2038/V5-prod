from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries, Order
from src.core.pipeline import V5Pipeline
from src.execution.position_store import Position
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module
import src.data.okx_instruments as okx_instruments


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(sym: str, close: float) -> MarketSeries:
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(80)]
    close_arr = [close for _ in ts]
    vol = [1000.0 for _ in ts]
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


def _position(symbol: str, qty: float, px: float) -> Position:
    return Position(
        symbol=symbol,
        qty=qty,
        avg_px=px,
        entry_ts="2026-04-18T00:00:00Z",
        highest_px=px,
        last_update_ts="2026-04-18T00:00:00Z",
        last_mark_px=px,
        unrealized_pnl_pct=0.0,
    )


def _cfg(tmp_path: Path) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.execution.anti_chase_enabled = True
    cfg.execution.anti_chase_max_entry_premium_pct = 1.0
    cfg.execution.anti_chase_max_add_notional_ratio = 1.0
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.dust_value_threshold = 0.5
    cfg.execution.dust_usdt_ignore = 1.0
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.min_trade_notional_base = 1.0
    cfg.budget.exchange_min_notional_enabled = False
    cfg.rebalance.deadband_trending = 0.0
    cfg.rebalance.deadband_sideways = 0.0
    cfg.rebalance.deadband_riskoff = 0.0
    cfg.rebalance.new_position_deadband_multiplier = 1.0
    cfg.alpha.use_fused_score_for_weighting = False
    return cfg


def _build_pipe(cfg: AppConfig, tmp_path: Path, monkeypatch, target_weights: dict[str, float]) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path

    class FakeInstrumentsCache:
        def get_spec(self, inst_id: str):
            return None

    monkeypatch.setattr(okx_instruments, "OKXSpotInstrumentsCache", FakeInstrumentsCache)

    pipe = V5Pipeline(cfg)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights=target_weights,
        selected=list(target_weights.keys()),
        entry_candidates=list(target_weights.keys()),
        volatilities={},
        notes="",
    )
    pipe.portfolio_engine._load_fused_signals = lambda: {}
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.profit_taking.evaluate = lambda *args, **kwargs: ("hold", 0.0, "none")
    pipe.profit_taking.register_position = lambda *args, **kwargs: None
    pipe.profit_taking.positions = {}
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    return pipe


def _run(pipe: V5Pipeline, positions: list[Position], cash_usdt: float, audit: DecisionAudit, px: float):
    return pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", px)},
        positions=positions,
        cash_usdt=cash_usdt,
        equity_peak_usdt=max(100.0, cash_usdt),
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )


def test_btc_dust_is_flat_for_anti_chase_add_size(tmp_path: Path, monkeypatch) -> None:
    px = 50_000.0
    dust_value = 0.000446
    cfg = _cfg(tmp_path)
    pipe = _build_pipe(cfg, tmp_path, monkeypatch, {"BTC/USDT": 0.16})
    audit = DecisionAudit(run_id="btc-dust-add-size")

    out = _run(pipe, [_position("BTC/USDT", dust_value / px, px)], 100.0, audit, px)

    assert len(out.orders) == 1
    assert out.orders[0].intent == "OPEN_LONG"
    assert not any(d.get("reason") == "anti_chase_add_size" for d in audit.router_decisions)

    create = next(d for d in audit.router_decisions if d.get("action") == "create")
    assert create["dust_position_ignored_for_add_size"] is True
    assert create["raw_held_value_usdt"] == dust_value
    assert create["effective_held_value_usdt"] == 0.0
    assert create["dust_threshold_usdt"] >= 1.0
    assert audit.counts["dust_position_ignored_for_add_size_count"] == 1


def test_real_position_still_triggers_anti_chase_add_size(tmp_path: Path, monkeypatch) -> None:
    px = 100.0
    cfg = _cfg(tmp_path)
    pipe = _build_pipe(cfg, tmp_path, monkeypatch, {"BTC/USDT": 0.24})
    audit = DecisionAudit(run_id="real-add-size")

    out = _run(pipe, [_position("BTC/USDT", 0.08, px)], 92.0, audit, px)

    assert out.orders == []
    decision = next(d for d in audit.router_decisions if d.get("reason") == "anti_chase_add_size")
    assert decision["held_value"] == 8.0
    assert decision["effective_held_value_usdt"] == 8.0
    assert decision["max_add_ratio"] == 1.0


def test_dust_residual_close_order_is_suppressed(tmp_path: Path, monkeypatch) -> None:
    px = 50_000.0
    dust_value = 0.000446
    cfg = _cfg(tmp_path)
    pipe = _build_pipe(cfg, tmp_path, monkeypatch, {})
    pipe.exit_policy.evaluate = lambda positions, market_data, regime_state: [
        Order(
            symbol="BTC/USDT",
            side="sell",
            intent="CLOSE_LONG",
            notional_usdt=dust_value,
            signal_price=px,
            meta={"reason": "regime_exit"},
        )
    ]
    audit = DecisionAudit(run_id="btc-dust-close")

    out = _run(pipe, [_position("BTC/USDT", dust_value / px, px)], 100.0, audit, px)

    assert out.orders == []
    decision = next(d for d in audit.router_decisions if d.get("reason") == "dust_residual_no_close_order")
    assert decision["raw_held_value_usdt"] == dust_value
    assert decision["effective_held_value_usdt"] == 0.0
    assert audit.counts["dust_residual_no_close_order_count"] == 1
