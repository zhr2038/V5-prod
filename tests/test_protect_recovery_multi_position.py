from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.fill_store import derive_runtime_auto_risk_eval_path
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


def _ms(value: int) -> int:
    return value * 1000


def _series(symbol: str, base: float, latest: float | None = None) -> MarketSeries:
    latest = base if latest is None else latest
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(10)]
    closes = [base for _ in range(9)] + [latest]
    return MarketSeries(
        symbol=symbol,
        timeframe="1h",
        ts=ts,
        open=closes,
        high=closes,
        low=closes,
        close=closes,
        volume=[1000.0 for _ in closes],
    )


def _market_data(*, positive: bool = True) -> dict[str, MarketSeries]:
    bump = 1.02 if positive else 1.0
    return {
        "BTC/USDT": _series("BTC/USDT", 70000.0, 70000.0 * bump),
        "ETH/USDT": _series("ETH/USDT", 3500.0, 3500.0 * bump),
        "SOL/USDT": _series("SOL/USDT", 150.0, 150.0 * bump),
        "BNB/USDT": _series("BNB/USDT", 650.0, 650.0 * bump),
    }


def _regime(state=RegimeState.TRENDING) -> RegimeResult:
    return RegimeResult(
        state=state,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=0.0 if state == RegimeState.RISK_OFF else 1.0,
    )


def _write_auto_risk_level(order_store_path: str, level: str) -> None:
    path = derive_runtime_auto_risk_eval_path(order_store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"current_level": level}), encoding="utf-8")


def _strategy_payload(symbols: list[str]) -> dict:
    signals = [
        {
            "symbol": symbol,
            "side": "buy",
            "score": 0.65,
            "metadata": {
                "z_factors": {
                    "f4_volume_expansion": 0.5,
                    "f5_rsi_trend_confirm": 0.4,
                }
            },
        }
        for symbol in symbols
    ]
    return {
        "strategies": [
            {
                "strategy": "Alpha6Factor",
                "type": "alpha_6factor",
                "allocation": 1.0,
                "total_signals": len(signals),
                "buy_signals": len(signals),
                "sell_signals": 0,
                "signals": signals,
            }
        ]
    }


class _DummyNegexp:
    def __init__(self, stats=None, blocked=None):
        self._stats = stats or {}
        self._blocked = blocked or {}

    def is_blocked(self, symbol):
        return self._blocked.get(symbol)

    def get_symbol_stats(self, symbol):
        return self._stats.get(symbol, {})

    def set_scope(self, **kwargs):
        return None

    def refresh(self, force=False):
        return {"stats": self._stats, "symbols": self._blocked}


def _cfg(tmp_path: Path, *, enabled: bool) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.execution.protect_recovery_multi_position_enabled = enabled
    cfg.execution.protect_recovery_allowed_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    cfg.execution.protect_recovery_max_positions = 2
    cfg.execution.protect_recovery_position_target_w = 0.08
    cfg.execution.protect_recovery_max_gross_exposure = 0.18
    cfg.execution.protect_recovery_min_positive_whitelist_4h_count = 3
    cfg.execution.market_impulse_probe_enabled = False
    cfg.execution.btc_leadership_probe_enabled = False
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    cfg.execution.min_trade_value_usdt = 0.0
    cfg.execution.protect_entry_confirm_rounds = 1
    cfg.budget.min_trade_notional_base = 5.0
    cfg.rebalance.deadband_trending = 0.01
    cfg.rebalance.new_position_deadband_multiplier = 1.0
    return cfg


def _pipe(cfg: AppConfig, tmp_path: Path, strategy_symbols: list[str]) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path / "reports"
    pipe = V5Pipeline(cfg)
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.profit_taking.should_exit_by_rank = lambda *args, **kwargs: (False, "rank_exit_not_triggered")
    pipe.portfolio_engine._load_fused_signals = lambda: {}
    pipe.alpha_engine.get_latest_strategy_signal_payload = lambda: _strategy_payload(strategy_symbols)
    pipe.alpha_engine.strategy_signals_path = lambda: tmp_path / "reports" / "strategy_signals.json"
    pipe.negative_expectancy_cooldown = _DummyNegexp()
    return pipe


def _portfolio_one_symbol(symbol: str = "ETH/USDT"):
    return SimpleNamespace(
        target_weights={symbol: 0.08},
        selected=[symbol],
        entry_candidates=[symbol],
        volatilities={},
        notes="",
    )


def _portfolio_empty():
    return SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )


def _run(pipe: V5Pipeline, audit: DecisionAudit, *, regime=RegimeState.TRENDING):
    return pipe.run(
        market_data_1h=_market_data(),
        positions=[],
        cash_usdt=1000.0,
        equity_peak_usdt=1000.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(
            raw_factors={},
            z_factors={},
            scores={"ETH/USDT": 0.90, "BTC/USDT": 0.80, "SOL/USDT": 0.70, "BNB/USDT": 0.95},
        ),
        precomputed_regime=_regime(regime),
    )


def test_protect_recovery_disabled_preserves_existing_target_behavior(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, enabled=False)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _pipe(cfg, tmp_path, ["ETH/USDT", "BTC/USDT", "SOL/USDT"])
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _portfolio_one_symbol("ETH/USDT")
    audit = DecisionAudit(run_id="protect-recovery-disabled")

    out = _run(pipe, audit)

    assert [order.symbol for order in out.orders if order.side == "buy"] == ["ETH/USDT"]
    assert not any(decision.get("reason") == "protect_recovery_multi_position_target" for decision in audit.router_decisions)


def test_protect_recovery_enabled_risk_off_does_not_enable(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, enabled=True)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _pipe(cfg, tmp_path, ["ETH/USDT", "BTC/USDT", "SOL/USDT"])
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _portfolio_empty()
    audit = DecisionAudit(run_id="protect-recovery-risk-off")

    out = _run(pipe, audit, regime=RegimeState.RISK_OFF)

    assert not [order for order in out.orders if order.side == "buy"]
    assert any(decision.get("reason") == "protect_recovery_risk_off" for decision in audit.router_decisions)


def test_protect_recovery_enabled_market_positive_can_hold_top2(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, enabled=True)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _pipe(cfg, tmp_path, ["ETH/USDT", "BTC/USDT", "SOL/USDT"])
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _portfolio_one_symbol("ETH/USDT")
    audit = DecisionAudit(run_id="protect-recovery-top2")

    out = _run(pipe, audit)

    buys = [order for order in out.orders if order.side == "buy"]
    assert [order.symbol for order in buys] == ["BTC/USDT", "ETH/USDT"]
    assert {order.meta["target_w"] for order in buys} == {0.08}
    assert all(order.meta.get("swing_hold_position") is True for order in buys)
    assert all(order.meta.get("protect_recovery_multi_position") is True for order in buys)
    assert audit.counts["protect_recovery_multi_position_selected_count"] == 2


def test_protect_recovery_excludes_negative_expectancy_symbol(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, enabled=True)
    cfg.execution.protect_recovery_allowed_symbols = ["BNB/USDT", "ETH/USDT", "BTC/USDT"]
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _pipe(cfg, tmp_path, ["BNB/USDT", "ETH/USDT", "BTC/USDT"])
    pipe.negative_expectancy_cooldown = _DummyNegexp(
        stats={"BNB/USDT": {"closed_cycles": 1, "net_expectancy_bps": -40.0}}
    )
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _portfolio_one_symbol("BNB/USDT")
    audit = DecisionAudit(run_id="protect-recovery-negexp")

    out = _run(pipe, audit)

    buys = [order.symbol for order in out.orders if order.side == "buy"]
    assert "BNB/USDT" not in buys
    assert buys == ["BTC/USDT", "ETH/USDT"]
    assert any(
        decision.get("symbol") == "BNB/USDT"
        and decision.get("reason") == "protect_recovery_negative_expectancy_excluded"
        for decision in audit.router_decisions
    )


def test_protect_recovery_clips_gross_exposure(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, enabled=True)
    cfg.execution.protect_recovery_max_gross_exposure = 0.10
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _pipe(cfg, tmp_path, ["ETH/USDT", "BTC/USDT", "SOL/USDT"])
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: _portfolio_one_symbol("ETH/USDT")
    audit = DecisionAudit(run_id="protect-recovery-gross-cap")

    out = _run(pipe, audit)

    buys = [order for order in out.orders if order.side == "buy"]
    target_sum = sum(float(order.meta.get("target_w") or 0.0) for order in buys)
    assert len(buys) == 2
    assert target_sum == pytest.approx(0.10)
    assert all(float(order.meta.get("target_w") or 0.0) <= 0.08 for order in buys)
