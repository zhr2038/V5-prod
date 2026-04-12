from datetime import datetime, timezone
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.position_store import Position
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
from src.risk.profit_taking import PositionProfitState
import src.core.pipeline as pipeline_module


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(sym: str, close: float) -> MarketSeries:
    ts = [_ms(1700000000 + i * 3600) for i in range(30)]
    close_arr = [close for _ in range(30)]
    vol = [1000.0 for _ in range(30)]
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
    pipe.profit_taking.state_file = tmp_path / "profit_taking_state.json"
    pipe.profit_taking.positions = {}
    return pipe


def test_strict_rank_exit_ignores_target_weight_and_profit_rank_relaxation(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "OKB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.rank_exit_strict_mode = True
    cfg.execution.min_hold_minutes_before_rank_exit = 0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"OKB/USDT": 0.75},
        selected=["OKB/USDT"],
        volatilities={},
        notes="",
    )

    pipe.profit_taking.positions["OKB/USDT"] = PositionProfitState(
        symbol="OKB/USDT",
        entry_price=100.0,
        entry_time=datetime.now(timezone.utc),
        highest_price=125.0,
        profit_high=0.25,
        current_stop=95.0,
        rank_exit_streak=1,
        last_rank=4,
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "OKB/USDT": _series("OKB/USDT", 100.0),
    }
    positions = [
        Position(
            symbol="OKB/USDT",
            qty=1.0,
            avg_px=100.0,
            entry_ts="2026-03-09T00:00:00Z",
            highest_px=125.0,
            last_update_ts="2026-03-09T00:00:00Z",
            last_mark_px=100.0,
            unrealized_pnl_pct=0.0,
        )
    ]
    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={
            "BTC/USDT": 1.0,
            "SOL/USDT": 0.9,
            "ETH/USDT": 0.8,
            "OKB/USDT": 0.7,
        },
    )

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=DecisionAudit(run_id="strict-rank-exit"),
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.symbol == "OKB/USDT"
    assert order.side == "sell"
    assert order.intent == "CLOSE_LONG"
    assert order.meta["reason"].startswith("rank_exit_")


def test_peak_drawdown_exit_generates_partial_sell_order(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "OKB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.min_hold_minutes_before_rank_exit = 0
    cfg.execution.peak_drawdown_exit.enabled = True

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"OKB/USDT": 0.50},
        selected=["OKB/USDT"],
        volatilities={},
        notes="",
    )

    pipe.profit_taking.register_position("OKB/USDT", 100.0, current_price=100.0)
    action, _, _ = pipe.profit_taking.evaluate("OKB/USDT", 112.0)
    assert action == "move_stop"

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "OKB/USDT": _series("OKB/USDT", 109.0),
    }
    positions = [
        Position(
            symbol="OKB/USDT",
            qty=1.0,
            avg_px=100.0,
            entry_ts="2026-03-09T00:00:00Z",
            highest_px=112.0,
            last_update_ts="2026-03-09T00:00:00Z",
            last_mark_px=109.0,
            unrealized_pnl_pct=0.09,
        )
    ]
    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={
            "BTC/USDT": 1.0,
            "OKB/USDT": 0.95,
        },
    )

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=100.0,
        equity_peak_usdt=209.0,
        audit=DecisionAudit(run_id="peak-drawdown"),
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.symbol == "OKB/USDT"
    assert order.side == "sell"
    assert order.intent == "REBALANCE"
    assert order.notional_usdt == pytest.approx(109.0 * 0.33)
    assert order.meta["reason"].startswith("profit_partial_peak_drawdown_8pct")


def test_peak_drawdown_config_preserves_zero_values(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.peak_drawdown_exit.enabled = True
    cfg.execution.peak_drawdown_exit.tier1_profit_pct = 0.0
    cfg.execution.peak_drawdown_exit.tier1_retrace_pct = 0.0
    cfg.execution.peak_drawdown_exit.tier1_sell_pct = 0.0
    cfg.execution.peak_drawdown_exit.tier2_profit_pct = 0.0
    cfg.execution.peak_drawdown_exit.tier2_retrace_pct = 0.0
    cfg.execution.peak_drawdown_exit.tier2_sell_pct = 0.0
    cfg.execution.peak_drawdown_exit.tier3_profit_pct = 0.0
    cfg.execution.peak_drawdown_exit.tier3_retrace_pct = 0.0
    cfg.execution.peak_drawdown_exit.tier3_sell_pct = 0.0

    pipe = _build_pipe(cfg, tmp_path)

    assert len(pipe.profit_taking.peak_drawdown_levels) == 3
    for level in pipe.profit_taking.peak_drawdown_levels:
        assert level.profit_pct == pytest.approx(0.0)
        assert level.retrace_pct == pytest.approx(0.0)
        assert level.sell_pct == pytest.approx(0.0)


def test_close_only_sell_not_blocked_by_cash_gate(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "SUI/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.rank_exit_strict_mode = True
    cfg.execution.min_hold_minutes_before_rank_exit = 0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        volatilities={},
        notes="",
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "SUI/USDT": _series("SUI/USDT", 1.0),
    }
    positions = [
        Position(
            symbol="SUI/USDT",
            qty=40.0,
            avg_px=1.0,
            entry_ts="2026-03-10T08:00:00Z",
            highest_px=1.05,
            last_update_ts="2026-03-10T08:00:00Z",
            last_mark_px=1.0,
            unrealized_pnl_pct=0.0,
        )
    ]
    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={
            "SUI/USDT": 1.0,
            "BTC/USDT": 0.5,
        },
    )
    audit = DecisionAudit(run_id="close-only-sell-no-cash")

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=1.0,
        equity_peak_usdt=41.0,
        audit=audit,
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.symbol == "SUI/USDT"
    assert order.side == "sell"
    assert order.intent == "REBALANCE"
    assert order.notional_usdt == pytest.approx(40.0)
    assert not any(
        d.get("symbol") == "SUI/USDT" and d.get("reason") == "insufficient_cash"
        for d in audit.router_decisions
    )


def test_rank_exit_audit_does_not_mislead_for_in_rank_position(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "XRP/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.rank_exit_strict_mode = True
    cfg.execution.min_hold_minutes_before_rank_exit = 30

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"XRP/USDT": 0.25},
        selected=["XRP/USDT"],
        volatilities={},
        notes="",
    )

    pipe.profit_taking.positions["XRP/USDT"] = PositionProfitState(
        symbol="XRP/USDT",
        entry_price=1.4,
        entry_time=datetime.now(timezone.utc),
        highest_price=1.45,
        profit_high=0.03,
        current_stop=1.3,
        rank_exit_streak=0,
        last_rank=3,
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "XRP/USDT": _series("XRP/USDT", 1.41),
    }
    positions = [
        Position(
            symbol="XRP/USDT",
            qty=10.0,
            avg_px=1.40,
            entry_ts="2026-03-10T10:00:00Z",
            highest_px=1.45,
            last_update_ts="2026-03-10T10:00:00Z",
            last_mark_px=1.41,
            unrealized_pnl_pct=0.007,
        )
    ]
    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={
            "BTC/USDT": 1.0,
            "OKB/USDT": 0.95,
            "XRP/USDT": 0.90,
            "ETH/USDT": 0.80,
        },
    )
    audit = DecisionAudit(run_id="rank-audit-in-rank")

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert out.orders == []
    notes = [str(n) for n in audit.notes]
    assert not any("Rank exit blocked by min-hold: XRP/USDT" in n for n in notes)
    assert not any("Rank exit strict mode: XRP/USDT" in n for n in notes)
