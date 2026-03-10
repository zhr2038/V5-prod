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
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(sym: str, close: float) -> MarketSeries:
    ts = [_ms(1700000000 + i * 3600) for i in range(30)]
    close_arr = [close for _ in range(30)]
    return MarketSeries(
        symbol=sym,
        timeframe="1h",
        ts=ts,
        open=close_arr,
        high=close_arr,
        low=close_arr,
        close=close_arr,
        volume=[1000.0 for _ in range(30)],
    )


def _regime() -> RegimeResult:
    return RegimeResult(
        state=RegimeState.SIDEWAYS,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.0,
    )


def _build_pipe(cfg: AppConfig) -> V5Pipeline:
    pipe = V5Pipeline(cfg)
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    return pipe


def test_pipeline_applies_negative_expectancy_penalty_before_allocate():
    cfg = AppConfig(symbols=["OKB/USDT", "HYPE/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.negative_expectancy_score_penalty_enabled = True
    cfg.execution.negative_expectancy_score_penalty_min_closed_cycles = 2
    cfg.execution.negative_expectancy_score_penalty_floor_bps = 5.0
    cfg.execution.negative_expectancy_score_penalty_per_bps = 0.015
    cfg.execution.negative_expectancy_score_penalty_max = 0.60
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_cooldown_enabled = False

    pipe = _build_pipe(cfg)
    captured = {}

    def _allocate(scores, market_data, regime_mult, audit=None):
        captured.update(scores)
        return SimpleNamespace(
            target_weights={},
            selected=[],
            entry_candidates=[],
            volatilities={},
            notes="",
        )

    pipe.portfolio_engine.allocate = _allocate
    pipe.negative_expectancy_cooldown.refresh = lambda force=False: {
        "stats": {
            "OKB/USDT": {
                "closed_cycles": 3,
                "expectancy_bps": -10.0,
                "pnl_sum_usdt": -0.30,
                "closed_notional_usdt": 100.0,
            }
        },
        "symbols": {},
    }

    market_data = {
        "OKB/USDT": _series("OKB/USDT", 100.0),
        "HYPE/USDT": _series("HYPE/USDT", 30.0),
    }
    alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"OKB/USDT": 0.60, "HYPE/USDT": 0.40})

    pipe.run(
        market_data_1h=market_data,
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=DecisionAudit(run_id="neg-penalty"),
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert captured["OKB/USDT"] == pytest.approx(0.375)
    assert captured["HYPE/USDT"] == pytest.approx(0.40)


def test_pipeline_blocks_open_long_on_negative_expectancy():
    cfg = AppConfig(symbols=["OKB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = True
    cfg.execution.negative_expectancy_open_block_min_closed_cycles = 2
    cfg.execution.negative_expectancy_open_block_floor_bps = 5.0
    cfg.execution.negative_expectancy_cooldown_enabled = False

    pipe = _build_pipe(cfg)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"OKB/USDT": 0.25},
        selected=["OKB/USDT"],
        entry_candidates=["OKB/USDT"],
        volatilities={},
        notes="",
    )
    pipe.negative_expectancy_cooldown.refresh = lambda force=False: {
        "stats": {
            "OKB/USDT": {
                "closed_cycles": 3,
                "expectancy_bps": 1.0,
                "pnl_sum_usdt": 0.01,
                "closed_notional_usdt": 100.0,
            }
        },
        "symbols": {},
    }
    pipe.negative_expectancy_cooldown.get_symbol_stats = lambda symbol: {
        "closed_cycles": 3,
        "expectancy_bps": 1.0,
        "cooldown_active": False,
    }

    market_data = {"OKB/USDT": _series("OKB/USDT", 100.0)}
    alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"OKB/USDT": 0.60})
    audit = DecisionAudit(run_id="neg-open-block")

    out = pipe.run(
        market_data_1h=market_data,
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert out.orders == []
    assert any(
        d.get("symbol") == "OKB/USDT" and d.get("reason") == "negative_expectancy_open_block"
        for d in audit.router_decisions
    )
