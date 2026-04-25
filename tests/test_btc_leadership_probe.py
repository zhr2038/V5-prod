from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.fill_store import derive_runtime_auto_risk_eval_path
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _btc_series(*, prior_high: float = 78000.0, latest: float = 78120.0) -> MarketSeries:
    closes = [prior_high for _ in range(30)] + [latest]
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(len(closes))]
    vol = [1000.0 for _ in closes]
    return MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=ts,
        open=list(closes),
        high=list(closes),
        low=list(closes),
        close=list(closes),
        volume=vol,
    )


def _regime(state: RegimeState = RegimeState.TRENDING) -> RegimeResult:
    return RegimeResult(
        state=state,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.0,
    )


def _write_auto_risk_level(order_store_path: str, level: str) -> None:
    path = derive_runtime_auto_risk_eval_path(order_store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"current_level": level}, ensure_ascii=False), encoding="utf-8")


def _alpha6_signal(*, score: float = 0.35, f4: float = 0.05, f5: float = 0.40) -> dict:
    factors = {
        "f4_volume_expansion": f4,
        "f5_rsi_trend_confirm": f5,
    }
    return {
        "symbol": "BTC/USDT",
        "side": "buy",
        "score": score,
        "confidence": 0.8,
        "metadata": {
            "raw_factors": dict(factors),
            "z_factors": dict(factors),
        },
    }


def _strategy_payload(alpha6_signal: dict | None = None) -> dict:
    signal = alpha6_signal if alpha6_signal is not None else _alpha6_signal()
    return {
        "strategies": [
            {
                "strategy": "Alpha6Factor",
                "type": "alpha_6factor",
                "allocation": 0.5,
                "total_signals": 1,
                "buy_signals": 1 if str(signal.get("side", "")).lower() == "buy" else 0,
                "sell_signals": 1 if str(signal.get("side", "")).lower() == "sell" else 0,
                "signals": [signal],
            }
        ]
    }


def _base_cfg(tmp_path: Path) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.budget.exchange_min_notional_enabled = False
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False
    return cfg


def _build_pipe(cfg: AppConfig, tmp_path: Path, payload: dict) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path
    pipe = V5Pipeline(cfg)
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.portfolio_engine._load_fused_signals = lambda: {}
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    pipe.alpha_engine.get_latest_strategy_signal_payload = lambda: payload
    pipe.alpha_engine.strategy_signals_path = lambda: tmp_path / "reports" / "runs" / "test" / "strategy_signals.json"
    pipe.profit_taking.state_file = tmp_path / "profit_taking_state.json"
    pipe.profit_taking.positions = {}
    pipe.negative_expectancy_cooldown.is_blocked = lambda symbol: None
    pipe.negative_expectancy_cooldown.get_symbol_stats = lambda symbol: {}
    return pipe


def _run_probe(
    tmp_path: Path,
    *,
    cfg: AppConfig | None = None,
    series: MarketSeries | None = None,
    regime: RegimeResult | None = None,
    payload: dict | None = None,
    cash_usdt: float = 1000.0,
) -> tuple:
    cfg = cfg or _base_cfg(tmp_path)
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, payload or _strategy_payload())
    audit = DecisionAudit(run_id="btc-leadership-probe")
    out = pipe.run(
        market_data_1h={"BTC/USDT": series or _btc_series()},
        positions=[],
        cash_usdt=cash_usdt,
        equity_peak_usdt=cash_usdt,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=regime or _regime(),
    )
    return out, audit, pipe


def test_btc_breakout_in_protect_opens_small_probe(tmp_path: Path) -> None:
    out, audit, _ = _run_probe(tmp_path)

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.symbol == "BTC/USDT"
    assert order.intent == "OPEN_LONG"
    assert order.meta["btc_leadership_probe"] is True
    assert order.meta["rolling_high"] == 78000.0
    assert order.meta["breakout_buffer_bps"] == 15.0
    assert order.meta["target_w"] == 0.08
    assert order.meta["bypassed_negative_expectancy"] is False
    assert audit.counts["btc_leadership_probe_candidate_count"] == 1
    assert audit.counts["btc_leadership_probe_open_count"] == 1
    assert not any(d.get("reason") == "anti_chase_add_size" for d in audit.router_decisions)


def test_btc_without_rolling_high_breakout_does_not_probe(tmp_path: Path) -> None:
    out, audit, _ = _run_probe(tmp_path, series=_btc_series(latest=78050.0))

    assert not out.orders
    assert any(d.get("reason") == "btc_leadership_probe_no_breakout" for d in audit.router_decisions)
    assert audit.counts["btc_leadership_probe_blocked_count"] == 1


def test_btc_probe_blocks_in_risk_off_regime(tmp_path: Path) -> None:
    out, audit, _ = _run_probe(tmp_path, regime=_regime(RegimeState.RISK_OFF))

    assert not out.orders
    assert any(d.get("reason") == "btc_leadership_probe_risk_off" for d in audit.router_decisions)
    assert audit.counts["btc_leadership_probe_blocked_count"] == 1


def test_btc_probe_blocks_active_negative_expectancy_cooldown(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.negative_expectancy_cooldown_enabled = True
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload())
    pipe.negative_expectancy_cooldown.is_blocked = lambda symbol: {
        "closed_cycles": 1,
        "net_expectancy_bps": -100.0,
        "remain_seconds": 3600.0,
    }
    audit = DecisionAudit(run_id="btc-leadership-probe-cooldown")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _btc_series()},
        positions=[],
        cash_usdt=1000.0,
        equity_peak_usdt=1000.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "negative_expectancy_cooldown" for d in audit.router_decisions)
    assert audit.counts["btc_leadership_probe_blocked_count"] == 1


def test_btc_probe_bypasses_single_mild_negative_cycle(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.negative_expectancy_open_block_enabled = True
    cfg.execution.negative_expectancy_open_block_min_closed_cycles = 1
    cfg.execution.negative_expectancy_open_block_floor_bps = 20.0
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload())
    pipe.negative_expectancy_cooldown.get_symbol_stats = lambda symbol: {
        "closed_cycles": 1,
        "net_expectancy_bps": -100.0,
    }
    audit = DecisionAudit(run_id="btc-leadership-probe-neg-bypass")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _btc_series()},
        positions=[],
        cash_usdt=1000.0,
        equity_peak_usdt=1000.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    assert out.orders[0].meta["bypassed_negative_expectancy"] is True
    assert audit.counts["btc_leadership_probe_negative_expectancy_bypass_count"] == 1


def test_btc_probe_does_not_bypass_two_negative_cycles(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.execution.negative_expectancy_open_block_enabled = True
    cfg.execution.negative_expectancy_open_block_min_closed_cycles = 1
    cfg.execution.negative_expectancy_open_block_floor_bps = 20.0
    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")
    pipe = _build_pipe(cfg, tmp_path, _strategy_payload())
    pipe.negative_expectancy_cooldown.get_symbol_stats = lambda symbol: {
        "closed_cycles": 2,
        "net_expectancy_bps": -100.0,
    }
    audit = DecisionAudit(run_id="btc-leadership-probe-neg-block")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _btc_series()},
        positions=[],
        cash_usdt=1000.0,
        equity_peak_usdt=1000.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(d.get("reason") == "negative_expectancy_open_block" for d in audit.router_decisions)
    assert audit.counts["btc_leadership_probe_blocked_count"] == 1


def test_btc_probe_dynamic_sizing_never_exceeds_max_target(tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path)
    cfg.budget.min_trade_notional_base = 12.0
    out, audit, _ = _run_probe(tmp_path, cfg=cfg, cash_usdt=100.0)

    assert not out.orders
    blocked = next(
        d for d in audit.router_decisions if d.get("reason") == "btc_leadership_probe_min_notional_unreachable"
    )
    assert blocked["target_w"] == 0.10
    assert blocked["max_target_w"] == 0.10
    assert audit.counts["btc_leadership_probe_blocked_count"] == 1
