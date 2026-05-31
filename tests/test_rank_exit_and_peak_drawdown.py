from datetime import datetime, timedelta, timezone
import json
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
from src.risk.profit_taking import PositionProfitState, ProfitTakingManager
import src.core.pipeline as pipeline_module


class _FixedClock:
    def __init__(self, now: datetime):
        self._now = now

    def now(self) -> datetime:
        return self._now


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


def _risk_off_regime() -> RegimeResult:
    return RegimeResult(
        state=RegimeState.RISK_OFF,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=0.0,
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


def _run_swing_min_hold_exit_case(
    tmp_path: Path,
    *,
    hold_hours: float,
    exit_reason: str,
    signal_price: float = 600.0,
    avg_px: float = 600.0,
    f5_rsi_trend_confirm: float = 0.35,
    swing_position: bool = True,
    regime_result: RegimeResult | None = None,
    cfg_mutator=None,
):
    now = datetime(2026, 5, 2, 5, 0, tzinfo=timezone.utc)
    entry_ts = (now - timedelta(hours=hold_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cfg = AppConfig(symbols=["BNB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.fee_bps = 10.0
    cfg.execution.slippage_bps = 5.0
    cfg.execution.swing_min_hold_exit_guard_enabled = True
    cfg.execution.swing_min_hold_hours = 24.0
    cfg.execution.protect_profit_lock_enabled = False
    cfg.execution.probe_exit_enabled = False
    if cfg_mutator is not None:
        cfg_mutator(cfg)

    pipe = _build_pipe(cfg, tmp_path)
    pipe.clock = _FixedClock(now)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BNB/USDT": 0.012},
        selected=["BNB/USDT"],
        volatilities={},
        notes="",
    )
    pipe.exit_policy.evaluate = lambda positions, market_data, regime_state: [
        pipeline_module.Order(
            symbol="BNB/USDT",
            side="sell",
            intent="CLOSE_LONG",
            notional_usdt=12.0,
            signal_price=signal_price,
            meta={"reason": exit_reason},
        )
    ]
    market_data = {"BNB/USDT": _series("BNB/USDT", signal_price)}
    tags = {}
    if swing_position:
        tags = {
            "swing_hold_position": True,
            "swing_entry_ts": entry_ts,
            "swing_min_hold_hours": 24.0,
            "entry_reason": "normal_entry",
            "f5_rsi_trend_confirm": f5_rsi_trend_confirm,
        }
    positions = [
        Position(
            symbol="BNB/USDT",
            qty=0.02,
            avg_px=avg_px,
            entry_ts=entry_ts,
            highest_px=630.0,
            last_update_ts=entry_ts,
            last_mark_px=signal_price,
            unrealized_pnl_pct=0.0,
            tags_json=json.dumps(tags),
        )
    ]
    audit = DecisionAudit(run_id=f"swing-min-hold-{exit_reason}-{hold_hours}")
    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=988.0,
        equity_peak_usdt=1000.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BNB/USDT": 1.0}),
        precomputed_regime=regime_result or _regime(),
    )
    return out, audit


def test_swing_atr_early_exit_guard_blocks_small_loss_before_24h(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(tmp_path, hold_hours=5.0, exit_reason="atr_trailing")

    assert not any(order.symbol == "BNB/USDT" and order.side == "sell" for order in out.orders)
    decision = next(d for d in audit.router_decisions if d.get("reason") == "swing_atr_early_exit_guard")
    assert decision["original_exit_reason"] == "atr_trailing"
    assert decision["hold_hours"] == pytest.approx(5.0)
    assert decision["min_hold_hours"] == pytest.approx(24.0)
    assert decision["required_hold_hours"] == pytest.approx(24.0)
    assert decision["net_bps"] == pytest.approx(-30.0)
    assert decision["f5_rsi_trend_confirm"] == pytest.approx(0.35)
    assert decision["exit_allowed_before_min_hold"] is False
    assert decision["exit_blocked_by_min_hold"] is True
    assert decision["exit_priority"] == "soft"
    assert decision["min_hold_block_reason"] == "swing_atr_early_exit_guard"
    assert decision["swing_min_hold_guard_checked"] is True
    assert decision["swing_min_hold_guard_blocked"] is True
    assert decision["soft_exit_blocked_by_min_hold"] is True
    assert decision["hard_exit_exception_reason"] == ""
    assert decision["hold_hours_at_exit_check"] == pytest.approx(5.0)
    assert decision["swing_min_hold_hours"] == pytest.approx(24.0)
    assert decision["would_exit_shadow"] is True
    assert decision["blocked_exit_reason"] == "swing_min_hold_soft_exit_blocked"
    assert decision["blocked_source_reason"] == "atr_trailing"
    assert audit.counts["swing_atr_early_exit_guard_count"] == 1


def test_swing_min_hold_blocks_atr_trailing_large_loss_before_24h(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(
        tmp_path,
        hold_hours=5.0,
        exit_reason="atr_trailing",
        avg_px=600.0,
        signal_price=591.0,
    )

    assert not any(order.symbol == "BNB/USDT" and order.side == "sell" for order in out.orders)
    decision = next(d for d in audit.router_decisions if d.get("reason") == "swing_atr_early_exit_guard")
    assert decision["original_exit_reason"] == "atr_trailing"
    assert decision["net_bps"] < -150.0
    assert decision["exit_priority"] == "soft"
    assert decision["swing_min_hold_guard_checked"] is True
    assert decision["swing_min_hold_guard_blocked"] is True
    assert decision["soft_exit_blocked_by_min_hold"] is True


def test_swing_min_hold_blocks_atr_trailing_risk_off_before_24h(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(
        tmp_path,
        hold_hours=5.0,
        exit_reason="atr_trailing",
        regime_result=_risk_off_regime(),
    )

    assert not any(order.symbol == "BNB/USDT" and order.side == "sell" for order in out.orders)
    decision = next(d for d in audit.router_decisions if d.get("reason") == "swing_atr_early_exit_guard")
    assert decision["risk_off"] is True
    assert decision["exit_allowed_before_min_hold"] is False
    assert decision["soft_exit_blocked_by_min_hold"] is True


def test_swing_atr_early_exit_guard_does_not_affect_non_swing_position(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(
        tmp_path,
        hold_hours=5.0,
        exit_reason="atr_trailing",
        swing_position=False,
    )

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.side == "sell")
    assert "swing_atr_early_exit_guard_active" not in order.meta
    assert not any(d.get("reason") == "swing_atr_early_exit_guard" for d in audit.router_decisions)


def test_swing_min_hold_allows_hard_stop_loss_before_24h(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(tmp_path, hold_hours=5.0, exit_reason="hard_stop_loss")

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.side == "sell")
    assert order.meta["exit_priority"] == "hard"
    assert order.meta["exit_allowed_before_min_hold"] is True
    assert order.meta["exit_blocked_by_min_hold"] is False
    assert order.meta["swing_min_hold_guard_checked"] is True
    assert order.meta["swing_min_hold_guard_blocked"] is False
    assert order.meta["soft_exit_blocked_by_min_hold"] is False
    assert order.meta["hard_exit_exception_reason"] == "hard_stop_loss"
    assert order.meta["hold_hours"] == pytest.approx(5.0)
    assert order.meta["hold_hours_at_exit_check"] == pytest.approx(5.0)
    assert order.meta["swing_min_hold_hours"] == pytest.approx(24.0)
    assert order.meta["would_exit_shadow"] is False
    assert order.meta["blocked_exit_reason"] == ""
    assert any(
        d.get("source_reason") == "hard_stop_loss"
        and d.get("exit_priority") == "hard"
        and d.get("exit_allowed_before_min_hold") is True
        for d in audit.router_decisions
    )


@pytest.mark.parametrize("exit_reason", ["stop_loss", "fixed_stop_loss"])
def test_swing_min_hold_allows_configured_stop_loss_before_24h(tmp_path, exit_reason):
    out, audit = _run_swing_min_hold_exit_case(tmp_path, hold_hours=5.0, exit_reason=exit_reason)

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.side == "sell")
    assert order.meta["exit_priority"] == "hard"
    assert order.meta["exit_allowed_before_min_hold"] is True
    assert order.meta["exit_blocked_by_min_hold"] is False
    assert order.meta["hard_exit_exception_reason"] == exit_reason
    assert order.meta["hold_hours"] == pytest.approx(5.0)
    assert not any(d.get("reason") == "swing_min_hold_exit_block" for d in audit.router_decisions)


def test_swing_min_hold_allows_risk_off_before_24h(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(tmp_path, hold_hours=5.0, exit_reason="risk_off")

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.side == "sell")
    assert order.meta["exit_priority"] == "hard"
    assert order.meta["exit_allowed_before_min_hold"] is True
    assert order.meta["exit_blocked_by_min_hold"] is False
    assert order.meta["hard_exit_exception_reason"] == "risk_off"
    assert order.meta["hold_hours"] == pytest.approx(5.0)
    assert not any(d.get("reason") == "swing_min_hold_exit_block" for d in audit.router_decisions)


def test_swing_min_hold_allows_atr_trailing_after_24h(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(tmp_path, hold_hours=25.0, exit_reason="atr_trailing")

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.side == "sell")
    assert order.meta["exit_priority"] == "soft"
    assert order.meta["exit_allowed_before_min_hold"] is False
    assert order.meta["exit_blocked_by_min_hold"] is False
    assert order.meta["swing_min_hold_guard_checked"] is True
    assert order.meta["swing_min_hold_guard_blocked"] is False
    assert order.meta["soft_exit_blocked_by_min_hold"] is False
    assert order.meta["hold_hours"] == pytest.approx(25.0)
    assert not any(d.get("reason") == "swing_min_hold_exit_block" for d in audit.router_decisions)


def test_swing_min_hold_allows_kill_switch_exit_at_any_hold(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(tmp_path, hold_hours=1.0, exit_reason="kill_switch")

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.side == "sell")
    assert order.meta["exit_priority"] == "hard"
    assert order.meta["exit_allowed_before_min_hold"] is True
    assert order.meta["exit_blocked_by_min_hold"] is False
    assert order.meta["hard_exit_exception_reason"] == "kill_switch"
    assert not any(d.get("reason") == "swing_min_hold_exit_block" for d in audit.router_decisions)


def test_swing_min_hold_allows_emergency_close_before_24h(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(tmp_path, hold_hours=5.0, exit_reason="emergency_close")

    order = next(order for order in out.orders if order.symbol == "BNB/USDT" and order.side == "sell")
    assert order.meta["exit_priority"] == "hard"
    assert order.meta["exit_allowed_before_min_hold"] is True
    assert order.meta["exit_blocked_by_min_hold"] is False
    assert order.meta["hard_exit_exception_reason"] == "emergency_close"
    assert not any(d.get("reason") in {"swing_min_hold_exit_block", "swing_atr_early_exit_guard"} for d in audit.router_decisions)


def test_swing_min_hold_blocks_profit_lock_soft_exit_before_min_hold(tmp_path):
    out, audit = _run_swing_min_hold_exit_case(
        tmp_path,
        hold_hours=5.0,
        exit_reason="protect_profit_lock_trailing",
    )

    assert not any(order.symbol == "BNB/USDT" and order.side == "sell" for order in out.orders)
    decision = next(d for d in audit.router_decisions if d.get("reason") == "swing_min_hold_exit_block")
    assert decision["source_reason"] == "protect_profit_lock_trailing"
    assert decision["exit_blocked_by_min_hold"] is True
    assert decision["exit_priority"] == "soft"
    assert decision["blocked_exit_reason"] == "swing_min_hold_soft_exit_blocked"


def test_rank_exit_does_not_close_when_target_still_positive_even_in_strict_mode(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "OKB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.rank_exit_strict_mode = True
    cfg.execution.rank_exit_require_zero_target = True
    cfg.execution.min_hold_minutes_before_rank_exit = 0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"OKB/USDT": 0.50},
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
    audit = DecisionAudit(run_id="strict-rank-exit-positive-target")

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=100.0,
        equity_peak_usdt=200.0,
        audit=audit,
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 0
    notes = [str(n) for n in audit.notes]
    assert any("rank_exit_target_still_positive: OKB/USDT" in n for n in notes)
    assert any(
        d.get("symbol") == "OKB/USDT"
        and d.get("action") == "skip"
        and d.get("reason") == "rank_exit_target_still_positive"
        and d.get("target_w") == pytest.approx(0.50)
        for d in audit.router_decisions
    )


def test_rank_exit_target_positive_guard_blocks_bnb_even_when_event_rank_is_good(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "BNB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.rank_exit_require_zero_target = True
    cfg.execution.min_hold_minutes_before_rank_exit = 0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BNB/USDT": 0.15},
        selected=["BNB/USDT"],
        volatilities={},
        notes="",
    )
    pipe.profit_taking.positions["BNB/USDT"] = PositionProfitState(
        symbol="BNB/USDT",
        entry_price=628.4,
        entry_time=datetime.now(timezone.utc),
        highest_price=628.4,
        profit_high=0.0,
        current_stop=600.0,
        rank_exit_streak=3,
        last_rank=4,
    )

    px = 628.4
    target_w = 0.15
    cash = (px / target_w) - px
    audit = DecisionAudit(run_id="bnb-rank-exit-target-positive")
    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0), "BNB/USDT": _series("BNB/USDT", px)},
        positions=[
            Position(
                symbol="BNB/USDT",
                qty=1.0,
                avg_px=px,
                entry_ts="2026-05-04T07:00:00Z",
                highest_px=px,
                last_update_ts="2026-05-04T07:00:00Z",
                last_mark_px=px,
                unrealized_pnl_pct=0.0,
            )
        ],
        cash_usdt=cash,
        equity_peak_usdt=cash + px,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(
            raw_factors={},
            z_factors={},
            scores={"BTC/USDT": 1.0, "BNB/USDT": 0.9, "ETH/USDT": 0.8},
        ),
        precomputed_regime=_regime(),
    )

    assert not any(
        order.side == "sell"
        and order.intent == "CLOSE_LONG"
        and str((order.meta or {}).get("reason", "")).startswith("rank_exit_")
        for order in out.orders
    )
    decision = next(
        d for d in audit.router_decisions
        if d.get("symbol") == "BNB/USDT" and d.get("reason") == "rank_exit_target_still_positive"
    )
    assert decision["action"] == "skip"
    assert decision["target_w"] == pytest.approx(0.15)
    assert decision["rank"] == 2


def test_rank_exit_zero_target_allows_close_when_rank_threshold_and_confirm_met(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "BNB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.rank_exit_max_rank = 3
    cfg.execution.rank_exit_confirm_rounds = 1
    cfg.execution.rank_exit_require_zero_target = True
    cfg.execution.min_hold_minutes_before_rank_exit = 0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        volatilities={},
        notes="",
    )
    pipe.profit_taking.positions["BNB/USDT"] = PositionProfitState(
        symbol="BNB/USDT",
        entry_price=628.4,
        entry_time=datetime.now(timezone.utc),
        highest_price=628.4,
        profit_high=0.0,
        current_stop=600.0,
        rank_exit_streak=0,
        last_rank=4,
    )

    px = 622.0
    audit = DecisionAudit(run_id="bnb-rank-exit-zero-target")
    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0), "BNB/USDT": _series("BNB/USDT", px)},
        positions=[
            Position(
                symbol="BNB/USDT",
                qty=1.0,
                avg_px=628.4,
                entry_ts="2026-05-04T07:00:00Z",
                highest_px=628.4,
                last_update_ts="2026-05-04T07:00:00Z",
                last_mark_px=px,
                unrealized_pnl_pct=-0.01,
            )
        ],
        cash_usdt=100.0,
        equity_peak_usdt=800.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(
            raw_factors={},
            z_factors={},
            scores={"BTC/USDT": 1.0, "ETH/USDT": 0.9, "SOL/USDT": 0.8, "BNB/USDT": 0.7},
        ),
        precomputed_regime=_regime(),
    )

    order = next(
        order for order in out.orders
        if order.symbol == "BNB/USDT"
        and order.side == "sell"
        and order.intent == "CLOSE_LONG"
    )
    assert str(order.meta["reason"]).startswith("rank_exit_rank_4_exceeds_3_streak_1")
    assert order.meta["rank_exit_validated_by_router"] is True
    assert any(
        d.get("symbol") == "BNB/USDT"
        and d.get("action") == "create"
        and d.get("source_reason") == order.meta["reason"]
        for d in audit.router_decisions
    )


def test_rank_exit_buffer_positions_delays_streak_start(tmp_path):
    pm = ProfitTakingManager(
        rank_exit_strict_mode=False,
        rank_exit_buffer_positions=2,
        state_path=str(tmp_path / "profit_taking_state.json"),
    )
    pm.positions["DOT/USDT"] = PositionProfitState(
        symbol="DOT/USDT",
        entry_price=10.0,
        entry_time=datetime.now(timezone.utc),
        highest_price=10.0,
        profit_high=0.0,
        current_stop=9.0,
        rank_exit_streak=0,
        last_rank=3,
    )

    should_exit, reason = pm.should_exit_by_rank(
        "DOT/USDT",
        current_rank=5,
        max_rank=3,
        confirm_rounds=2,
        buffer_positions=2,
    )
    assert should_exit is False
    assert reason.startswith("rank_exit_buffered")
    assert pm.positions["DOT/USDT"].rank_exit_streak == 0

    should_exit, reason = pm.should_exit_by_rank(
        "DOT/USDT",
        current_rank=6,
        max_rank=3,
        confirm_rounds=2,
        buffer_positions=2,
    )
    assert should_exit is False
    assert reason == "rank_exit_pending_1/2"
    assert pm.positions["DOT/USDT"].rank_exit_streak == 1


def test_profit_taking_zulu_entry_time_summary_uses_utc_state(tmp_path):
    pm = ProfitTakingManager(state_path=str(tmp_path / "profit_taking_state.json"))

    pm.register_position(
        "ETH/USDT",
        100.0,
        current_price=101.0,
        entry_ts="2026-03-09T00:00:00Z",
    )

    summary = pm.get_position_summary("ETH/USDT", 101.0)
    state = pm.positions["ETH/USDT"]
    payload = json.loads((tmp_path / "profit_taking_state.json").read_text(encoding="utf-8"))
    assert summary is not None
    assert isinstance(summary["days_held"], int)
    assert state.entry_time.tzinfo is not None
    assert state.entry_time.utcoffset() == timedelta(0)
    assert payload["ETH/USDT"]["entry_time"].endswith("+00:00")


def test_profit_taking_action_timestamps_are_utc(tmp_path):
    pm = ProfitTakingManager(
        rank_exit_strict_mode=True,
        state_path=str(tmp_path / "profit_taking_state.json"),
    )
    pm.register_position("ETH/USDT", 100.0, current_price=100.0)

    pm.evaluate("ETH/USDT", 120.0)
    action, _, _ = pm.evaluate("ETH/USDT", 120.0)
    assert action == "sell_partial"
    assert pm.positions["ETH/USDT"].partial_sell_time is not None
    assert pm.positions["ETH/USDT"].partial_sell_time.utcoffset() == timedelta(0)

    should_exit, _ = pm.should_exit_by_rank("ETH/USDT", current_rank=5, max_rank=3, confirm_rounds=1)
    assert should_exit is True
    assert pm.positions["ETH/USDT"].last_rank_exit_time is not None
    assert pm.positions["ETH/USDT"].last_rank_exit_time.utcoffset() == timedelta(0)


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


def test_take_profit_sell_all_generates_close_long_order(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "OKB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.take_profit_sell_all_pct = 0.10

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"OKB/USDT": 0.40},
        selected=["OKB/USDT"],
        volatilities={},
        notes="",
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "OKB/USDT": _series("OKB/USDT", 110.0),
    }
    positions = [
        Position(
            symbol="OKB/USDT",
            qty=1.0,
            avg_px=100.0,
            entry_ts="2026-03-09T00:00:00Z",
            highest_px=110.0,
            last_update_ts="2026-03-09T00:00:00Z",
            last_mark_px=110.0,
            unrealized_pnl_pct=0.10,
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
        equity_peak_usdt=210.0,
        audit=DecisionAudit(run_id="take-profit-sell-all"),
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.symbol == "OKB/USDT"
    assert order.side == "sell"
    assert order.intent == "CLOSE_LONG"
    assert order.notional_usdt == pytest.approx(110.0)
    assert order.meta["reason"] == "profit_taking_take_profit_10pct"


def test_take_profit_sell_all_uses_latest_bar_when_market_series_is_unsorted(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "OKB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.take_profit_sell_all_pct = 0.10

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"OKB/USDT": 0.40},
        selected=["OKB/USDT"],
        volatilities={},
        notes="",
    )

    ordered_ts = [_ms(1700000000 + i * 3600) for i in range(30)]
    latest_ts = ordered_ts[-1]
    older_ts = ordered_ts[:-1]
    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "OKB/USDT": MarketSeries(
            symbol="OKB/USDT",
            timeframe="1h",
            ts=[latest_ts, *older_ts],
            open=[110.0, *[80.0 for _ in older_ts]],
            high=[111.0, *[81.0 for _ in older_ts]],
            low=[109.0, *[79.0 for _ in older_ts]],
            close=[110.0, *[80.0 for _ in older_ts]],
            volume=[1000.0 for _ in range(30)],
        ),
    }
    positions = [
        Position(
            symbol="OKB/USDT",
            qty=1.0,
            avg_px=100.0,
            entry_ts="2026-03-09T00:00:00Z",
            highest_px=110.0,
            last_update_ts="2026-03-09T00:00:00Z",
            last_mark_px=110.0,
            unrealized_pnl_pct=0.10,
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
        equity_peak_usdt=210.0,
        audit=DecisionAudit(run_id="take-profit-sell-all-unsorted"),
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.symbol == "OKB/USDT"
    assert order.signal_price == 110.0
    assert order.notional_usdt == pytest.approx(110.0)
    assert order.meta["reason"] == "profit_taking_take_profit_10pct"


def test_take_profit_sell_all_uses_highest_price_from_hold_cycle(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "OKB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.take_profit_sell_all_pct = 0.10

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"OKB/USDT": 0.40},
        selected=["OKB/USDT"],
        volatilities={},
        notes="",
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "OKB/USDT": MarketSeries(
            symbol="OKB/USDT",
            timeframe="1h",
            ts=[_ms(1700000000 + i * 3600) for i in range(30)],
            open=[109.0 for _ in range(30)],
            high=[110.5 for _ in range(30)],
            low=[108.0 for _ in range(30)],
            close=[109.0 for _ in range(30)],
            volume=[1000.0 for _ in range(30)],
        ),
    }
    positions = [
        Position(
            symbol="OKB/USDT",
            qty=1.0,
            avg_px=100.0,
            entry_ts="2026-03-09T00:00:00Z",
            highest_px=110.5,
            last_update_ts="2026-03-09T00:00:00Z",
            last_mark_px=109.0,
            unrealized_pnl_pct=0.09,
        )
    ]
    alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0, "OKB/USDT": 0.95})

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=100.0,
        equity_peak_usdt=210.0,
        audit=DecisionAudit(run_id="take-profit-highest-persisted"),
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.intent == "CLOSE_LONG"
    assert order.meta["reason"] == "profit_taking_take_profit_10pct"


def test_peak_drawdown_exit_uses_intrabar_low_after_profit_threshold(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "OKB/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.peak_drawdown_exit.enabled = True

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"OKB/USDT": 0.50},
        selected=["OKB/USDT"],
        volatilities={},
        notes="",
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "OKB/USDT": MarketSeries(
            symbol="OKB/USDT",
            timeframe="1h",
            ts=[_ms(1700000000 + i * 3600) for i in range(30)],
            open=[109.5 for _ in range(30)],
            high=[110.0 for _ in range(30)],
            low=[106.5 for _ in range(30)],
            close=[109.5 for _ in range(30)],
            volume=[1000.0 for _ in range(30)],
        ),
    }
    positions = [
        Position(
            symbol="OKB/USDT",
            qty=1.0,
            avg_px=100.0,
            entry_ts="2026-03-09T00:00:00Z",
            highest_px=110.0,
            last_update_ts="2026-03-09T00:00:00Z",
            last_mark_px=109.5,
            unrealized_pnl_pct=0.095,
        )
    ]
    alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0, "OKB/USDT": 0.95})

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=100.0,
        equity_peak_usdt=210.0,
        audit=DecisionAudit(run_id="peak-drawdown-intrabar-low"),
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert len(out.orders) == 1
    order = out.orders[0]
    assert order.intent == "REBALANCE"
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


def test_zero_target_close_sell_bypasses_turnover_cap(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "SUI/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.max_rebalance_turnover_per_cycle = 0.10

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
        scores={"SUI/USDT": 1.0, "BTC/USDT": 0.5},
    )
    audit = DecisionAudit(run_id="zero-target-close-bypass")

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
    assert bool((order.meta or {}).get("bypass_turnover_cap_for_exit")) is True
    assert (order.meta or {}).get("turnover_cap_bypass_reason") == "zero_target_close"
    assert any(
        d.get("symbol") == "SUI/USDT"
        and d.get("reason") == "zero_target_close"
        and d.get("bypass_turnover_cap_for_exit") is True
        for d in audit.router_decisions
    )


def test_exit_signal_sell_bypasses_turnover_cap_while_buy_is_blocked(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT", "SUI/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.max_rebalance_turnover_per_cycle = 0.05
    cfg.budget.min_trade_notional_base = 10.0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    pipe.exit_policy.evaluate = lambda positions, market_data, regime_state: [
        pipeline_module.Order(
            symbol="SUI/USDT",
            side="sell",
            intent="CLOSE_LONG",
            notional_usdt=40.0,
            signal_price=1.0,
            meta={"reason": "atr_trailing"},
        )
    ]

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
        scores={"BTC/USDT": 1.0, "SUI/USDT": 0.5},
    )
    audit = DecisionAudit(run_id="exit-priority-bypass")

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=100.0,
        equity_peak_usdt=140.0,
        audit=audit,
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert any(order.symbol == "SUI/USDT" and order.side == "sell" for order in out.orders)
    assert not any(order.symbol == "BTC/USDT" and order.side == "buy" for order in out.orders)
    assert any(
        d.get("symbol") == "SUI/USDT"
        and d.get("reason") == "exit_signal_priority"
        and d.get("bypass_turnover_cap_for_exit") is True
        for d in audit.router_decisions
    )
    assert any(
        d.get("symbol") == "BTC/USDT"
        and d.get("reason") == "turnover_cap"
        for d in audit.router_decisions
    )


def test_open_long_buy_still_blocked_when_turnover_cap_is_full(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.max_rebalance_turnover_per_cycle = 0.05
    cfg.budget.min_trade_notional_base = 10.0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        volatilities={},
        notes="",
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
    }
    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={"BTC/USDT": 1.0},
    )
    audit = DecisionAudit(run_id="buy-turnover-block")

    out = pipe.run(
        market_data_1h=market_data,
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(
        d.get("symbol") == "BTC/USDT"
        and d.get("reason") == "turnover_cap"
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


def test_dust_position_does_not_trigger_anti_chase_add_size(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.anti_chase_enabled = True
    cfg.execution.anti_chase_max_add_notional_ratio = 1.0
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.min_trade_notional_base = 10.0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 0.15},
        selected=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="dust-anti-chase-open")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[
            Position(
                symbol="BTC/USDT",
                qty=0.00000000892,
                avg_px=50000.0,
                entry_ts="2026-04-24T08:00:00Z",
                highest_px=50000.0,
                last_update_ts="2026-04-24T08:00:00Z",
                last_mark_px=50000.0,
                unrealized_pnl_pct=0.0,
            )
        ],
        cash_usdt=106.96,
        equity_peak_usdt=107.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any(order.symbol == "BTC/USDT" and order.side == "buy" for order in out.orders)
    order = next(order for order in out.orders if order.symbol == "BTC/USDT" and order.side == "buy")
    assert order.intent == "OPEN_LONG"
    assert not any(d.get("reason") == "anti_chase_add_size" for d in audit.router_decisions)
    assert any(
        d.get("symbol") == "BTC/USDT"
        and d.get("dust_position_ignored_for_add_size") is True
        and d.get("held_value_usdt") == pytest.approx(0.000446)
        for d in audit.router_decisions
    )
    assert audit.counts["dust_position_ignored_for_add_size_count"] == 1


def test_real_position_still_triggers_anti_chase_add_size(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.anti_chase_enabled = True
    cfg.execution.anti_chase_max_add_notional_ratio = 1.0
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.min_trade_notional_base = 10.0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 0.225},
        selected=["BTC/USDT"],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="real-position-anti-chase")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[
            Position(
                symbol="BTC/USDT",
                qty=0.00016,
                avg_px=50000.0,
                entry_ts="2026-04-24T08:00:00Z",
                highest_px=50000.0,
                last_update_ts="2026-04-24T08:00:00Z",
                last_mark_px=50000.0,
                unrealized_pnl_pct=0.0,
            )
        ],
        cash_usdt=99.0,
        equity_peak_usdt=107.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(
        d.get("symbol") == "BTC/USDT"
        and d.get("reason") == "anti_chase_add_size"
        and d.get("held_value") == pytest.approx(8.0)
        for d in audit.router_decisions
    )


def test_dust_residual_does_not_generate_close_order(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.min_trade_notional_base = 10.0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="dust-no-close")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[
            Position(
                symbol="BTC/USDT",
                qty=0.00000000892,
                avg_px=50000.0,
                entry_ts="2026-04-24T08:00:00Z",
                highest_px=50000.0,
                last_update_ts="2026-04-24T08:00:00Z",
                last_mark_px=50000.0,
                unrealized_pnl_pct=0.0,
            )
        ],
        cash_usdt=106.96,
        equity_peak_usdt=107.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert not out.orders
    assert any(
        d.get("symbol") == "BTC/USDT"
        and d.get("reason") == "dust_residual_no_close_order"
        and d.get("held_value_usdt") == pytest.approx(0.000446)
        for d in audit.router_decisions
    )
    assert audit.counts["dust_residual_no_close_order_count"] == 1


def test_non_dust_position_zero_target_close_unchanged(tmp_path):
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.alpha.use_fused_score_for_weighting = False
    cfg.execution.min_trade_value_usdt = 10.0
    cfg.budget.min_trade_notional_base = 10.0

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="non-dust-close")

    out = pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[
            Position(
                symbol="BTC/USDT",
                qty=0.00024,
                avg_px=50000.0,
                entry_ts="2026-04-24T08:00:00Z",
                highest_px=50000.0,
                last_update_ts="2026-04-24T08:00:00Z",
                last_mark_px=50000.0,
                unrealized_pnl_pct=0.0,
            )
        ],
        cash_usdt=95.0,
        equity_peak_usdt=107.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert any(order.symbol == "BTC/USDT" and order.side == "sell" for order in out.orders)
    assert not any(d.get("reason") == "dust_residual_no_close_order" for d in audit.router_decisions)
