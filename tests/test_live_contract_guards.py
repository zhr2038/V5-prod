from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import main as main_module
from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries, Order
from src.core.pipeline import V5Pipeline
from src.execution.position_store import Position
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(sym: str, close: float) -> MarketSeries:
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(30)]
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


def test_live_whitelist_blocks_non_whitelist_router_symbols(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.mode = "live"
    cfg.alpha.use_fused_score_for_weighting = False

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"BTC/USDT": 0.50, "ETH/USDT": 0.50},
        selected=["BTC/USDT", "ETH/USDT"],
        entry_candidates=["BTC/USDT", "ETH/USDT"],
        volatilities={},
        notes="",
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50_000.0),
        "ETH/USDT": _series("ETH/USDT", 2_500.0),
    }
    positions = [
        Position(
            symbol="ETH/USDT",
            qty=1.0,
            avg_px=2_500.0,
            entry_ts="2026-04-16T00:00:00Z",
            highest_px=2_500.0,
            last_update_ts="2026-04-16T00:00:00Z",
            last_mark_px=2_500.0,
            unrealized_pnl_pct=0.0,
        )
    ]
    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={"BTC/USDT": 1.0, "ETH/USDT": 0.9},
    )
    audit = DecisionAudit(run_id="live-whitelist-router")

    out = pipe.run(
        market_data_1h=market_data,
        positions=positions,
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=alpha,
        precomputed_regime=_regime(),
    )

    assert all(
        str(decision.get("symbol") or "") in {"", "BTC/USDT"}
        for decision in (audit.router_decisions or [])
    )
    assert all(order.symbol == "BTC/USDT" for order in (out.orders or []))
    assert any("live whitelist enforced" in note for note in (audit.notes or []))


def test_negative_expectancy_rank_guard_records_preselection_blockers(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT"])
    pipe = _build_pipe(cfg, tmp_path)
    audit = DecisionAudit(run_id="negexp-rank-guard")
    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={"BTC/USDT": 1.0, "ETH/USDT": 0.8},
    )

    adjusted = pipe._apply_negative_expectancy_rank_guard(
        alpha,
        {"symbols": {"BTC/USDT": {"remain_seconds": 3600}}, "stats": {}},
        positions=[],
        audit=audit,
    )

    assert adjusted.scores["BTC/USDT"] < adjusted.scores["ETH/USDT"]
    assert audit.rejects["negative_expectancy_cooldown"] == 1
    assert audit.counts["negative_expectancy_cooldown"] == 1
    assert any("reason=negative_expectancy_cooldown" in note for note in (audit.notes or []))


def test_pipeline_negative_expectancy_refresh_sets_live_scope(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.mode = "live"
    pipe = _build_pipe(cfg, tmp_path)

    calls: dict[str, object] = {}

    class _DummyCooldown:
        def set_scope(self, **kwargs):
            calls.update(kwargs)

        def refresh(self, force: bool = False):
            calls["force"] = force
            return {}

    pipe.negative_expectancy_cooldown = _DummyCooldown()

    positions = [
        Position(
            symbol="ETH/USDT",
            qty=1.0,
            avg_px=2500.0,
            entry_ts="2026-04-16T00:00:00Z",
            highest_px=2500.0,
            last_update_ts="2026-04-16T00:00:00Z",
            last_mark_px=2500.0,
            unrealized_pnl_pct=0.0,
        )
    ]

    pipe._refresh_negative_expectancy_state_with_scope(
        positions=positions,
        managed_symbols=["SOL/USDT"],
        audit=None,
    )

    assert calls["whitelist_symbols"] == ["BTC/USDT"]
    assert calls["open_position_symbols"] == ["ETH/USDT"]
    assert calls["managed_symbols"] == ["SOL/USDT"]
    assert isinstance(calls["config_fingerprint"], str) and len(str(calls["config_fingerprint"])) == 16
    assert calls["force"] is False


def test_negative_expectancy_refresh_writes_release_start_to_decision_audit(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.mode = "live"
    pipe = _build_pipe(cfg, tmp_path)

    class _DummyCooldown:
        cfg = SimpleNamespace(state_path=str(tmp_path / "negative_expectancy_state.json"))

        def set_scope(self, **kwargs):
            pass

        def refresh(self, force: bool = False):
            return {
                "config_fingerprint": "scope-fp",
                "release_start_ts": 1_776_000_000_000,
                "release_start_ts_status": "ok",
                "warnings": [],
                "symbols": {},
                "stats": {},
                "scope_symbols": ["BTC/USDT"],
            }

    pipe.negative_expectancy_cooldown = _DummyCooldown()
    audit = DecisionAudit(run_id="negexp-release")

    pipe._refresh_negative_expectancy_state_with_scope(
        positions=[],
        managed_symbols=["BTC/USDT"],
        audit=audit,
    )

    assert audit.negative_expectancy_state["config_fingerprint"] == "scope-fp"
    assert audit.negative_expectancy_state["release_start_ts"] == 1_776_000_000_000
    assert audit.negative_expectancy_state["release_start_ts_status"] == "ok"
    assert any("release_start_ts=1776000000000" in note for note in audit.notes)


def test_pipeline_negative_expectancy_refresh_logs_not_observable_release_start(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.mode = "live"
    pipe = _build_pipe(cfg, tmp_path)

    class _DummyCooldown:
        cfg = SimpleNamespace(state_path=str(tmp_path / "negative_expectancy_state.json"))

        def set_scope(self, **kwargs):
            pass

        def refresh(self, force: bool = False):
            return {
                "symbols": {},
                "stats": {},
                "scope_symbols": ["BTC/USDT"],
                "release_start_ts": "not_observable",
                "release_start_ts_status": "not_observable",
                "warnings": ["negative_expectancy_release_start_ts_not_observable"],
            }

    pipe.negative_expectancy_cooldown = _DummyCooldown()
    audit = DecisionAudit(run_id="negexp-refresh")

    pipe._refresh_negative_expectancy_state_with_scope(
        positions=[],
        managed_symbols=[],
        audit=audit,
    )

    assert any("release_start_ts=not_observable" in note for note in audit.notes)
    assert any("NegativeExpectancy warning: negative_expectancy_release_start_ts_not_observable" in note for note in audit.notes)


def test_decision_audit_record_gate_dedupes_symbol_reason() -> None:
    audit = DecisionAudit(run_id="negexp-dedupe")

    audit.record_gate("negative_expectancy_open_block", symbol="BTC/USDT")
    audit.record_gate("negative_expectancy_open_block", symbol="BTC/USDT")
    audit.record_gate("negative_expectancy_open_block", symbol="ETH/USDT")

    assert audit.rejects["negative_expectancy_open_block"] == 2
    assert audit.counts["negative_expectancy_open_block"] == 2


def test_decision_audit_record_count_dedupes_without_rejecting() -> None:
    audit = DecisionAudit(run_id="negexp-penalty")

    audit.record_count("negative_expectancy_score_penalty", symbol="BTC/USDT")
    audit.record_count("negative_expectancy_score_penalty", symbol="BTC/USDT")
    audit.record_count("negative_expectancy_score_penalty", symbol="ETH/USDT")

    assert audit.counts["negative_expectancy_score_penalty"] == 2
    assert audit.rejects.get("negative_expectancy_score_penalty", 0) == 0


def test_write_effective_live_config_writes_required_keys(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT"])
    cfg.execution.mode = "live"
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.universe.enabled = False
    cfg.universe.use_universe_symbols = False
    cfg.alpha.alpha158_overlay.enabled = False
    cfg.alpha.long_top_pct = 0.50
    cfg.alpha.min_score_threshold = 0.10
    cfg.execution.fee_bps = 10
    cfg.execution.cost_aware_roundtrip_cost_bps = 30
    cfg.execution.rank_exit_max_rank = 5
    cfg.execution.rank_exit_confirm_rounds = 3
    cfg.execution.rank_exit_strict_mode = False
    cfg.execution.min_hold_minutes_before_rank_exit = 180
    cfg.execution.min_hold_minutes_before_regime_exit = 240
    cfg.execution.max_rebalance_turnover_per_cycle = 0.15
    state_path = tmp_path / "reports" / "negative_expectancy_cooldown.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "config_fingerprint": main_module.negative_expectancy_config_fingerprint(cfg),
                "release_start_ts": 1_776_000_000_000,
                "release_start_ts_status": "ok",
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )

    out_path = main_module._write_effective_live_config(cfg)
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    assert payload["symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert payload["universe"]["enabled"] is False
    assert payload["universe"]["use_universe_symbols"] is False
    assert payload["alpha"]["alpha158_overlay"]["enabled"] is False
    assert payload["alpha"]["long_top_pct"] == pytest.approx(0.50)
    assert payload["alpha"]["min_score_threshold"] == pytest.approx(0.10)
    assert payload["execution"]["fee_bps"] == pytest.approx(10.0)
    assert payload["execution"]["cost_aware_roundtrip_cost_bps"] == pytest.approx(30.0)
    assert payload["execution"]["rank_exit_max_rank"] == 5
    assert payload["execution"]["rank_exit_confirm_rounds"] == 3
    assert payload["execution"]["rank_exit_strict_mode"] is False
    assert payload["execution"]["min_hold_minutes_before_rank_exit"] == 180
    assert payload["execution"]["min_hold_minutes_before_regime_exit"] == 240
    assert payload["execution"]["max_rebalance_turnover_per_cycle"] == pytest.approx(0.15)
    for key in main_module.BTC_LEADERSHIP_PROBE_CONFIG_KEYS:
        assert key in payload["execution"]
    for key in main_module.PROBE_EXIT_CONFIG_KEYS:
        assert key in payload["execution"]
    for key in main_module.PROTECT_PROFIT_LOCK_CONFIG_KEYS:
        assert key in payload["execution"]
    for key in main_module.SAME_SYMBOL_REENTRY_GUARD_CONFIG_KEYS:
        assert key in payload["execution"]
    assert payload["execution"]["btc_leadership_probe_enabled"] is True
    assert payload["execution"]["btc_leadership_probe_min_alpha6_score"] == pytest.approx(0.30)
    assert payload["execution"]["btc_leadership_probe_time_stop_hours"] == 8
    assert payload["execution"]["probe_exit_enabled"] is True
    assert payload["execution"]["probe_time_stop_hours"] == 8
    assert payload["execution"]["probe_ignore_normal_zero_target_close"] is True
    assert payload["execution"]["probe_min_hold_hours_before_zero_target_close"] == 4
    assert payload["execution"]["probe_allow_zero_target_close_on_risk_off"] is True
    assert payload["execution"]["probe_allow_zero_target_close_on_hard_negative_expectancy"] is False
    assert payload["execution"]["protect_profit_lock_enabled"] is True
    assert payload["execution"]["protect_profit_lock_min_net_bps"] == pytest.approx(100.0)
    assert payload["execution"]["protect_profit_lock_trailing_gap_bps"] == pytest.approx(60.0)
    assert payload["execution"]["same_symbol_reentry_guard_enabled"] is True
    assert payload["execution"]["same_symbol_reentry_cooldown_hours_after_profit_lock"] == 6
    assert payload["execution"]["negative_expectancy_release_start_ts"] == 1_776_000_000_000
    assert payload["execution"]["negative_expectancy_release_start_ts_status"] == "ok"
    assert payload["execution"]["negative_expectancy_release_start_ts_warning"] == ""


def test_write_effective_live_config_writes_btc_probe_defaults_when_yaml_omits_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.mode = "live"
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    monkeypatch.setattr(main_module.time, "time", lambda: 2_000_000.0)

    out_path = main_module._write_effective_live_config(cfg)
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    for key in main_module.BTC_LEADERSHIP_PROBE_CONFIG_KEYS:
        assert key in payload["execution"]
    for key in main_module.PROBE_EXIT_CONFIG_KEYS:
        assert key in payload["execution"]
    for key in main_module.PROTECT_PROFIT_LOCK_CONFIG_KEYS:
        assert key in payload["execution"]
    for key in main_module.SAME_SYMBOL_REENTRY_GUARD_CONFIG_KEYS:
        assert key in payload["execution"]
    assert payload["execution"]["btc_leadership_probe_enabled"] is True
    assert payload["execution"]["btc_leadership_probe_only_in_protect"] is True
    assert payload["execution"]["btc_leadership_probe_target_w"] == pytest.approx(0.08)
    assert payload["execution"]["btc_leadership_probe_dynamic_sizing_enabled"] is True
    assert payload["execution"]["btc_leadership_probe_max_target_w"] == pytest.approx(0.10)
    assert payload["execution"]["btc_leadership_probe_cooldown_hours"] == 8
    assert payload["execution"]["btc_leadership_probe_lookback_hours"] == 24
    assert payload["execution"]["btc_leadership_probe_breakout_buffer_bps"] == pytest.approx(15.0)
    assert payload["execution"]["btc_leadership_probe_min_alpha6_score"] == pytest.approx(0.30)
    assert payload["execution"]["btc_leadership_probe_min_f5_rsi"] == pytest.approx(0.30)
    assert payload["execution"]["btc_leadership_probe_min_f4_volume"] == pytest.approx(-0.10)
    assert payload["execution"]["btc_leadership_probe_require_regime_not_risk_off"] is True
    assert payload["execution"]["btc_leadership_probe_allow_single_negative_cycle_bypass"] is True
    assert payload["execution"]["btc_leadership_probe_max_negative_cycles_to_bypass"] == 1
    assert payload["execution"]["btc_leadership_probe_min_net_expectancy_bps_to_bypass"] == pytest.approx(-120.0)
    assert payload["execution"]["btc_leadership_probe_time_stop_hours"] == 8
    assert payload["execution"]["probe_exit_enabled"] is True
    assert payload["execution"]["probe_take_profit_net_bps"] == pytest.approx(80.0)
    assert payload["execution"]["probe_stop_loss_net_bps"] == pytest.approx(-50.0)
    assert payload["execution"]["probe_trailing_enable_after_net_bps"] == pytest.approx(50.0)
    assert payload["execution"]["probe_trailing_gap_bps"] == pytest.approx(25.0)
    assert payload["execution"]["probe_time_stop_hours"] == 8
    assert payload["execution"]["probe_time_stop_min_net_bps"] == pytest.approx(10.0)
    assert payload["execution"]["probe_ignore_normal_zero_target_close"] is True
    assert payload["execution"]["probe_min_hold_hours_before_zero_target_close"] == 4
    assert payload["execution"]["probe_allow_zero_target_close_on_risk_off"] is True
    assert payload["execution"]["probe_allow_zero_target_close_on_hard_negative_expectancy"] is False
    assert payload["execution"]["protect_profit_lock_enabled"] is True
    assert payload["execution"]["protect_profit_lock_min_net_bps"] == pytest.approx(100.0)
    assert payload["execution"]["protect_profit_lock_breakeven_plus_bps"] == pytest.approx(20.0)
    assert payload["execution"]["protect_profit_lock_trailing_start_net_bps"] == pytest.approx(150.0)
    assert payload["execution"]["protect_profit_lock_trailing_gap_bps"] == pytest.approx(60.0)
    assert payload["execution"]["protect_profit_lock_strong_start_net_bps"] == pytest.approx(200.0)
    assert payload["execution"]["protect_profit_lock_strong_trailing_gap_bps"] == pytest.approx(50.0)
    assert payload["execution"]["same_symbol_reentry_guard_enabled"] is True
    assert payload["execution"]["same_symbol_reentry_cooldown_hours_after_profit_lock"] == 6
    assert payload["execution"]["same_symbol_reentry_cooldown_hours_after_probe_stop"] == 8
    assert payload["execution"]["same_symbol_reentry_cooldown_hours_after_probe_take_profit"] == 4
    assert payload["execution"]["same_symbol_reentry_allow_breakout"] is True
    assert payload["execution"]["same_symbol_reentry_breakout_above_last_high_bps"] == pytest.approx(20.0)
    assert payload["execution"]["same_symbol_reentry_breakout_above_exit_bps"] == pytest.approx(50.0)
    assert payload["execution"]["same_symbol_reentry_apply_to_market_impulse_probe"] is True
    assert payload["execution"]["same_symbol_reentry_apply_to_btc_leadership_probe"] is True
    assert payload["execution"]["same_symbol_reentry_apply_to_normal_entry"] is True
    assert payload["execution"]["negative_expectancy_release_start_ts"] == 2_000_000_000
    assert payload["execution"]["negative_expectancy_release_start_ts_status"] == "ok"
    assert payload["execution"]["negative_expectancy_release_start_ts_warning"] == ""


def test_write_effective_live_config_recovers_not_observable_release_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.mode = "live"
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    state_path = tmp_path / "reports" / "negative_expectancy_cooldown.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "config_fingerprint": main_module.negative_expectancy_config_fingerprint(cfg),
                "release_start_ts": "not_observable",
                "release_start_ts_status": "not_observable",
                "symbols": {"BTC/USDT": {"cooldown_until_ms": 9999999999999}},
                "stats": {"BTC/USDT": {"closed_cycles": 3}},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(main_module.time, "time", lambda: 2_000_000.0)

    out_path = main_module._write_effective_live_config(cfg)
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert payload["execution"]["negative_expectancy_release_start_ts"] == 2_000_000_000
    assert payload["execution"]["negative_expectancy_release_start_ts_status"] == "recovered"
    assert "negative_expectancy_release_start_ts_recovered" in payload["execution"]["negative_expectancy_release_start_ts_warning"]
    assert state["release_start_ts"] == 2_000_000_000
    assert state["release_start_ts_status"] == "recovered"
    assert state["symbols"] == {}
    assert state["stats"] == {}


def test_main_live_preflight_blocks_before_provider_and_order_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.mode = "live"
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.exchange.api_key = "key"
    cfg.exchange.api_secret = "secret"
    cfg.exchange.passphrase = "pass"

    monkeypatch.setenv("V5_LIVE_ARM", "YES")
    monkeypatch.setattr(main_module, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(main_module, "setup_logging", lambda *args, **kwargs: None)

    calls = {"preflight": 0, "provider": 0}

    def _fake_preflight(*args, **kwargs):
        calls["preflight"] += 1
        raise RuntimeError("live preflight blocked before routing: decision=ABORT reason=test")

    def _fake_build_provider(_cfg):
        calls["provider"] += 1
        raise AssertionError("provider should not be built after preflight failure")

    monkeypatch.setattr(main_module, "_run_live_preflight_or_raise", _fake_preflight)
    monkeypatch.setattr(main_module, "build_provider", _fake_build_provider)

    with pytest.raises(RuntimeError, match="live preflight blocked before routing"):
        main_module.main()

    assert calls == {"preflight": 1, "provider": 0}
    assert (tmp_path / "reports" / "effective_live_config.json").exists()


def test_run_live_preflight_respects_sell_only_fail_action(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.mode = "live"
    cfg.execution.preflight_fail_action = "sell_only"
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())

    dummy_result = SimpleNamespace(decision="SELL_ONLY", reason="status_stale", details={})
    dummy_client = object()

    class DummyPreflight:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def run(self, **kwargs):
            return dummy_result

    monkeypatch.setattr("src.execution.live_preflight.LivePreflight", DummyPreflight)
    monkeypatch.setattr("src.execution.okx_private_client.OKXPrivateClient", lambda exchange: dummy_client)

    audit = DecisionAudit(run_id="preflight-sell-only")
    client, result = main_module._run_live_preflight_or_raise(
        cfg,
        store=None,
        acc_store=None,
        audit=audit,
        runtime_run_dir=tmp_path,
    )

    assert client is dummy_client
    assert result is dummy_result


def test_apply_live_preflight_order_restrictions_filters_buy_orders() -> None:
    orders = [
        Order(symbol="BTC/USDT", side="buy", intent="OPEN_LONG", notional_usdt=10.0, signal_price=100.0, meta={}),
        Order(symbol="ETH/USDT", side="buy", intent="REBALANCE", notional_usdt=5.0, signal_price=200.0, meta={}),
        Order(symbol="SOL/USDT", side="sell", intent="CLOSE_LONG", notional_usdt=8.0, signal_price=20.0, meta={}),
        Order(symbol="ADA/USDT", side="buy", intent="REPAY_LIABILITY", notional_usdt=4.0, signal_price=1.0, meta={}),
    ]
    audit = DecisionAudit(run_id="preflight-filter")

    filtered = main_module._apply_live_preflight_order_restrictions(
        orders=orders,
        live_preflight_result=SimpleNamespace(decision="SELL_ONLY", reason="status_stale"),
        audit=audit,
        log=None,
    )

    assert [(order.symbol, order.side, order.intent) for order in filtered] == [
        ("SOL/USDT", "sell", "CLOSE_LONG"),
        ("ADA/USDT", "buy", "REPAY_LIABILITY"),
    ]
    assert any("live preflight sell-only filtered buy orders" in note for note in (audit.notes or []))


def test_apply_live_preflight_order_restrictions_still_filters_buy_orders_for_allow_action() -> None:
    orders = [
        Order(symbol="BTC/USDT", side="buy", intent="OPEN_LONG", notional_usdt=10.0, signal_price=100.0, meta={}),
        Order(symbol="SOL/USDT", side="sell", intent="CLOSE_LONG", notional_usdt=8.0, signal_price=20.0, meta={}),
    ]

    filtered = main_module._apply_live_preflight_order_restrictions(
        orders=orders,
        live_preflight_result=SimpleNamespace(decision="SELL_ONLY", reason="status_stale"),
        audit=None,
        log=None,
    )

    assert [(order.symbol, order.side, order.intent) for order in filtered] == [
        ("SOL/USDT", "sell", "CLOSE_LONG"),
    ]
