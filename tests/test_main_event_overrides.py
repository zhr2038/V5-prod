from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import main as main_mod
from main import _merge_event_close_override_orders
from src.core.models import ExecutionReport
from src.core.models import Order
from src.execution.event_action_bridge import persist_event_actions
from src.execution.position_store import PositionStore


def test_event_close_override_appends_close_long(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    monkeypatch.chdir(tmp_path)

    db_path = reports_dir / "positions.sqlite"
    store = PositionStore(str(db_path))
    store.upsert_buy("MON/USDT", qty=100.0, px=1.0, now_ts="2026-04-03T15:00:00Z")

    persist_event_actions(
        actions=[{"symbol": "MON/USDT", "action": "close", "reason": "take_profit_5%", "priority": 0}],
        target_run_id="20260403_15",
        path=str(reports_dir / "event_driven_actions.json"),
    )

    merged = _merge_event_close_override_orders(
        orders=[],
        positions=store.list(),
        prices={"MON/USDT": 1.2},
        run_id="20260403_15",
        audit=None,
    )

    assert len(merged) == 1
    order = merged[0]
    assert isinstance(order, Order)
    assert order.symbol == "MON/USDT"
    assert order.side == "sell"
    assert order.intent == "CLOSE_LONG"
    assert order.notional_usdt == 120.0
    assert order.meta["source"] == "event_driven_override"


def test_event_close_override_does_not_duplicate_existing_close(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    monkeypatch.chdir(tmp_path)

    db_path = reports_dir / "positions.sqlite"
    store = PositionStore(str(db_path))
    store.upsert_buy("MON/USDT", qty=100.0, px=1.0, now_ts="2026-04-03T15:00:00Z")

    persist_event_actions(
        actions=[{"symbol": "MON/USDT", "action": "close", "reason": "take_profit_5%", "priority": 0}],
        target_run_id="20260403_15",
        path=str(reports_dir / "event_driven_actions.json"),
    )

    merged = _merge_event_close_override_orders(
        orders=[
            Order(
                symbol="MON/USDT",
                side="sell",
                intent="CLOSE_LONG",
                notional_usdt=120.0,
                signal_price=1.2,
                meta={},
            )
        ],
        positions=store.list(),
        prices={"MON/USDT": 1.2},
        run_id="20260403_15",
        audit=None,
    )

    assert len(merged) == 1


def test_main_reaches_market_data_validation_after_fetch(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    cfg = SimpleNamespace(
        symbols=["BTC/USDT"],
        timeframe_main="1H",
        universe=SimpleNamespace(
            enabled=False,
            use_universe_symbols=False,
        ),
    )

    class FakeAudit:
        def __init__(self, *args, **kwargs) -> None:
            self.universe_config = {}

        def add_note(self, *_args, **_kwargs) -> None:
            pass

    class FakeProvider:
        def fetch_ohlcv(self, symbols, timeframe, limit, end_ts_ms=None):
            assert symbols == ["BTC/USDT"]
            assert timeframe == "1H"
            assert limit == 24 * 60
            assert end_ts_ms is None
            return {"BTC/USDT": SimpleNamespace(ts=[1], close=[1.0], high=[1.0])}

    class ReachedValidation(RuntimeError):
        pass

    monkeypatch.setattr(main_mod, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(main_mod, "setup_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_mod, "build_provider", lambda _cfg: FakeProvider())
    monkeypatch.setattr(main_mod, "PositionStore", lambda path: SimpleNamespace(list=lambda: []))
    monkeypatch.setattr(main_mod, "AccountStore", lambda path: SimpleNamespace())

    import src.reporting.decision_audit as decision_audit_mod

    monkeypatch.setattr(decision_audit_mod, "DecisionAudit", FakeAudit)
    monkeypatch.setattr(
        main_mod,
        "_validate_market_data_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(ReachedValidation("called")),
    )

    with pytest.raises(ReachedValidation, match="called"):
        main_mod.main()


def test_main_market_data_validation_respects_zero_min_coverage_ratio(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    cfg = SimpleNamespace(
        symbols=["BTC/USDT"],
        timeframe_main="1H",
        universe=SimpleNamespace(
            enabled=False,
            use_universe_symbols=False,
            min_data_coverage_ratio=0.0,
        ),
    )

    class FakeAudit:
        def __init__(self, *args, **kwargs) -> None:
            self.universe_config = {}

        def add_note(self, *_args, **_kwargs) -> None:
            pass

    class FakeProvider:
        def fetch_ohlcv(self, symbols, timeframe, limit, end_ts_ms=None):
            assert symbols == ["BTC/USDT"]
            assert timeframe == "1H"
            assert limit == 24 * 60
            assert end_ts_ms is None
            return {"BTC/USDT": SimpleNamespace(ts=[1], close=[1.0], high=[1.0])}

    class ReachedValidation(RuntimeError):
        pass

    captured = {}

    def _capture_validate(*args, **kwargs):
        captured["min_coverage_ratio"] = kwargs["min_coverage_ratio"]
        raise ReachedValidation("called")

    monkeypatch.setattr(main_mod, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(main_mod, "setup_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_mod, "build_provider", lambda _cfg: FakeProvider())
    monkeypatch.setattr(main_mod, "PositionStore", lambda path: SimpleNamespace(list=lambda: []))
    monkeypatch.setattr(main_mod, "AccountStore", lambda path: SimpleNamespace())

    import src.reporting.decision_audit as decision_audit_mod

    monkeypatch.setattr(decision_audit_mod, "DecisionAudit", FakeAudit)
    monkeypatch.setattr(main_mod, "_validate_market_data_snapshot", _capture_validate)

    with pytest.raises(ReachedValidation, match="called"):
        main_mod.main()

    assert captured["min_coverage_ratio"] == 0.0


def test_main_order_arbitration_respects_zero_open_long_cooldown(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    cfg = SimpleNamespace(
        symbols=["BTC/USDT"],
        timeframe_main="1H",
        universe=SimpleNamespace(
            enabled=False,
            use_universe_symbols=False,
        ),
        budget=SimpleNamespace(
            live_equity_cap_usdt=None,
            action_enabled=False,
        ),
        execution=SimpleNamespace(
            order_state_machine_path=str(tmp_path / "reports" / "order_state_machine.json"),
            open_long_cooldown_minutes=0,
            mode="dry_run",
        ),
    )

    class FakeAudit:
        def __init__(self, *args, **kwargs) -> None:
            self.universe_config = {}
            self.counts = {}
            self.budget = {}

        def add_note(self, *_args, **_kwargs) -> None:
            pass

        def reject(self, *_args, **_kwargs) -> None:
            pass

        def save(self, *_args, **_kwargs) -> None:
            pass

    class FakeProvider:
        def fetch_ohlcv(self, symbols, timeframe, limit, end_ts_ms=None):
            assert symbols == ["BTC/USDT"]
            assert timeframe == "1H"
            assert limit == 24 * 60
            assert end_ts_ms is None
            return {"BTC/USDT": SimpleNamespace(close=[1.0], high=[1.0])}

    class FakePositionStore:
        def __init__(self, path: str) -> None:
            self.path = path

        def list(self):
            return []

        def mark_position(self, *args, **kwargs) -> None:
            pass

    class FakeAccountStore:
        def __init__(self, path: str) -> None:
            self.path = path
            self._acc = SimpleNamespace(cash_usdt=100.0, equity_peak_usdt=100.0)

        def get(self):
            return self._acc

        def set(self, acc) -> None:
            self._acc = acc

    class FakePipeline:
        def __init__(self, *_args, **_kwargs) -> None:
            self.regime_engine = SimpleNamespace(
                detect=lambda _btc: SimpleNamespace(
                    state=SimpleNamespace(value="SIDEWAYS"),
                    multiplier=1.0,
                    atr_pct=0.0,
                    ma20=0.0,
                    ma60=0.0,
                )
            )
            self.alpha_engine = SimpleNamespace(
                set_regime_context=lambda *_args, **_kwargs: None,
                compute_snapshot=lambda _market_data: SimpleNamespace(scores={}, ranks={}, raw={}),
            )

        def run(self, **_kwargs):
            return SimpleNamespace(
                alpha=SimpleNamespace(scores={}, ranks={}, raw={}),
                regime=SimpleNamespace(state=SimpleNamespace(value="SIDEWAYS"), multiplier=1.0),
                portfolio=SimpleNamespace(selected=[]),
                orders=[],
            )

    class FakeICMonitor:
        def update(self, **_kwargs):
            return None

    class StopAfterArbitration(RuntimeError):
        pass

    captured = {}

    def _fake_arbitrate_orders(*, orders, positions, run_id, cooldown_minutes, state_path):
        captured["orders"] = list(orders)
        captured["positions"] = list(positions)
        captured["run_id"] = run_id
        captured["cooldown_minutes"] = cooldown_minutes
        captured["state_path"] = state_path
        return list(orders), []

    class RaisingTradeLogWriter:
        def __init__(self, *args, **kwargs) -> None:
            raise StopAfterArbitration("captured")

    monkeypatch.setattr(main_mod, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(main_mod, "setup_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_mod, "build_provider", lambda _cfg: FakeProvider())
    monkeypatch.setattr(main_mod, "PositionStore", FakePositionStore)
    monkeypatch.setattr(main_mod, "AccountStore", FakeAccountStore)
    monkeypatch.setattr(main_mod, "_validate_market_data_snapshot", lambda **kwargs: (True, "ok", kwargs["market_data"]))
    monkeypatch.setattr(main_mod, "_merge_event_close_override_orders", lambda **kwargs: list(kwargs["orders"]))
    monkeypatch.setattr(main_mod, "get_live_equity_from_okx", lambda: 100.0)
    monkeypatch.setattr(main_mod, "ALPHA_HISTORY_ENABLED", False)

    import src.alpha.ic_monitor as ic_monitor_mod
    import src.core.pipeline as pipeline_mod
    import src.execution.order_arbitrator as order_arbitrator_mod
    import src.reporting.decision_audit as decision_audit_mod
    import src.reporting.trade_log as trade_log_mod

    monkeypatch.setattr(decision_audit_mod, "DecisionAudit", FakeAudit)
    monkeypatch.setattr(pipeline_mod, "V5Pipeline", FakePipeline)
    monkeypatch.setattr(ic_monitor_mod, "AlphaICMonitor", FakeICMonitor)
    monkeypatch.setattr(order_arbitrator_mod, "arbitrate_orders", _fake_arbitrate_orders)
    monkeypatch.setattr(trade_log_mod, "TradeLogWriter", RaisingTradeLogWriter)

    with pytest.raises(StopAfterArbitration, match="captured"):
        main_mod.main()

    assert captured["cooldown_minutes"] == 0
    assert captured["state_path"] == str(tmp_path / "reports" / "order_state_machine.json")


def test_main_writes_run_artifacts_into_runtime_reports_dir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("V5_RUN_ID", "shadow_run")
    monkeypatch.setenv("V5_WINDOW_START_TS", "1700000000")
    monkeypatch.setenv("V5_WINDOW_END_TS", "1700003600")

    cfg = SimpleNamespace(
        symbols=["BTC/USDT"],
        timeframe_main="1H",
        universe=SimpleNamespace(
            enabled=False,
            use_universe_symbols=False,
            min_data_coverage_ratio=0.0,
        ),
        budget=SimpleNamespace(
            live_equity_cap_usdt=None,
            action_enabled=False,
            turnover_budget_per_day=1_000_000.0,
            cost_budget_bps_per_day=10_000.0,
            min_trade_notional_base=5.0,
        ),
        execution=SimpleNamespace(
            order_store_path="reports/shadow_runtime/orders.sqlite",
            order_state_machine_path=str(tmp_path / "reports" / "shadow_runtime" / "order_state_machine.json"),
            mode="dry_run",
        ),
        exchange=SimpleNamespace(api_key="", api_secret="", passphrase=""),
    )

    class FakeProvider:
        def fetch_ohlcv(self, symbols, timeframe, limit, end_ts_ms=None):
            assert symbols == ["BTC/USDT"]
            return {"BTC/USDT": SimpleNamespace(ts=[1], close=[100.0], high=[101.0])}

        def fetch_top_of_book(self, symbols):
            assert symbols == ["BTC/USDT"]
            return {"BTC/USDT": {"bid": 99.0, "ask": 101.0}}

    class FakePositionStore:
        def __init__(self, path: str) -> None:
            self.path = path
            captured["position_store_path"] = path

        def list(self):
            return []

    class FakeAccountStore:
        def __init__(self, path: str) -> None:
            self.path = path
            captured["account_store_path"] = path
            self._acc = SimpleNamespace(cash_usdt=100.0, equity_peak_usdt=100.0)

        def get(self):
            return self._acc

        def set(self, acc) -> None:
            self._acc = acc

    class FakePipeline:
        def __init__(self, *_args, **_kwargs) -> None:
            self.regime_engine = SimpleNamespace(
                detect=lambda _btc: SimpleNamespace(
                    state=SimpleNamespace(value="SIDEWAYS"),
                    multiplier=1.0,
                    atr_pct=0.0,
                    ma20=0.0,
                    ma60=0.0,
                )
            )
            self.alpha_engine = SimpleNamespace(
                set_regime_context=lambda *_args, **_kwargs: None,
                compute_snapshot=lambda _market_data: SimpleNamespace(
                    scores={},
                    ranks={},
                    raw={},
                    raw_factors={},
                    z_factors={},
                    raw_scores={},
                    telemetry_scores={},
                    base_scores={},
                    base_raw_scores={},
                    ml_attribution_scores={},
                    ml_overlay_scores={},
                    ml_overlay_raw_scores={},
                    ml_runtime={},
                ),
            )

        def run(self, **_kwargs):
            alpha = self.alpha_engine.compute_snapshot({})
            regime = self.regime_engine.detect(None)
            portfolio = SimpleNamespace(selected=[])
            return SimpleNamespace(
                alpha=alpha,
                regime=regime,
                portfolio=portfolio,
                orders=[],
            )

    class FakeICMonitor:
        def update(self, **_kwargs):
            return None

    class FakeExecutionEngine:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def execute(self, orders):
            return ExecutionReport(timestamp="2026-04-08T00:00:00Z", dry_run=True, orders=list(orders), notes="")

    captured: dict[str, str] = {}

    def _fake_dump_run_artifacts(*, reports_dir, alpha, regime, portfolio, execution):
        captured["dump_reports_dir"] = str(reports_dir)

    def _fake_append_spread_snapshot(event, base_dir="reports/spread_snapshots"):
        captured["spread_base_dir"] = str(base_dir)
        return Path(base_dir) / "20260101.jsonl"

    monkeypatch.setattr(main_mod, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(main_mod, "setup_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_mod, "build_provider", lambda _cfg: FakeProvider())
    monkeypatch.setattr(main_mod, "PositionStore", FakePositionStore)
    monkeypatch.setattr(main_mod, "AccountStore", FakeAccountStore)
    monkeypatch.setattr(main_mod, "ExecutionEngine", FakeExecutionEngine)
    monkeypatch.setattr(main_mod, "_validate_market_data_snapshot", lambda **kwargs: (True, "ok", kwargs["market_data"]))
    monkeypatch.setattr(main_mod, "_merge_event_close_override_orders", lambda **kwargs: list(kwargs["orders"]))
    monkeypatch.setattr(main_mod, "dump_run_artifacts", _fake_dump_run_artifacts)
    monkeypatch.setattr(main_mod, "ALPHA_HISTORY_ENABLED", False)

    import src.alpha.ic_monitor as ic_monitor_mod
    import src.core.pipeline as pipeline_mod
    import src.execution.order_arbitrator as order_arbitrator_mod
    import src.reporting.spread_snapshots as spread_snapshots_mod

    monkeypatch.setattr(ic_monitor_mod, "AlphaICMonitor", FakeICMonitor)
    monkeypatch.setattr(pipeline_mod, "V5Pipeline", FakePipeline)
    monkeypatch.setattr(order_arbitrator_mod, "arbitrate_orders", lambda **kwargs: (list(kwargs["orders"]), []))
    monkeypatch.setattr(spread_snapshots_mod, "append_spread_snapshot", _fake_append_spread_snapshot)

    main_mod.main()

    shadow_run_dir = tmp_path / "reports" / "shadow_runtime" / "runs" / "shadow_run"
    root_run_dir = tmp_path / "reports" / "runs" / "shadow_run"

    assert (shadow_run_dir / "decision_audit.json").exists()
    assert (shadow_run_dir / "equity.jsonl").exists()
    assert (shadow_run_dir / "trades.csv").exists()
    assert (shadow_run_dir / "summary.json").exists()
    assert (shadow_run_dir / "spread_snapshot.json").exists()
    assert not root_run_dir.exists()
    assert Path(captured["dump_reports_dir"]).resolve() == (tmp_path / "reports" / "shadow_runtime").resolve()
    assert Path(captured["spread_base_dir"]).resolve() == (tmp_path / "reports" / "shadow_runtime" / "spread_snapshots").resolve()
    assert Path(captured["position_store_path"]).resolve() == (tmp_path / "reports" / "shadow_runtime" / "positions.sqlite").resolve()
    assert Path(captured["account_store_path"]).resolve() == (tmp_path / "reports" / "shadow_runtime" / "positions.sqlite").resolve()
