from __future__ import annotations

from pathlib import Path

from src.monitoring import smart_alert as smart_alert_module


def test_resolve_paths_uses_prefixed_runtime_alert_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        smart_alert_module,
        "_load_active_config",
        lambda workspace: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        smart_alert_module,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = smart_alert_module._resolve_paths(workspace=tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "shadow_orders.sqlite")
    assert paths.alerts_state_file == (tmp_path / "reports" / "shadow_alerts_state.json").resolve()
    assert paths.ic_file == (tmp_path / "reports" / "shadow_ic_diagnostics_30d_20u.json").resolve()


def test_resolve_paths_uses_suffixed_runtime_alert_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        smart_alert_module,
        "_load_active_config",
        lambda workspace: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        smart_alert_module,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    paths = smart_alert_module._resolve_paths(workspace=tmp_path)

    assert paths.orders_db == (tmp_path / "reports" / "orders_accelerated.sqlite")
    assert paths.alerts_state_file == (tmp_path / "reports" / "alerts_state_accelerated.json").resolve()
    assert paths.ic_file == (tmp_path / "reports" / "ic_diagnostics_30d_20u_accelerated.json").resolve()


def test_check_signal_no_trade_ignores_exit_only_rounds(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit: [
        {"counts": {"selected": 2, "orders_rebalance": 0, "orders_exit": 1}},
        {"counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 2}},
    ]
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    assert engine.check_signal_no_trade() is None


def test_check_signal_no_trade_alerts_when_selected_without_any_orders(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit: [
        {"counts": {"selected": 2, "orders_rebalance": 0, "orders_exit": 0}},
        {"counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 0}},
    ]
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_signal_no_trade()

    assert alert is not None
    assert alert["type"] == "signal_no_trade"


def test_check_signal_no_trade_ignores_known_policy_blockers(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit: [
        {
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 0,
                "negative_expectancy_open_block": 2,
            },
            "router_decisions": [],
        },
        {
            "counts": {
                "selected": 1,
                "orders_rebalance": 0,
                "orders_exit": 0,
            },
            "router_decisions": [{"reason": "deadband"}],
        },
    ]
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    assert engine.check_signal_no_trade() is None


def test_check_signal_no_trade_alerts_when_blockers_only_cover_subset(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit: [
        {
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 0,
                "negative_expectancy_open_block": 1,
            },
            "router_decisions": [],
        },
        {
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 0,
            },
            "router_decisions": [{"reason": "deadband", "symbol": "BTC/USDT"}],
        },
    ]
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_signal_no_trade()

    assert alert is not None
    assert alert["type"] == "signal_no_trade"


def test_check_no_buy_in_market_ignores_known_policy_blockers(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit: [
        {
            "regime": "TRENDING",
            "counts": {
                "selected": 1,
                "orders_rebalance": 0,
                "orders_exit": 0,
                "negative_expectancy_cooldown": 1,
            },
            "router_decisions": [],
        }
    ]
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    assert engine.check_no_buy_in_market() is None


def test_check_no_buy_in_market_alerts_for_unblocked_signals(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit: [
        {
            "regime": "SIDEWAYS",
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 0,
            },
            "router_decisions": [],
        }
    ]
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_no_buy_in_market()

    assert alert is not None
    assert alert["type"] == "no_buy_in_market"


def test_check_no_buy_in_market_does_not_treat_score_penalty_as_blocker(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit: [
        {
            "regime": "TRENDING",
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 0,
                "negative_expectancy_score_penalty": 2,
            },
            "router_decisions": [],
        }
    ]
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_no_buy_in_market()

    assert alert is not None
    assert alert["type"] == "no_buy_in_market"


def test_check_no_buy_in_market_alerts_when_blockers_only_cover_subset(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit: [
        {
            "regime": "TRENDING",
            "counts": {
                "selected": 3,
                "orders_rebalance": 0,
                "orders_exit": 0,
                "negative_expectancy_cooldown": 1,
            },
            "router_decisions": [{"reason": "deadband", "symbol": "BTC/USDT"}],
        }
    ]
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_no_buy_in_market()

    assert alert is not None
    assert alert["type"] == "no_buy_in_market"
