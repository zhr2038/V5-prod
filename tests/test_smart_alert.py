from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.monitoring import smart_alert as smart_alert_module


@pytest.fixture(autouse=True)
def _runtime_config(monkeypatch, tmp_path: Path) -> Path:
    config_path = tmp_path / "configs" / "live_prod.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/orders.sqlite\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        smart_alert_module,
        "resolve_runtime_config_path",
        lambda project_root=None: str(config_path),
    )
    return config_path


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


def test_load_active_config_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = tmp_path / "configs" / "missing.yaml"
    monkeypatch.setattr(
        smart_alert_module,
        "resolve_runtime_config_path",
        lambda project_root=None: str(missing),
    )

    try:
        smart_alert_module._load_active_config(workspace=tmp_path)
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_check_signal_no_trade_ignores_exit_only_rounds(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {"counts": {"selected": 2, "orders_rebalance": 0, "orders_exit": 1}},
        {"counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 2}},
    ]
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    assert engine.check_signal_no_trade() is None


def test_load_recent_run_audits_ignores_stale_runs(tmp_path: Path) -> None:
    runs_dir = tmp_path / "reports" / "runs"
    fresh = runs_dir / "fresh_run"
    stale = runs_dir / "stale_run"
    fresh.mkdir(parents=True, exist_ok=True)
    stale.mkdir(parents=True, exist_ok=True)
    (fresh / "decision_audit.json").write_text('{"counts": {}}', encoding="utf-8")
    (stale / "decision_audit.json").write_text('{"counts": {}}', encoding="utf-8")

    now = datetime.now().timestamp()
    fresh_ts = now - 1800
    stale_ts = (datetime.now() - timedelta(hours=8)).timestamp()
    import os
    os.utime(fresh / "decision_audit.json", (fresh_ts, fresh_ts))
    os.utime(stale / "decision_audit.json", (stale_ts, stale_ts))
    os.utime(fresh, (stale_ts, stale_ts))
    os.utime(stale, (fresh_ts + 500, fresh_ts + 500))

    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    audits = engine._load_recent_run_audits(limit=5, max_age_hours=6)

    assert len(audits) == 1


def test_check_signal_no_trade_alerts_when_selected_without_any_orders(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {
            "counts": {"selected": 2, "orders_rebalance": 0, "orders_exit": 0},
            "router_decisions": [{"symbol": "BTC/USDT", "action": "skip", "reason": "insufficient_cash"}],
        },
        {
            "counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 0},
            "router_decisions": [{"symbol": "ETH/USDT", "action": "skip", "reason": "cost_aware_edge"}],
        },
    ]
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_signal_no_trade()

    assert alert is not None
    assert alert["type"] == "signal_no_trade"


def test_check_signal_no_trade_ignores_stale_runs(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: []
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    assert engine.check_signal_no_trade() is None


def test_load_recent_run_audits_prefers_run_id_epoch_when_file_mtime_is_misleading(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    runs_dir = engine.paths.runs_dir
    runs_dir.mkdir(parents=True, exist_ok=True)

    now = smart_alert_module.datetime.now()
    older_run_name = (now - smart_alert_module.timedelta(hours=1)).strftime("%Y%m%d_%H")
    newer_run_name = now.strftime("%Y%m%d_%H")
    older_run = runs_dir / older_run_name
    newer_run = runs_dir / newer_run_name
    older_run.mkdir()
    newer_run.mkdir()

    older_audit = older_run / "decision_audit.json"
    newer_audit = newer_run / "decision_audit.json"
    older_audit.write_text(json.dumps({"run_id": older_run_name, "counts": {"selected": 1}}), encoding="utf-8")
    newer_audit.write_text(json.dumps({"run_id": newer_run_name, "counts": {"selected": 2}}), encoding="utf-8")

    now_ts = smart_alert_module.datetime.now().timestamp()
    os.utime(older_audit, (now_ts - 100, now_ts - 100))
    os.utime(newer_audit, (now_ts - 13 * 3600, now_ts - 13 * 3600))

    audits = engine._load_recent_run_audits(limit=2, max_age_hours=6)

    assert len(audits) == 2
    assert audits[0]["run_id"] == newer_run_name


def test_check_signal_no_trade_ignores_known_policy_blockers(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
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
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 0,
                "negative_expectancy_open_block": 1,
            },
            "router_decisions": [{"symbol": "ETH/USDT", "action": "skip", "reason": "insufficient_cash"}],
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


def test_check_signal_no_trade_ignores_runs_without_router_evidence(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {"counts": {"selected": 2, "orders_rebalance": 0, "orders_exit": 0}, "router_decisions": []},
        {"counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 0}, "router_decisions": []},
    ]
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    assert engine.check_signal_no_trade() is None


def test_check_no_buy_in_market_ignores_known_policy_blockers(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
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


def test_check_no_buy_in_market_ignores_exit_only_rounds(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {
            "regime": "TRENDING",
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 1,
            },
            "router_decisions": [],
        }
    ]
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    assert engine.check_no_buy_in_market() is None


def test_check_no_buy_in_market_alerts_for_unblocked_signals(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {
            "regime": "SIDEWAYS",
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 0,
            },
            "router_decisions": [{"symbol": "BTC/USDT", "action": "skip", "reason": "insufficient_cash"}],
        }
    ]
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_no_buy_in_market()

    assert alert is not None
    assert alert["type"] == "no_buy_in_market"


def test_check_no_buy_in_market_ignores_stale_runs(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: []
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    assert engine.check_no_buy_in_market() is None


def test_check_no_buy_in_market_does_not_treat_score_penalty_as_blocker(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {
            "regime": "TRENDING",
            "counts": {
                "selected": 2,
                "orders_rebalance": 0,
                "orders_exit": 0,
                "negative_expectancy_score_penalty": 2,
            },
            "router_decisions": [{"symbol": "BTC/USDT", "action": "skip", "reason": "insufficient_cash"}],
        }
    ]
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_no_buy_in_market()

    assert alert is not None
    assert alert["type"] == "no_buy_in_market"


def test_check_no_buy_in_market_alerts_when_blockers_only_cover_subset(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {
            "regime": "TRENDING",
            "counts": {
                "selected": 3,
                "orders_rebalance": 0,
                "orders_exit": 0,
                "negative_expectancy_cooldown": 1,
            },
            "router_decisions": [
                {"reason": "deadband", "symbol": "BTC/USDT"},
                {"reason": "insufficient_cash", "symbol": "ETH/USDT"},
            ],
        }
    ]
    engine._count_recent_buy_fills = lambda hours=6: 0
    engine._should_alert = lambda alert_type, cooldown_minutes=60: True

    alert = engine.check_no_buy_in_market()

    assert alert is not None
    assert alert["type"] == "no_buy_in_market"


def test_check_no_buy_in_market_ignores_runs_without_router_evidence(tmp_path: Path) -> None:
    engine = smart_alert_module.SmartAlertEngine(workspace=tmp_path)
    engine._load_recent_run_audits = lambda limit, max_age_hours=None: [
        {
            "regime": "TRENDING",
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

    assert engine.check_no_buy_in_market() is None
