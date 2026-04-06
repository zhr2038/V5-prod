from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.reconcile_guard_once as reconcile_guard_once


def test_reconcile_guard_once_uses_schema_default_dust_ignore(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    out_path = tmp_path / "reconcile_status.json"
    positions_db = tmp_path / "positions.sqlite"
    expected_cfg = (Path(reconcile_guard_once.__file__).resolve().parents[1] / "configs" / "live_prod.yaml").resolve()
    expected_env = (Path(reconcile_guard_once.__file__).resolve().parents[1] / ".env").resolve()

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        symbols=["BTC/USDT"],
        execution=SimpleNamespace(reconcile_ccy_mode="universe_only"),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            captured["client_exchange"] = kwargs.get("exchange")

        def close(self) -> None:
            pass

    class CapturingReconcileEngine:
        def __init__(self, **kwargs) -> None:
            captured["thresholds"] = kwargs["thresholds"]

        def reconcile(self, *, out_path: str, universe_bases, ccy_mode: str):
            captured["universe_bases"] = list(universe_bases)
            captured["ccy_mode"] = ccy_mode
            return {"ts_ms": 1000, "ok": True, "reason": None}

    class DummyGuard:
        def __init__(self, cfg) -> None:
            captured["guard_cfg"] = cfg

        def apply(self):
            return {
                "ok": True,
                "reason": "ok",
                "category": "OK",
                "failure_state": {"consecutive_hard": 0, "consecutive_soft": 0},
                "kill_switch": {"enabled": False},
            }

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return cfg

    monkeypatch.setattr(reconcile_guard_once, "load_config", _fake_load_config)
    monkeypatch.setattr(reconcile_guard_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(reconcile_guard_once, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_guard_once, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_guard_once, "ReconcileEngine", CapturingReconcileEngine)
    monkeypatch.setattr(reconcile_guard_once, "KillSwitchGuard", DummyGuard)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "reconcile_guard_once.py",
            "--out",
            str(out_path),
            "--positions-db",
            str(positions_db),
        ],
    )

    reconcile_guard_once.main()

    assert captured["config_path"] == expected_cfg
    assert captured["env_path"] == expected_env
    assert captured["thresholds"].abs_usdt_tol == 50.0
    assert captured["thresholds"].dust_usdt_ignore == 1.0
    assert captured["universe_bases"] == ["BTC"]
    assert captured["ccy_mode"] == "universe_only"
    assert out_path.exists()


def test_reconcile_guard_once_respects_zero_abs_usdt_tol_from_config(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    out_path = tmp_path / "reconcile_status.json"
    positions_db = tmp_path / "positions.sqlite"

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        symbols=["BTC/USDT"],
        execution=SimpleNamespace(reconcile_ccy_mode="universe_only", reconcile_abs_usdt_tol=0.0),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class CapturingReconcileEngine:
        def __init__(self, **kwargs) -> None:
            captured["thresholds"] = kwargs["thresholds"]

        def reconcile(self, *, out_path: str, universe_bases, ccy_mode: str):
            return {"ts_ms": 1000, "ok": True, "reason": None}

    class DummyGuard:
        def __init__(self, cfg) -> None:
            pass

        def apply(self):
            return {
                "ok": True,
                "reason": "ok",
                "category": "OK",
                "failure_state": {"consecutive_hard": 0, "consecutive_soft": 0},
                "kill_switch": {"enabled": False},
            }

    monkeypatch.setattr(reconcile_guard_once, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(reconcile_guard_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(reconcile_guard_once, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_guard_once, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_guard_once, "ReconcileEngine", CapturingReconcileEngine)
    monkeypatch.setattr(reconcile_guard_once, "KillSwitchGuard", DummyGuard)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "reconcile_guard_once.py",
            "--config",
            "configs/live_prod.yaml",
            "--env",
            ".env",
            "--out",
            str(out_path),
            "--positions-db",
            str(positions_db),
        ],
    )

    reconcile_guard_once.main()

    assert captured["thresholds"].abs_usdt_tol == 0.0


def test_reconcile_guard_once_defaults_runtime_paths_to_config_and_repo_root(monkeypatch) -> None:
    captured = {}
    workspace = Path(reconcile_guard_once.__file__).resolve().parents[1]

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        symbols=["BTC/USDT"],
        execution=SimpleNamespace(
            reconcile_ccy_mode="universe_only",
            reconcile_status_path="reports/custom_reconcile_status.json",
            kill_switch_path="reports/custom_kill_switch.json",
        ),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class CapturingReconcileEngine:
        def __init__(self, **kwargs) -> None:
            captured["position_store_path"] = Path(kwargs["position_store"].path).resolve()
            captured["account_store_path"] = Path(kwargs["account_store"].path).resolve()

        def reconcile(self, *, out_path: str, universe_bases, ccy_mode: str):
            captured["out_path"] = Path(out_path).resolve()
            return {"ts_ms": 1000, "ok": True, "reason": None}

    class DummyGuard:
        def __init__(self, cfg) -> None:
            captured["guard_cfg"] = cfg

        def apply(self):
            return {
                "ok": True,
                "reason": "ok",
                "category": "OK",
                "failure_state": {"consecutive_hard": 0, "consecutive_soft": 0},
                "kill_switch": {"enabled": False},
            }

    monkeypatch.setattr(reconcile_guard_once, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(reconcile_guard_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(reconcile_guard_once, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_guard_once, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_guard_once, "ReconcileEngine", CapturingReconcileEngine)
    monkeypatch.setattr(reconcile_guard_once, "KillSwitchGuard", DummyGuard)
    monkeypatch.setattr(sys, "argv", ["reconcile_guard_once.py"])

    reconcile_guard_once.main()

    assert captured["position_store_path"] == (workspace / "reports" / "positions.sqlite").resolve()
    assert captured["account_store_path"] == (workspace / "reports" / "positions.sqlite").resolve()
    assert captured["out_path"] == (workspace / "reports" / "custom_reconcile_status.json").resolve()
    assert Path(captured["guard_cfg"].reconcile_status_path).resolve() == (
        workspace / "reports" / "custom_reconcile_status.json"
    ).resolve()
    assert Path(captured["guard_cfg"].failure_state_path).resolve() == (
        workspace / "reports" / "reconcile_failure_state.json"
    ).resolve()
    assert Path(captured["guard_cfg"].kill_switch_path).resolve() == (
        workspace / "reports" / "custom_kill_switch.json"
    ).resolve()
