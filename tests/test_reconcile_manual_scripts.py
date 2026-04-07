from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.reconcile_once as reconcile_once
import scripts.reconcile_with_retry as reconcile_with_retry


def test_reconcile_once_uses_config_defaults(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    out_path = tmp_path / "reconcile_status.json"
    positions_db = tmp_path / "positions.sqlite"
    expected_cfg = (Path(reconcile_once.__file__).resolve().parents[1] / "configs" / "live_prod.yaml").resolve()
    expected_env = (Path(reconcile_once.__file__).resolve().parents[1] / ".env").resolve()

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(reconcile_abs_usdt_tol=50.0, reconcile_dust_usdt_ignore=1.0),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class CapturingReconcileEngine:
        def __init__(self, **kwargs) -> None:
            captured["thresholds"] = kwargs["thresholds"]

        def reconcile(self, *, out_path: str):
            captured["out_path"] = out_path
            return {"ok": True, "reason": None, "stats": {}}

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return cfg

    monkeypatch.setattr(reconcile_once, "load_config", _fake_load_config)
    monkeypatch.setattr(reconcile_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(reconcile_once, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_once, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_once, "ReconcileEngine", CapturingReconcileEngine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "reconcile_once.py",
            "--out",
            str(out_path),
            "--positions-db",
            str(positions_db),
        ],
    )

    reconcile_once.main()

    assert captured["config_path"] == expected_cfg
    assert captured["env_path"] == expected_env
    assert captured["thresholds"].abs_usdt_tol == 50.0
    assert captured["thresholds"].dust_usdt_ignore == 1.0
    assert captured["out_path"] == str(out_path)


def test_reconcile_with_retry_respects_zero_config_thresholds(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    out_path = tmp_path / "reconcile_status.json"
    positions_db = tmp_path / "positions.sqlite"

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(reconcile_abs_usdt_tol=0.0, reconcile_dust_usdt_ignore=0.0),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class CapturingReconcileEngine:
        def __init__(self, **kwargs) -> None:
            captured["thresholds"] = kwargs["thresholds"]

        def reconcile(self, *, out_path: str):
            return {"ok": True, "reason": None, "stats": {}}

    monkeypatch.setattr(reconcile_with_retry, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(reconcile_with_retry, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(reconcile_with_retry, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_with_retry, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_with_retry, "ReconcileEngine", CapturingReconcileEngine)
    monkeypatch.setattr(reconcile_with_retry, "load_kill_switch", lambda path: {"enabled": False})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "reconcile_with_retry.py",
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

    reconcile_with_retry.main()

    assert captured["thresholds"].abs_usdt_tol == 0.0
    assert captured["thresholds"].dust_usdt_ignore == 0.0


def test_reconcile_once_defaults_runtime_paths_to_config_and_repo_root(monkeypatch) -> None:
    captured = {}
    workspace = Path(reconcile_once.__file__).resolve().parents[1]

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(
            reconcile_abs_usdt_tol=50.0,
            reconcile_dust_usdt_ignore=1.0,
            reconcile_status_path="reports/custom_reconcile_status.json",
        ),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class CapturingReconcileEngine:
        def __init__(self, **kwargs) -> None:
            captured["positions_db_path"] = Path(kwargs["position_store"].path).resolve()
            captured["account_db_path"] = Path(kwargs["account_store"].path).resolve()

        def reconcile(self, *, out_path: str):
            captured["out_path"] = Path(out_path).resolve()
            return {"ok": True, "reason": None, "stats": {}}

    monkeypatch.setattr(reconcile_once, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(reconcile_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(reconcile_once, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_once, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_once, "ReconcileEngine", CapturingReconcileEngine)
    monkeypatch.setattr(sys, "argv", ["reconcile_once.py"])

    reconcile_once.main()

    assert captured["positions_db_path"] == (workspace / "reports" / "positions.sqlite").resolve()
    assert captured["account_db_path"] == (workspace / "reports" / "positions.sqlite").resolve()
    assert captured["out_path"] == (workspace / "reports" / "custom_reconcile_status.json").resolve()


def test_reconcile_with_retry_defaults_runtime_paths_to_config_and_repo_root(monkeypatch) -> None:
    captured = {}
    workspace = Path(reconcile_with_retry.__file__).resolve().parents[1]

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(
            reconcile_abs_usdt_tol=50.0,
            reconcile_dust_usdt_ignore=1.0,
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
            captured["positions_db_path"] = Path(kwargs["position_store"].path).resolve()
            captured["account_db_path"] = Path(kwargs["account_store"].path).resolve()

        def reconcile(self, *, out_path: str):
            captured["out_path"] = Path(out_path).resolve()
            return {"ok": True, "reason": None, "stats": {}}

    def _fake_load_kill_switch(path: str) -> dict:
        captured["kill_switch_load_path"] = Path(path).resolve()
        return {"enabled": True}

    def _fake_disable_kill_switch(path: str) -> None:
        captured["kill_switch_disable_path"] = Path(path).resolve()

    monkeypatch.setattr(reconcile_with_retry, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(reconcile_with_retry, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(reconcile_with_retry, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_with_retry, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_with_retry, "ReconcileEngine", CapturingReconcileEngine)
    monkeypatch.setattr(reconcile_with_retry, "load_kill_switch", _fake_load_kill_switch)
    monkeypatch.setattr(reconcile_with_retry, "disable_kill_switch", _fake_disable_kill_switch)
    monkeypatch.setattr(sys, "argv", ["reconcile_with_retry.py"])

    reconcile_with_retry.main()

    expected_kill = (workspace / "reports" / "custom_kill_switch.json").resolve()
    assert captured["positions_db_path"] == (workspace / "reports" / "positions.sqlite").resolve()
    assert captured["account_db_path"] == (workspace / "reports" / "positions.sqlite").resolve()
    assert captured["out_path"] == (workspace / "reports" / "custom_reconcile_status.json").resolve()
    assert captured["kill_switch_load_path"] == expected_kill
    assert captured["kill_switch_disable_path"] == expected_kill


def test_reconcile_with_retry_keeps_manual_kill_switch_enabled(monkeypatch) -> None:
    captured = {}

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(
            reconcile_abs_usdt_tol=50.0,
            reconcile_dust_usdt_ignore=1.0,
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
            pass

        def reconcile(self, *, out_path: str):
            return {"ok": True, "reason": None, "stats": {}}

    def _fake_disable_kill_switch(path: str) -> None:
        captured["kill_switch_disable_path"] = path

    monkeypatch.setattr(reconcile_with_retry, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(reconcile_with_retry, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(reconcile_with_retry, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_with_retry, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(reconcile_with_retry, "ReconcileEngine", CapturingReconcileEngine)
    monkeypatch.setattr(
        reconcile_with_retry,
        "load_kill_switch",
        lambda path: {"enabled": True, "manual": True, "trigger": "manual"},
    )
    monkeypatch.setattr(reconcile_with_retry, "disable_kill_switch", _fake_disable_kill_switch)
    monkeypatch.setattr(sys, "argv", ["reconcile_with_retry.py"])

    reconcile_with_retry.main()

    assert "kill_switch_disable_path" not in captured
