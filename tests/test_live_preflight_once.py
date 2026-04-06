from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.live_preflight_once as live_preflight_once


def test_live_preflight_once_defaults_to_live_prod_config(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    expected_cfg = (Path(live_preflight_once.__file__).resolve().parents[1] / "configs" / "live_prod.yaml").resolve()
    expected_env = (Path(live_preflight_once.__file__).resolve().parents[1] / ".env").resolve()

    cfg = SimpleNamespace(exchange=SimpleNamespace(), execution=SimpleNamespace())

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class DummyPreflight:
        def __init__(self, execution_cfg, **kwargs) -> None:
            captured["execution_cfg"] = execution_cfg

        def run(self, *, max_pages: int, max_status_age_sec: int):
            captured["max_pages"] = max_pages
            captured["max_status_age_sec"] = max_status_age_sec
            return SimpleNamespace(decision="ALLOW", reconcile_ok=True, ledger_ok=True, kill_switch_enabled=False)

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return cfg

    monkeypatch.setattr(live_preflight_once, "load_config", _fake_load_config)
    monkeypatch.setattr(live_preflight_once, "PositionStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(live_preflight_once, "AccountStore", lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(live_preflight_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(live_preflight_once, "LivePreflight", DummyPreflight)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "live_preflight_once.py",
            "--positions-db",
            str(tmp_path / "positions.sqlite"),
            "--bills-db",
            str(tmp_path / "bills.sqlite"),
        ],
    )

    live_preflight_once.main()

    assert captured["config_path"] == expected_cfg
    assert captured["env_path"] == expected_env
    assert captured["max_pages"] == 5
    assert captured["max_status_age_sec"] == 180


def test_live_preflight_once_defaults_runtime_paths_to_repo_root(monkeypatch) -> None:
    captured = {}
    workspace = Path(live_preflight_once.__file__).resolve().parents[1]

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(
            reconcile_status_path="reports/custom_reconcile_status.json",
            kill_switch_path="reports/custom_kill_switch.json",
        ),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    class DummyPreflight:
        def __init__(self, execution_cfg, **kwargs) -> None:
            captured["execution_cfg"] = execution_cfg
            captured["kwargs"] = kwargs

        def run(self, *, max_pages: int, max_status_age_sec: int):
            return SimpleNamespace(decision="ALLOW", reconcile_ok=True, ledger_ok=True, kill_switch_enabled=False)

    def _fake_position_store(path: str):
        captured["positions_db_path"] = Path(path).resolve()
        return SimpleNamespace(path=path)

    def _fake_account_store(path: str):
        captured["account_db_path"] = Path(path).resolve()
        return SimpleNamespace(path=path)

    monkeypatch.setattr(live_preflight_once, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(live_preflight_once, "PositionStore", _fake_position_store)
    monkeypatch.setattr(live_preflight_once, "AccountStore", _fake_account_store)
    monkeypatch.setattr(live_preflight_once, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(live_preflight_once, "LivePreflight", DummyPreflight)
    monkeypatch.setattr(sys, "argv", ["live_preflight_once.py"])

    live_preflight_once.main()

    assert captured["positions_db_path"] == (workspace / "reports" / "positions.sqlite").resolve()
    assert captured["account_db_path"] == (workspace / "reports" / "positions.sqlite").resolve()
    assert Path(captured["kwargs"]["bills_db_path"]).resolve() == (workspace / "reports" / "bills.sqlite").resolve()
    assert Path(captured["kwargs"]["ledger_state_path"]).resolve() == (workspace / "reports" / "ledger_state.json").resolve()
    assert Path(captured["kwargs"]["ledger_status_path"]).resolve() == (workspace / "reports" / "ledger_status.json").resolve()
    assert Path(captured["kwargs"]["reconcile_status_path"]).resolve() == (
        workspace / "reports" / "custom_reconcile_status.json"
    ).resolve()
    assert Path(captured["execution_cfg"].kill_switch_path).resolve() == (
        workspace / "reports" / "custom_kill_switch.json"
    ).resolve()
