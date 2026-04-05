from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.reconcile_guard_once as reconcile_guard_once


def test_reconcile_guard_once_uses_schema_default_dust_ignore(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    out_path = tmp_path / "reconcile_status.json"
    positions_db = tmp_path / "positions.sqlite"

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

    assert captured["thresholds"].dust_usdt_ignore == 1.0
    assert captured["universe_bases"] == ["BTC"]
    assert captured["ccy_mode"] == "universe_only"
    assert out_path.exists()
