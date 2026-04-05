from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.emergency_close_all as emergency_close_all


def test_emergency_close_all_uses_project_root_for_config_env_and_report(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    fake_root = tmp_path / "repo"
    fake_reports = fake_root / "reports"
    fake_configs = fake_root / "configs"
    fake_reports.mkdir(parents=True)
    fake_configs.mkdir(parents=True)
    (fake_configs / "live_prod.yaml").write_text("exchange: {}\n", encoding="utf-8")
    (fake_root / ".env").write_text("EXAMPLE=1\n", encoding="utf-8")

    monkeypatch.setattr(emergency_close_all, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(emergency_close_all, "REPORTS_DIR", fake_reports)
    monkeypatch.setattr(emergency_close_all, "REPORT_PATH", fake_reports / "emergency_close_report.json")
    monkeypatch.chdir(tmp_path)

    class DummyExchange:
        def __init__(self) -> None:
            self._calls = 0
            captured["closed"] = False

        def fetch_balance(self):
            self._calls += 1
            return {"total": {"USDT": 100.0}}

        def close(self) -> None:
            captured["closed"] = True

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return SimpleNamespace(exchange=SimpleNamespace())

    monkeypatch.setattr(emergency_close_all, "load_config", _fake_load_config)
    monkeypatch.setattr(emergency_close_all, "_build_exchange", DummyExchange)
    monkeypatch.setattr(sys, "argv", ["emergency_close_all.py"])

    emergency_close_all.main()

    assert captured["config_path"] == (fake_root / "configs" / "live_prod.yaml").resolve()
    assert captured["env_path"] == (fake_root / ".env").resolve()
    assert captured["closed"] is True
    assert (fake_reports / "emergency_close_report.json").exists()
    assert not (tmp_path / "reports" / "emergency_close_report.json").exists()

    payload = json.loads((fake_reports / "emergency_close_report.json").read_text(encoding="utf-8"))
    assert payload["sold"] == []
    assert payload["skipped_dust"] == []
    assert payload["errors"] == []
    assert payload["final_usdt"] == 100.0
