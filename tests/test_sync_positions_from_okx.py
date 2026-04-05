from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import scripts.sync_positions_from_okx as sync_positions_from_okx


def test_sync_positions_uses_project_root_for_config_env_and_reports(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    fake_root = tmp_path / "repo"
    fake_reports = fake_root / "reports"
    fake_configs = fake_root / "configs"
    fake_reports.mkdir(parents=True)
    fake_configs.mkdir(parents=True)
    (fake_configs / "live_prod.yaml").write_text("exchange: {}\n", encoding="utf-8")
    (fake_root / ".env").write_text("EXAMPLE=1\n", encoding="utf-8")

    monkeypatch.setattr(sync_positions_from_okx, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(sync_positions_from_okx, "REPORTS_DIR", fake_reports)
    monkeypatch.setattr(sync_positions_from_okx, "POSITIONS_DB", fake_reports / "positions.sqlite")
    monkeypatch.setattr(sync_positions_from_okx, "EQUITY_FILE", fake_reports / "equity_validation.json")
    monkeypatch.setenv("V5_LIVE_ARM", "YES")
    monkeypatch.chdir(tmp_path)

    cfg = SimpleNamespace(exchange=SimpleNamespace())

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            captured["client_kwargs"] = kwargs
            captured["closed"] = False

        def get_balance(self):
            return SimpleNamespace(
                data={
                    "data": [
                        {
                            "totalEq": "100.0",
                            "details": [
                                {"ccy": "USDT", "eq": "100.0", "availBal": "100.0", "liab": "0"},
                            ],
                        }
                    ]
                }
            )

        def close(self) -> None:
            captured["closed"] = True

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return cfg

    monkeypatch.setattr(sync_positions_from_okx, "load_config", _fake_load_config)
    monkeypatch.setattr(sync_positions_from_okx, "OKXPrivateClient", DummyClient)

    result = sync_positions_from_okx.sync_positions()

    assert captured["config_path"] == (fake_root / "configs" / "live_prod.yaml").resolve()
    assert captured["env_path"] == (fake_root / ".env").resolve()
    assert captured["client_kwargs"] == {"exchange": cfg.exchange}
    assert captured["closed"] is True
    assert result is not None
    assert result["positions_count"] == 0
    assert (fake_reports / "positions.sqlite").exists()
    assert (fake_reports / "equity_validation.json").exists()
    assert not (tmp_path / "reports" / "positions.sqlite").exists()

    conn = sqlite3.connect(str(fake_reports / "positions.sqlite"))
    try:
        count = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    finally:
        conn.close()
    assert count == 0
