from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.fill_sync as fill_sync


def test_fill_sync_defaults_to_repo_reports_db(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    fake_root = tmp_path / "repo"
    fake_reports = fake_root / "reports"
    fake_configs = fake_root / "configs"
    fake_reports.mkdir(parents=True)
    fake_configs.mkdir(parents=True)
    (fake_configs / "live_prod.yaml").write_text("exchange: {}\n", encoding="utf-8")
    (fake_root / ".env").write_text("EXAMPLE=1\n", encoding="utf-8")

    monkeypatch.setattr(fill_sync, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(api_key="k", api_secret="s", passphrase="p"),
    )

    class DummyStore:
        def __init__(self, *, path: str) -> None:
            captured["db_path"] = Path(path).resolve()

        def count(self) -> int:
            return 0

        def get_state(self, key: str):
            return None

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            captured["client_kwargs"] = kwargs
            captured["closed"] = False

        def close(self) -> None:
            captured["closed"] = True

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return cfg

    def _fake_sync_once(*, store, client, limit: int, max_pages: int) -> int:
        captured["limit"] = limit
        captured["max_pages"] = max_pages
        return 0

    monkeypatch.setattr(fill_sync, "load_config", _fake_load_config)
    monkeypatch.setattr(fill_sync, "FillStore", DummyStore)
    monkeypatch.setattr(fill_sync, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(fill_sync, "sync_once", _fake_sync_once)
    monkeypatch.setattr(sys, "argv", ["fill_sync.py"])

    fill_sync.main()

    assert captured["config_path"] == (fake_root / "configs" / "live_prod.yaml").resolve()
    assert captured["env_path"] == (fake_root / ".env").resolve()
    assert captured["db_path"] == (fake_root / "reports" / "fills.sqlite").resolve()
    assert captured["client_kwargs"] == {"exchange": cfg.exchange}
    assert captured["limit"] == 100
    assert captured["max_pages"] == 20
    assert captured["closed"] is True
    assert not (tmp_path / "reports" / "fills.sqlite").exists()
