from __future__ import annotations

import sqlite3

import scripts.collect_market_data as collect_market_data


def test_resolve_runtime_inputs_uses_runtime_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(collect_market_data, "PROJECT_ROOT", tmp_path)
    config_path, env_path, db_path = collect_market_data._resolve_runtime_inputs(
        "configs/runtime.yaml",
        ".env.runtime",
        "reports/custom_alpha.db",
    )
    assert config_path == str((tmp_path / "configs" / "runtime.yaml").resolve())
    assert env_path == str((tmp_path / ".env.runtime").resolve())
    assert db_path == str((tmp_path / "reports" / "custom_alpha.db").resolve())


def test_load_seed_symbols_falls_back_when_alpha_snapshots_missing(tmp_path):
    db_path = tmp_path / "alpha_history.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE market_data_1h(symbol TEXT)")
    conn.commit()
    conn.close()

    assert collect_market_data._load_seed_symbols(str(db_path)) == collect_market_data.DEFAULT_SYMBOLS


def test_main_passes_runtime_paths_to_loader(monkeypatch, tmp_path):
    monkeypatch.setenv("V5_LIVE_ARM", "YES")
    monkeypatch.setattr(collect_market_data, "PROJECT_ROOT", tmp_path)

    captured = {}

    def _fake_load_config(config_path, env_path=None):
        captured["config_path"] = config_path
        captured["env_path"] = env_path
        return {}

    class _FakeProvider:
        def __init__(self, cfg):
            self.ccxt = object()

    monkeypatch.setattr(collect_market_data, "load_config", _fake_load_config)
    monkeypatch.setattr(collect_market_data, "OKXCCXTProvider", _FakeProvider)
    monkeypatch.setattr(collect_market_data, "fetch_historical_data", lambda *args, **kwargs: [])

    db_path = tmp_path / "reports" / "alpha_history.db"

    collect_market_data.main(["--config", "configs/runtime.yaml", "--env", ".env.runtime"])

    assert captured == {
        "config_path": str((tmp_path / "configs" / "runtime.yaml").resolve()),
        "env_path": str((tmp_path / ".env.runtime").resolve()),
    }
    assert db_path.exists()
