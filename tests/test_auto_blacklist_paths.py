from __future__ import annotations

import json

from src.utils import auto_blacklist


def test_auto_blacklist_add_symbol_anchors_default_path_to_workspace(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(elsewhere)
    monkeypatch.setattr(auto_blacklist, "PROJECT_ROOT", workspace.resolve())

    auto_blacklist.add_symbol("BTC/USDT", reason="unit-test", ttl_sec=None)

    path = workspace / "reports" / "auto_blacklist.json"
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["symbols"] == ["BTC/USDT"]
    assert payload["entries"][0]["symbol"] == "BTC/USDT"
    assert not (elsewhere / "reports" / "auto_blacklist.json").exists()


def test_auto_blacklist_read_symbols_anchors_default_path_to_workspace(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    elsewhere = tmp_path / "elsewhere"
    path = workspace / "reports" / "auto_blacklist.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "symbols": ["ETH/USDT"],
                "entries": [
                    {
                        "symbol": "ETH/USDT",
                        "reason": "unit-test",
                        "ts_ms": 1_710_000_000_000,
                        "expires_ts_ms": None,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    elsewhere.mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(elsewhere)
    monkeypatch.setattr(auto_blacklist, "PROJECT_ROOT", workspace.resolve())

    assert auto_blacklist.read_symbols() == ["ETH/USDT"]
    assert not (elsewhere / "reports" / "auto_blacklist.json").exists()
