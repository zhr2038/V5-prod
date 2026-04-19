from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.universe_guard as universe_guard


def test_check_universe_reports_resolved_blacklist_path(tmp_path: Path, capsys) -> None:
    universe_path = tmp_path / "reports" / "universe_cache.json"
    blacklist_path = tmp_path / "configs" / "custom_blacklist.json"
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    universe_path.write_text(
        json.dumps({"symbols": ["PEPE/USDT", "BTC/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    ok = universe_guard.check_universe(universe_path=universe_path, blacklist_path=blacklist_path)

    output = capsys.readouterr().out
    assert ok is False
    assert str(blacklist_path.resolve()) in output


def test_check_universe_passes_clean_universe(tmp_path: Path, capsys) -> None:
    universe_path = tmp_path / "reports" / "universe_cache.json"
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    universe_path.write_text(
        json.dumps({"symbols": ["BTC/USDT", "ETH/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    ok = universe_guard.check_universe(universe_path=universe_path)

    output = capsys.readouterr().out
    assert ok is True
    assert "币池检查通过" in output


def test_build_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(universe_guard, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(universe_guard, "load_runtime_config", lambda project_root=None: {})

    with pytest.raises(ValueError, match="live_prod.yaml"):
        universe_guard.build_paths(tmp_path)
