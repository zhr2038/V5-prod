from __future__ import annotations

import json
from pathlib import Path

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
