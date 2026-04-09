from __future__ import annotations

import json
from pathlib import Path

import scripts.universe_guard as universe_guard


def test_build_paths_anchors_universe_guard_to_repo_root(tmp_path: Path) -> None:
    paths = universe_guard.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports"
    assert paths.configs_dir == tmp_path / "configs"
    assert paths.universe_path == tmp_path / "reports" / "universe_cache.json"
    assert paths.blacklist_path == tmp_path / "configs" / "blacklist.json"


def test_check_universe_uses_workspace_reports_file(monkeypatch, tmp_path: Path, capsys) -> None:
    fake_root = tmp_path / "repo"
    reports_dir = fake_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "universe_cache.json").write_text(
        json.dumps({"symbols": ["BTC/USDT", "PEPE/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(universe_guard, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)

    ok = universe_guard.check_universe()

    output = capsys.readouterr().out
    assert ok is False
    assert "PEPE/USDT" in output
    assert not (tmp_path / "reports" / "universe_cache.json").exists()


def test_auto_blacklist_suspicious_writes_workspace_blacklist(monkeypatch, tmp_path: Path, capsys) -> None:
    fake_root = tmp_path / "repo"
    reports_dir = fake_root / "reports"
    configs_dir = fake_root / "configs"
    reports_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "universe_cache.json").write_text(
        json.dumps({"symbols": ["BTC/USDT", "MERL/USDT", "DOGE/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (configs_dir / "blacklist.json").write_text(
        json.dumps({"symbols": ["BTC/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(universe_guard, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)

    added = universe_guard.auto_blacklist_suspicious()

    assert added == ["MERL/USDT", "DOGE/USDT"]
    payload = json.loads((configs_dir / "blacklist.json").read_text(encoding="utf-8"))
    assert payload["symbols"] == ["BTC/USDT", "MERL/USDT", "DOGE/USDT"]
    assert not (tmp_path / "configs" / "blacklist.json").exists()
    assert "MERL/USDT" in capsys.readouterr().out


def test_build_paths_uses_active_universe_config_paths(tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    configs_dir = fake_root / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "universe:",
                "  cache_path: reports/shadow_runtime/universe_cache.json",
                "  blacklist_path: configs/shadow_blacklist.json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    paths = universe_guard.build_paths(fake_root)

    assert paths.reports_dir == (fake_root / "reports" / "shadow_runtime").resolve()
    assert paths.configs_dir == (fake_root / "configs").resolve()
    assert paths.universe_path == (fake_root / "reports" / "shadow_runtime" / "universe_cache.json").resolve()
    assert paths.blacklist_path == (fake_root / "configs" / "shadow_blacklist.json").resolve()


def test_universe_guard_uses_active_runtime_universe_and_blacklist_paths(monkeypatch, tmp_path: Path, capsys) -> None:
    fake_root = tmp_path / "repo"
    reports_dir = fake_root / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    configs_dir = fake_root / "configs"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)

    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "universe:",
                "  cache_path: reports/shadow_runtime/universe_cache.json",
                "  blacklist_path: configs/shadow_blacklist.json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (reports_dir / "universe_cache.json").write_text(
        json.dumps({"symbols": ["BTC/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (runtime_dir / "universe_cache.json").write_text(
        json.dumps({"symbols": ["BTC/USDT", "PEPE/USDT", "MERL/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (configs_dir / "shadow_blacklist.json").write_text(
        json.dumps({"symbols": ["BTC/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(universe_guard, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)

    ok = universe_guard.check_universe()
    added = universe_guard.auto_blacklist_suspicious()

    output = capsys.readouterr().out
    payload = json.loads((configs_dir / "shadow_blacklist.json").read_text(encoding="utf-8"))

    assert ok is False
    assert "PEPE/USDT" in output
    assert added == ["PEPE/USDT", "MERL/USDT"]
    assert payload["symbols"] == ["BTC/USDT", "PEPE/USDT", "MERL/USDT"]
    assert not (configs_dir / "blacklist.json").exists()
