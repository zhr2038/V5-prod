from __future__ import annotations

from pathlib import Path

from src.data.universe import okx_universe


def test_universe_provider_resolves_relative_cache_path_from_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(okx_universe, "PROJECT_ROOT", tmp_path)

    provider = okx_universe.OKXUniverseProvider(cache_path="reports/universe_cache.json")

    assert provider.cache_path == (tmp_path / "reports" / "universe_cache.json").resolve()


def test_universe_provider_keeps_absolute_cache_path(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(okx_universe, "PROJECT_ROOT", tmp_path)
    explicit = (tmp_path / "custom" / "universe.json").resolve()

    provider = okx_universe.OKXUniverseProvider(cache_path=str(explicit))

    assert provider.cache_path == explicit
