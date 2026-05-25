from __future__ import annotations

from pathlib import Path

import pytest

import scripts.sync_positions_from_okx as sync_positions


def test_format_follow_up_config_path_returns_runtime_relative_path(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "shadow_live.yaml").resolve()
    monkeypatch.setattr(sync_positions, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        sync_positions,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected),
    )

    path = sync_positions._format_follow_up_config_path(None)

    assert path == "configs/shadow_live.yaml"


def test_format_follow_up_config_path_keeps_absolute_outside_project(monkeypatch, tmp_path: Path) -> None:
    external = (tmp_path.parent / "external_live.yaml").resolve()
    monkeypatch.setattr(sync_positions, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        sync_positions,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(external),
    )

    path = sync_positions._format_follow_up_config_path(None)

    assert path == str(external)


def test_resolve_active_config_path_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(sync_positions, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        sync_positions,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        sync_positions._resolve_active_config_path()


def test_fetch_okx_ticker_last_uses_params(monkeypatch) -> None:
    captured = {}

    class _Response:
        status_code = 200

        def json(self):
            return {"code": "0", "data": [{"last": "123.45"}]}

    def fake_get(url, *, params=None, timeout=0, **kwargs):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return _Response()

    monkeypatch.setattr(sync_positions.requests, "get", fake_get)

    assert sync_positions._fetch_okx_ticker_last("ETH-USDT") == 123.45
    assert captured == {
        "url": "https://www.okx.com/api/v5/market/ticker",
        "params": {"instId": "ETH-USDT"},
        "timeout": 5,
    }
