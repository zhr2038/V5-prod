from __future__ import annotations

import json
from pathlib import Path

from src.utils import auto_blacklist


def test_resolve_auto_blacklist_path_uses_prefixed_runtime_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        auto_blacklist,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        auto_blacklist,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )

    path = auto_blacklist.resolve_auto_blacklist_path(project_root=tmp_path)

    assert path == (tmp_path / "reports" / "shadow_auto_blacklist.json").resolve()


def test_add_symbol_writes_prefixed_runtime_blacklist(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        auto_blacklist,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        auto_blacklist,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )
    monkeypatch.setattr(auto_blacklist, "PROJECT_ROOT", tmp_path)

    auto_blacklist.add_symbol("PEPE/USDT", reason="spread_too_wide")

    runtime_path = tmp_path / "reports" / "shadow_auto_blacklist.json"
    root_path = tmp_path / "reports" / "auto_blacklist.json"

    assert runtime_path.exists()
    assert not root_path.exists()
    payload = json.loads(runtime_path.read_text(encoding="utf-8"))
    assert payload["symbols"] == ["PEPE/USDT"]


def test_resolve_auto_blacklist_path_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        auto_blacklist,
        "load_runtime_config",
        lambda project_root=None: {},
    )

    try:
        auto_blacklist.resolve_auto_blacklist_path(project_root=tmp_path)
    except ValueError as exc:
        assert "live_prod.yaml" in str(exc)
    else:
        raise AssertionError("expected ValueError")
