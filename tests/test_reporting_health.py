from __future__ import annotations

from pathlib import Path

from src.reporting import health as reporting_health


def test_resolve_active_config_path_uses_runtime_config_helper(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "runtime_live.yaml").resolve()
    monkeypatch.setattr(
        reporting_health,
        "resolve_runtime_config_path",
        lambda project_root=None: str(expected),
    )

    path = reporting_health._resolve_active_config_path()

    assert path == expected
