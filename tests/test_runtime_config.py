from __future__ import annotations

from pathlib import Path

from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path, resolve_runtime_path


def test_runtime_config_prefers_live_prod_and_resolves_absolute(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "config.yaml").write_text("base: true\n", encoding="utf-8")
    (tmp_path / "configs" / "live_prod.yaml").write_text("prod: true\n", encoding="utf-8")
    monkeypatch.delenv("V5_CONFIG", raising=False)

    resolved = resolve_runtime_config_path(project_root=tmp_path)
    assert Path(resolved) == (tmp_path / "configs" / "live_prod.yaml").resolve()


def test_runtime_config_respects_env_and_resolves_env_path_absolute(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "env-picked.yaml").write_text("prod: true\n", encoding="utf-8")

    monkeypatch.setenv("V5_CONFIG", "configs/env-picked.yaml")

    resolved_cfg = resolve_runtime_config_path(project_root=tmp_path)
    resolved_env = resolve_runtime_env_path(".env", project_root=tmp_path)

    assert Path(resolved_cfg) == (tmp_path / "configs" / "env-picked.yaml").resolve()
    assert Path(resolved_env) == (tmp_path / ".env").resolve()


def test_runtime_path_defaults_to_project_root(tmp_path: Path) -> None:
    resolved = resolve_runtime_path(default="reports/runtime.json", project_root=tmp_path)
    assert Path(resolved) == (tmp_path / "reports" / "runtime.json").resolve()
