from __future__ import annotations

from pathlib import Path

from configs import runtime_config


def test_resolve_runtime_config_path_skips_retired_live_20u_real(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("V5_CONFIG", raising=False)
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "live_20u_real.yaml").write_text("legacy: true\n", encoding="utf-8")

    resolved = runtime_config.resolve_runtime_config_path(project_root=tmp_path)

    assert resolved == str((tmp_path / "configs" / "live_prod.yaml").resolve())


def test_resolve_runtime_config_path_prefers_config_when_live_prod_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("V5_CONFIG", raising=False)
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "config.yaml").write_text("mode: test\n", encoding="utf-8")

    resolved = runtime_config.resolve_runtime_config_path(project_root=tmp_path)

    assert resolved == str((tmp_path / "configs" / "config.yaml").resolve())


def test_resolve_runtime_env_path_uses_explicit_arg_before_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("V5_ENV", "configs/runtime.env")
    resolved = runtime_config.resolve_runtime_env_path(".env.override", project_root=tmp_path)
    assert resolved == str((tmp_path / ".env.override").resolve())


def test_resolve_runtime_env_path_uses_v5_env_when_arg_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("V5_ENV", "configs/runtime.env")
    resolved = runtime_config.resolve_runtime_env_path(project_root=tmp_path)
    assert resolved == str((tmp_path / "configs" / "runtime.env").resolve())


def test_resolve_runtime_env_path_treats_dotenv_arg_as_default_and_uses_v5_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("V5_ENV", "configs/runtime.env")
    resolved = runtime_config.resolve_runtime_env_path(".env", project_root=tmp_path)
    assert resolved == str((tmp_path / "configs" / "runtime.env").resolve())


def test_resolve_runtime_env_path_falls_back_to_dotenv(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("V5_ENV", raising=False)
    resolved = runtime_config.resolve_runtime_env_path(project_root=tmp_path)
    assert resolved == str((tmp_path / ".env").resolve())
