from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _resolve_path(raw_path: str, *, project_root: Path) -> str:
    path = Path(str(raw_path).strip())
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return str(path)


def resolve_runtime_config_path(raw_config_path: str | None = None, *, project_root: Path | None = None) -> str:
    root = (project_root or PROJECT_ROOT).resolve()

    if raw_config_path is not None and str(raw_config_path).strip():
        return _resolve_path(str(raw_config_path), project_root=root)

    env_cfg = os.getenv("V5_CONFIG", "").strip()
    if env_cfg:
        return _resolve_path(env_cfg, project_root=root)

    for candidate in ("configs/live_prod.yaml", "configs/live_20u_real.yaml", "configs/config.yaml"):
        path = root / candidate
        if path.exists():
            return str(path.resolve())

    return str((root / "configs/live_prod.yaml").resolve())


def resolve_runtime_env_path(raw_env_path: str | None = None, *, project_root: Path | None = None) -> str:
    root = (project_root or PROJECT_ROOT).resolve()
    value = str(raw_env_path).strip() if raw_env_path is not None else ".env"
    return _resolve_path(value or ".env", project_root=root)


def resolve_runtime_path(
    raw_path: str | None = None,
    *,
    default: str,
    project_root: Path | None = None,
) -> str:
    root = (project_root or PROJECT_ROOT).resolve()
    value = str(raw_path).strip() if raw_path is not None else ""
    return _resolve_path(value or default, project_root=root)
