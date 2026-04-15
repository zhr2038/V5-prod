from __future__ import annotations

from pathlib import Path

import scripts.task_config_compat as compat


def test_walk_forward_compat_does_not_pin_config_path(tmp_path: Path) -> None:
    task = compat.load_task_config_with_compat(
        tmp_path,
        "configs/research/walk_forward.yaml",
        loader=lambda path: {},
    )

    walk_cfg = task.get("walk_forward") or {}
    assert "config_path" not in walk_cfg
    assert walk_cfg["env_path"] == ".env"


def test_walk_forward_prod_cache_compat_does_not_pin_config_path(tmp_path: Path) -> None:
    task = compat.load_task_config_with_compat(
        tmp_path,
        "configs/research/walk_forward_prod_cache.yaml",
        loader=lambda path: {},
    )

    walk_cfg = task.get("walk_forward") or {}
    assert "config_path" not in walk_cfg
    assert walk_cfg["provider"] == "cache"
