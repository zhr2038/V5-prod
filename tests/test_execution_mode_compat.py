from __future__ import annotations

import pytest

from configs.schema import ExecutionConfig


def test_compat_dry_run_true_maps_to_mode_dry_run() -> None:
    cfg = ExecutionConfig.model_validate({"dry_run": True})
    assert cfg.mode == "dry_run"


def test_compat_dry_run_false_maps_to_mode_live() -> None:
    cfg = ExecutionConfig.model_validate({"dry_run": False})
    assert cfg.mode == "live"


def test_mode_explicit_overrides_dry_run() -> None:
    cfg = ExecutionConfig.model_validate({"dry_run": True, "mode": "live"})
    assert cfg.mode == "live"


def test_default_mode_is_dry_run() -> None:
    cfg = ExecutionConfig()
    assert cfg.mode == "dry_run"


def test_invalid_mode_rejected() -> None:
    with pytest.raises(ValueError):
        ExecutionConfig.model_validate({"mode": "paper"})
