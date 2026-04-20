from __future__ import annotations

from pathlib import Path

import pytest

import scripts.reconcile_once as reconcile_once
import scripts.reconcile_with_retry as reconcile_with_retry


@pytest.mark.parametrize("module", [reconcile_once, reconcile_with_retry])
def test_reconcile_cli_resolve_active_config_path_fails_fast_when_config_is_missing(module, monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        module,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
        raising=False,
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        module._resolve_active_config_path()
