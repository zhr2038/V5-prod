from __future__ import annotations

from pathlib import Path

import pytest

import scripts.ledger_once as ledger_once
import scripts.live_preflight_once as live_preflight_once
import scripts.reconcile_guard_once as reconcile_guard_once


@pytest.mark.parametrize(
    ("module", "resolver_name"),
    [
        (live_preflight_once, "_resolve_active_config_path"),
        (ledger_once, "_resolve_active_config_path"),
        (reconcile_guard_once, "_resolve_active_config_path"),
    ],
)
def test_runtime_cli_helpers_accept_valid_runtime_config(monkeypatch, tmp_path: Path, module, resolver_name: str) -> None:
    config_path = (tmp_path / "configs" / "runtime.yaml").resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        module,
        "PROJECT_ROOT",
        tmp_path,
    )

    def _resolve(raw_config_path=None, project_root=None):
        return str(config_path)

    def _load(raw_config_path=None, project_root=None):
        return {"execution": {"order_store_path": "reports/orders.sqlite"}}

    monkeypatch.setattr("configs.runtime_config.resolve_runtime_config_path", _resolve)
    monkeypatch.setattr("configs.runtime_config.load_runtime_config", _load)

    resolved = getattr(module, resolver_name)("configs/runtime.yaml")

    assert resolved == str(config_path)


@pytest.mark.parametrize(
    ("module", "resolver_name"),
    [
        (live_preflight_once, "_resolve_active_config_path"),
        (ledger_once, "_resolve_active_config_path"),
        (reconcile_guard_once, "_resolve_active_config_path"),
    ],
)
def test_runtime_cli_helpers_fail_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path, module, resolver_name: str) -> None:
    config_path = (tmp_path / "configs" / "runtime.yaml").resolve()
    monkeypatch.setattr(
        module,
        "PROJECT_ROOT",
        tmp_path,
    )

    def _resolve(raw_config_path=None, project_root=None):
        return str(config_path)

    def _load(raw_config_path=None, project_root=None):
        return {}

    monkeypatch.setattr("configs.runtime_config.resolve_runtime_config_path", _resolve)
    monkeypatch.setattr("configs.runtime_config.load_runtime_config", _load)

    with pytest.raises(ValueError, match="runtime.yaml"):
        getattr(module, resolver_name)("configs/runtime.yaml")
