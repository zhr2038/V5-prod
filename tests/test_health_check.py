from __future__ import annotations

from pathlib import Path

import scripts.health_check as health_check


def test_resolve_health_output_path_uses_prefixed_runtime_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        health_check,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )
    monkeypatch.setattr(health_check, "WORKSPACE", tmp_path)
    monkeypatch.setattr(health_check, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(health_check, "HEALTH_FILE", (tmp_path / "reports" / "health_status.json"))

    path = health_check._resolve_health_output_path()

    assert path == (tmp_path / "reports" / "shadow_health_status.json").resolve()


def test_resolve_health_output_path_uses_suffixed_runtime_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        health_check,
        "load_runtime_config",
        lambda project_root=None: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_path",
        lambda raw_path=None, default="reports/orders.sqlite", project_root=None: str(
            (tmp_path / (raw_path or default)).resolve()
        ),
    )
    monkeypatch.setattr(health_check, "WORKSPACE", tmp_path)
    monkeypatch.setattr(health_check, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(health_check, "HEALTH_FILE", (tmp_path / "reports" / "health_status.json"))

    path = health_check._resolve_health_output_path()

    assert path == (tmp_path / "reports" / "health_status_accelerated.json").resolve()


def test_resolve_health_env_path_uses_runtime_env_helper(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "live.env").resolve()
    monkeypatch.setattr(
        health_check,
        "resolve_runtime_env_path",
        lambda project_root=None: str(expected),
    )
    monkeypatch.setattr(health_check, "WORKSPACE", tmp_path)

    path = health_check._resolve_health_env_path()

    assert path == expected


def test_resolve_live_timer_unit_name_ignores_retired_live_20u(monkeypatch) -> None:
    monkeypatch.setattr(health_check.shutil, "which", lambda _: "/bin/systemctl")
    monkeypatch.setattr(
        health_check,
        "_get_unit_load_state",
        lambda unit: "loaded" if unit == "v5-live-20u.user.timer" else "not-found",
    )

    assert health_check.resolve_live_timer_unit_name() == "v5-prod.user.timer"


def test_check_okx_api_warns_with_runtime_env_filename(monkeypatch, tmp_path: Path) -> None:
    expected = (tmp_path / "configs" / "live.env").resolve()
    monkeypatch.setattr(
        health_check,
        "_resolve_health_env_path",
        lambda: expected,
    )
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("OKX_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("OKX_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)
    monkeypatch.delenv("OKX_API_PASSPHRASE", raising=False)

    result = health_check.HealthChecker().check_okx_api()

    assert result["status"] == "warning"
    assert result["details"] == "API credentials missing in runtime env file: live.env"
