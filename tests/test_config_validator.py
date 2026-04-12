from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import scripts.config_validator as config_validator


def test_resolve_workspace_defaults_to_repo_root(monkeypatch) -> None:
    monkeypatch.delenv("V5_WORKSPACE", raising=False)

    workspace = config_validator.resolve_workspace()

    assert workspace == config_validator.Path(config_validator.__file__).resolve().parents[1]


def test_check_env_variables_loads_root_dotenv(monkeypatch, tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "EXCHANGE_API_KEY=test-key",
                "EXCHANGE_API_SECRET=test-secret",
                "EXCHANGE_PASSPHRASE=test-passphrase",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(config_validator, "WORKSPACE", tmp_path)
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    validator = config_validator.ConfigValidator()
    validator.check_env_variables()

    assert validator.errors == []
    assert "缺少.env文件，将使用系统环境变量" not in validator.warnings


def test_check_timers_uses_current_production_timer_names(monkeypatch) -> None:
    captured = {}

    def _fake_run(cmd, capture_output=True, text=True, timeout=None, check=None):
        captured["cmd"] = cmd
        captured["timeout"] = timeout
        captured["check"] = check
        return SimpleNamespace(
            stdout="\n".join(config_validator.CURRENT_PRODUCTION_TIMERS),
            stderr="",
            returncode=0,
        )

    monkeypatch.setattr(config_validator.sys.modules["subprocess"], "run", _fake_run)

    validator = config_validator.ConfigValidator()
    validator.check_timers()

    assert captured["cmd"] == ["systemctl", "--user", "list-timers", "--all", "--no-pager"]
    assert captured["timeout"] == 10
    assert captured["check"] is False
    assert validator.warnings == []
    assert validator.checks_passed == len(config_validator.CURRENT_PRODUCTION_TIMERS)


def test_run_all_checks_defaults_to_live_prod_config() -> None:
    assert config_validator.ConfigValidator.run_all_checks.__defaults__ == ("live_prod.yaml",)


def test_check_database_uses_runtime_db_paths_from_active_config(monkeypatch, tmp_path) -> None:
    workspace = tmp_path
    configs_dir = workspace / "configs"
    reports_dir = workspace / "reports"
    shadow_dir = reports_dir / "shadow_runtime"
    configs_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir.mkdir(parents=True, exist_ok=True)

    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
                "account: {}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    for db_name, table_name in (
        ("orders.sqlite", "orders"),
        ("positions.sqlite", "positions"),
        ("fills.sqlite", "fills"),
    ):
        db_path = shadow_dir / db_name
        con = sqlite3.connect(str(db_path))
        try:
            con.execute(f"CREATE TABLE {table_name} (id INTEGER)")
            con.commit()
        finally:
            con.close()

    monkeypatch.setattr(config_validator, "WORKSPACE", workspace)
    monkeypatch.setattr(config_validator, "CONFIG_DIR", configs_dir)
    monkeypatch.setattr(config_validator, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(config_validator, "DATA_DIR", workspace / "data")

    validator = config_validator.ConfigValidator()
    validator.check_yaml_config("live_prod.yaml")
    validator.check_database()

    assert validator.errors == []
    assert validator.warnings == []
    assert validator.checks_passed >= 4
def test_check_timers_warns_when_systemctl_call_fails(monkeypatch) -> None:
    def _fake_run(*args, **kwargs):
        raise TimeoutError('systemctl timed out')

    monkeypatch.setattr(config_validator.sys.modules["subprocess"], "run", _fake_run)

    validator = config_validator.ConfigValidator()
    validator.check_timers()

    assert validator.checks_passed == 0
    assert validator.warnings == ["定时任务检查失败: systemctl timed out"]


