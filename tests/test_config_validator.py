from __future__ import annotations

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

    def _fake_run(cmd, capture_output=True, text=True):
        captured["cmd"] = cmd
        return SimpleNamespace(
            stdout="\n".join(config_validator.CURRENT_PRODUCTION_TIMERS),
            stderr="",
            returncode=0,
        )

    monkeypatch.setattr(config_validator.sys.modules["subprocess"], "run", _fake_run)

    validator = config_validator.ConfigValidator()
    validator.check_timers()

    assert captured["cmd"] == ["systemctl", "--user", "list-timers", "--all", "--no-pager"]
    assert validator.warnings == []
    assert validator.checks_passed == len(config_validator.CURRENT_PRODUCTION_TIMERS)


def test_run_all_checks_defaults_to_live_prod_config() -> None:
    assert config_validator.ConfigValidator.run_all_checks.__defaults__ == ("live_prod.yaml",)
