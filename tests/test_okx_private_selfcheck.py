from __future__ import annotations

from pathlib import Path

import scripts.okx_private_selfcheck as selfcheck


def test_resolve_runtime_entry_paths_uses_runtime_helpers(monkeypatch, tmp_path: Path) -> None:
    expected_cfg = (tmp_path / "configs" / "selfcheck.yaml").resolve()
    expected_env = (tmp_path / "configs" / "selfcheck.env").resolve()

    monkeypatch.setattr(selfcheck, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        selfcheck,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected_cfg),
    )
    monkeypatch.setattr(
        selfcheck,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str(expected_env),
    )
    monkeypatch.setattr(
        selfcheck,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {"execution": {"order_store_path": "reports/orders.sqlite"}},
    )

    cfg_path, env_path = selfcheck._resolve_runtime_entry_paths(None, None)

    assert cfg_path == str(expected_cfg)
    assert env_path == str(expected_env)


def test_main_passes_cli_paths_to_loader(monkeypatch, tmp_path: Path) -> None:
    expected_cfg = (tmp_path / "configs" / "selfcheck.yaml").resolve()
    expected_env = (tmp_path / "configs" / "selfcheck.env").resolve()
    seen: dict[str, str] = {}

    monkeypatch.setattr(selfcheck, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        selfcheck,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected_cfg),
    )
    monkeypatch.setattr(
        selfcheck,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str(expected_env),
    )
    monkeypatch.setattr(
        selfcheck,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {"execution": {"order_store_path": "reports/orders.sqlite"}},
    )

    class _Cfg:
        exchange = object()

    class _Client:
        def __init__(self, exchange):
            self.exchange = exchange

        def get_balance(self, ccy=None):
            return type("Resp", (), {"http_status": 200, "okx_code": "0", "okx_msg": "", "data": {}})()

        def close(self):
            return None

    def fake_load_config(path, env_path=None):
        seen["config"] = path
        seen["env"] = env_path
        return _Cfg()

    monkeypatch.setattr(selfcheck, "load_config", fake_load_config)
    monkeypatch.setattr(selfcheck, "OKXPrivateClient", _Client)

    selfcheck.main(["--config", "configs/any.yaml", "--env", "configs/any.env"])

    assert seen == {"config": str(expected_cfg), "env": str(expected_env)}


def test_resolve_runtime_entry_paths_fails_fast_when_runtime_config_is_empty(monkeypatch, tmp_path: Path) -> None:
    expected_cfg = (tmp_path / "configs" / "selfcheck.yaml").resolve()
    expected_env = (tmp_path / "configs" / "selfcheck.env").resolve()

    monkeypatch.setattr(selfcheck, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        selfcheck,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected_cfg),
    )
    monkeypatch.setattr(
        selfcheck,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str(expected_env),
    )
    monkeypatch.setattr(
        selfcheck,
        "load_runtime_config",
        lambda raw_config_path=None, project_root=None: {},
    )

    try:
        selfcheck._resolve_runtime_entry_paths(None, None)
    except ValueError as exc:
        assert str(expected_cfg) in str(exc)
    else:
        raise AssertionError("expected ValueError")
