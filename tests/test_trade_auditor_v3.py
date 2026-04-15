from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import scripts.trade_auditor_v3 as auditor_mod


def test_get_okx_balance_prefers_total_equity_and_eq_usd(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        auditor_mod,
        "load_exchange_credentials",
        lambda _paths=None: ("k", "s", "p"),
    )

    payload = {
        "code": "0",
        "data": [
            {
                "totalEq": "123.45",
                "details": [
                    {"ccy": "USDT", "eq": "90.12", "eqUsd": "90.12"},
                    {"ccy": "BTC", "eq": "0.001", "eqUsd": "75.5"},
                    {"ccy": "DOGE", "eq": "10", "eqUsd": "0.9"},
                ],
            }
        ],
    }

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    monkeypatch.setattr(auditor_mod.requests, "get", lambda *args, **kwargs: _Resp())

    auditor = auditor_mod.TradeAuditorV3(workspace=tmp_path)
    result = auditor.get_okx_balance()

    assert result == {
        "usdt": 90.12,
        "total_eq_usdt": 123.45,
        "positions": ["BTC: 0.00 ($75.50)"],
    }


def test_get_okx_balance_sanitizes_request_errors(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        auditor_mod,
        "load_exchange_credentials",
        lambda _paths=None: ("k", "s", "p"),
    )
    monkeypatch.setattr(
        auditor_mod.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("/home/ubuntu/clawd/v5-prod/.env missing")),
    )

    auditor = auditor_mod.TradeAuditorV3(workspace=tmp_path)
    result = auditor.get_okx_balance()

    assert result == {"error": "api unavailable", "detail": "RuntimeError"}


def test_trade_auditor_v3_main_passes_cli_paths(monkeypatch, tmp_path: Path) -> None:
    expected_cfg = (tmp_path / "configs" / "auditor.yaml").resolve()
    expected_env = (tmp_path / "configs" / "auditor.env").resolve()
    seen: dict[str, str] = {}

    monkeypatch.setattr(auditor_mod, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected_cfg),
    )
    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str(expected_env),
    )

    original_init = auditor_mod.TradeAuditorV3.__init__

    def fake_init(self, workspace=None, *, config_path=None, env_path=None):
        seen["config"] = config_path
        seen["env"] = env_path
        self.paths = SimpleNamespace()
        self.issues = []
        self.warnings = []
        self.info = []

    monkeypatch.setattr(auditor_mod.TradeAuditorV3, "__init__", fake_init)
    monkeypatch.setattr(auditor_mod.TradeAuditorV3, "run", lambda self: "ok")

    try:
        auditor_mod.main(["--config", "configs/x.yaml", "--env", "configs/x.env"])
    finally:
        monkeypatch.setattr(auditor_mod.TradeAuditorV3, "__init__", original_init)

    assert seen == {"config": "configs/x.yaml", "env": "configs/x.env"}


def test_build_paths_uses_runtime_entry_helpers(monkeypatch, tmp_path: Path) -> None:
    expected_cfg = (tmp_path / "configs" / "auditor.yaml").resolve()
    expected_env = (tmp_path / "configs" / "auditor.env").resolve()

    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected_cfg),
    )
    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str(expected_env),
    )

    paths = auditor_mod.build_paths(tmp_path, config_path="configs/x.yaml", env_path="configs/x.env")

    assert paths.env_path == expected_env
