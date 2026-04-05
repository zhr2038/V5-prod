from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.emergency_liquidate as emergency_liquidate


def test_emergency_liquidate_defaults_to_live_prod_config_and_repo_env(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    project_root = Path(emergency_liquidate.__file__).resolve().parents[1]
    expected_cfg = (project_root / "configs" / "live_prod.yaml").resolve()
    expected_env = (project_root / ".env").resolve()
    cfg = SimpleNamespace(exchange=SimpleNamespace())

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            captured["client_kwargs"] = kwargs
            captured["closed"] = False

        def get_balance(self):
            return SimpleNamespace(data={"data": [{"details": []}]})

        def close(self) -> None:
            captured["closed"] = True

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return cfg

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(emergency_liquidate, "load_config", _fake_load_config)
    monkeypatch.setattr(emergency_liquidate, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(sys, "argv", ["emergency_liquidate.py"])

    emergency_liquidate.main()

    assert captured["config_path"] == expected_cfg
    assert captured["env_path"] == expected_env
    assert captured["client_kwargs"] == {"exchange": cfg.exchange}
    assert captured["closed"] is True
