from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import scripts.okx_private_selfcheck as okx_private_selfcheck


def test_okx_private_selfcheck_defaults_to_runtime_live_prod(monkeypatch) -> None:
    captured = {}
    expected_cfg = (Path(okx_private_selfcheck.__file__).resolve().parents[1] / "configs" / "live_prod.yaml").resolve()
    expected_env = (Path(okx_private_selfcheck.__file__).resolve().parents[1] / ".env").resolve()

    cfg = SimpleNamespace(exchange=SimpleNamespace())

    class DummyClient:
        def __init__(self, *, exchange) -> None:
            captured["exchange"] = exchange

        def get_balance(self, *, ccy=None):
            captured["ccy"] = ccy
            return SimpleNamespace(http_status=200, okx_code="0", okx_msg="", data={"data": []})

        def close(self) -> None:
            captured["closed"] = True

    def _fake_load_config(config_path, *, env_path):
        captured["config_path"] = Path(config_path).resolve()
        captured["env_path"] = Path(env_path).resolve()
        return cfg

    monkeypatch.setattr(okx_private_selfcheck, "load_config", _fake_load_config)
    monkeypatch.setattr(okx_private_selfcheck, "OKXPrivateClient", DummyClient)

    okx_private_selfcheck.main()

    assert captured["config_path"] == expected_cfg
    assert captured["env_path"] == expected_env
    assert captured["ccy"] == "USDT"
    assert captured["closed"] is True
