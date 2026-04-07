from __future__ import annotations

import os

import scripts.sell_fil as sell_fil


def test_sell_fil_build_paths_anchor_env_to_workspace(tmp_path) -> None:
    paths = sell_fil.build_paths(workspace=tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.env_path == (tmp_path / ".env").resolve()


def test_sell_fil_load_runtime_env_reads_workspace_env(tmp_path, monkeypatch) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "EXCHANGE_API_KEY=test-key",
                "EXCHANGE_API_SECRET=test-secret",
                "EXCHANGE_PASSPHRASE=test-passphrase",
            ]
        ),
        encoding="utf-8",
    )
    for key in ("EXCHANGE_API_KEY", "EXCHANGE_API_SECRET", "EXCHANGE_PASSPHRASE"):
        monkeypatch.delenv(key, raising=False)

    sell_fil.load_runtime_env(sell_fil.build_paths(workspace=tmp_path))

    assert os.getenv("EXCHANGE_API_KEY") == "test-key"
    assert os.getenv("EXCHANGE_API_SECRET") == "test-secret"
    assert os.getenv("EXCHANGE_PASSPHRASE") == "test-passphrase"


def test_sell_fil_main_uses_workspace_env_and_submits_market_sell(tmp_path, monkeypatch, capsys) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "EXCHANGE_API_KEY=test-key",
                "EXCHANGE_API_SECRET=test-secret",
                "EXCHANGE_PASSPHRASE=test-passphrase",
            ]
        ),
        encoding="utf-8",
    )
    for key in ("EXCHANGE_API_KEY", "EXCHANGE_API_SECRET", "EXCHANGE_PASSPHRASE"):
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setattr(sell_fil, "PROJECT_ROOT", tmp_path.resolve())

    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def _fake_okx_request(method: str, path: str, body=None, **kwargs):
        calls.append((method, path, body))
        if path == "/api/v5/account/balance":
            return {
                "code": "0",
                "data": [{"details": [{"ccy": "FIL", "availBal": "2.5"}]}],
            }
        if path == "/api/v5/trade/order":
            return {"code": "0", "data": [{"ordId": "12345"}]}
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr(sell_fil, "okx_request", _fake_okx_request)

    rc = sell_fil.main([])

    assert rc == 0
    assert calls == [
        ("GET", "/api/v5/account/balance", None),
        (
            "POST",
            "/api/v5/trade/order",
            {
                "instId": "FIL-USDT",
                "tdMode": "cash",
                "side": "sell",
                "ordType": "market",
                "sz": "2.5",
            },
        ),
    ]

    output = capsys.readouterr().out
    assert "available FIL: 2.5" in output
    assert "ord_id=12345" in output
