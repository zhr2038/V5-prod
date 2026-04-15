from __future__ import annotations

import os

from src.risk import live_equity_fetcher


class _FakeExchange:
    def __init__(self, cfg, seen):
        seen.update(cfg)

    def fetch_balance(self, params):
        return {"total": {"USDT": 12.5}}


def test_get_live_equity_from_okx_uses_runtime_env_path(monkeypatch, tmp_path):
    env_path = tmp_path / ".env.runtime"
    env_path.write_text(
        "\n".join(
            [
                "EXCHANGE_API_KEY=runtime-key",
                "EXCHANGE_API_SECRET=runtime-secret",
                "EXCHANGE_PASSPHRASE=runtime-pass",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "EXCHANGE_API_KEY=root-key",
                "EXCHANGE_API_SECRET=root-secret",
                "EXCHANGE_PASSPHRASE=root-pass",
            ]
        ),
        encoding="utf-8",
    )

    seen = {}
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)
    monkeypatch.setattr(
        live_equity_fetcher.ccxt,
        "okx",
        lambda cfg: _FakeExchange(cfg, seen),
    )

    equity = live_equity_fetcher.get_live_equity_from_okx(
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert equity == 12.5
    assert seen["apiKey"] == "runtime-key"
    assert seen["secret"] == "runtime-secret"
    assert seen["password"] == "runtime-pass"


def test_check_budget_limit_passes_runtime_env_path(monkeypatch, tmp_path):
    captured = {}

    def _fake_get_live_equity_from_okx(*, env_path=None, project_root=None):
        captured["env_path"] = env_path
        captured["project_root"] = project_root
        return 10.0

    monkeypatch.setattr(live_equity_fetcher, "get_live_equity_from_okx", _fake_get_live_equity_from_okx)

    result = live_equity_fetcher.check_budget_limit(
        20.0,
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert result["ok"] is True
    assert result["current"] == 10.0
    assert captured == {"env_path": ".env.runtime", "project_root": tmp_path}
