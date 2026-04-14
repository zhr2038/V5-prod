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
