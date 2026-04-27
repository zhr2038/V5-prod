from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.auto_sync_before_trade as auto_sync_before_trade


def test_resolve_active_config_path_fails_fast_when_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "live_prod.yaml").resolve()
    monkeypatch.setattr(auto_sync_before_trade, "WORKSPACE", tmp_path)
    monkeypatch.setattr(
        auto_sync_before_trade,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
        raising=False,
    )

    with pytest.raises(FileNotFoundError, match="runtime config not found"):
        auto_sync_before_trade._resolve_active_config_path()


def test_balance_account_extracts_account_level_total_equity() -> None:
    resp = SimpleNamespace(
        data={
            "data": [
                {
                    "totalEq": "106.8",
                    "details": [{"ccy": "USDT", "cashBal": "106.8"}],
                }
            ]
        }
    )

    account = auto_sync_before_trade._balance_account(resp)

    assert account["totalEq"] == "106.8"


def test_write_equity_validation_snapshot_refreshes_runtime_file(tmp_path: Path) -> None:
    equity_file = tmp_path / "reports" / "equity_validation.json"

    data = auto_sync_before_trade._write_equity_validation_snapshot(
        equity_file=equity_file,
        okx_total_eq=106.8,
        usdt_balance=100.0,
        total_position_value=6.8,
        okx_positions={"BTC/USDT": {"qty": 0.001, "eq_usd": 6.8}},
    )

    written = json.loads(equity_file.read_text(encoding="utf-8"))
    assert written == data
    assert written["okx_total_eq"] == pytest.approx(106.8)
    assert written["calculated_total_eq"] == pytest.approx(106.8)
    assert written["positions"][0]["price"] == pytest.approx(6800.0)
    assert written["source"] == "auto_sync_before_trade"
