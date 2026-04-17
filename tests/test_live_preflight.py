from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from configs.schema import ExecutionConfig
from src.execution import live_preflight


class _DummyBillsStore:
    def __init__(self, path: str):
        self.path = path

    def count(self) -> int:
        return 0


class _DummyLedgerEngine:
    def __init__(self, okx, bills_store, state_path: str):
        self.state_path = state_path

    def run(self, out_path: str):
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(
            json.dumps({"generated_ts_ms": 1, "ok": False, "reason": "ledger_not_ok"}),
            encoding="utf-8",
        )
        return {"generated_ts_ms": 1, "ok": False, "reason": "ledger_not_ok"}


def test_live_preflight_does_not_force_allow_when_ledger_is_not_ok(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(live_preflight, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(live_preflight, "BillsStore", _DummyBillsStore)
    monkeypatch.setattr(live_preflight, "LedgerEngine", _DummyLedgerEngine)
    monkeypatch.setattr(live_preflight, "bills_sync_once", lambda **kwargs: 0)
    monkeypatch.setattr(
        live_preflight,
        "check_okx_borrows",
        lambda *_args, **_kwargs: SimpleNamespace(ok=True, reason="ok", items=[]),
    )
    monkeypatch.setattr(live_preflight, "_read_json", lambda _path: {"generated_ts_ms": 1})

    cfg = ExecutionConfig(
        order_store_path="reports/orders.sqlite",
        allow_trade_on_small_reconcile_drift=True,
        enforce_account_config_check=False,
    )

    preflight = live_preflight.LivePreflight(
        cfg,
        okx=SimpleNamespace(get_balance=lambda: SimpleNamespace(data={"data": []})),
        position_store=None,
        account_store=None,
    )
    monkeypatch.setattr(preflight, "_status_is_fresh", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        preflight,
        "_run_reconcile_guard",
        lambda **_kwargs: {
            "generated_ts_ms": 1,
            "ok": False,
            "reason": "base_mismatch",
            "kill_switch": {"enabled": False},
        },
    )

    result = preflight.run()

    assert result.decision == "SELL_ONLY"
    assert result.reconcile_ok is False
    assert result.ledger_ok is False
    assert result.reason == "reconcile_or_ledger_not_ok"
    assert (result.details or {}).get("reconcile_warn") is None
