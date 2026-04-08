import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import configs.loader as config_loader
import src.execution.account_store as account_store_module
import src.execution.okx_private_client as okx_private_client_module
import src.execution.position_store as position_store_module
import scripts.auto_sync_before_trade as auto_sync_before_trade
from scripts.auto_sync_before_trade import _sync_local_store_to_okx_snapshot
from src.execution.position_store import PositionStore


def test_sync_preserves_entry_ts_for_existing_symbol(tmp_path):
    db = tmp_path / "positions.sqlite"
    store = PositionStore(path=str(db))
    original_entry_ts = "2026-03-10T08:00:00Z"
    store.upsert_buy("SUI/USDT", qty=10.0, px=1.0, now_ts=original_entry_ts)

    stats = _sync_local_store_to_okx_snapshot(
        store,
        {"SUI/USDT": 10.0},
        {"SUI/USDT": {"qty": 9.5, "eq_usd": 9.5}},
    )

    pos = store.get("SUI/USDT")
    assert pos is not None
    assert pos.qty == pytest.approx(9.5)
    assert pos.entry_ts == original_entry_ts
    assert stats == {"closed": 0, "updated": 1, "created": 0}


def test_sync_closes_missing_symbol_and_creates_new_symbol(tmp_path):
    db = tmp_path / "positions.sqlite"
    store = PositionStore(path=str(db))
    store.upsert_buy("OLD/USDT", qty=1.0, px=2.0, now_ts="2026-03-10T07:00:00Z")

    stats = _sync_local_store_to_okx_snapshot(
        store,
        {"OLD/USDT": 1.0},
        {"NEW/USDT": {"qty": 2.0, "eq_usd": 10.0}},
    )

    assert store.get("OLD/USDT") is None
    new_pos = store.get("NEW/USDT")
    assert new_pos is not None
    assert new_pos.qty == pytest.approx(2.0)
    assert new_pos.avg_px == pytest.approx(5.0)
    assert stats == {"closed": 1, "updated": 0, "created": 1}


def test_auto_sync_before_trade_respects_custom_runtime_status_paths(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    reports_dir = workspace / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    custom_reconcile = reports_dir / "custom_reconcile_status.json"
    custom_kill = reports_dir / "custom_kill_switch.json"
    custom_failure = reports_dir / "custom_reconcile_failure_state.json"
    default_kill = reports_dir / "kill_switch.json"
    failure_state_path = reports_dir / "reconcile_failure_state.json"

    custom_kill.write_text(
        json.dumps({"kill_switch": {"enabled": True, "manual": False}}, ensure_ascii=False),
        encoding="utf-8",
    )
    default_kill.write_text(json.dumps({"enabled": True, "manual": False}, ensure_ascii=False), encoding="utf-8")
    custom_failure.write_text(
        json.dumps(
            {
                "consecutive_hard": 4,
                "consecutive_soft": 3,
                "consecutive_ok": 0,
                "last_reason": "custom_stale",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    failure_state_path.write_text(
        json.dumps(
            {
                "consecutive_hard": 3,
                "consecutive_soft": 2,
                "consecutive_ok": 0,
                "last_reason": "stale",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, exchange):
            captured["exchange"] = exchange

        def get_balance(self, ccy=None):
            return SimpleNamespace(
                data={
                    "data": [
                        {
                            "details": [
                                {"ccy": "USDT", "cashBal": "100", "availBal": "100", "eq": "100"},
                            ]
                        }
                    ]
                }
            )

        def close(self):
            captured["client_closed"] = True

    class FakePositionStore:
        def __init__(self, path):
            captured["position_store_path"] = Path(path).resolve()

        def list(self):
            return []

        def get(self, symbol):
            return None

        def close_long(self, symbol):
            return False

        def set_qty(self, sym, qty):
            raise AssertionError("set_qty should not be called in this scenario")

        def upsert_buy(self, sym, qty, px):
            raise AssertionError("upsert_buy should not be called in this scenario")

    class FakeAccountStore:
        def __init__(self, path):
            captured["account_store_path"] = Path(path).resolve()

        def get(self):
            return SimpleNamespace(cash_usdt=0.0, equity_peak_usdt=0.0)

        def set(self, state):
            captured["account_cash"] = state.cash_usdt
            captured["account_peak"] = state.equity_peak_usdt

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        symbols=[],
        universe=SimpleNamespace(symbols=[]),
        execution=SimpleNamespace(
            reconcile_status_path="reports/custom_reconcile_status.json",
            kill_switch_path="reports/custom_kill_switch.json",
            reconcile_failure_state_path="reports/custom_reconcile_failure_state.json",
        ),
    )

    monkeypatch.setattr(auto_sync_before_trade, "WORKSPACE", workspace)
    monkeypatch.setattr(config_loader, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(okx_private_client_module, "OKXPrivateClient", FakeClient)
    monkeypatch.setattr(position_store_module, "PositionStore", FakePositionStore)
    monkeypatch.setattr(account_store_module, "AccountStore", FakeAccountStore)

    assert auto_sync_before_trade.main() == 0

    assert captured["position_store_path"] == (reports_dir / "positions.sqlite").resolve()
    assert captured["account_store_path"] == (reports_dir / "positions.sqlite").resolve()
    assert captured["client_closed"] is True

    reconcile_payload = json.loads(custom_reconcile.read_text(encoding="utf-8"))
    assert reconcile_payload["ok"] is True
    assert reconcile_payload["source"] == "auto_sync"

    kill_payload = json.loads(custom_kill.read_text(encoding="utf-8"))
    assert kill_payload["enabled"] is False
    assert kill_payload["auto_sync_cleared"] is True
    assert kill_payload["kill_switch"]["enabled"] is False

    untouched_default_kill = json.loads(default_kill.read_text(encoding="utf-8"))
    assert untouched_default_kill["enabled"] is True

    failure_payload = json.loads(custom_failure.read_text(encoding="utf-8"))
    assert failure_payload["consecutive_hard"] == 0
    assert failure_payload["consecutive_soft"] == 0
    assert failure_payload["consecutive_ok"] == 1
    assert failure_payload["last_reason"] == "auto_sync_reset"

    untouched_default_failure = json.loads(failure_state_path.read_text(encoding="utf-8"))
    assert untouched_default_failure["consecutive_hard"] == 3
    assert untouched_default_failure["consecutive_soft"] == 2
    assert untouched_default_failure["consecutive_ok"] == 0
    assert untouched_default_failure["last_reason"] == "stale"


def test_auto_sync_before_trade_clears_string_auto_kill_switch(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    reports_dir = workspace / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    custom_reconcile = reports_dir / "custom_reconcile_status.json"
    custom_kill = reports_dir / "custom_kill_switch.json"
    custom_failure = reports_dir / "custom_reconcile_failure_state.json"

    custom_kill.write_text(
        json.dumps(
            {"kill_switch": {"enabled": "true", "manual": "false", "trigger": "auto"}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    custom_failure.write_text(
        json.dumps(
            {
                "consecutive_hard": 4,
                "consecutive_soft": 3,
                "consecutive_ok": 0,
                "last_reason": "custom_stale",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class FakeClient:
        def __init__(self, exchange):
            self.exchange = exchange

        def get_balance(self, ccy=None):
            return SimpleNamespace(
                data={
                    "data": [
                        {
                            "details": [
                                {"ccy": "USDT", "cashBal": "100", "availBal": "100", "eq": "100"},
                            ]
                        }
                    ]
                }
            )

        def close(self):
            pass

    class FakePositionStore:
        def __init__(self, path):
            self.path = path

        def list(self):
            return []

        def get(self, symbol):
            return None

        def close_long(self, symbol):
            return False

        def set_qty(self, sym, qty):
            raise AssertionError("set_qty should not be called in this scenario")

        def upsert_buy(self, sym, qty, px):
            raise AssertionError("upsert_buy should not be called in this scenario")

    class FakeAccountStore:
        def __init__(self, path):
            self.path = path

        def get(self):
            return SimpleNamespace(cash_usdt=0.0, equity_peak_usdt=0.0)

        def set(self, state):
            self.state = state

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        symbols=[],
        universe=SimpleNamespace(symbols=[]),
        execution=SimpleNamespace(
            reconcile_status_path="reports/custom_reconcile_status.json",
            kill_switch_path="reports/custom_kill_switch.json",
            reconcile_failure_state_path="reports/custom_reconcile_failure_state.json",
        ),
    )

    monkeypatch.setattr(auto_sync_before_trade, "WORKSPACE", workspace)
    monkeypatch.setattr(config_loader, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(okx_private_client_module, "OKXPrivateClient", FakeClient)
    monkeypatch.setattr(position_store_module, "PositionStore", FakePositionStore)
    monkeypatch.setattr(account_store_module, "AccountStore", FakeAccountStore)

    assert auto_sync_before_trade.main() == 0

    reconcile_payload = json.loads(custom_reconcile.read_text(encoding="utf-8"))
    assert reconcile_payload["ok"] is True

    kill_payload = json.loads(custom_kill.read_text(encoding="utf-8"))
    assert kill_payload["enabled"] is False
    assert kill_payload["kill_switch"]["enabled"] is False
    assert kill_payload["auto_sync_cleared"] is True
