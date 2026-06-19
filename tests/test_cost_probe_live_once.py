from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from configs.schema import AppConfig
from scripts.cost_probe_live_once import (
    _authorization_hmac_signature,
    _cost_probe_config_sha,
    _current_code_sha,
    _roundtrip_cost_fields,
    build_live_probe_preflight,
    run_live_probe_once,
)


class _Response:
    def __init__(self, data):
        self.data = data


class _FakeOKX:
    def __init__(
        self,
        *,
        partial_exit: bool = False,
        initial_base_balance: str = "0",
        base_fee_on_entry: bool = False,
        raise_after_entry: bool = False,
        entry_get_order_fails: bool = False,
    ) -> None:
        self.placed: list[dict] = []
        self.partial_exit = partial_exit
        self.balance_qty = Decimal(initial_base_balance)
        self.base_fee_on_entry = base_fee_on_entry
        self.raise_after_entry = raise_after_entry
        self.entry_get_order_fails = entry_get_order_fails
        self.orders_by_clid: dict[str, dict] = {}
        self.settled_clids: set[str] = set()
        self.cancels: list[dict] = []

    def request(self, method, path, *, params=None, json_body=None):
        if path == "/api/v5/public/instruments":
            return _Response(
                {
                    "code": "0",
                    "data": [
                        {
                            "instId": params["instId"],
                            "minSz": "0.00001",
                            "lotSz": "0.000001",
                            "tickSz": "0.1",
                        }
                    ],
                }
            )
        if path == "/api/v5/market/ticker":
            return _Response(
                {
                    "code": "0",
                    "data": [
                        {
                            "instId": params["instId"],
                            "bidPx": "49990",
                            "askPx": "50010",
                            "last": "50000",
                        }
                    ],
                }
            )
        if path == "/api/v5/trade/orders-pending":
            return _Response({"code": "0", "data": []})
        if path == "/api/v5/trade/fills":
            return _Response({"code": "0", "data": []})
        if path == "/api/v5/trade/cancel-order":
            self.cancels.append(dict(json_body or {}))
            return _Response({"code": "0", "data": [{"sCode": "0"}]})
        raise AssertionError(f"unexpected request {method} {path}")

    def place_order(self, payload, *, exp_time_ms=None):
        order = dict(payload)
        self.placed.append(order)
        self.orders_by_clid[order["clOrdId"]] = order
        if order["side"] == "buy" and self.raise_after_entry:
            self._settle_order(order)
            raise RuntimeError("network_after_entry_submit")
        return _Response({"code": "0", "data": [{"sCode": "0", "ordId": f"okx-{len(self.placed)}"}]})

    def get_order(self, *, inst_id, cl_ord_id):
        if self.entry_get_order_fails and cl_ord_id.endswith("E"):
            raise RuntimeError("entry_lookup_failed")
        order = self.orders_by_clid.get(cl_ord_id, {"side": "buy" if cl_ord_id.endswith("E") else "sell", "sz": "0.000099"})
        return self._settle_order(order)

    def cancel_order(self, *, inst_id, ord_id=None, cl_ord_id=None):
        payload = {"instId": inst_id, "ordId": ord_id or "", "clOrdId": cl_ord_id or ""}
        self.cancels.append(payload)
        return _Response({"code": "0", "data": [{"sCode": "0"}]})

    def _settle_order(self, order: dict) -> _Response:
        cl_ord_id = str(order.get("clOrdId") or "")
        side = str(order.get("side") or "")
        fill_qty = Decimal(str(order.get("sz") or "0.000099"))
        state = "filled"
        if cl_ord_id.endswith("X") and self.partial_exit:
            fill_qty = min(fill_qty, Decimal("0.00005"))
            state = "partially_filled"
        fee = Decimal("-0.00000001") if side == "buy" and self.base_fee_on_entry else Decimal("-0.01")
        fee_ccy = "BTC" if side == "buy" and self.base_fee_on_entry else "USDT"
        if cl_ord_id not in self.settled_clids:
            if side == "buy":
                self.balance_qty += fill_qty + (fee if fee_ccy == "BTC" else Decimal("0"))
            elif side == "sell":
                self.balance_qty = max(self.balance_qty - fill_qty, Decimal("0"))
            self.settled_clids.add(cl_ord_id)
        return _Response(
            {
                "code": "0",
                "data": [
                    {
                        "instId": str(order.get("instId") or "BTC-USDT"),
                        "clOrdId": cl_ord_id,
                        "ordId": f"okx-{cl_ord_id[-1]}",
                        "side": side,
                        "state": state,
                        "accFillSz": format(fill_qty, "f"),
                        "avgPx": "50010",
                        "fee": format(fee, "f"),
                        "feeCcy": fee_ccy,
                    }
                ],
            }
        )

    def get_balance(self, ccy=None):
        details = (
            [
                {
                    "ccy": "BTC",
                    "availBal": format(self.balance_qty, "f"),
                    "cashBal": format(self.balance_qty, "f"),
                    "eq": format(self.balance_qty, "f"),
                    "eqUsd": "0",
                },
                {
                    "ccy": "USDT",
                    "availBal": "100",
                    "cashBal": "100",
                    "eq": "100",
                    "eqUsd": "100",
                },
            ]
            if ccy is None
            else [
                {
                    "ccy": ccy,
                    "availBal": format(self.balance_qty, "f"),
                    "cashBal": format(self.balance_qty, "f"),
                    "eq": format(self.balance_qty, "f"),
                    "eqUsd": "0",
                }
            ]
        )
        return _Response({"code": "0", "data": [{"details": details}]})


@pytest.fixture(autouse=True)
def _live_probe_test_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("V5_COST_PROBE_AUTH_HMAC_SECRET", "unit-test-secret")
    monkeypatch.setenv("V5_COST_PROBE_AUTH_OPERATORS", "operator")
    monkeypatch.setenv("V5_COST_PROBE_LIVE_LOCK_PATH", str(tmp_path / "cost-probe-live-once.lock"))


def test_cost_probe_live_once_waits_for_operator_execution_confirmation(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX()

    result = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=False,
        operator_confirmed=False,
    )

    assert result["state"] == "READY_FOR_OPERATOR_CONFIRMATION"
    assert result["approved_live_order_execution"] is False
    assert fake.placed == []
    assert result["instrument_preflight"]["order_plan"]["base_qty"] == "0.000099"


def test_cost_probe_live_once_blocks_incomplete_manual_authorization(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg, acknowledged_risks=["one_time_live_cost_probe"])

    result = build_live_probe_preflight(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=_FakeOKX(),
        project_root=tmp_path,
    )

    assert result["state"] == "NOT_READY"
    assert "manual_authorization_acknowledgements_missing" in result["blockers"]


def test_cost_probe_live_once_blocks_future_manual_authorization(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    issued = datetime(2099, 1, 1, tzinfo=UTC)
    auth_path = _write_auth(
        tmp_path,
        cfg=cfg,
        issued_at=_iso(issued),
        expires_at=_iso(issued + timedelta(minutes=4)),
    )

    result = build_live_probe_preflight(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=_FakeOKX(),
        project_root=tmp_path,
    )

    assert result["state"] == "NOT_READY"
    assert "manual_authorization_issued_at_in_future" in result["blockers"]


def test_cost_probe_live_once_rejects_text_only_signature(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg, valid_signature=False)

    result = build_live_probe_preflight(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=_FakeOKX(),
        project_root=tmp_path,
    )

    assert result["state"] == "NOT_READY"
    assert "manual_authorization_signature_invalid" in result["blockers"]


def test_cost_probe_live_once_execute_uses_entry_and_immediate_exit_with_fake_okx(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX()

    result = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )

    assert result["state"] == "COMPLETED"
    assert [order["side"] for order in fake.placed] == ["buy", "sell"]
    assert fake.placed[0]["ordType"] == "ioc"
    assert fake.placed[1]["ordType"] == "ioc"
    assert fake.placed[0]["clOrdId"][:-1] == fake.placed[1]["clOrdId"][:-1]
    assert (tmp_path / "reports" / "cost_probe_order_events.jsonl").exists()
    assert (tmp_path / "reports" / "cost_probe_roundtrip_events.jsonl").exists()
    assert not auth_path.exists()
    assert (tmp_path / "auth.consumed.json").exists()
    assert result["completed"] is True
    assert result["flat_verification"]["flat_verified"] is True


def test_cost_probe_live_once_auth_notional_caps_actual_order_size(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg, max_notional_usdt=1.0)
    fake = _FakeOKX()

    result = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )

    assert result["state"] == "COMPLETED"
    entry = fake.placed[0]
    assert Decimal(entry["sz"]) * Decimal(entry["px"]) <= Decimal("1.0")
    assert entry["sz"] == "0.000019"


def test_cost_probe_live_once_exit_uses_available_base_after_base_fee(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX(base_fee_on_entry=True)

    result = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )

    assert result["state"] == "COMPLETED"
    assert fake.placed[1]["side"] == "sell"
    assert fake.placed[1]["sz"] == "0.000098"
    assert result["entry_state"]["_unsellable_dust_qty"] == "0.000001"


def test_cost_probe_live_once_exchange_min_preflight_can_clear_pending_blocker(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    cfg.execution.cost_probe_use_exchange_min_notional = True
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)

    result = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=_FakeOKX(),
        project_root=tmp_path,
        execute_live_order=False,
        operator_confirmed=False,
    )

    assert result["state"] == "READY_FOR_OPERATOR_CONFIRMATION"
    assert "exchange_min_notional_check_pending" not in result["blockers"]
    assert result["p3_preflight"]["instrument_preflight_passed"] is True
    assert result["p3_preflight"]["normalized_qty"] == "0.000099"


def test_cost_probe_live_once_partial_exit_is_incomplete_and_triggers_emergency_flatten(
    tmp_path: Path,
) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX(partial_exit=True, initial_base_balance="0.000003")

    result = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )

    assert result["state"] == "INCOMPLETE"
    assert result["roundtrip_status"] == "incomplete_exit"
    assert result["completed"] is False
    assert result["emergency_flatten"]["attempted"] is True
    assert fake.placed[-1]["side"] == "sell"
    assert fake.placed[-1]["sz"] == "0.000049"
    assert [order["side"] for order in fake.placed] == ["buy", "sell", "sell"]
    kill_switch = json.loads((tmp_path / "runtime" / "kill_switch.json").read_text(encoding="utf-8"))
    assert kill_switch["enabled"] is True
    assert kill_switch["sell_only_on_error"] is True


def test_cost_probe_live_once_exception_after_unknown_entry_attempt_flattens_balance_delta(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX(raise_after_entry=True, entry_get_order_fails=True)

    result = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )

    assert result["state"] == "ABORTED_KILL_SWITCH_ENABLED"
    assert result["emergency_flatten"]["attempted"] is True
    assert result["emergency_flatten"]["sell_qty"] == "0.000099"
    assert [order["side"] for order in fake.placed] == ["buy", "sell"]
    assert fake.cancels


def test_cost_probe_live_once_global_lock_blocks_second_executor_before_consuming_auth(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    lock_path = Path(os.environ["V5_COST_PROBE_LIVE_LOCK_PATH"])
    lock_path.write_text("held\n", encoding="utf-8")
    fake = _FakeOKX()

    result = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )

    assert result["state"] == "NOT_READY"
    assert "cost_probe_global_execution_lock_held" in result["blockers"]
    assert fake.placed == []
    assert auth_path.exists()


def test_cost_probe_live_once_rejects_reused_consumed_authorization(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX()

    first = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )
    second = run_live_probe_once(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )

    assert first["state"] == "COMPLETED"
    assert second["state"] == "NOT_READY"
    assert "manual_authorization_file_missing_or_invalid" in second["blockers"]
    assert len(fake.placed) == 2


def test_cost_probe_roundtrip_cost_converts_base_fee_to_usdt() -> None:
    cost = _roundtrip_cost_fields(
        {
            "instId": "BTC-USDT",
            "accFillSz": "0.001",
            "avgPx": "50000",
            "fee": "-0.000001",
            "feeCcy": "BTC",
        },
        {
            "instId": "BTC-USDT",
            "accFillSz": "0.001",
            "avgPx": "50000",
            "fee": "-0.05",
            "feeCcy": "USDT",
        },
        "BTC/USDT",
    )

    assert cost["entry_fee_usdt"] == "-0.05"
    assert cost["exit_fee_usdt"] == "-0.05"
    assert cost["net_pnl_usdt"] == "-0.1"
    assert cost["fee_conversion_warnings"] == ""


def _ready_cost_probe_config(tmp_path: Path) -> AppConfig:
    cfg = AppConfig()
    cfg.execution.order_store_path = str(tmp_path / "runtime" / "orders.sqlite")
    cfg.execution.kill_switch_path = str(tmp_path / "runtime" / "kill_switch.json")
    cfg.execution.reconcile_status_path = str(tmp_path / "runtime" / "reconcile_status.json")
    cfg.execution.cost_bootstrap_enabled = True
    cfg.execution.cost_probe_enabled = True
    cfg.execution.cost_probe_dry_run = True
    cfg.execution.cost_probe_live_enabled = False
    cfg.execution.cost_probe_use_exchange_min_notional = False
    cfg.execution.cost_probe_symbols = ["BTC/USDT"]
    cfg.execution.cost_probe_max_orders_per_day = 2
    cfg.execution.cost_probe_max_roundtrips_per_symbol_per_day = 1
    cfg.execution.cost_probe_max_notional_usdt = 5.0
    return cfg


def _write_auth(
    tmp_path: Path,
    *,
    cfg: AppConfig,
    acknowledged_risks=None,
    max_notional_usdt: float = 5.0,
    issued_at: str | None = None,
    expires_at: str | None = None,
    signed_by: str = "operator",
    valid_signature: bool = True,
) -> Path:
    path = tmp_path / "auth.pending.json"
    now = datetime.now(UTC)
    payload = {
        "scope": "v5_cost_probe_live_once",
        "authorization_id": "auth-1",
        "nonce": "nonce-1",
        "code_sha": _current_code_sha(tmp_path),
        "config_sha256": _cost_probe_config_sha(cfg),
        "signed_by": signed_by,
        "signature": "",
        "approved_live_order_execution": True,
        "symbol": "BTC/USDT",
        "max_notional_usdt": max_notional_usdt,
        "issued_at": issued_at or _iso(now - timedelta(seconds=10)),
        "expires_at": expires_at or _iso(now + timedelta(minutes=4)),
        "acknowledged_risks": acknowledged_risks
        or [
            "one_time_live_cost_probe",
            "immediate_flat_exit",
            "max_open_seconds_60",
            "kill_switch_on_error",
            "sell_only_on_error",
        ],
    }
    payload["signature"] = (
        _authorization_hmac_signature(payload, "unit-test-secret")
        if valid_signature
        else "manual-attestation"
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _write_clean_runtime_state(project_root: Path) -> None:
    runtime_dir = project_root / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "kill_switch.json").write_text(json.dumps({"enabled": False}), encoding="utf-8")
    (runtime_dir / "reconcile_status.json").write_text(
        json.dumps({"ok": True, "generated_ts_ms": 1_788_000_000_000}),
        encoding="utf-8",
    )
    with sqlite3.connect(str(runtime_dir / "orders.sqlite")) as con:
        con.execute("CREATE TABLE IF NOT EXISTS orders (state TEXT)")
    with sqlite3.connect(str(runtime_dir / "positions.sqlite")) as con:
        con.execute("CREATE TABLE IF NOT EXISTS positions (symbol TEXT, qty REAL)")
    with sqlite3.connect(str(runtime_dir / "fills.sqlite")) as con:
        con.execute("CREATE TABLE IF NOT EXISTS fills (id TEXT)")
