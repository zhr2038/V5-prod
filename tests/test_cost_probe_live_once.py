from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback.
    fcntl = None  # type: ignore[assignment]

from configs.schema import AppConfig
from scripts.cost_probe_live_once import (
    _authorization_hmac_signature,
    _consume_authorization_file,
    _cost_probe_config_sha,
    _current_code_sha,
    _persist_live_execution_status,
    _persist_preflight_snapshot,
    _reconcile_probe_dust_accepted,
    _roundtrip_cost_fields,
    build_live_probe_preflight,
    run_live_probe_once,
)
from scripts.create_cost_probe_authorization import build_authorization_payload
from src.execution.account_store import AccountStore
from src.execution.position_store import PositionStore


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
        quote_balance: str = "100",
        ticker_bids: list[str] | None = None,
        unknown_fee_ccy: bool = False,
        raise_on_emergency_flatten: bool = False,
        base_fee_qty: str = "0.00000001",
        lot_sz: str = "0.000001",
        sell_residual_qty: str | None = None,
    ) -> None:
        self.placed: list[dict] = []
        self.partial_exit = partial_exit
        self.balance_qty = Decimal(initial_base_balance)
        self.base_fee_on_entry = base_fee_on_entry
        self.raise_after_entry = raise_after_entry
        self.entry_get_order_fails = entry_get_order_fails
        self.quote_balance = Decimal(quote_balance)
        self.ticker_bids = list(ticker_bids or ["49990"])
        self.unknown_fee_ccy = unknown_fee_ccy
        self.raise_on_emergency_flatten = raise_on_emergency_flatten
        self.base_fee_qty = Decimal(base_fee_qty)
        self.lot_sz = str(lot_sz)
        self.sell_residual_qty = (
            Decimal(sell_residual_qty) if sell_residual_qty is not None else None
        )
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
                            "state": "live",
                            "minSz": "0.00001",
                            "lotSz": self.lot_sz,
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
                            "bidPx": self._next_bid(),
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
        if order["side"] == "sell" and "F" in str(order.get("clOrdId") or "") and self.raise_on_emergency_flatten:
            raise RuntimeError("emergency_flatten_submit_failed")
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

    def get_order_fills(self, *, inst_id, ord_id, cl_ord_id):
        order = self.orders_by_clid.get(cl_ord_id)
        if not order:
            return _Response({"code": "0", "data": []})
        return _Response({"code": "0", "data": [self._fill_row(order)]})

    def cancel_order(self, *, inst_id, ord_id=None, cl_ord_id=None):
        payload = {"instId": inst_id, "ordId": ord_id or "", "clOrdId": cl_ord_id or ""}
        self.cancels.append(payload)
        return _Response({"code": "0", "data": [{"sCode": "0"}]})

    def _settle_order(self, order: dict) -> _Response:
        cl_ord_id = str(order.get("clOrdId") or "")
        side = str(order.get("side") or "")
        fill_qty = self._fill_qty(order)
        state = "filled"
        if cl_ord_id.endswith("X") and self.partial_exit:
            state = "partially_filled"
        fee = -self.base_fee_qty if side == "buy" and self.base_fee_on_entry else Decimal("-0.01")
        fee_ccy = "OKB" if self.unknown_fee_ccy else ("BTC" if side == "buy" and self.base_fee_on_entry else "USDT")
        if cl_ord_id not in self.settled_clids:
            if side == "buy":
                self.balance_qty += fill_qty + (fee if fee_ccy == "BTC" else Decimal("0"))
            elif side == "sell":
                next_balance = max(self.balance_qty - fill_qty, Decimal("0"))
                self.balance_qty = (
                    max(next_balance, self.sell_residual_qty)
                    if self.sell_residual_qty is not None
                    else next_balance
                )
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

    def _fill_qty(self, order: dict) -> Decimal:
        qty = Decimal(str(order.get("sz") or "0.000099"))
        if str(order.get("clOrdId") or "").endswith("X") and self.partial_exit:
            return min(qty, Decimal("0.00005"))
        return qty

    def _fill_row(self, order: dict) -> dict:
        cl_ord_id = str(order.get("clOrdId") or "")
        side = str(order.get("side") or "")
        fee = -self.base_fee_qty if side == "buy" and self.base_fee_on_entry else Decimal("-0.01")
        fee_ccy = "OKB" if self.unknown_fee_ccy else ("BTC" if side == "buy" and self.base_fee_on_entry else "USDT")
        return {
            "instId": str(order.get("instId") or "BTC-USDT"),
            "clOrdId": cl_ord_id,
            "ordId": f"okx-{cl_ord_id[-1]}",
            "tradeId": f"trade-{cl_ord_id}",
            "side": side,
            "fillSz": format(self._fill_qty(order), "f"),
            "fillPx": "50010",
            "fee": format(fee, "f"),
            "feeCcy": fee_ccy,
        }

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
                    "availBal": format(self.quote_balance if str(ccy).upper() == "USDT" else self.balance_qty, "f"),
                    "cashBal": format(self.quote_balance if str(ccy).upper() == "USDT" else self.balance_qty, "f"),
                    "eq": format(self.quote_balance if str(ccy).upper() == "USDT" else self.balance_qty, "f"),
                    "eqUsd": format(self.quote_balance if str(ccy).upper() == "USDT" else Decimal("0"), "f"),
                }
            ]
        )
        return _Response({"code": "0", "data": [{"details": details}]})

    def _next_bid(self) -> str:
        if len(self.ticker_bids) > 1:
            return self.ticker_bids.pop(0)
        return self.ticker_bids[0]


class _InspectingOKX(_FakeOKX):
    def __init__(self, status_path: Path) -> None:
        super().__init__()
        self.status_path = status_path
        self.intent_status: dict | None = None

    def place_order(self, payload, *, exp_time_ms=None):
        if payload["side"] == "buy":
            self.intent_status = json.loads(self.status_path.read_text(encoding="utf-8"))
        return super().place_order(payload, exp_time_ms=exp_time_ms)


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
    status_path = _persist_live_execution_status(result, tmp_path / "reports")
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["status"] == "AUTH_VALIDATED"
    assert status["authorization_validated"] is True
    assert status["no_order_submitted"] is True
    assert status["approved_live_order_execution"] is False
    assert status["instrument_preflight_passed"] is True
    assert status["instrument_state"] == "live"
    assert status["quote_balance_sufficient"] is True
    assert status["quote_balance"] == "100"
    assert status["quote_required"] == "5.05"
    assert status["order_plan"]["base_qty"] == "0.000099"


def test_cost_probe_live_once_persists_latest_p3_preflight_snapshot(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "cost_probe_summary.json").write_text(
        json.dumps(
            {
                "state": "NO_PLAN_ROWS",
                "dry_run": True,
                "live_enabled": False,
                "no_order_submitted": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    result = {
        "p3_preflight": {
            "state": "READY_FOR_MANUAL_AUTHORIZATION",
            "offline_plan_state": "NO_PLAN_ROWS",
            "online_exchange_preflight_state": "READY_FOR_MANUAL_AUTHORIZATION",
            "effective_preflight_state": "READY_FOR_MANUAL_AUTHORIZATION",
            "manual_probe_symbol": "BTC/USDT",
            "approved_live_order_execution": False,
            "ready_to_request_manual_live_probe": True,
            "blockers": [],
        }
    }

    path = _persist_preflight_snapshot(result, reports)

    assert path == tmp_path / "reports" / "cost_probe_p3_preflight.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["state"] == "READY_FOR_MANUAL_AUTHORIZATION"
    assert payload["manual_probe_symbol"] == "BTC/USDT"
    assert payload["approved_live_order_execution"] is False
    summary = json.loads((reports / "cost_probe_summary.json").read_text(encoding="utf-8"))
    assert summary["state"] == "NO_PLAN_ROWS"
    assert summary["offline_plan_state"] == "NO_PLAN_ROWS"
    assert summary["online_exchange_preflight_state"] == "READY_FOR_MANUAL_AUTHORIZATION"
    assert summary["effective_preflight_state"] == "READY_FOR_MANUAL_AUTHORIZATION"
    assert summary["effective_preflight_ready"] is True


def test_cost_probe_live_once_persists_live_execution_status_for_closed_flat(tmp_path: Path) -> None:
    result = {
        "state": "COMPLETED",
        "manual_probe_symbol": "BTC/USDT",
        "authorization_id": "auth-1",
        "execution_completed": True,
        "entry_order_id": "entry-1",
        "exit_order_id": "exit-1",
        "entry_filled_qty": "0.0001",
        "exit_filled_qty": "0.0001",
        "flat_verified": True,
        "exchange_flat_verified": True,
        "local_flat_verified": True,
        "reconcile_ok": True,
        "cost_evidence_complete": True,
        "eligible_for_cost_model": True,
        "eligible_for_live_cost_coverage": False,
        "source": "bootstrap_cost_probe",
        "sample_origin": "cost_probe",
    }

    path = _persist_live_execution_status(result, tmp_path / "reports")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["status"] == "CLOSED_FLAT"
    assert payload["entry_submitted"] is True
    assert payload["exit_submitted"] is True
    assert payload["execution_completed"] is True
    assert payload["eligible_for_live_cost_coverage"] is False


def test_cost_probe_live_execution_status_uses_recovered_order_fills(tmp_path: Path) -> None:
    result = {
        "state": "RECOVERY_ONLY",
        "execution_status": "RECOVERY_ONLY",
        "manual_probe_symbol": "BTC/USDT",
        "authorization_id": "auth-1",
        "authorization_consumed": True,
        "entry_order_id": "entry-1",
        "exit_order_id": "exit-1",
        "entry_state": {"accFillSz": "0.00007747"},
        "exit_state": {"accFillSz": "0.00007739"},
        "recovery_required": True,
    }

    path = _persist_live_execution_status(result, tmp_path / "reports")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["entry_filled"] is True
    assert payload["exit_filled"] is True
    assert payload["entry_filled_qty"] == "0.00007747"
    assert payload["exit_filled_qty"] == "0.00007739"


def test_create_cost_probe_authorization_payload_signs_required_context() -> None:
    now = datetime(2026, 6, 20, 12, tzinfo=UTC)
    preflight = {
        "required_authorization": {
            "code_sha": "abc123",
            "config_sha256": "cfg456",
            "symbol": "BTC/USDT",
            "max_notional_usdt": "5",
        }
    }

    payload = build_authorization_payload(
        preflight=preflight,
        signed_by="operator",
        secret="unit-test-secret",
        ttl_sec=300,
        authorization_id="auth-1",
        nonce="nonce-1",
        now=now,
    )

    assert payload["scope"] == "v5_cost_probe_live_once"
    assert payload["approved_live_order_execution"] is True
    assert payload["symbol"] == "BTC/USDT"
    assert payload["max_notional_usdt"] == "5"
    assert payload["signature"] == _authorization_hmac_signature(payload, "unit-test-secret")
    assert payload["issued_at"] == "2026-06-20T12:00:00Z"
    assert payload["expires_at"] == "2026-06-20T12:05:00Z"


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


def test_cost_probe_live_once_signature_covers_operator_scope_and_approval(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    payload = json.loads(auth_path.read_text(encoding="utf-8"))
    payload["approved_live_order_execution"] = False
    payload["signed_by"] = "intruder"
    auth_path.write_text(json.dumps(payload), encoding="utf-8")

    result = build_live_probe_preflight(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=_FakeOKX(),
        project_root=tmp_path,
    )

    assert result["state"] == "NOT_READY"
    assert "manual_authorization_not_approved" in result["blockers"]
    assert "manual_authorization_signed_by_not_allowed" in result["blockers"]
    assert "manual_authorization_signature_invalid" in result["blockers"]


def test_cost_probe_live_once_consume_revalidates_authorization_under_lock(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    preflight = build_live_probe_preflight(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=_FakeOKX(),
        project_root=tmp_path,
    )
    payload = json.loads(auth_path.read_text(encoding="utf-8"))
    payload["approved_live_order_execution"] = False
    payload["signature"] = _authorization_hmac_signature(payload, "unit-test-secret")
    auth_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="manual_authorization_changed_after_preflight"):
        _consume_authorization_file(auth_path, preflight=preflight, cfg=cfg, project_root=tmp_path)

    assert auth_path.exists()
    assert not (tmp_path / "auth.consumed.json").exists()


def test_cost_probe_live_once_blocks_insufficient_quote_balance(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)

    result = build_live_probe_preflight(
        cfg,
        reports_dir=tmp_path / "reports",
        auth_path=auth_path,
        okx=_FakeOKX(quote_balance="5"),
        project_root=tmp_path,
    )

    assert result["state"] == "NOT_READY"
    assert "quote_balance_insufficient_for_authorized_notional" in result["blockers"]


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
    roundtrip = json.loads((tmp_path / "reports" / "cost_probe_roundtrip_events.jsonl").read_text().splitlines()[-1])
    assert roundtrip["cost_evidence_complete"] is True
    assert roundtrip["entry_has_fill_rows"] == "true"
    assert roundtrip["exit_has_fill_rows"] == "true"
    assert roundtrip["entry_has_fill_evidence"] == "true"
    assert roundtrip["exit_has_fill_evidence"] == "true"
    assert "entry_fee_usdt_applied_to_net" in roundtrip


def test_cost_probe_live_once_persists_entry_submit_intent_before_order(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    status_path = tmp_path / "reports" / "cost_probe_live_execution_status.json"
    fake = _InspectingOKX(status_path)

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
    assert fake.intent_status is not None
    assert fake.intent_status["status"] == "ENTRY_SUBMIT_INTENT"
    assert fake.intent_status["authorization_consumed"] is True
    assert fake.intent_status["entry_client_order_id"].endswith("E")
    assert fake.intent_status["recovery_required"] is True
    final_status = json.loads(status_path.read_text(encoding="utf-8"))
    assert final_status["status"] == "CLOSED_FLAT"
    assert final_status["recovery_required"] is False


def test_cost_probe_live_once_blocks_new_probe_when_prior_status_requires_recovery(
    tmp_path: Path,
) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "cost_probe_live_execution_status.json").write_text(
        json.dumps(
            {
                "status": "ENTRY_SUBMITTED",
                "manual_probe_symbol": "BTC/USDT",
                "authorization_id": "old-auth",
                "authorization_nonce": "old-nonce",
                "entry_client_order_id": "cpoldE",
                "execution_completed": False,
                "flat_verified": False,
            }
        ),
        encoding="utf-8",
    )
    fake = _FakeOKX()

    result = run_live_probe_once(
        cfg,
        reports_dir=reports,
        auth_path=auth_path,
        okx=fake,
        project_root=tmp_path,
        execute_live_order=True,
        operator_confirmed=True,
    )

    assert result["state"] == "NOT_READY"
    assert result["execution_status"] == "RECOVERY_REQUIRED"
    assert result["blockers"] == ["cost_probe_recovery_required"]
    assert fake.placed == []
    assert auth_path.exists()


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


def test_cost_probe_live_once_accepts_base_fee_net_exit_as_flat(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX(
        base_fee_on_entry=True,
        base_fee_qty="0.00000008",
        lot_sz="0.00000001",
    )

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
    assert result["flat_verification"]["entry_base_fee_qty"] == "0.00000008"
    assert result["flat_verification"]["entry_base_fee_reflected_in_exit_qty"] is True
    assert result["flat_verification"]["flat_verified"] is True


def test_cost_probe_live_once_accepts_base_fee_dust_residual_as_flat(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX(
        base_fee_on_entry=True,
        base_fee_qty="0.00000008",
        lot_sz="0.00000001",
        sell_residual_qty="0.00000001134",
    )

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
    assert result["flat_verification"]["exchange_base_delta_from_baseline"] == "0.00000001134"
    assert result["flat_verification"]["base_flat_tolerance"] == "0.00000008"
    assert result["flat_verification"]["exchange_flat_verified"] is True
    assert result["flat_verification"]["reconcile_ok"] is True


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
    assert result["p3_preflight"]["instrument_minimum_verified"] is True
    assert result["p3_preflight"]["exchange_min_notional_verified"] is True
    assert result["p3_preflight"]["offline_plan_state"] == "NO_PLAN_ROWS"
    assert result["p3_preflight"]["online_exchange_preflight_state"] == "READY_FOR_MANUAL_AUTHORIZATION"
    assert result["p3_preflight"]["effective_preflight_state"] == "READY_FOR_MANUAL_AUTHORIZATION"
    assert result["p3_preflight"]["exit_policy"] == "immediate_flat"
    assert result["p3_preflight"]["max_open_seconds"] == 60
    assert (
        result["p3_preflight"]["next_action"]
        == "create_signed_authorization_and_run_no_order_validation"
    )
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


def test_cost_probe_live_once_records_kill_switch_when_emergency_flatten_throws(
    tmp_path: Path,
) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX(
        partial_exit=True,
        initial_base_balance="0.000003",
        raise_on_emergency_flatten=True,
    )

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
    assert result["emergency_flatten"]["status"] == "emergency_flatten_error"
    kill_switch = json.loads((tmp_path / "runtime" / "kill_switch.json").read_text(encoding="utf-8"))
    assert kill_switch["enabled"] is True
    assert kill_switch["sell_only_on_error"] is True


def test_cost_probe_live_once_emergency_flatten_refreshes_exit_bid(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX(partial_exit=True, initial_base_balance="0.000003", ticker_bids=["49990", "49700"])

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
    assert fake.placed[-1]["side"] == "sell"
    assert fake.placed[-1]["px"] == "49650.3"
    assert result["emergency_flatten"]["attempts"][0]["fresh_bid_px"] == "49700"


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
    lock_fd = None
    if fcntl is None:
        lock_path.write_text("held\n", encoding="utf-8")
    else:
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fake = _FakeOKX()

    try:
        result = run_live_probe_once(
            cfg,
            reports_dir=tmp_path / "reports",
            auth_path=auth_path,
            okx=fake,
            project_root=tmp_path,
            execute_live_order=True,
            operator_confirmed=True,
        )
    finally:
        if lock_fd is not None:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    assert result["state"] == "NOT_READY"
    assert "cost_probe_global_execution_lock_held" in result["blockers"]
    assert fake.placed == []
    assert auth_path.exists()


def test_cost_probe_live_once_revalidates_runtime_guards_under_global_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX()

    class DirtyRuntimeLock:
        def __enter__(self):
            (tmp_path / "runtime" / "kill_switch.json").write_text(
                json.dumps({"enabled": True, "reason": "dirty_under_lock"}),
                encoding="utf-8",
            )
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(
        "scripts.cost_probe_live_once._global_probe_execution_lock",
        lambda: DirtyRuntimeLock(),
    )

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
    assert result["live_order_effect"] == "none_runtime_revalidation_failed_no_order"
    assert result["authorization_consumed"] is False
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


def test_cost_probe_live_once_reconcile_dust_acceptance_is_reason_whitelisted() -> None:
    accepted = {
        "ok": False,
        "reason": "probe_dust_only",
        "diffs": [
            {"ccy": "BTC", "delta": "0.00000001", "enforced": True},
            {"ccy": "USDT", "delta": "0.01", "enforced": True},
        ],
    }
    stale = {
        **accepted,
        "reason": "probe_dust_only",
        "detail": "stale reconcile snapshot",
    }
    usdt_mismatch = {
        **accepted,
        "reason": "usdt_mismatch",
    }
    other_symbol = {
        **accepted,
        "diffs": [
            {"ccy": "BTC", "delta": "0.00000001", "enforced": True},
            {"ccy": "ETH", "delta": "0.1", "enforced": True},
        ],
    }

    assert _reconcile_probe_dust_accepted(
        accepted,
        symbol="BTC/USDT",
        base_tolerance=Decimal("0.000001"),
        quote_tolerance=Decimal("1"),
    )
    assert not _reconcile_probe_dust_accepted(
        stale,
        symbol="BTC/USDT",
        base_tolerance=Decimal("0.000001"),
        quote_tolerance=Decimal("1"),
    )
    assert not _reconcile_probe_dust_accepted(
        usdt_mismatch,
        symbol="BTC/USDT",
        base_tolerance=Decimal("0.000001"),
        quote_tolerance=Decimal("1"),
    )
    assert not _reconcile_probe_dust_accepted(
        other_symbol,
        symbol="BTC/USDT",
        base_tolerance=Decimal("0.000001"),
        quote_tolerance=Decimal("1"),
    )


def test_cost_probe_roundtrip_cost_converts_base_fee_to_usdt() -> None:
    cost = _roundtrip_cost_fields(
        {
            "instId": "BTC-USDT",
            "_fills": [
                {
                    "fillSz": "0.001",
                    "fillPx": "50000",
                    "fee": "-0.000001",
                    "feeCcy": "BTC",
                }
            ],
        },
        {
            "instId": "BTC-USDT",
            "_fills": [
                {
                    "fillSz": "0.001",
                    "fillPx": "50000",
                    "fee": "-0.05",
                    "feeCcy": "USDT",
                }
            ],
        },
        "BTC/USDT",
    )

    assert cost["entry_fee_usdt"] == "-0.05"
    assert cost["exit_fee_usdt"] == "-0.05"
    assert cost["net_pnl_usdt"] == "-0.1"
    assert cost["fee_conversion_warnings"] == ""
    assert cost["cost_evidence_complete"] == "true"
    assert cost["entry_has_fill_rows"] == "true"
    assert cost["exit_has_fill_rows"] == "true"
    assert cost["entry_has_fill_evidence"] == "true"
    assert cost["exit_has_fill_evidence"] == "true"


def test_cost_probe_roundtrip_cost_does_not_double_count_reflected_base_fee() -> None:
    cost = _roundtrip_cost_fields(
        {
            "instId": "BTC-USDT",
            "_fills": [
                {
                    "fillSz": "0.001",
                    "fillPx": "50000",
                    "fee": "-0.000001",
                    "feeCcy": "BTC",
                }
            ],
        },
        {
            "instId": "BTC-USDT",
            "_fills": [
                {
                    "fillSz": "0.000999",
                    "fillPx": "50000",
                    "fee": "-0.05",
                    "feeCcy": "USDT",
                }
            ],
        },
        "BTC/USDT",
    )

    assert cost["entry_fee_usdt"] == "-0.05"
    assert cost["entry_fee_usdt_applied_to_net"] == "0"
    assert cost["entry_base_fee_reflected_in_exit_qty"] == "true"
    assert cost["entry_base_fee_ledger_adjustment_usdt"] == "0.05"
    assert cost["net_pnl_usdt"] == "-0.1"
    assert cost["cost_evidence_complete"] == "true"


def test_cost_probe_roundtrip_cost_accepts_order_detail_fill_evidence() -> None:
    cost = _roundtrip_cost_fields(
        {
            "instId": "BTC-USDT",
            "accFillSz": "0.00007747",
            "avgPx": "64469.2",
            "fillSz": "0.00007747",
            "fillPx": "64469.2",
            "tradeId": "entry-trade",
            "fillTime": "1782049315199",
            "fee": "-0.00000007747",
            "feeCcy": "BTC",
        },
        {
            "instId": "BTC-USDT",
            "accFillSz": "0.00007739",
            "avgPx": "64469.1",
            "fillSz": "0.00007739",
            "fillPx": "64469.1",
            "tradeId": "exit-trade",
            "fillTime": "1782049316894",
            "fee": "-0.004989263649",
            "feeCcy": "USDT",
        },
        "BTC/USDT",
    )

    assert cost["entry_has_fill_rows"] == "false"
    assert cost["exit_has_fill_rows"] == "false"
    assert cost["entry_has_fill_evidence"] == "true"
    assert cost["exit_has_fill_evidence"] == "true"
    assert cost["fee_conversion_warnings"] == ""
    assert cost["cost_evidence_complete"] == "true"


def test_cost_probe_roundtrip_with_unknown_fee_is_not_cost_model_eligible(tmp_path: Path) -> None:
    cfg = _ready_cost_probe_config(tmp_path)
    _write_clean_runtime_state(tmp_path)
    auth_path = _write_auth(tmp_path, cfg=cfg)
    fake = _FakeOKX(unknown_fee_ccy=True)

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
    assert result["execution_completed"] is True
    assert result["cost_evidence_complete"] is False
    assert result["eligible_for_cost_model"] is False
    assert "fee_ccy_conversion_unavailable:OKB" in result["fee_conversion_warnings"]


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
    PositionStore(path=str(runtime_dir / "positions.sqlite"))
    AccountStore(path=str(runtime_dir / "positions.sqlite"))
    with sqlite3.connect(str(runtime_dir / "fills.sqlite")) as con:
        con.execute("CREATE TABLE IF NOT EXISTS fills (id TEXT)")
