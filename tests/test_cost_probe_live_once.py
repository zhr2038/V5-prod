from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from configs.schema import AppConfig
from scripts.cost_probe_live_once import (
    _cost_probe_config_sha,
    _current_code_sha,
    build_live_probe_preflight,
    run_live_probe_once,
)


class _Response:
    def __init__(self, data):
        self.data = data


class _FakeOKX:
    def __init__(self, *, partial_exit: bool = False) -> None:
        self.placed = []
        self.partial_exit = partial_exit
        self.balance_qty = "0"

    def request(self, method, path, *, params=None):
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
        raise AssertionError(f"unexpected request {method} {path}")

    def place_order(self, payload, *, exp_time_ms=None):
        self.placed.append(dict(payload))
        return _Response({"code": "0", "data": [{"sCode": "0", "ordId": f"okx-{len(self.placed)}"}]})

    def get_order(self, *, inst_id, cl_ord_id):
        side = "buy" if cl_ord_id.endswith("E") else "sell"
        fill_qty = "0.000099"
        state = "filled"
        if cl_ord_id.endswith("E"):
            self.balance_qty = "0.000099"
        elif cl_ord_id.endswith("X") and self.partial_exit:
            fill_qty = "0.00005"
            state = "partially_filled"
            self.balance_qty = "0.000049"
        else:
            self.balance_qty = "0"
        return _Response(
            {
                "code": "0",
                "data": [
                    {
                        "instId": inst_id,
                        "clOrdId": cl_ord_id,
                        "ordId": f"okx-{cl_ord_id[-1]}",
                        "side": side,
                        "state": state,
                        "accFillSz": fill_qty,
                        "avgPx": "50010",
                        "fee": "-0.00000001",
                        "feeCcy": "BTC",
                    }
                ],
            }
        )

    def get_balance(self, ccy=None):
        details = (
            [
                {
                    "ccy": "BTC",
                    "availBal": self.balance_qty,
                    "cashBal": self.balance_qty,
                    "eq": self.balance_qty,
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
                    "availBal": self.balance_qty,
                    "cashBal": self.balance_qty,
                    "eq": self.balance_qty,
                    "eqUsd": "0",
                }
            ]
        )
        return _Response(
            {
                "code": "0",
                "data": [
                    {
                        "details": details,
                    }
                ],
            }
        )


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
    assert (tmp_path / "reports" / "cost_probe_order_events.jsonl").exists()
    assert (tmp_path / "reports" / "cost_probe_roundtrip_events.jsonl").exists()
    assert not auth_path.exists()
    assert (tmp_path / "auth.consumed.json").exists()
    assert result["completed"] is True
    assert result["flat_verification"]["flat_verified"] is True


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
    fake = _FakeOKX(partial_exit=True)

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
    assert [order["side"] for order in fake.placed] == ["buy", "sell", "sell"]
    kill_switch = json.loads((tmp_path / "runtime" / "kill_switch.json").read_text(encoding="utf-8"))
    assert kill_switch["enabled"] is True
    assert kill_switch["sell_only_on_error"] is True


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


def _write_auth(tmp_path: Path, *, cfg: AppConfig, acknowledged_risks=None) -> Path:
    path = tmp_path / "auth.pending.json"
    payload = {
        "scope": "v5_cost_probe_live_once",
        "authorization_id": "auth-1",
        "nonce": "nonce-1",
        "code_sha": _current_code_sha(tmp_path),
        "config_sha256": _cost_probe_config_sha(cfg),
        "signed_by": "operator",
        "signature": "manual-attestation",
        "approved_live_order_execution": True,
        "symbol": "BTC/USDT",
        "max_notional_usdt": 5.0,
        "issued_at": "2099-01-01T00:00:00Z",
        "expires_at": "2099-01-01T00:04:00Z",
        "acknowledged_risks": acknowledged_risks
        or [
            "one_time_live_cost_probe",
            "immediate_flat_exit",
            "max_open_seconds_60",
            "kill_switch_on_error",
            "sell_only_on_error",
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


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
