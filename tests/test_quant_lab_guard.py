from __future__ import annotations

import csv
import datetime as dt
import json
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from src.core.models import Order
from src.execution.quant_lab_client import QuantLabClient, QuantLabResponse
from src.execution.quant_lab_guard import QuantLabGuard
from src.reporting import metrics, summary_writer


class _Response:
    status_code = 200
    text = '{"status":"ok"}'

    def json(self):
        return {"status": "ok"}


class _FakeSession:
    def __init__(self) -> None:
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return _Response()


def test_quant_lab_client_uses_get_and_redacts_token(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("QUANT_LAB_API_TOKEN", "super-secret-token")
    session = _FakeSession()
    log_path = tmp_path / "quant_lab_requests.jsonl"
    client = QuantLabClient(
        base_url="http://quant-lab.local",
        request_log_path=log_path,
        run_id="run-1",
        session=session,
    )

    response = client.health()

    assert response.ok is True
    assert session.calls[0]["url"] == "http://quant-lab.local/v1/health"
    assert session.calls[0]["headers"]["Authorization"] == "Bearer super-secret-token"
    text = log_path.read_text(encoding="utf-8")
    assert "super-secret-token" not in text
    row = json.loads(text)
    assert row["method"] == "GET"
    assert row["auth_present"] is True


class _FakeQuantLabClient:
    phase = "live_preflight"

    def __init__(self, *, permission_ok=True, permission=None, cost_ok=True):
        self.permission_ok = permission_ok
        self.permission = permission or {"permission": "SELL_ONLY"}
        self.cost_ok = cost_ok
        self.cost_calls = []

    def health(self):
        return QuantLabResponse(endpoint="/v1/health", ok=True, data={"status": "ok"})

    def live_permission(self, *, strategy="v5", version="v1"):
        return QuantLabResponse(
            endpoint="/v1/risk/live-permission",
            ok=self.permission_ok,
            status_code=200 if self.permission_ok else 503,
            data=self.permission,
            error=None if self.permission_ok else "http_503",
        )

    def cost_estimate(self, **kwargs):
        self.cost_calls.append(kwargs)
        return QuantLabResponse(
            endpoint="/v1/costs/estimate",
            ok=self.cost_ok,
            status_code=200 if self.cost_ok else 503,
            data={"total_bps": 12.5, "source": "public_spread_proxy"} if self.cost_ok else {},
            error=None if self.cost_ok else "http_503",
        )

    def gate_decision(self, alpha_id):
        return QuantLabResponse(endpoint=f"/v1/gates/decision/{alpha_id}", ok=True, data={"decision": "ALLOW"})


def test_quant_lab_guard_sell_only_filters_buy_and_preserves_sell(tmp_path: Path) -> None:
    client = _FakeQuantLabClient(permission={"permission": "SELL_ONLY"})
    guard = QuantLabGuard(
        client=client,
        fail_policy="sell_only",
        usage_log_path=tmp_path / "quant_lab_usage.jsonl",
        run_id="run-2",
    )
    assert guard.refresh_permission() == "SELL_ONLY"

    orders = [
        Order("BTC/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {}),
        Order("ETH/USDT", "sell", "CLOSE_LONG", 8.0, 200.0, {}),
    ]
    filtered, summary = guard.filter_orders(orders)

    assert [order.symbol for order in filtered] == ["ETH/USDT"]
    assert summary["orders_filtered"] == 1
    assert summary["cost_estimate_count"] == 2
    assert orders[0].meta["quant_lab"]["filtered"] is True
    assert orders[1].meta["quant_lab"]["cost"]["cost_source"] == "public_spread_proxy"
    rows = [
        json.loads(line)
        for line in (tmp_path / "quant_lab_usage.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(row.get("event_type") == "permission" for row in rows)
    assert any(row.get("event_type") == "order_filter" and row.get("filtered") for row in rows)


def test_quant_lab_guard_unavailable_uses_fail_policy_sell_only(tmp_path: Path) -> None:
    client = _FakeQuantLabClient(permission_ok=False, permission={})
    guard = QuantLabGuard(
        client=client,
        fail_policy="sell_only",
        usage_log_path=tmp_path / "quant_lab_usage.jsonl",
        run_id="run-3",
    )

    assert guard.refresh_permission() == "SELL_ONLY"
    filtered, summary = guard.filter_orders(
        [Order("BTC/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {})]
    )

    assert filtered == []
    assert summary["fallback_used"] is True
    assert guard.filtered_orders[0]["filter_reason"] == "quant_lab_permission_fallback_sell_only"


def test_summary_writer_includes_quant_lab_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)
    run_dir = tmp_path / "reports" / "runs" / "test_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        json.dumps({"ts": "2026-05-11T00:00:00Z", "equity": 100.0}) + "\n",
        encoding="utf-8",
    )
    (run_dir / "trades.csv").write_text(
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct\n",
        encoding="utf-8",
    )
    (run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "quant_lab": {
                    "enabled": True,
                    "permission": {
                        "decision": "SELL_ONLY",
                        "effective_decision": "SELL_ONLY",
                        "fallback_used": False,
                        "fail_policy": "sell_only",
                    },
                    "cost_estimates": [{"fallback_used": False}, {"fallback_used": True}],
                    "filtered_orders": [
                        {"filtered": True, "side": "buy"},
                        {"filtered": False, "side": "sell"},
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/test_run")

    assert summary["quant_lab"]["enabled"] is True
    assert summary["quant_lab"]["permission_decision"] == "SELL_ONLY"
    assert summary["quant_lab"]["orders_filtered"] == 1
    assert summary["quant_lab"]["cost_fallback_count"] == 1


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _extract_member(tf: tarfile.TarFile, suffix: str) -> str:
    matches = [name for name in tf.getnames() if name.endswith(suffix)]
    assert matches, suffix
    return matches[0]


def _bash_path(path: Path) -> str:
    text = str(path)
    if len(text) >= 3 and text[1] == ":":
        tail = text[3:].replace("\\", "/")
        return f"/mnt/{text[0].lower()}/{tail}"
    return text.replace("\\", "/")


def _windows_path_from_bash(path_text: str) -> Path:
    if os.name == "nt" and path_text.startswith("/") and shutil.which("wsl.exe"):
        converted = subprocess.check_output(["wsl.exe", "wslpath", "-w", path_text], text=True).strip()
        return Path(converted)
    return Path(path_text)


def test_bundle_includes_quant_lab_standard_telemetry(tmp_path: Path) -> None:
    if shutil.which("bash") is None:
        pytest.skip("bash is required for generate_v5_bundle_remote.sh")

    root = tmp_path / "root"
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    run_id = now.strftime("%Y%m%d_%H")
    _write_text(root / "configs/live_prod.yaml", "execution:\n  quant_lab_enabled: true\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        _write_json(root / "reports" / f"{name}.json", {"ok": True})
    _write_json(root / "reports/effective_live_config.json", {"execution": {"quant_lab_enabled": True}})
    _write_text(root / "logs/v5_runtime.log", "fixture log\n")
    audit = {
        "now_ts": int(now.timestamp()),
        "window_end_ts": int(now.timestamp()),
        "router_decisions": [],
        "quant_lab": {
            "enabled": True,
            "permission": {
                "decision": "SELL_ONLY",
                "effective_decision": "SELL_ONLY",
                "fallback_used": False,
                "fail_policy": "sell_only",
            },
            "cost_estimates": [
                {
                    "symbol": "BTC/USDT",
                    "side": "buy",
                    "intent": "OPEN_LONG",
                    "notional_usdt": 10.0,
                    "alpha_id": "v5_live",
                    "cost_bps": 12.5,
                    "cost_source": "public_spread_proxy",
                    "fallback_used": False,
                }
            ],
            "filtered_orders": [
                {
                    "symbol": "BTC/USDT",
                    "side": "buy",
                    "intent": "OPEN_LONG",
                    "filtered": True,
                    "order_decision": "SELL_ONLY",
                    "filter_reason": "quant_lab_permission_sell_only",
                }
            ],
        },
    }
    run_dir = root / "reports" / "runs" / "prod" / run_id
    _write_json(run_dir / "decision_audit.json", audit)
    _write_json(run_dir / "summary.json", {"run_id": run_id, "quant_lab": {"enabled": True}})
    _write_text(run_dir / "equity.jsonl", "{}\n")
    _write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    _write_text(
        root / "reports" / "quant_lab_usage.jsonl",
        json.dumps(
            {
                "ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "run_id": run_id,
                "event_type": "permission",
                "quant_lab_decision": "SELL_ONLY",
                "effective_decision": "SELL_ONLY",
                "fail_policy": "sell_only",
                "fallback_used": False,
            }
        )
        + "\n",
    )
    _write_text(
        root / "reports" / "quant_lab_requests.jsonl",
        json.dumps(
            {
                "ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "run_id": run_id,
                "method": "GET",
                "endpoint": "/v1/risk/live-permission",
                "ok": True,
                "auth_present": False,
            }
        )
        + "\n",
    )

    script = Path(__file__).resolve().parents[1] / "scripts" / "generate_v5_bundle_remote.sh"
    proc = subprocess.run(
        ["bash", _bash_path(script), _bash_path(root)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    bundle_path = None
    for line in proc.stdout.splitlines():
        if line.startswith("BUNDLE_PATH="):
            bundle_path = _windows_path_from_bash(line.split("=", 1)[1])
            break
    assert bundle_path is not None and bundle_path.is_file(), proc.stdout + proc.stderr
    try:
        with tarfile.open(bundle_path, "r:gz") as tf:
            names = tf.getnames()
            assert any(name.endswith("raw/reports/quant_lab_usage.jsonl") for name in names)
            assert any(name.endswith("raw/reports/quant_lab_requests.jsonl") for name in names)
            compliance = list(
                csv.DictReader(
                    tf.extractfile(_extract_member(tf, "summaries/quant_lab_compliance.csv"))
                    .read()
                    .decode()
                    .splitlines()
                )
            )
            costs = list(
                csv.DictReader(
                    tf.extractfile(_extract_member(tf, "summaries/quant_lab_cost_usage.csv"))
                    .read()
                    .decode()
                    .splitlines()
                )
            )
            fallbacks = list(
                csv.DictReader(
                    tf.extractfile(_extract_member(tf, "summaries/quant_lab_fallbacks.csv"))
                    .read()
                    .decode()
                    .splitlines()
                )
            )
        assert any(row["effective_decision"] == "SELL_ONLY" for row in compliance)
        assert any(row["symbol"] == "BTC/USDT" and row["cost_bps"] == "12.5" for row in costs)
        assert fallbacks == []
    finally:
        bundle_path.unlink(missing_ok=True)
        Path(f"{bundle_path}.sha256").unlink(missing_ok=True)
        shutil.rmtree(Path("/tmp") / bundle_path.name.removesuffix(".tar.gz"), ignore_errors=True)
