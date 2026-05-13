from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from configs.schema import AppConfig
from src.core.models import Order
from src.quant_lab_client.guard import QuantLabGuard
from src.quant_lab_client.models import CostEstimate, RiskPermission


class _Client:
    phase = "live"

    def __init__(self, *, permission="SELL_ONLY", fail_permission=False, fail_cost=False) -> None:
        self.permission = permission
        self.fail_permission = fail_permission
        self.fail_cost = fail_cost
        self.run_id = "r"

    def get_health(self):
        return SimpleNamespace(status="ok", mode="read-only")

    def get_live_permission(self, *, strategy: str, version: str):
        if self.fail_permission:
            raise RuntimeError("unavailable secret-token")
        return RiskPermission(
            strategy=strategy,
            version=version,
            permission=self.permission,
            allowed_modes=[self.permission.lower()],
            reasons=["required_alpha_gate_quarantine"] if self.permission == "SELL_ONLY" else [],
            cost_model_version="cost_bucket_daily:2026-05-11",
            gate_version="bootstrap.quarantine.v1",
        )

    def estimate_cost(self, *, symbol: str, regime: str, notional_usdt: float, quantile: str, **kwargs):
        if self.fail_cost:
            raise RuntimeError("cost unavailable")
        return CostEstimate(
            symbol=symbol.replace("/", "-"),
            regime=regime,
            notional_usdt=notional_usdt,
            quantile=quantile,
            total_cost_bps=1.0,
            source="public_spread_proxy",
            fallback_level="PUBLIC_SPREAD_PROXY",
            sample_count=100,
            cost_model_version="cost_bucket_daily:2026-05-11",
        )


def _guard(tmp_path: Path, cfg: AppConfig, client: _Client) -> QuantLabGuard:
    return QuantLabGuard(client=client, cfg=cfg.quant_lab, usage_log_path=tmp_path / "usage.jsonl", run_id="run-1")


def test_guard_sell_only_filters_buy_and_preserves_sell(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "enforce"
    guard = _guard(tmp_path, cfg, _Client(permission="SELL_ONLY"))

    result = guard.check_startup_permission(cfg, "run-1")
    kept = guard.filter_orders_by_permission(
        [
            Order("BTC/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {}),
            Order("ETH/USDT", "sell", "CLOSE_LONG", 8.0, 200.0, {}),
        ],
        result,
    )

    assert result.permission == "SELL_ONLY"
    assert [order.symbol for order in kept] == ["ETH/USDT"]
    rows = [json.loads(line) for line in (tmp_path / "usage.jsonl").read_text(encoding="utf-8").splitlines()]
    assert any(row.get("event_type") == "filter_order" and row.get("order_filtered") for row in rows)


def test_guard_abort_filters_all(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "enforce"
    guard = _guard(tmp_path, cfg, _Client(permission="ABORT"))
    result = guard.check_startup_permission(cfg, "run-1")
    kept = guard.filter_orders_by_permission([Order("ETH/USDT", "sell", "CLOSE_LONG", 8.0, 200.0, {})], result)
    assert kept == []


def test_guard_audits_api_env_file_status(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    client = _Client(permission="ALLOW")
    client.api_env_path_present = True
    client.api_env_secure_permissions = False
    client.api_env_token_loaded = False
    client.api_env_warning = "api_env_permissions_too_open:0644"
    guard = _guard(tmp_path, cfg, client)

    guard.check_startup_permission(cfg, "run-1")

    row = json.loads((tmp_path / "usage.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["api_env_path_present"] is True
    assert row["api_env_secure_permissions"] is False
    assert row["api_env_token_loaded"] is False
    assert row["api_env_warning"] == "api_env_permissions_too_open:0644"
    summary = guard.summary_payload()
    assert summary["api_env_path_present"] is True
    assert summary["api_env_secure_permissions"] is False
    assert summary["api_env_token_loaded"] is False


def test_guard_unavailable_fail_policies(tmp_path: Path) -> None:
    for policy, expected, fallback in [
        ("sell_only", "SELL_ONLY", True),
        ("abort", "ABORT", True),
        ("allow_local_fallback", "ALLOW", True),
    ]:
        cfg = AppConfig()
        cfg.quant_lab.enabled = True
        cfg.quant_lab.fail_policy = policy
        guard = _guard(tmp_path / policy, cfg, _Client(fail_permission=True))
        result = guard.check_startup_permission(cfg, "run-1")
        assert result.permission == expected
        assert result.fallback_used is fallback


def test_guard_cost_fallback_to_local(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "enforce"
    cfg.execution.fee_bps = 10
    cfg.execution.slippage_bps = 5
    cfg.execution.cost_aware_roundtrip_cost_bps = None
    guard = _guard(tmp_path, cfg, _Client(permission="ALLOW", fail_cost=True))
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost(
        [Order("BTC/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 60})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    assert rows[0]["fallback_used"] is True
    assert rows[0]["total_cost_bps"] == 30.0
    assert rows[0]["effective_total_cost_bps"] == 30.0
    assert rows[0]["local_cost_bps"] == 30.0
    assert rows[0]["local_cost_source"] == "roundtrip_fee_slippage"
