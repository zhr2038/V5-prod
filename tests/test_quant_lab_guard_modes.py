from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from configs.schema import AppConfig
from src.core.models import Order
from src.quant_lab_client.guard import QuantLabGuard
from src.quant_lab_client.models import CostEstimate, RiskPermission


class _ModeClient:
    phase = "live"

    def __init__(self, *, permission: str = "SELL_ONLY") -> None:
        self.permission = permission
        self.permission_calls = 0
        self.cost_calls = 0
        self.run_id = "run-mode"

    def get_health(self):
        return SimpleNamespace(status="ok", mode="read-only")

    def get_live_permission(self, *, strategy: str, version: str):
        self.permission_calls += 1
        return RiskPermission(
            strategy=strategy,
            version=version,
            permission=self.permission,
            reasons=["required_alpha_gate_quarantine"] if self.permission == "SELL_ONLY" else [],
        )

    def estimate_cost(self, *, symbol: str, regime: str, notional_usdt: float, quantile: str, **kwargs):
        self.cost_calls += 1
        return CostEstimate(
            symbol=symbol.replace("/", "-"),
            regime=regime,
            notional_usdt=notional_usdt,
            quantile=quantile,
            total_cost_bps=1.0,
            source="public_spread_proxy",
            fallback_level="PUBLIC_SPREAD_PROXY",
            cost_model_version="cost_bucket_daily:2026-05-11",
        )


def _cfg(tmp_path: Path, mode: str) -> AppConfig:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = mode
    cfg.quant_lab.runtime_override_path = str(tmp_path / f"{mode}_override.json")
    cfg.quant_lab.cost_min_edge_multiplier = 1.5
    cfg.quant_lab.min_cost_bps_floor = 5.0
    cfg.execution.fee_bps = 0.0
    cfg.execution.slippage_bps = 0.0
    return cfg


def _guard(tmp_path: Path, cfg: AppConfig, client: _ModeClient) -> QuantLabGuard:
    return QuantLabGuard(client=client, cfg=cfg.quant_lab, usage_log_path=tmp_path / "usage.jsonl", run_id="run-mode")


def _orders() -> list[Order]:
    return [
        Order("BTC/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 1.0}),
        Order("ETH/USDT", "sell", "CLOSE_LONG", 10.0, 100.0, {"expected_edge_bps": 20.0}),
    ]


def test_local_only_mode_does_not_call_quant_lab(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "local_only")
    client = _ModeClient()
    guard = _guard(tmp_path, cfg, client)

    result = guard.check_startup_permission(cfg, "run-mode")
    kept = guard.filter_orders_by_permission(_orders(), result)
    kept, rows = guard.enrich_orders_with_cost(kept, "normal", cfg)

    assert result.permission == "ALLOW_LOCAL"
    assert result.called_api is False
    assert client.permission_calls == 0
    assert client.cost_calls == 0
    assert len(kept) == 2
    assert rows == []


def test_shadow_mode_calls_api_but_does_not_filter(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "shadow")
    client = _ModeClient(permission="SELL_ONLY")
    guard = _guard(tmp_path, cfg, client)

    result = guard.check_startup_permission(cfg, "run-mode")
    kept = guard.filter_orders_by_permission(_orders(), result)
    kept, rows = guard.enrich_orders_with_cost(kept, "normal", cfg)
    summary = guard.summary_payload(orders_before=2, orders_after=len(kept))

    assert client.permission_calls == 1
    assert client.cost_calls == 2
    assert len(kept) == 2
    assert rows[0]["would_filter_by_cost"] is True
    assert rows[0]["actually_filtered"] is False
    assert summary["would_filter_by_permission_count"] == 1
    assert summary["filtered_by_permission_count"] == 0
    assert summary["would_filter_by_cost_count"] == 1
    assert summary["filtered_by_cost_count"] == 0
    usage = [json.loads(line) for line in (tmp_path / "usage.jsonl").read_text(encoding="utf-8").splitlines()]
    assert any(row.get("mode") == "shadow" and row.get("hypothetical") for row in usage)


def test_shadow_raw_abort_records_effective_allow_and_would_block(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "shadow")
    client = _ModeClient(permission="ABORT")
    guard = _guard(tmp_path, cfg, client)

    result = guard.check_startup_permission(cfg, "run-mode")
    guard.record_final_permission(local_preflight_permission="ALLOW", final_permission="ALLOW")

    assert result.permission == "ABORT"
    assert result.effective_permission_decision == "ALLOW"
    assert result.would_block_if_enforced is True
    rows = [json.loads(line) for line in (tmp_path / "usage.jsonl").read_text(encoding="utf-8").splitlines()]
    final = [row for row in rows if row.get("event_type") == "final_permission"][-1]
    assert final["raw_permission_decision"] == "ABORT"
    assert final["effective_permission_decision"] == "ALLOW"
    assert final["would_block_if_enforced"] is True
    assert final["permission_gate_enforced"] is False


def test_shadow_missing_edge_is_hypothetical_only(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "shadow")
    client = _ModeClient(permission="ALLOW")
    guard = _guard(tmp_path, cfg, client)

    guard.check_startup_permission(cfg, "run-mode")
    kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    assert rows[0]["filter_reason"] == "expected_edge_missing_no_filter"
    assert rows[0]["warning"] == "expected_edge_missing_cost_gate_not_verified"
    assert rows[0]["cost_gate_verified"] is False
    assert rows[0]["would_filter_by_cost"] is True
    assert rows[0]["actually_filtered"] is False
    assert rows[0]["hypothetical"] is True


def test_cost_only_mode_applies_only_cost_gate(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "cost_only")
    client = _ModeClient(permission="SELL_ONLY")
    guard = _guard(tmp_path, cfg, client)

    result = guard.check_startup_permission(cfg, "run-mode")
    permission_kept = guard.filter_orders_by_permission(_orders(), result)
    final_kept, _rows = guard.enrich_orders_with_cost(permission_kept, "normal", cfg)

    assert [order.side for order in permission_kept] == ["buy", "sell"]
    assert [order.side for order in final_kept] == ["sell"]
    assert guard.summary_payload()["filtered_by_permission_count"] == 0
    assert guard.summary_payload()["filtered_by_cost_count"] == 1


def test_permission_only_mode_applies_only_permission_gate(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "permission_only")
    client = _ModeClient(permission="SELL_ONLY")
    guard = _guard(tmp_path, cfg, client)

    result = guard.check_startup_permission(cfg, "run-mode")
    permission_kept = guard.filter_orders_by_permission(_orders(), result)
    final_kept, rows = guard.enrich_orders_with_cost(permission_kept, "normal", cfg)

    assert [order.side for order in permission_kept] == ["sell"]
    assert [order.side for order in final_kept] == ["sell"]
    assert rows == []
    assert client.cost_calls == 0


def test_enforce_mode_applies_permission_and_cost_gates(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    client = _ModeClient(permission="SELL_ONLY")
    guard = _guard(tmp_path, cfg, client)

    result = guard.check_startup_permission(cfg, "run-mode")
    permission_kept = guard.filter_orders_by_permission(_orders(), result)
    final_kept, _rows = guard.enrich_orders_with_cost(
        [Order("ETH/USDT", "sell", "CLOSE_LONG", 10.0, 100.0, {"expected_edge_bps": 1.0})],
        "normal",
        cfg,
    )

    assert [order.side for order in permission_kept] == ["sell"]
    assert final_kept == []
    summary = guard.summary_payload()
    assert summary["filtered_by_permission_count"] == 1
    assert summary["filtered_by_cost_count"] == 1


def test_enforce_missing_edge_buy_blocks(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    client = _ModeClient(permission="ALLOW")
    guard = _guard(tmp_path, cfg, client)

    guard.check_startup_permission(cfg, "run-mode")
    kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {})],
        "normal",
        cfg,
    )

    assert kept == []
    assert rows[0]["filter_reason"] == "expected_edge_missing_block"
    assert rows[0]["would_filter_by_cost"] is True
    assert rows[0]["actually_filtered"] is True
    assert guard.summary_payload()["filtered_by_cost_count"] == 1


def test_enforce_missing_edge_sell_close_does_not_filter(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    client = _ModeClient(permission="ALLOW")
    guard = _guard(tmp_path, cfg, client)

    guard.check_startup_permission(cfg, "run-mode")
    kept, rows = guard.enrich_orders_with_cost(
        [Order("ETH/USDT", "sell", "CLOSE_LONG", 10.0, 100.0, {})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    assert rows[0]["filter_reason"] == "expected_edge_missing_close_no_filter"
    assert rows[0]["would_filter_by_cost"] is False
    assert rows[0]["actually_filtered"] is False
