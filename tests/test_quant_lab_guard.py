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

    def __init__(
        self,
        *,
        permission="SELL_ONLY",
        fail_health=False,
        fail_permission=False,
        fail_cost=False,
        cost_source="public_spread_proxy",
        fallback_level="PUBLIC_SPREAD_PROXY",
        sample_count=100,
        cost_model_version="cost_bucket_daily:2026-05-11",
        cost_trusted_for_live=None,
        cost_trusted_for_live_canary=None,
        cost_trusted_for_live_scale=None,
        cost_trust_level=None,
        allowed_live_modes=None,
        permission_allowed_live_modes=None,
        permission_live_block_reasons=None,
        deep_health_status="ok",
        fail_deep_health=False,
    ) -> None:
        self.permission = permission
        self.fail_health = fail_health
        self.fail_permission = fail_permission
        self.fail_cost = fail_cost
        self.cost_source = cost_source
        self.fallback_level = fallback_level
        self.sample_count = sample_count
        self.cost_model_version = cost_model_version
        self.cost_trusted_for_live = cost_trusted_for_live
        self.cost_trusted_for_live_canary = cost_trusted_for_live_canary
        self.cost_trusted_for_live_scale = cost_trusted_for_live_scale
        self.cost_trust_level = cost_trust_level
        self.allowed_live_modes = allowed_live_modes
        self.permission_allowed_live_modes = permission_allowed_live_modes
        self.permission_live_block_reasons = list(permission_live_block_reasons or [])
        self.deep_health_status = deep_health_status
        self.fail_deep_health = fail_deep_health
        self.run_id = "r"
        self.cost_kwargs = []
        self.permission_calls = 0

    def get_health(self):
        if self.fail_health:
            raise RuntimeError("health unavailable secret-token")
        return SimpleNamespace(status="ok", mode="read-only")

    def get_deep_health(self):
        if self.fail_deep_health:
            raise RuntimeError("deep health unavailable secret-token")
        is_warning = self.deep_health_status == "warning"
        is_critical = self.deep_health_status == "critical"
        return SimpleNamespace(
            status=self.deep_health_status,
            mode="read-only",
            overall_status=self.deep_health_status,
            service_health={"status": "OK", "mode": "read-only", "transport": "OK"},
            data_quality={
                "status": "CRITICAL" if is_critical else ("WARN" if is_warning else "OK"),
                "warnings": ["data_health_critical"] if is_critical else (["cost_health_warning"] if is_warning else []),
            },
            live_entry_readiness={
                "status": "BLOCKED" if is_critical else "READY",
                "veto_status": "VETO_READY",
                "entry_status": "BLOCKED" if is_critical else "ENTRY_READY",
                "scale_status": "BLOCKED" if is_critical else "SCALE_READY",
            },
            warnings=["data_health_critical"] if is_critical else (["cost_health_warning"] if is_warning else []),
            cost_health={
                "status": self.deep_health_status,
                "fallback_ratio": 1.0 if is_warning else 0.0,
                "hard_fallback_ratio": 0.0,
                "soft_fallback_ratio": 1.0 if is_warning else 0.0,
                "actual_rows": 0 if is_warning else 3,
                "mixed_rows": 0,
                "proxy_rows": 33 if is_warning else 0,
                "global_default_rows": 0,
                "proxy_only_count": 33 if is_warning else 0,
                "symbols_missing_cost": ["ALLO-USDT"] if is_warning else [],
                "warnings": ["soft_fallback_ratio_gt_0.5"] if is_warning else [],
            },
            data_health={"status": "critical" if is_critical else "ok"},
            risk_permission_dependency_meta={"status": "ok"},
        )

    def get_live_permission(self, *, strategy: str, version: str):
        self.permission_calls += 1
        if self.fail_permission:
            raise RuntimeError("unavailable secret-token")
        return RiskPermission(
            strategy=strategy,
            version=version,
            permission=self.permission,
            allowed_modes=[self.permission.lower()],
            allowed_live_modes=self.permission_allowed_live_modes,
            reasons=["required_alpha_gate_quarantine"] if self.permission == "SELL_ONLY" else [],
            live_block_reasons=self.permission_live_block_reasons,
            cost_model_version="cost_bucket_daily:2026-05-11",
            gate_version="bootstrap.quarantine.v1",
        )

    def estimate_cost(self, *, symbol: str, regime: str, notional_usdt: float, quantile: str, **kwargs):
        if self.fail_cost:
            raise RuntimeError("cost unavailable")
        self.cost_kwargs.append({"symbol": symbol, "regime": regime, "notional_usdt": notional_usdt, "quantile": quantile, **kwargs})
        return CostEstimate(
            symbol=symbol.replace("/", "-"),
            regime=regime,
            notional_usdt=notional_usdt,
            quantile=quantile,
            total_cost_bps=25.0 if self.cost_source == "global_default" else 1.0,
            source=self.cost_source,
            fallback_level=self.fallback_level,
            sample_count=self.sample_count,
            cost_model_version=self.cost_model_version,
            total_cost_bps_p50=20.0 if self.cost_source == "global_default" else 0.8,
            total_cost_bps_p75=25.0 if self.cost_source == "global_default" else 1.0,
            total_cost_bps_p90=30.0 if self.cost_source == "global_default" else 2.0,
            cost_quality="proxy" if self.cost_trusted_for_live is False else "mixed",
            cost_trusted_for_live=self.cost_trusted_for_live,
            cost_trusted_for_live_canary=self.cost_trusted_for_live_canary,
            cost_trusted_for_live_scale=self.cost_trusted_for_live_scale,
            cost_trust_level=self.cost_trust_level,
            raw_response={"allowed_live_modes": self.allowed_live_modes}
            if self.allowed_live_modes is not None
            else {},
        )


def _guard(tmp_path: Path, cfg: AppConfig, client: _Client) -> QuantLabGuard:
    cfg.quant_lab.enforce_readiness_enabled = False
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
    assert any(
        row.get("event_type") == "permission_audit"
        and row.get("legacy_event_type") == "filter_order"
        and row.get("order_filtered")
        for row in rows
    )


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


def test_refresh_permission_health_failure_uses_fail_policy_without_permission_call(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "enforce"
    cfg.quant_lab.fail_policy = "sell_only"
    client = _Client(permission="ALLOW", fail_health=True)
    guard = _guard(tmp_path, cfg, client)

    decision = guard.refresh_permission(include_health=True)

    assert decision == "SELL_ONLY"
    assert client.permission_calls == 0
    assert guard.permission_result.fallback_used is True
    assert guard.permission_result.fallback_reason == "quant_lab_health_unavailable_sell_only"
    assert guard.permission_result.reasons == ["quant_lab_health_unavailable"]
    rows = [json.loads(line) for line in (tmp_path / "usage.jsonl").read_text(encoding="utf-8").splitlines()]
    assert rows[0]["event_type"] == "health_check"
    assert rows[0]["success"] is False
    assert rows[-1]["event_type"] == "fallback"
    assert rows[-1]["endpoint_path"] == "/v1/health"
    assert rows[-1]["permission"] == "SELL_ONLY"
    assert "secret-token" not in (tmp_path / "usage.jsonl").read_text(encoding="utf-8")


def test_refresh_permission_deep_health_critical_consumes_remote_abort(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "enforce"
    cfg.quant_lab.fail_policy = "sell_only"
    client = _Client(permission="ABORT", deep_health_status="critical")
    guard = _guard(tmp_path, cfg, client)

    decision = guard.refresh_permission(include_health=True)

    assert decision == "ABORT"
    assert client.permission_calls == 1
    assert guard.permission_result.fallback_used is False
    assert guard.permission_result.raw_permission_decision == "ABORT"
    assert guard.permission_result.would_block_if_enforced is True
    rows = [json.loads(line) for line in (tmp_path / "usage.jsonl").read_text(encoding="utf-8").splitlines()]
    assert rows[0]["event_type"] == "health_check"
    assert rows[0]["success"] is True
    assert rows[0]["deep_health_status"] == "critical"
    assert rows[0]["deep_health_overall_status"] == "critical"
    assert rows[0]["deep_service_health_status"] == "OK"
    assert rows[0]["deep_data_quality_status"] == "CRITICAL"
    assert rows[0]["deep_live_entry_readiness_status"] == "BLOCKED"
    assert rows[0]["deep_live_entry_veto_status"] == "VETO_READY"
    assert rows[-1]["event_type"] != "fallback"
    assert "secret-token" not in (tmp_path / "usage.jsonl").read_text(encoding="utf-8")


def test_refresh_permission_deep_health_transport_failure_uses_fail_policy_without_permission_call(
    tmp_path: Path,
) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "enforce"
    cfg.quant_lab.fail_policy = "sell_only"
    client = _Client(permission="ALLOW", fail_deep_health=True)
    guard = _guard(tmp_path, cfg, client)

    decision = guard.refresh_permission(include_health=True)

    assert decision == "SELL_ONLY"
    assert client.permission_calls == 0
    assert guard.permission_result.fallback_used is True
    assert guard.permission_result.fallback_reason == "quant_lab_health_unavailable_sell_only"
    rows = [json.loads(line) for line in (tmp_path / "usage.jsonl").read_text(encoding="utf-8").splitlines()]
    assert rows[0]["event_type"] == "health_check"
    assert rows[0]["success"] is False
    assert rows[-1]["event_type"] == "fallback"
    assert "secret-token" not in (tmp_path / "usage.jsonl").read_text(encoding="utf-8")


def test_refresh_permission_records_deep_health_warning(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    client = _Client(permission="ALLOW", deep_health_status="warning")
    guard = _guard(tmp_path, cfg, client)

    decision = guard.refresh_permission(include_health=True)

    assert decision == "ALLOW"
    rows = [json.loads(line) for line in (tmp_path / "usage.jsonl").read_text(encoding="utf-8").splitlines()]
    health_row = rows[0]
    assert health_row["event_type"] == "health_check"
    assert health_row["success"] is True
    assert health_row["deep_health_status"] == "warning"
    assert health_row["deep_health_warnings"] == ["cost_health_warning"]
    assert health_row["deep_cost_health_status"] == "warning"
    assert health_row["deep_cost_fallback_ratio"] == 1.0
    assert health_row["deep_cost_hard_fallback_ratio"] == 0.0
    assert health_row["deep_cost_soft_fallback_ratio"] == 1.0
    assert health_row["deep_cost_actual_rows"] == 0
    assert health_row["deep_cost_proxy_rows"] == 33
    assert health_row["deep_cost_proxy_only_count"] == 33
    assert health_row["deep_cost_symbols_missing"] == ["ALLO-USDT"]
    assert health_row["deep_cost_warnings"] == ["soft_fallback_ratio_gt_0.5"]


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
    assert rows[0]["fallback_used"] is True


def test_guard_public_spread_proxy_cost_is_not_degraded(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    client = _Client(permission="ALLOW", cost_source="public_spread_proxy", fallback_level="PUBLIC_SPREAD_PROXY")
    guard = _guard(tmp_path, cfg, client)
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 60})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    assert rows[0]["request_symbol"] == "BNB/USDT"
    assert rows[0]["normalized_symbol"] == "BNB-USDT"
    assert rows[0]["response_symbol"] == "BNB-USDT"
    assert rows[0]["cost_source"] == "public_spread_proxy"
    assert rows[0]["degraded_cost_model"] is False
    assert rows[0]["fallback_used_for_cost_model"] is False
    assert rows[0]["cost_gate_enforced"] is False
    assert client.cost_kwargs[0]["symbol"] == "BNB/USDT"


def test_guard_global_default_cost_is_degraded_not_normal(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    client = _Client(
        permission="ALLOW",
        cost_source="global_default",
        fallback_level="GLOBAL_DEFAULT",
        sample_count=0,
        cost_model_version="global_default_v0",
    )
    guard = _guard(tmp_path, cfg, client)
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 60})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    row = rows[0]
    assert row["cost_source"] == "global_default"
    assert row["fallback_level"] == "GLOBAL_DEFAULT"
    assert row["sample_count"] == 0
    assert row["cost_model_version"] == "global_default_v0"
    assert row["selected_total_cost_bps"] == 25.0
    assert row["degraded_cost_model"] is True
    assert row["fallback_used"] is False
    assert row["fallback_used_for_cost_model"] is True
    assert row["fallback_reason"] == "global_default_cost"
    assert row["diagnosis"] == "global_default_cost"


def test_guard_missing_edge_shadow_warns_and_is_not_verified(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    guard = _guard(tmp_path, cfg, _Client(permission="ALLOW"))
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost([Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {})], "normal", cfg)

    assert len(kept) == 1
    assert rows[0]["filter_reason"] == "expected_edge_missing_no_filter"
    assert rows[0]["warning"] == "expected_edge_missing_cost_gate_not_verified"
    assert rows[0]["cost_gate_verified"] is False
    assert rows[0]["would_block_by_cost"] is True
    assert rows[0]["cost_gate_enforced"] is False


def test_live_cost_trust_guard_observe_only_records_would_block(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "observe_only"
    guard = _guard(
        tmp_path,
        cfg,
        _Client(permission="ALLOW", cost_trusted_for_live=False, allowed_live_modes=[]),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, _rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 80, "strategy_candidate": "f3_dominant_entry"})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    impact = guard.live_guard_rows[-1]
    assert impact["would_be_blocked_by_quant_lab_no_live_modes"] is True
    assert impact["would_be_blocked_by_cost_trust_guard"] is True
    assert impact["would_be_blocked_by_shadow_live_whitelist"] is True
    assert impact["blocked_by_cost_trust_guard"] is False
    assert impact["allowed_live_modes"] == "[]"
    assert impact["final_decision_actual"] == "ALLOW"
    assert impact["guard_enforced"] is False
    assert impact["guard_mode"] == "observe_only"


def test_live_cost_trust_guard_block_mode_still_observe_only(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_non_whitelist_only"
    guard = _guard(
        tmp_path,
        cfg,
        _Client(permission="ALLOW", cost_trusted_for_live=False, allowed_live_modes=[]),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 80, "strategy_candidate": "f3_dominant_entry"})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    assert rows[0]["filter_reason"] == "cost_gate_passed"
    impact = guard.live_guard_rows[-1]
    assert impact["would_be_blocked_by_cost_trust_guard"] is True
    assert impact["would_be_blocked_by_shadow_live_whitelist"] is True
    assert impact["blocked_by_cost_trust_guard"] is False
    assert impact["guard_enforced"] is False
    assert "strategy_not_in_canary_whitelist" in impact["cost_trust_block_reasons"]


def test_live_cost_trust_guard_blocks_non_whitelist_in_cost_mode(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "cost_only"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_non_whitelist_only"
    guard = _guard(
        tmp_path,
        cfg,
        _Client(permission="ALLOW", cost_trusted_for_live=False, allowed_live_modes=[]),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 80, "strategy_candidate": "f3_dominant_entry"})],
        "normal",
        cfg,
    )

    assert kept == []
    assert rows[0]["filter_reason"] == "cost_trust_guard_blocked"
    assert rows[0]["actually_filtered"] is True
    assert rows[0]["actually_filtered_by_cost"] is False
    assert rows[0]["actually_filtered_by_live_guard"] is True
    assert rows[0]["blocked_by_cost_trust_guard"] is True
    impact = guard.live_guard_rows[-1]
    assert impact["final_decision_before_guard"] == "ALLOW"
    assert impact["final_decision_after_guard"] == "BLOCKED_COST_TRUST_GUARD"
    assert impact["final_decision_actual"] == "BLOCKED_COST_TRUST_GUARD"
    assert impact["guard_enforced"] is True
    assert impact["blocked_by_cost_trust_guard"] is True
    assert impact["blocked_by_quant_lab_no_live_modes"] is True
    assert impact["blocked_by_shadow_live_whitelist"] is True
    assert guard.summary_payload()["live_guard_actual_block_count"] == 1


def test_live_cost_trust_guard_uses_permission_live_modes_when_cost_payload_omits_them(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "cost_only"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_non_whitelist_only"
    guard = _guard(
        tmp_path,
        cfg,
        _Client(
            permission="ALLOW",
            cost_trusted_for_live=True,
            permission_allowed_live_modes=[],
            permission_live_block_reasons=["middle_layer_read_only"],
        ),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 80, "strategy_candidate": "f3_dominant_entry"})],
        "normal",
        cfg,
    )

    assert kept == []
    assert rows[0]["filter_reason"] == "cost_trust_guard_blocked"
    impact = guard.live_guard_rows[-1]
    assert impact["allowed_live_modes"] == "[]"
    assert impact["cost_trusted_for_live"] is True
    assert impact["would_be_blocked_by_quant_lab_no_live_modes"] is True
    assert impact["would_be_blocked_by_cost_trust_guard"] is False
    assert impact["blocked_by_quant_lab_no_live_modes"] is True
    assert "quant_lab_allowed_live_modes_empty" in impact["cost_trust_block_reasons"]


def test_live_cost_trust_guard_allows_btc_strict_probe_exception(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "cost_only"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_non_whitelist_only"
    guard = _guard(
        tmp_path,
        cfg,
        _Client(
            permission="ALLOW",
            cost_trusted_for_live=True,
            cost_trusted_for_live_canary=True,
            cost_trusted_for_live_scale=False,
            cost_trust_level="CANARY",
            allowed_live_modes=["canary"],
        ),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, _rows = guard.enrich_orders_with_cost(
        [
            Order(
                "BTC/USDT",
                "buy",
                "OPEN_LONG",
                10.0,
                100.0,
                {"expected_edge_bps": 80, "entry_reason": "btc_leadership_probe", "btc_leadership_probe": True},
            )
        ],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    impact = guard.live_guard_rows[-1]
    assert impact["guard_enforced"] is True
    assert impact["whitelist_strategy_match"] is True
    assert impact["cost_trust_exception"] is True
    assert impact["cost_trust_level"] == "CANARY"
    assert impact["canary_live_allowed_by_cost_trust"] is True
    assert impact["normal_live_allowed_by_cost_trust"] is False
    assert impact["would_be_blocked_by_shadow_live_whitelist"] is False
    assert impact["blocked_by_cost_trust_guard"] is False


def test_live_cost_trust_guard_blocks_paper_only_even_when_whitelisted(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "cost_only"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_non_whitelist_only"
    guard = _guard(
        tmp_path,
        cfg,
        _Client(
            permission="ALLOW",
            cost_trusted_for_live=False,
            cost_trusted_for_live_canary=False,
            cost_trusted_for_live_scale=False,
            cost_trust_level="PAPER_ONLY",
            allowed_live_modes=["canary"],
        ),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost(
        [
            Order(
                "BTC/USDT",
                "buy",
                "OPEN_LONG",
                10.0,
                100.0,
                {"expected_edge_bps": 80, "entry_reason": "btc_leadership_probe", "btc_leadership_probe": True},
            )
        ],
        "normal",
        cfg,
    )

    assert kept == []
    assert rows[0]["filter_reason"] == "cost_trust_guard_blocked"
    impact = guard.live_guard_rows[-1]
    assert impact["whitelist_strategy_match"] is True
    assert impact["cost_trust_exception"] is False
    assert impact["cost_trust_level"] == "PAPER_ONLY"
    assert impact["blocked_by_cost_trust_guard"] is True
    assert "cost_trust_level_paper_only" in impact["cost_trust_block_reasons"]


def test_live_cost_trust_guard_allows_scale_ready_normal_live_without_whitelist(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "cost_only"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_non_whitelist_only"
    guard = _guard(
        tmp_path,
        cfg,
        _Client(
            permission="ALLOW",
            cost_trusted_for_live=True,
            cost_trusted_for_live_canary=True,
            cost_trusted_for_live_scale=True,
            cost_trust_level="SCALE_READY",
            allowed_live_modes=["normal_live"],
        ),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, _rows = guard.enrich_orders_with_cost(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 80, "strategy_candidate": "f3_dominant_entry"})],
        "normal",
        cfg,
    )

    assert len(kept) == 1
    impact = guard.live_guard_rows[-1]
    assert impact["whitelist_strategy_match"] is False
    assert impact["cost_trust_level"] == "SCALE_READY"
    assert impact["normal_live_allowed_by_cost_trust"] is True
    assert impact["blocked_by_cost_trust_guard"] is False


def test_live_cost_trust_guard_blocks_canary_notional_over_limit(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "cost_only"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_non_whitelist_only"
    cfg.quant_lab.live_cost_trust_guard.canary_max_notional_usdt = 20.0
    guard = _guard(
        tmp_path,
        cfg,
        _Client(
            permission="ALLOW",
            cost_trusted_for_live=True,
            cost_trusted_for_live_canary=True,
            cost_trusted_for_live_scale=False,
            cost_trust_level="CANARY",
            allowed_live_modes=["canary"],
        ),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, rows = guard.enrich_orders_with_cost(
        [
            Order(
                "BTC/USDT",
                "buy",
                "OPEN_LONG",
                25.0,
                100.0,
                {"expected_edge_bps": 80, "entry_reason": "btc_leadership_probe", "btc_leadership_probe": True},
            )
        ],
        "normal",
        cfg,
    )

    assert kept == []
    assert rows[0]["filter_reason"] == "cost_trust_guard_blocked"
    impact = guard.live_guard_rows[-1]
    assert impact["cost_trust_level"] == "CANARY"
    assert impact["would_be_blocked_by_canary_notional"] is True
    assert impact["blocked_by_canary_notional"] is True
    assert "canary_notional_exceeds_limit" in impact["cost_trust_block_reasons"]


def test_live_cost_trust_guard_never_blocks_close_or_paper_shadow(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "cost_only"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_all_untrusted_open"
    guard = _guard(
        tmp_path,
        cfg,
        _Client(permission="ALLOW", cost_trusted_for_live=False, allowed_live_modes=[]),
    )
    guard.check_startup_permission(cfg, "run-1")

    kept, _rows = guard.enrich_orders_with_cost(
        [
            Order("BNB/USDT", "sell", "CLOSE_LONG", 10.0, 100.0, {}),
            Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 80, "paper_strategy": True}),
        ],
        "normal",
        cfg,
    )

    assert [order.intent for order in kept] == ["CLOSE_LONG", "OPEN_LONG"]
    close_impact, paper_impact = guard.live_guard_rows[-2:]
    assert close_impact["blocked_by_cost_trust_guard"] is False
    assert close_impact["would_be_blocked_by_cost_trust_guard"] is False
    assert close_impact["guard_enforced"] is True
    assert "exit_bypass" in close_impact["cost_trust_block_reasons"]
    assert paper_impact["blocked_by_cost_trust_guard"] is False
    assert paper_impact["would_be_blocked_by_cost_trust_guard"] is False
    assert paper_impact["guard_enforced"] is True
    assert paper_impact["paper_or_shadow_bypassed"] is True


def test_live_cost_trusted_for_live_does_not_bypass_permission_gate(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "enforce"
    cfg.quant_lab.live_cost_trust_guard.enabled = True
    cfg.quant_lab.live_cost_trust_guard.mode = "block_non_whitelist_only"
    guard = _guard(tmp_path, cfg, _Client(permission="SELL_ONLY", cost_trusted_for_live=True))
    guard.check_startup_permission(cfg, "run-1")

    kept, summary = guard.filter_orders(
        [Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 100.0, {"expected_edge_bps": 80})]
    )

    assert kept == []
    assert summary["filtered_by_permission_count"] == 1
