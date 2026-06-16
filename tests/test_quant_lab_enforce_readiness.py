from __future__ import annotations

import json
from pathlib import Path

from configs.schema import AppConfig
from src.quant_lab_client.guard import QuantLabGuard
from src.quant_lab_client.mode import CONTRACT_VERSION, QuantLabMode, evaluate_enforce_readiness
from src.quant_lab_client.models import RiskPermission


class _ReadyClient:
    phase = "live"

    def __init__(self, *, permission: str = "ALLOW", status: str = "ACTIVE_ALLOW") -> None:
        self.permission = permission
        self.status = status
        self.run_id = "readiness-run"
        self.permission_calls = 0

    def get_live_permission(self, *, strategy: str, version: str, **_kwargs) -> RiskPermission:
        self.permission_calls += 1
        return RiskPermission(
            strategy=strategy,
            version=version,
            permission=self.permission,
            permission_status=self.status,
            status=self.status,
            enforceable=True,
            expires_at="2999-01-01T00:00:00Z",
            as_of_ts="2026-05-14T00:00:00Z",
            contract_version=CONTRACT_VERSION,
        )


def _ready_snapshot(**overrides) -> dict:
    payload = {
        "remote_permission_status": "ACTIVE_ALLOW",
        "remote_permission_enforceable": True,
        "remote_permission_expires_at": "2999-01-01T00:00:00Z",
        "quant_lab_cost_usage_rows": 10,
        "cost_degraded_count": 0,
        "global_default_cost_count": 0,
        "quant_lab_request_count": 10,
        "quant_lab_fallback_count": 0,
        "telemetry_contract_version": CONTRACT_VERSION,
        "telemetry_schema_version": "1.0.0",
        "summary_trade_count_mismatch_count": 0,
    }
    payload.update(overrides)
    return payload


def _cfg(tmp_path: Path, snapshot: dict) -> AppConfig:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "enforce"
    cfg.quant_lab.fail_policy = "sell_only"
    cfg.quant_lab.allow_runtime_override = False
    cfg.quant_lab.enforce_readiness_path = str(tmp_path / "readiness.json")
    Path(cfg.quant_lab.enforce_readiness_path).write_text(json.dumps(snapshot), encoding="utf-8")
    return cfg


def test_readiness_blocked_enforce_requested_downgrades_to_shadow(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, _ready_snapshot(global_default_cost_count=2))
    guard = QuantLabGuard(client=_ReadyClient(), cfg=cfg.quant_lab, usage_log_path=tmp_path / "usage.jsonl")

    result = guard.check_startup_permission(cfg, "readiness-run")

    assert result.quant_lab_requested_mode == "enforce"
    assert result.quant_lab_effective_mode == "shadow"
    assert result.enforce_readiness_status == "BLOCKED"
    assert "global_default_cost_count_high" in result.enforce_blocked_reasons
    assert guard.mode == QuantLabMode.SHADOW
    assert result.apply_permission_gate is False


def test_readiness_ready_enforce_requested_remains_enforce(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, _ready_snapshot())
    guard = QuantLabGuard(client=_ReadyClient(), cfg=cfg.quant_lab, usage_log_path=tmp_path / "usage.jsonl")

    result = guard.check_startup_permission(cfg, "readiness-run")

    assert result.quant_lab_requested_mode == "enforce"
    assert result.quant_lab_effective_mode == "enforce"
    assert result.enforce_readiness_status == "READY"
    assert result.enforce_blocked_reasons == []
    assert guard.mode == QuantLabMode.ENFORCE
    assert result.apply_permission_gate is True


def test_readiness_contract_version_mismatch_blocks_enforce(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, _ready_snapshot())
    permission = RiskPermission(
        permission="ALLOW",
        permission_status="ACTIVE_ALLOW",
        enforceable=True,
        expires_at="2999-01-01T00:00:00Z",
        contract_version="unexpected.contract.v0",
    )

    result = evaluate_enforce_readiness(cfg.quant_lab, permission=permission, readiness_payload=_ready_snapshot())

    assert result.status == "BLOCKED"
    assert result.contract_version_match is False
    assert "contract_version_mismatch" in result.reasons


def test_readiness_blocks_unknown_active_permission_status(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, _ready_snapshot())

    result = evaluate_enforce_readiness(
        cfg.quant_lab,
        readiness_payload=_ready_snapshot(remote_permission_status="ACTIVE_UNKNOWN"),
    )

    assert result.status == "BLOCKED"
    assert result.remote_permission_status == "ACTIVE_UNKNOWN"
    assert "remote_permission_not_active" in result.reasons


def test_readiness_high_global_default_cost_blocks_enforce(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, _ready_snapshot(global_default_cost_count=1))

    result = evaluate_enforce_readiness(cfg.quant_lab, readiness_payload=_ready_snapshot(global_default_cost_count=1))

    assert result.status == "BLOCKED"
    assert result.global_default_cost_count == 1
    assert "global_default_cost_count_high" in result.reasons


def test_readiness_ignores_legacy_global_default_when_current_contract_is_clean(tmp_path: Path) -> None:
    cfg = _cfg(
        tmp_path,
        _ready_snapshot(
            quant_lab_cost_usage_rows=2,
            cost_degraded_count=1,
            global_default_cost_count=1,
            cost_usage_legacy_rows=1,
            cost_usage_current_contract_rows=1,
            post_deployment_cost_usage_rows=1,
            legacy_global_default_cost_count=1,
            current_contract_global_default_cost_count=0,
            post_deployment_global_default_cost_count=0,
            current_contract_cost_degraded_count=0,
            post_deployment_cost_degraded_count=0,
        ),
    )

    result = evaluate_enforce_readiness(
        cfg.quant_lab,
        readiness_payload=_ready_snapshot(
            quant_lab_cost_usage_rows=2,
            cost_degraded_count=1,
            global_default_cost_count=1,
            cost_usage_legacy_rows=1,
            cost_usage_current_contract_rows=1,
            post_deployment_cost_usage_rows=1,
            legacy_global_default_cost_count=1,
            current_contract_global_default_cost_count=0,
            post_deployment_global_default_cost_count=0,
            current_contract_cost_degraded_count=0,
            post_deployment_cost_degraded_count=0,
        ),
    )

    assert result.status == "READY"
    assert result.global_default_cost_count == 0
    assert "global_default_cost_count_high" not in result.reasons
    assert "cost_degraded_rate_high" not in result.reasons


def test_readiness_blocks_current_contract_global_default(tmp_path: Path) -> None:
    cfg = _cfg(
        tmp_path,
        _ready_snapshot(
            cost_usage_current_contract_rows=1,
            post_deployment_cost_usage_rows=1,
            current_contract_global_default_cost_count=1,
            post_deployment_global_default_cost_count=1,
            current_contract_cost_degraded_count=1,
            post_deployment_cost_degraded_count=1,
        ),
    )

    result = evaluate_enforce_readiness(
        cfg.quant_lab,
        readiness_payload=_ready_snapshot(
            cost_usage_current_contract_rows=1,
            post_deployment_cost_usage_rows=1,
            current_contract_global_default_cost_count=1,
            post_deployment_global_default_cost_count=1,
            current_contract_cost_degraded_count=1,
            post_deployment_cost_degraded_count=1,
        ),
    )

    assert result.status == "BLOCKED"
    assert result.global_default_cost_count == 1
    assert "global_default_cost_count_high" in result.reasons
