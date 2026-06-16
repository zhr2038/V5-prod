from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from configs.schema import AppConfig
from src.core.models import Order
from src.quant_lab_client.guard import QuantLabGuard
from src.quant_lab_client.models import RiskPermission


class _PermissionClient:
    phase = "live"

    def __init__(
        self,
        *,
        permission: str,
        permission_status: str,
        expires_at: str = "2999-01-01T00:00:00Z",
        enforceable: bool | None = True,
    ) -> None:
        self.permission = permission
        self.permission_status = permission_status
        self.expires_at = expires_at
        self.enforceable = enforceable
        self.run_id = "permission-contract-run"
        self.permission_calls = 0

    def get_health(self):
        return SimpleNamespace(status="ok", mode="read-only")

    def get_live_permission(self, *, strategy: str, version: str) -> RiskPermission:
        self.permission_calls += 1
        return RiskPermission(
            strategy=strategy,
            version=version,
            permission=self.permission,
            permission_status=self.permission_status,
            status=self.permission_status,
            enforceable=self.enforceable,
            allowed_modes=["sell_only"] if self.permission == "SELL_ONLY" else [],
            max_gross_exposure_usdt=1000.0,
            max_single_order_usdt=100.0,
            as_of_ts="2026-05-14T00:00:00Z",
            created_at="2026-05-14T00:00:00Z",
            expires_at=self.expires_at,
            source_bundle_ts="2026-05-13T23:55:00Z",
            telemetry_latest_ts="2026-05-13T23:56:00Z",
            risk_reason_codes=[self.permission_status.lower()],
            contract_version="fixture.contract.v1",
        )


def _cfg(tmp_path: Path, mode: str) -> AppConfig:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = mode
    cfg.quant_lab.fail_policy = "sell_only"
    cfg.quant_lab.runtime_override_path = str(tmp_path / f"{mode}.json")
    cfg.quant_lab.enforce_readiness_enabled = False
    return cfg


def _guard(tmp_path: Path, cfg: AppConfig, client: _PermissionClient) -> QuantLabGuard:
    return QuantLabGuard(client=client, cfg=cfg.quant_lab, usage_log_path=tmp_path / "usage.jsonl", run_id="permission-contract-run")


def _buy() -> Order:
    return Order("BNB/USDT", "buy", "OPEN_LONG", 10.0, 600.0, {"expected_edge_bps": 80.0})


def _close() -> Order:
    return Order("BNB/USDT", "sell", "CLOSE_LONG", 10.0, 600.0, {"reduce_only": True})


def test_risk_permission_response_contract_fields_are_parsed() -> None:
    permission = RiskPermission.from_payload(
        {
            "strategy": "v5",
            "version": "5.0.0",
            "permission_status": "ACTIVE_SELL_ONLY",
            "enforceable": True,
            "allowed_modes": ["sell_only"],
            "max_gross_exposure_usdt": 123.4,
            "max_single_order_usdt": 12.3,
            "as_of_ts": "2026-05-14T00:00:00Z",
            "expires_at": "2026-05-14T00:10:00Z",
            "source_bundle_ts": "2026-05-13T23:55:00Z",
            "telemetry_latest_ts": "2026-05-13T23:56:00Z",
            "risk_reason_codes": ["required_alpha_gate_quarantine"],
            "contract_version": "fixture.contract.v1",
        }
    )

    assert permission.permission == "SELL_ONLY"
    assert permission.permission_status == "ACTIVE_SELL_ONLY"
    assert permission.enforceable is True
    assert permission.max_gross_exposure_usdt == 123.4
    assert permission.max_single_order_usdt == 12.3
    assert permission.as_of_ts == "2026-05-14T00:00:00Z"
    assert permission.expires_at == "2026-05-14T00:10:00Z"
    assert permission.source_bundle_ts == "2026-05-13T23:55:00Z"
    assert permission.telemetry_latest_ts == "2026-05-13T23:56:00Z"
    assert permission.risk_reason_codes == ["required_alpha_gate_quarantine"]
    assert permission.contract_version == "fixture.contract.v1"


def test_risk_permission_string_false_enforceable_stays_false() -> None:
    permission = RiskPermission.from_payload(
        {
            "strategy": "v5",
            "version": "5.0.0",
            "permission_status": "ACTIVE_ALLOW",
            "permission": "ALLOW",
            "enforceable": "false",
        }
    )

    assert permission.enforceable is False


def test_shadow_active_abort_effective_allow_with_would_block(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "shadow")
    guard = _guard(tmp_path, cfg, _PermissionClient(permission="ABORT", permission_status="ACTIVE_ABORT"))

    result = guard.check_startup_permission(cfg, "permission-contract-run")
    kept = guard.filter_orders_by_permission([_buy()], result)

    assert result.raw_permission_decision == "ABORT"
    assert result.raw_permission_status == "ACTIVE_ABORT"
    assert result.effective_permission_decision == "ALLOW"
    assert result.permission == "ABORT"
    assert result.would_block_if_enforced is True
    assert result.shadow_override_reason == "quant_lab_shadow_mode"
    assert len(kept) == 1
    assert kept[0].meta["quant_lab"]["would_block_if_enforced"] is True
    assert kept[0].meta["quant_lab"]["order_filtered"] is False


def test_enforce_active_abort_blocks_new_entry(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    guard = _guard(tmp_path, cfg, _PermissionClient(permission="ABORT", permission_status="ACTIVE_ABORT"))

    result = guard.check_startup_permission(cfg, "permission-contract-run")
    kept = guard.filter_orders_by_permission([_buy()], result)

    assert result.effective_permission_decision == "ABORT"
    assert kept == []


def test_enforce_active_sell_only_allows_close_and_blocks_open(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    guard = _guard(tmp_path, cfg, _PermissionClient(permission="SELL_ONLY", permission_status="ACTIVE_SELL_ONLY"))

    result = guard.check_startup_permission(cfg, "permission-contract-run")
    kept = guard.filter_orders_by_permission([_buy(), _close()], result)

    assert result.effective_permission_decision == "SELL_ONLY"
    assert [order.side for order in kept] == ["sell"]


def test_enforce_stale_abort_treats_as_sell_only(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    guard = _guard(tmp_path, cfg, _PermissionClient(permission="ABORT", permission_status="STALE_ABORT"))

    result = guard.check_startup_permission(cfg, "permission-contract-run")
    kept = guard.filter_orders_by_permission([_buy(), _close()], result)

    assert result.effective_permission_decision == "SELL_ONLY"
    assert result.remote_permission_status == "STALE_ABORT"
    assert "remote_permission_not_fresh" in result.reasons
    assert [order.side for order in kept] == ["sell"]


def test_expired_active_abort_marks_contract_violation_and_treats_expired(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    guard = _guard(
        tmp_path,
        cfg,
        _PermissionClient(
            permission="ABORT",
            permission_status="ACTIVE_ABORT",
            expires_at="2000-01-01T00:00:00Z",
        ),
    )

    result = guard.check_startup_permission(cfg, "permission-contract-run")
    kept = guard.filter_orders_by_permission([_buy(), _close()], result)

    assert result.permission_contract_violation is True
    assert result.remote_permission_status == "EXPIRED_ACTIVE_ABORT"
    assert result.effective_permission_decision == "SELL_ONLY"
    assert "remote_permission_not_fresh" in result.reasons
    assert [order.side for order in kept] == ["sell"]


def test_enforce_active_allow_not_enforceable_degrades_to_sell_only(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    guard = _guard(
        tmp_path,
        cfg,
        _PermissionClient(
            permission="ALLOW",
            permission_status="ACTIVE_ALLOW",
            enforceable=False,
        ),
    )

    result = guard.check_startup_permission(cfg, "permission-contract-run")
    kept = guard.filter_orders_by_permission([_buy(), _close()], result)

    assert result.permission_contract_violation is True
    assert result.raw_permission_enforceable is False
    assert result.effective_permission_decision == "SELL_ONLY"
    assert "remote_permission_not_enforceable" in result.reasons
    assert [order.side for order in kept] == ["sell"]


def test_enforce_active_abort_not_enforceable_stays_abort(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    guard = _guard(
        tmp_path,
        cfg,
        _PermissionClient(
            permission="ABORT",
            permission_status="ACTIVE_ABORT",
            enforceable=False,
        ),
    )

    result = guard.check_startup_permission(cfg, "permission-contract-run")
    kept = guard.filter_orders_by_permission([_buy(), _close()], result)

    assert result.permission_contract_violation is True
    assert result.raw_permission_enforceable is False
    assert result.effective_permission_decision == "ABORT"
    assert "remote_permission_not_enforceable" in result.reasons
    assert kept == []


def test_enforce_missing_permission_status_and_expiry_degrades_allow(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "enforce")
    guard = _guard(
        tmp_path,
        cfg,
        _PermissionClient(
            permission="ALLOW",
            permission_status="",
            expires_at="",
        ),
    )

    result = guard.check_startup_permission(cfg, "permission-contract-run")
    kept = guard.filter_orders_by_permission([_buy(), _close()], result)

    assert result.permission_contract_violation is True
    assert result.remote_permission_status == "MISSING_PERMISSION_STATUS"
    assert result.effective_permission_decision == "SELL_ONLY"
    assert "remote_permission_status_incomplete" in result.reasons
    assert [order.side for order in kept] == ["sell"]
