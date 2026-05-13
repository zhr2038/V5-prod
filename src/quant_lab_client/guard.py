from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from src.core.models import Order
from src.reporting.quant_lab_audit import append_quant_lab_usage, sanitize_quant_lab_obj, utc_now_iso

from .client import QuantLabClient
from .cost_gate import CostGateResult, apply_quant_lab_cost_gate, local_cost_detail_for_order, order_expected_edge_detail
from .exceptions import QuantLabError
from .mode import QuantLabMode, QuantLabModeResolution, resolve_quant_lab_mode
from .models import CostEstimate, RiskPermission, symbol_to_quant_lab_symbol
from .permissions import ABORT, ALLOW, ALLOW_LOCAL, SELL_ONLY, normalize_permission


CONTRACT_VERSION = "v5.quant_lab.telemetry.v2"


@dataclass
class QuantLabGuardResult:
    enabled: bool
    permission: str
    allowed_modes: List[Any] = field(default_factory=list)
    reasons: List[Any] = field(default_factory=list)
    cost_model_version: Optional[str] = None
    gate_version: Optional[str] = None
    fallback_used: bool = False
    fallback_reason: Optional[str] = None
    response_ts: Optional[str] = None
    error_type: Optional[str] = None
    error_message_sanitized: Optional[str] = None
    mode: str = "shadow"
    mode_source: str = "config"
    mode_override_path: Optional[str] = None
    called_api: bool = False
    apply_permission_gate: bool = False
    apply_cost_gate: bool = False
    permission_gate_enforced: bool = False
    cost_gate_enforced: bool = False
    skipped_reason: Optional[str] = None
    raw_permission_decision: Optional[str] = None
    local_mode: Optional[str] = None
    effective_permission_decision: Optional[str] = None
    would_block_if_enforced: bool = False
    remote_permission_as_of_ts: Optional[str] = None
    remote_permission_expires_at: Optional[str] = None
    remote_permission_status: Optional[str] = None
    contract_version: str = CONTRACT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _fail_policy_action(fail_policy: str) -> str:
    policy = str(fail_policy or "sell_only").strip().lower()
    if policy in {"allow_local_fallback", "allow"}:
        return ALLOW_LOCAL
    if policy == "abort":
        return ABORT
    return SELL_ONLY


def _ql_cfg(cfg: Any) -> Any:
    return getattr(cfg, "quant_lab", cfg)


def _get_cfg(cfg: Any, name: str, default: Any) -> Any:
    return getattr(_ql_cfg(cfg), name, default)


def _is_sell_or_close(order: Order) -> bool:
    if bool((getattr(order, "meta", None) or {}).get("reduce_only")):
        return True
    side = str(getattr(order, "side", "") or "").lower()
    intent = str(getattr(order, "intent", "") or "").upper()
    if side == "sell":
        return True
    return intent in {"CLOSE_LONG", "CLOSE", "REDUCE_ONLY"}


def _permission_would_block(permission: str) -> bool:
    normalized = normalize_permission(permission, allow_local=True)
    return normalized in {ABORT, SELL_ONLY}


def _local_cost_estimate(order: Order, cfg: Any, *, regime: str, quantile: str) -> CostEstimate:
    local_cost, local_cost_source = local_cost_detail_for_order(order, cfg)
    return CostEstimate(
        symbol=symbol_to_quant_lab_symbol(getattr(order, "symbol", "")),
        regime=str(regime or "normal"),
        notional_usdt=float(getattr(order, "notional_usdt", 0.0) or 0.0),
        quantile=str(quantile or "p75"),
        total_cost_bps=local_cost,
        cost_bps=local_cost,
        fallback_level="LOCAL_COST_MODEL",
        source="local_fallback",
        cost_model_version=f"v5_local_{local_cost_source}",
    )


@dataclass
class QuantLabGuard:
    client: Optional[QuantLabClient] = None
    cfg: Any = None
    usage_log_path: str | Path = "reports/quant_lab_usage.jsonl"
    run_id: Optional[str] = None
    phase: str = "live"
    permission_result: QuantLabGuardResult = field(
        default_factory=lambda: QuantLabGuardResult(enabled=False, permission=ALLOW_LOCAL, mode="local_only")
    )
    mode_resolution: QuantLabModeResolution = field(
        default_factory=lambda: QuantLabModeResolution(mode=QuantLabMode.LOCAL_ONLY)
    )
    events: List[Dict[str, Any]] = field(default_factory=list)
    cost_rows: List[Dict[str, Any]] = field(default_factory=list)
    filtered_orders: List[Dict[str, Any]] = field(default_factory=list)
    request_count: int = 0
    request_error_count: int = 0
    local_preflight_permission: Optional[str] = None
    final_permission: Optional[str] = None

    def __post_init__(self) -> None:
        if self.cfg is None:
            return
        try:
            self.mode_resolution = resolve_quant_lab_mode(self.cfg)
        except Exception:
            return

    def _refresh_mode_from_cfg(self) -> None:
        if self.cfg is None:
            return
        try:
            self.mode_resolution = resolve_quant_lab_mode(self.cfg)
        except Exception:
            return

    @property
    def mode(self) -> QuantLabMode:
        return self.mode_resolution.mode

    @property
    def called_api(self) -> bool:
        return bool(_get_cfg(self.cfg, "enabled", True)) and bool(self.client is not None and self.mode != QuantLabMode.LOCAL_ONLY)

    @property
    def apply_permission_gate(self) -> bool:
        return bool(_get_cfg(self.cfg, "enabled", True)) and self.mode in {QuantLabMode.PERMISSION_ONLY, QuantLabMode.ENFORCE}

    @property
    def apply_cost_gate(self) -> bool:
        return bool(_get_cfg(self.cfg, "enabled", True)) and self.mode in {QuantLabMode.COST_ONLY, QuantLabMode.ENFORCE}

    def _mode_fields(self, *, enforced: Optional[bool] = None, hypothetical: Optional[bool] = None) -> Dict[str, Any]:
        client = self.client
        return {
            "mode": self.mode.value,
            "local_mode": self.mode.value,
            "mode_source": self.mode_resolution.mode_source,
            "mode_override_path": self.mode_resolution.override_path,
            "contract_version": CONTRACT_VERSION,
            "quant_lab_config_source": _get_cfg(self.cfg, "quant_lab_config_source", "unknown"),
            "legacy_execution_quant_lab_ignored": bool(_get_cfg(self.cfg, "legacy_execution_quant_lab_ignored", False)),
            "called_api": self.called_api,
            "apply_permission_gate": self.apply_permission_gate,
            "apply_cost_gate": self.apply_cost_gate,
            "permission_gate_enforced": self.apply_permission_gate,
            "cost_gate_enforced": self.apply_cost_gate,
            "api_env_path_present": getattr(client, "api_env_path_present", None) if client is not None else None,
            "api_env_secure_permissions": getattr(client, "api_env_secure_permissions", None) if client is not None else None,
            "api_env_token_loaded": getattr(client, "api_env_token_loaded", False) if client is not None else False,
            "api_env_warning": getattr(client, "api_env_warning", None) if client is not None else None,
            "enforced": bool(enforced) if enforced is not None else bool(self.apply_permission_gate or self.apply_cost_gate),
            "hypothetical": bool(hypothetical) if hypothetical is not None else bool(self.mode == QuantLabMode.SHADOW),
        }

    @classmethod
    def from_config(
        cls,
        quant_lab_cfg: Any,
        *,
        run_id: Optional[str] = None,
        phase: str = "live_preflight",
        http_client: Optional[Any] = None,
    ) -> "QuantLabGuard":
        mode_resolution = resolve_quant_lab_mode(quant_lab_cfg)
        enabled = bool(getattr(quant_lab_cfg, "enabled", False))
        if not enabled:
            return cls.disabled(quant_lab_cfg, run_id=run_id, mode_resolution=mode_resolution)
        client: Optional[QuantLabClient] = None
        if mode_resolution.mode != QuantLabMode.LOCAL_ONLY:
            client = QuantLabClient.from_config(
                quant_lab_cfg,
                run_id=run_id,
                phase=phase,
                http_client=http_client,
                mode=mode_resolution.mode.value,
            )
        return cls(
            client=client,
            cfg=quant_lab_cfg,
            usage_log_path=str(getattr(quant_lab_cfg, "audit_path", "reports/quant_lab_usage.jsonl") or "reports/quant_lab_usage.jsonl"),
            run_id=run_id,
            phase=phase,
            mode_resolution=mode_resolution,
        )

    @classmethod
    def disabled(
        cls,
        cfg: Any = None,
        *,
        run_id: Optional[str] = None,
        mode_resolution: Optional[QuantLabModeResolution] = None,
    ) -> "QuantLabGuard":
        resolution = mode_resolution or QuantLabModeResolution(mode=QuantLabMode.LOCAL_ONLY)
        return cls(
            client=None,
            cfg=cfg,
            run_id=run_id,
            mode_resolution=resolution,
            permission_result=QuantLabGuardResult(
                enabled=False,
                permission=ALLOW_LOCAL,
                reasons=["quant_lab_disabled"],
                mode=resolution.mode.value,
                mode_source=resolution.mode_source,
                mode_override_path=resolution.override_path,
                called_api=False,
                apply_permission_gate=False,
                apply_cost_gate=False,
                skipped_reason="quant_lab_disabled",
                raw_permission_decision=ALLOW_LOCAL,
                local_mode=resolution.mode.value,
                effective_permission_decision="LOCAL_ONLY",
                would_block_if_enforced=False,
                contract_version=CONTRACT_VERSION,
            ),
        )

    def _emit_usage(self, row: Mapping[str, Any]) -> None:
        payload = sanitize_quant_lab_obj({"run_id": self.run_id, "phase": self.phase, **self._mode_fields(), **dict(row)})
        self.events.append(dict(payload))
        if bool(_get_cfg(self.cfg, "audit_enabled", True)):
            append_quant_lab_usage(self.usage_log_path, payload)

    def record_final_permission(self, *, local_preflight_permission: str, final_permission: str) -> None:
        self.local_preflight_permission = str(local_preflight_permission or "")
        self.final_permission = str(final_permission or "")
        raw_permission = self.permission_result.raw_permission_decision or self.permission_result.permission
        would_block = _permission_would_block(raw_permission)
        self._emit_usage(
            {
                "event_type": "final_permission",
                "permission": self.permission_result.permission,
                "quant_lab_permission": self.permission_result.permission,
                "raw_permission_decision": raw_permission,
                "local_mode": self.mode.value,
                "local_preflight_permission": self.local_preflight_permission,
                "final_permission": self.final_permission,
                "effective_permission_decision": self.final_permission,
                "would_block_if_enforced": would_block,
                "fallback_used": bool(self.permission_result.fallback_used),
                "fallback_reason": self.permission_result.fallback_reason,
                "remote_permission_as_of_ts": self.permission_result.remote_permission_as_of_ts or self.permission_result.response_ts,
                "remote_permission_expires_at": self.permission_result.remote_permission_expires_at,
                "remote_permission_status": self.permission_result.remote_permission_status,
                "contract_version": CONTRACT_VERSION,
                "success": True,
                "enforced": self.apply_permission_gate,
                "hypothetical": not self.apply_permission_gate,
            }
        )

    def check_startup_permission(self, cfg: Any = None, run_id: Optional[str] = None) -> QuantLabGuardResult:
        if cfg is not None:
            self.cfg = _ql_cfg(cfg)
            self._refresh_mode_from_cfg()
        if run_id:
            self.run_id = run_id
            if self.client is not None:
                self.client.run_id = run_id
        qcfg = _ql_cfg(self.cfg)
        if self.mode == QuantLabMode.LOCAL_ONLY:
            result = QuantLabGuardResult(
                enabled=bool(getattr(qcfg, "enabled", True)),
                permission=ALLOW_LOCAL,
                reasons=["quant_lab_local_only"],
                response_ts=utc_now_iso(),
                mode=self.mode.value,
                mode_source=self.mode_resolution.mode_source,
                mode_override_path=self.mode_resolution.override_path,
                called_api=False,
                apply_permission_gate=False,
                apply_cost_gate=False,
                permission_gate_enforced=False,
                cost_gate_enforced=False,
                skipped_reason="quant_lab_local_only",
                raw_permission_decision=ALLOW_LOCAL,
                local_mode=self.mode.value,
                effective_permission_decision="LOCAL_ONLY",
                would_block_if_enforced=False,
                remote_permission_as_of_ts=None,
                contract_version=CONTRACT_VERSION,
            )
            self.permission_result = result
            self._emit_usage(
                {
                    "event_type": "live_permission",
                    "permission": ALLOW_LOCAL,
                    "quant_lab_permission": ALLOW_LOCAL,
                    "raw_permission_decision": ALLOW_LOCAL,
                    "local_mode": self.mode.value,
                    "final_permission": "LOCAL_ONLY",
                    "effective_permission_decision": "LOCAL_ONLY",
                    "would_block_if_enforced": False,
                    "success": True,
                    "called_api": False,
                    "fallback_used": False,
                    "skipped_reason": "quant_lab_local_only",
                    "contract_version": CONTRACT_VERSION,
                }
            )
            return result

        if not bool(getattr(qcfg, "enabled", False)) or self.client is None:
            result = QuantLabGuardResult(
                enabled=False,
                permission=ALLOW_LOCAL,
                reasons=["quant_lab_disabled"],
                response_ts=utc_now_iso(),
                mode=self.mode.value,
                mode_source=self.mode_resolution.mode_source,
                mode_override_path=self.mode_resolution.override_path,
                called_api=False,
                apply_permission_gate=False,
                apply_cost_gate=False,
                skipped_reason="quant_lab_disabled",
                raw_permission_decision=ALLOW_LOCAL,
                local_mode=self.mode.value,
                effective_permission_decision="LOCAL_ONLY",
                would_block_if_enforced=False,
                contract_version=CONTRACT_VERSION,
            )
            self.permission_result = result
            self._emit_usage(
                {
                    "event_type": "live_permission",
                    "permission": ALLOW_LOCAL,
                    "quant_lab_permission": ALLOW_LOCAL,
                    "raw_permission_decision": ALLOW_LOCAL,
                    "local_mode": self.mode.value,
                    "final_permission": "LOCAL_ONLY",
                    "effective_permission_decision": "LOCAL_ONLY",
                    "would_block_if_enforced": False,
                    "success": True,
                    "fallback_used": False,
                    "called_api": False,
                    "contract_version": CONTRACT_VERSION,
                }
            )
            return result

        if not bool(getattr(qcfg, "risk_permission_enabled", True)):
            result = QuantLabGuardResult(
                enabled=True,
                permission=ALLOW,
                reasons=["risk_permission_disabled"],
                response_ts=utc_now_iso(),
                mode=self.mode.value,
                mode_source=self.mode_resolution.mode_source,
                mode_override_path=self.mode_resolution.override_path,
                called_api=self.called_api,
                apply_permission_gate=False,
                apply_cost_gate=self.apply_cost_gate,
                permission_gate_enforced=False,
                cost_gate_enforced=self.apply_cost_gate,
                raw_permission_decision=ALLOW,
                local_mode=self.mode.value,
                effective_permission_decision=ALLOW,
                would_block_if_enforced=False,
                contract_version=CONTRACT_VERSION,
            )
            self.permission_result = result
            self._emit_usage(
                {
                    "event_type": "live_permission",
                    "permission": ALLOW,
                    "quant_lab_permission": ALLOW,
                    "raw_permission_decision": ALLOW,
                    "local_mode": self.mode.value,
                    "final_permission": ALLOW,
                    "effective_permission_decision": ALLOW,
                    "would_block_if_enforced": False,
                    "success": True,
                    "fallback_used": False,
                    "contract_version": CONTRACT_VERSION,
                }
            )
            return result

        try:
            self.request_count += 1
            permission: RiskPermission = self.client.get_live_permission(
                strategy=str(getattr(qcfg, "strategy_name", "v5") or "v5"),
                version=str(getattr(qcfg, "strategy_version", "5.0.0") or "5.0.0"),
            )
            raw_permission = normalize_permission(permission.permission)
            effective_permission = raw_permission if self.apply_permission_gate else ALLOW
            result = QuantLabGuardResult(
                enabled=True,
                permission=raw_permission,
                allowed_modes=list(permission.allowed_modes or []),
                reasons=list(permission.reasons or []),
                cost_model_version=permission.cost_model_version,
                gate_version=permission.gate_version,
                fallback_used=False,
                response_ts=permission.created_at or utc_now_iso(),
                mode=self.mode.value,
                mode_source=self.mode_resolution.mode_source,
                mode_override_path=self.mode_resolution.override_path,
                called_api=True,
                apply_permission_gate=self.apply_permission_gate,
                apply_cost_gate=self.apply_cost_gate,
                permission_gate_enforced=self.apply_permission_gate,
                cost_gate_enforced=self.apply_cost_gate,
                raw_permission_decision=raw_permission,
                local_mode=self.mode.value,
                effective_permission_decision=effective_permission,
                would_block_if_enforced=_permission_would_block(raw_permission),
                remote_permission_as_of_ts=permission.created_at,
                remote_permission_expires_at=permission.expires_at,
                remote_permission_status=permission.status,
                contract_version=permission.contract_version or CONTRACT_VERSION,
            )
            self.permission_result = result
            self._emit_usage(
                {
                    "event_type": "live_permission",
                    "strategy": str(getattr(qcfg, "strategy_name", "v5") or "v5"),
                    "strategy_version": str(getattr(qcfg, "strategy_version", "5.0.0") or "5.0.0"),
                    "endpoint": "/v1/risk/live-permission",
                    "status": "ok",
                    "success": True,
                    "permission": result.permission,
                    "quant_lab_permission": result.permission,
                    "raw_permission_decision": raw_permission,
                    "local_mode": self.mode.value,
                    "final_permission": effective_permission,
                    "effective_permission_decision": effective_permission,
                    "would_block_if_enforced": result.would_block_if_enforced,
                    "fallback_used": False,
                    "fallback_reason": None,
                    "remote_permission_as_of_ts": result.remote_permission_as_of_ts or result.response_ts,
                    "remote_permission_expires_at": result.remote_permission_expires_at,
                    "remote_permission_status": result.remote_permission_status,
                    "contract_version": result.contract_version,
                    "allowed_modes": result.allowed_modes,
                    "cost_model_version": result.cost_model_version,
                    "gate_version": result.gate_version,
                    "enforced": self.apply_permission_gate,
                    "hypothetical": not self.apply_permission_gate,
                }
            )
            return result
        except Exception as exc:
            self.request_error_count += 1
            action = _fail_policy_action(str(getattr(qcfg, "fail_policy", "sell_only") or "sell_only"))
            permission = ALLOW if action == ALLOW_LOCAL else action
            effective_permission = permission if self.apply_permission_gate else ALLOW
            result = QuantLabGuardResult(
                enabled=True,
                permission=permission,
                allowed_modes=["local"] if action == ALLOW_LOCAL else [permission.lower()],
                reasons=["quant_lab_permission_unavailable"],
                fallback_used=True,
                fallback_reason=f"quant_lab_unavailable_{str(getattr(qcfg, 'fail_policy', 'sell_only')).lower()}",
                response_ts=utc_now_iso(),
                error_type=type(exc).__name__,
                error_message_sanitized=str(sanitize_quant_lab_obj(str(exc)[:300])),
                mode=self.mode.value,
                mode_source=self.mode_resolution.mode_source,
                mode_override_path=self.mode_resolution.override_path,
                called_api=True,
                apply_permission_gate=self.apply_permission_gate,
                apply_cost_gate=self.apply_cost_gate,
                permission_gate_enforced=self.apply_permission_gate,
                cost_gate_enforced=self.apply_cost_gate,
                raw_permission_decision="UNAVAILABLE",
                local_mode=self.mode.value,
                effective_permission_decision=effective_permission,
                would_block_if_enforced=_permission_would_block(permission),
                remote_permission_as_of_ts=None,
                remote_permission_expires_at=None,
                remote_permission_status="unavailable",
                contract_version=CONTRACT_VERSION,
            )
            self.permission_result = result
            self._emit_usage(
                {
                    "event_type": "fallback",
                    "endpoint": "/v1/risk/live-permission",
                    "status": "error",
                    "success": False,
                    "permission": result.permission,
                    "quant_lab_permission": result.permission,
                    "raw_permission_decision": "UNAVAILABLE",
                    "local_mode": self.mode.value,
                    "final_permission": effective_permission,
                    "effective_permission_decision": effective_permission,
                    "would_block_if_enforced": result.would_block_if_enforced,
                    "fallback_used": True,
                    "fallback_reason": result.fallback_reason,
                    "remote_permission_as_of_ts": None,
                    "remote_permission_expires_at": None,
                    "remote_permission_status": "unavailable",
                    "contract_version": CONTRACT_VERSION,
                    "error_type": result.error_type,
                    "error_message_sanitized": result.error_message_sanitized,
                }
            )
            return result

    def refresh_permission(self, *, include_health: bool = True) -> str:
        if include_health and self.client is not None and bool(_get_cfg(self.cfg, "enabled", True)):
            try:
                self.client.get_health()
                self._emit_usage({"event_type": "health", "endpoint": "/v1/health", "success": True, "status": "ok"})
            except Exception as exc:
                self._emit_usage(
                    {
                        "event_type": "health",
                        "endpoint": "/v1/health",
                        "success": False,
                        "error_type": type(exc).__name__,
                        "error_message_sanitized": str(sanitize_quant_lab_obj(str(exc)[:300])),
                    }
                )
        return self.check_startup_permission(self.cfg, self.run_id).permission

    def filter_orders_by_permission(
        self,
        orders: Iterable[Order],
        permission_result: Optional[QuantLabGuardResult | str] = None,
    ) -> List[Order]:
        source = list(orders or [])
        if isinstance(permission_result, QuantLabGuardResult):
            permission = permission_result.permission
        elif permission_result:
            permission = str(permission_result)
        else:
            permission = self.permission_result.permission
        permission = normalize_permission(permission, allow_local=True)
        if permission == ALLOW_LOCAL:
            permission = ALLOW
        final_permission = self.final_permission or permission
        raw_permission = self.permission_result.raw_permission_decision or self.permission_result.permission
        kept: List[Order] = []
        for order in source:
            would_filter = False
            actually_filtered = False
            reason = ""
            if permission == ABORT:
                would_filter = True
                reason = "quant_lab_abort"
            elif permission == SELL_ONLY and not _is_sell_or_close(order):
                would_filter = True
                reason = "quant_lab_sell_only"
            if self.apply_permission_gate:
                actually_filtered = would_filter
            meta = dict(getattr(order, "meta", None) or {})
            qmeta = dict(meta.get("quant_lab") or {})
            qmeta.update(
                {
                    "permission": self.permission_result.permission,
                    "quant_lab_permission": self.permission_result.permission,
                    "raw_permission_decision": raw_permission,
                    "local_mode": self.mode.value,
                    "final_permission": final_permission,
                    "effective_permission_decision": final_permission,
                    "would_block_if_enforced": would_filter,
                    "fallback_used": bool(self.permission_result.fallback_used),
                    "fallback_reason": self.permission_result.fallback_reason,
                    "remote_permission_as_of_ts": self.permission_result.remote_permission_as_of_ts or self.permission_result.response_ts,
                    "remote_permission_expires_at": self.permission_result.remote_permission_expires_at,
                    "remote_permission_status": self.permission_result.remote_permission_status,
                    "contract_version": self.permission_result.contract_version,
                    "permission_gate_enforced": self.apply_permission_gate,
                    "cost_gate_enforced": self.apply_cost_gate,
                    "would_filter_by_permission": would_filter,
                    "order_filtered": actually_filtered,
                    "actually_filtered": actually_filtered,
                    "filter_reason": reason,
                    "response_ts": self.permission_result.response_ts or utc_now_iso(),
                }
            )
            meta["quant_lab"] = qmeta
            order.meta = sanitize_quant_lab_obj(meta)
            row = {
                "event_type": "filter_order",
                "symbol": getattr(order, "symbol", None),
                "side": getattr(order, "side", None),
                "intent": getattr(order, "intent", None),
                "notional_usdt": float(getattr(order, "notional_usdt", 0.0) or 0.0),
                "permission": self.permission_result.permission,
                "quant_lab_permission": self.permission_result.permission,
                "raw_permission_decision": raw_permission,
                "local_mode": self.mode.value,
                "final_permission": final_permission,
                "effective_permission_decision": final_permission,
                "would_block_if_enforced": would_filter,
                "fallback_used": bool(self.permission_result.fallback_used),
                "fallback_reason": self.permission_result.fallback_reason,
                "remote_permission_as_of_ts": self.permission_result.remote_permission_as_of_ts or self.permission_result.response_ts,
                "remote_permission_expires_at": self.permission_result.remote_permission_expires_at,
                "remote_permission_status": self.permission_result.remote_permission_status,
                "contract_version": self.permission_result.contract_version,
                "would_filter": would_filter,
                "would_filter_by_permission": would_filter,
                "order_filtered": actually_filtered,
                "actually_filtered": actually_filtered,
                "filter_reason": reason,
                "enforced": self.apply_permission_gate,
                "hypothetical": bool(would_filter and not self.apply_permission_gate),
            }
            self.filtered_orders.append(dict(sanitize_quant_lab_obj(row)))
            if would_filter or actually_filtered:
                self._emit_usage(row)
            if not actually_filtered:
                kept.append(order)
        return kept

    def enrich_orders_with_cost(
        self,
        orders: Iterable[Order],
        regime: str,
        cfg: Any,
    ) -> Tuple[List[Order], List[Dict[str, Any]]]:
        source = list(orders or [])
        qcfg = _ql_cfg(cfg)
        if self.mode == QuantLabMode.LOCAL_ONLY:
            self._emit_usage(
                {
                    "event_type": "cost_estimate",
                    "status": "local_only",
                    "success": True,
                    "called_api": False,
                    "fallback_used": False,
                    "source": "local_only",
                    "cost_source": "local_only",
                    "skipped_reason": "quant_lab_local_only",
                }
            )
            return source, []
        if self.mode == QuantLabMode.PERMISSION_ONLY:
            self._emit_usage(
                {
                    "event_type": "cost_estimate",
                    "status": "permission_only_skip_cost",
                    "success": True,
                    "called_api": self.called_api,
                    "fallback_used": False,
                    "source": "permission_only",
                    "cost_source": "permission_only_skip_cost",
                }
            )
            return source, []
        if not bool(getattr(qcfg, "enabled", False)) or not bool(getattr(qcfg, "cost_enabled", True)):
            self._emit_usage({"event_type": "cost_estimate", "status": "disabled", "success": True, "fallback_used": False})
            return source, []

        kept: List[Order] = []
        rows: List[Dict[str, Any]] = []
        quantile = str(getattr(qcfg, "cost_quantile", "p75") or "p75")
        strategy_id = str(getattr(qcfg, "strategy_name", "v5") or "v5")
        for idx, order in enumerate(source):
            fallback_used = False
            fallback_reason = None
            original_symbol = str(getattr(order, "symbol", "") or "")
            normalized_symbol = symbol_to_quant_lab_symbol(original_symbol)
            side = str(getattr(order, "side", "") or "").lower()
            expected_edge_for_request, _expected_edge_source = order_expected_edge_detail(order)
            request_id = f"{self.run_id or 'run'}:cost:{idx}:{normalized_symbol}"
            try:
                if self.client is None:
                    raise QuantLabError("quant-lab client disabled")
                self.request_count += 1
                estimate = self.client.estimate_cost(
                    symbol=original_symbol,
                    regime=str(regime or "normal"),
                    notional_usdt=float(getattr(order, "notional_usdt", 0.0) or 0.0),
                    quantile=quantile,
                    side=side,
                    strategy_id=strategy_id,
                    expected_edge_bps=expected_edge_for_request,
                    request_id=request_id,
                    venue="OKX",
                    instrument_type="spot",
                )
            except Exception as exc:
                self.request_error_count += 1
                if bool(getattr(qcfg, "cost_fallback_to_local", True)):
                    estimate = _local_cost_estimate(order, cfg, regime=regime, quantile=quantile)
                    fallback_used = True
                    fallback_reason = "quant_lab_cost_unavailable_local_fallback"
                else:
                    fail_policy = str(getattr(qcfg, "fail_policy", "sell_only") or "sell_only").lower()
                    if fail_policy == "abort" and self.apply_cost_gate:
                        self._emit_usage(
                            {
                                "event_type": "fallback",
                                "reason": "quant_lab_cost_unavailable_abort",
                                "fallback_policy": fail_policy,
                                "action_taken": "abort_orders",
                                "error_type": type(exc).__name__,
                                "error_message_sanitized": str(sanitize_quant_lab_obj(str(exc)[:300])),
                            }
                        )
                        return [], rows
                    if fail_policy == "sell_only" and self.apply_cost_gate and not _is_sell_or_close(order):
                        self._emit_usage(
                            {
                                "event_type": "filter_order",
                                "symbol": getattr(order, "symbol", None),
                                "permission": SELL_ONLY,
                                "final_permission": SELL_ONLY,
                                "order_filtered": True,
                                "filter_reason": "quant_lab_cost_unavailable_sell_only",
                            }
                        )
                        continue
                    estimate = _local_cost_estimate(order, cfg, regime=regime, quantile=quantile)
                    fallback_used = True
                    fallback_reason = "quant_lab_cost_unavailable_local_fallback"

            gate: CostGateResult = apply_quant_lab_cost_gate(order, estimate, cfg, mode=self.mode.value)
            actually_filtered = bool(gate.filtered and self.apply_cost_gate)
            row = {
                "event_type": "cost_estimate",
                "symbol": original_symbol,
                "normalized_symbol": normalized_symbol,
                "venue": "OKX",
                "instrument_type": "spot",
                "side": side,
                "intent": getattr(order, "intent", None),
                "strategy_id": strategy_id,
                "request_id": request_id,
                "regime": gate.regime,
                "notional_usdt": gate.notional_usdt,
                "quantile": gate.quantile,
                "fee_bps": gate.fee_bps,
                "slippage_bps": gate.slippage_bps,
                "spread_bps": gate.spread_bps,
                "total_cost_bps": gate.total_cost_bps,
                "effective_total_cost_bps": gate.effective_total_cost_bps,
                "local_cost_bps": gate.local_cost_bps,
                "local_cost_source": gate.local_cost_source,
                "fallback_level": gate.fallback_level,
                "source": gate.source,
                "cost_source": gate.source,
                "sample_count": gate.sample_count,
                "cost_model_version": gate.cost_model_version,
                "total_cost_bps_p50": estimate.total_cost_bps_p50,
                "total_cost_bps_p75": estimate.total_cost_bps_p75,
                "total_cost_bps_p90": estimate.total_cost_bps_p90,
                "expected_edge_bps": gate.expected_edge_bps,
                "expected_edge_source": gate.expected_edge_source or gate.proxy_source,
                "min_required_edge_bps": gate.min_required_edge_bps,
                "required_edge_bps": estimate.required_edge_bps
                if estimate.required_edge_bps is not None
                else gate.min_required_edge_bps,
                "proxy_source": gate.proxy_source,
                "passed": gate.passed,
                "filtered": gate.filtered,
                "filter_reason": gate.reason,
                "would_filter": bool(gate.filtered),
                "would_filter_by_cost": bool(gate.filtered),
                "would_block_by_cost": bool(gate.filtered),
                "actually_filtered": actually_filtered,
                "order_filtered": actually_filtered,
                "cost_gate_enforced": self.apply_cost_gate,
                "enforced": self.apply_cost_gate,
                "hypothetical": bool(gate.filtered and not self.apply_cost_gate),
                "fallback_used": fallback_used,
                "fallback_reason": fallback_reason or estimate.fallback_reason,
                "success": True,
            }
            rows.append(dict(sanitize_quant_lab_obj(row)))
            self.cost_rows.append(dict(sanitize_quant_lab_obj(row)))
            self._emit_usage(row)
            meta = dict(getattr(order, "meta", None) or {})
            qmeta = dict(meta.get("quant_lab") or {})
            qmeta.update(
                {
                    "permission": self.permission_result.permission,
                    "raw_permission_decision": self.permission_result.raw_permission_decision or self.permission_result.permission,
                    "local_mode": self.mode.value,
                    "final_permission": qmeta.get("final_permission") or self.permission_result.permission,
                    "effective_permission_decision": qmeta.get("effective_permission_decision")
                    or qmeta.get("final_permission")
                    or self.permission_result.permission,
                    "would_block_if_enforced": qmeta.get("would_block_if_enforced", False),
                    "normalized_symbol": normalized_symbol,
                    "venue": "OKX",
                    "instrument_type": "spot",
                    "strategy_id": strategy_id,
                    "request_id": request_id,
                    "cost_model_version": gate.cost_model_version,
                    "cost_quantile": gate.quantile,
                    "fee_bps": gate.fee_bps,
                    "slippage_bps": gate.slippage_bps,
                    "spread_bps": gate.spread_bps,
                    "total_cost_bps": gate.total_cost_bps,
                    "effective_total_cost_bps": gate.effective_total_cost_bps,
                    "local_cost_bps": gate.local_cost_bps,
                    "local_cost_source": gate.local_cost_source,
                    "fallback_level": gate.fallback_level,
                    "source": gate.source,
                    "cost_source": gate.source,
                    "sample_count": gate.sample_count,
                    "total_cost_bps_p50": estimate.total_cost_bps_p50,
                    "total_cost_bps_p75": estimate.total_cost_bps_p75,
                    "total_cost_bps_p90": estimate.total_cost_bps_p90,
                    "expected_edge_bps": gate.expected_edge_bps,
                    "expected_edge_source": gate.expected_edge_source or gate.proxy_source,
                    "min_required_edge_bps": gate.min_required_edge_bps,
                    "required_edge_bps": estimate.required_edge_bps
                    if estimate.required_edge_bps is not None
                    else gate.min_required_edge_bps,
                    "proxy_source": gate.proxy_source,
                    "cost_gate_passed": gate.passed,
                    "cost_gate_enforced": self.apply_cost_gate,
                    "permission_gate_enforced": self.apply_permission_gate,
                    "would_filter_by_cost": bool(gate.filtered),
                    "would_block_by_cost": bool(gate.filtered),
                    "actually_filtered_by_cost": actually_filtered,
                    "fallback_used": bool(qmeta.get("fallback_used") or fallback_used),
                    "fallback_reason": fallback_reason or estimate.fallback_reason or qmeta.get("fallback_reason"),
                    "response_ts": utc_now_iso(),
                }
            )
            meta["quant_lab"] = qmeta
            order.meta = sanitize_quant_lab_obj(meta)
            if not actually_filtered:
                kept.append(order)
            else:
                self.filtered_orders.append(
                    {
                        "event_type": "filter_order",
                        "symbol": getattr(order, "symbol", None),
                        "side": getattr(order, "side", None),
                        "intent": getattr(order, "intent", None),
                        "order_filtered": True,
                        "filter_reason": gate.reason,
                    }
                )
                self._emit_usage(
                    {
                        "event_type": "filter_order",
                        "symbol": getattr(order, "symbol", None),
                        "side": getattr(order, "side", None),
                        "intent": getattr(order, "intent", None),
                        "order_filtered": True,
                        "filter_reason": gate.reason,
                    }
                )
        return kept, rows

    def filter_orders(self, orders: Iterable[Order]) -> Tuple[List[Order], Dict[str, Any]]:
        source = list(orders or [])
        before = len(source)
        filtered = self.filter_orders_by_permission(source, self.permission_result)
        filtered, rows = self.enrich_orders_with_cost(filtered, str(_get_cfg(self.cfg, "cost_regime", "normal") or "normal"), self.cfg)
        if self.apply_permission_gate:
            filtered = self.filter_orders_by_permission(filtered, self.permission_result)
        summary = self.summary_payload(orders_before=before, orders_after=len(filtered))
        return filtered, summary

    def summary_payload(self, *, orders_before: Optional[int] = None, orders_after: Optional[int] = None) -> Dict[str, Any]:
        permission = self.permission_result
        permission_rows = [
            row
            for row in self.filtered_orders
            if str(row.get("filter_reason", "")).startswith("quant_lab_sell") or row.get("filter_reason") == "quant_lab_abort"
        ]
        would_filter_by_permission = len([row for row in permission_rows if row.get("would_filter") or row.get("would_filter_by_permission")])
        filtered_by_permission = len([row for row in permission_rows if row.get("actually_filtered") or row.get("order_filtered")])
        would_filter_by_cost = len([row for row in self.cost_rows if row.get("would_filter") or row.get("would_filter_by_cost")])
        filtered_by_cost = len(
            [row for row in self.cost_rows if row.get("actually_filtered") or row.get("order_filtered")]
        )
        cost_fallback = len([row for row in self.cost_rows if row.get("fallback_used")])
        final_permission = self.final_permission or ("LOCAL_ONLY" if self.mode == QuantLabMode.LOCAL_ONLY else permission.permission)
        client = self.client
        return sanitize_quant_lab_obj(
            {
                "enabled": bool(permission.enabled),
                "mode": self.mode.value,
                "mode_source": self.mode_resolution.mode_source,
                "mode_override_path": self.mode_resolution.override_path,
                "quant_lab_config_source": _get_cfg(self.cfg, "quant_lab_config_source", "unknown"),
                "legacy_execution_quant_lab_ignored": bool(_get_cfg(self.cfg, "legacy_execution_quant_lab_ignored", False)),
                "called_api": self.called_api,
                "apply_permission_gate": self.apply_permission_gate,
                "apply_cost_gate": self.apply_cost_gate,
                "permission_gate_enforced": self.apply_permission_gate,
                "cost_gate_enforced": self.apply_cost_gate,
                "api_env_path_present": getattr(client, "api_env_path_present", None) if client is not None else None,
                "api_env_secure_permissions": getattr(client, "api_env_secure_permissions", None) if client is not None else None,
                "api_env_token_loaded": getattr(client, "api_env_token_loaded", False) if client is not None else False,
                "api_env_warning": getattr(client, "api_env_warning", None) if client is not None else None,
                "permission": permission.permission,
                "quant_lab_permission": permission.permission,
                "raw_permission_decision": permission.raw_permission_decision or permission.permission,
                "local_mode": self.mode.value,
                "local_preflight_permission": self.local_preflight_permission,
                "final_permission": final_permission,
                "effective_permission_decision": final_permission,
                "would_block_if_enforced": permission.would_block_if_enforced,
                "remote_permission_as_of_ts": permission.remote_permission_as_of_ts or permission.response_ts,
                "remote_permission_expires_at": permission.remote_permission_expires_at,
                "remote_permission_status": permission.remote_permission_status,
                "contract_version": permission.contract_version,
                "allowed_modes": permission.allowed_modes,
                "risk_permission_reasons": permission.reasons,
                "cost_model_version": permission.cost_model_version
                or next((row.get("cost_model_version") for row in self.cost_rows if row.get("cost_model_version")), None),
                "gate_version": permission.gate_version,
                "fallback_used": bool(permission.fallback_used or cost_fallback),
                "fallback_reason": permission.fallback_reason,
                "request_count": int(self.request_count),
                "request_error_count": int(self.request_error_count),
                "cost_request_count": len(self.cost_rows),
                "cost_fallback_count": cost_fallback,
                "would_filter_by_permission_count": would_filter_by_permission,
                "filtered_by_permission_count": filtered_by_permission,
                "would_filter_by_cost_count": would_filter_by_cost,
                "filtered_by_cost_count": filtered_by_cost,
                "orders_before": orders_before,
                "orders_after": orders_after,
                "orders_filtered": None if orders_before is None or orders_after is None else max(0, int(orders_before) - int(orders_after)),
                "fail_policy": str(_get_cfg(self.cfg, "fail_policy", "sell_only") or "sell_only"),
                "cost_quantile": str(_get_cfg(self.cfg, "cost_quantile", "p75") or "p75"),
                "cost_min_edge_multiplier": float(_get_cfg(self.cfg, "cost_min_edge_multiplier", 1.5) or 1.5),
                "min_cost_bps_floor": float(_get_cfg(self.cfg, "min_cost_bps_floor", 5.0) or 0.0),
                "last_response_ts": permission.response_ts,
                "skipped_reason": permission.skipped_reason,
                "mode_warning": self.mode_resolution.warning,
            }
        )

    def audit_payload(self) -> Dict[str, Any]:
        payload = self.summary_payload()
        payload["permission_result"] = self.permission_result.to_dict()
        payload["cost_estimates"] = list(self.cost_rows)
        payload["filtered_orders"] = list(self.filtered_orders)
        payload["events_tail"] = self.events[-50:]
        return sanitize_quant_lab_obj(payload)
