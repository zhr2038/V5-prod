from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from src.core.models import Order
from src.reporting.quant_lab_audit import (
    EVENT_TYPE_FALLBACK,
    EVENT_TYPE_PERMISSION_AUDIT,
    EVENT_TYPE_REQUEST,
    append_quant_lab_usage,
    normalize_quant_lab_event,
    sanitize_quant_lab_obj,
    utc_now_iso,
)

from .client import QuantLabClient
from .cost_gate import CostGateResult, apply_quant_lab_cost_gate, local_cost_detail_for_order, order_expected_edge_detail
from .exceptions import QuantLabError
from .mode import QuantLabMode, QuantLabModeResolution, evaluate_enforce_readiness, resolve_quant_lab_mode
from .models import CostEstimate, RiskPermission, symbol_to_quant_lab_symbol
from .permissions import ABORT, ALLOW, ALLOW_LOCAL, SELL_ONLY, normalize_permission


CONTRACT_VERSION = "v5.quant_lab.telemetry.v2"


@dataclass
class QuantLabGuardResult:
    enabled: bool
    permission: str
    allowed_modes: List[Any] = field(default_factory=list)
    allowed_live_modes: Optional[List[Any]] = None
    reasons: List[Any] = field(default_factory=list)
    live_block_reasons: List[Any] = field(default_factory=list)
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
    raw_permission_status: Optional[str] = None
    raw_permission_enforceable: Optional[bool] = None
    local_mode: Optional[str] = None
    effective_permission_decision: Optional[str] = None
    would_block_if_enforced: bool = False
    shadow_override_reason: Optional[str] = None
    remote_permission_as_of_ts: Optional[str] = None
    remote_permission_expires_at: Optional[str] = None
    remote_permission_status: Optional[str] = None
    remote_permission_source_bundle_ts: Optional[str] = None
    remote_permission_telemetry_latest_ts: Optional[str] = None
    remote_permission_contract_version: Optional[str] = None
    permission_contract_violation: bool = False
    contract_version: str = CONTRACT_VERSION
    request_id: Optional[str] = None
    original_event_id: Optional[str] = None
    quant_lab_requested_mode: Optional[str] = None
    quant_lab_effective_mode: Optional[str] = None
    enforce_readiness_status: Optional[str] = None
    enforce_blocked_reasons: List[str] = field(default_factory=list)
    enforce_blocked_reason: Optional[str] = None
    contract_version_match: Optional[bool] = None
    telemetry_schema_version_match: Optional[bool] = None

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


def _is_live_open_candidate(order: Order) -> bool:
    if _is_sell_or_close(order):
        return False
    meta = dict(getattr(order, "meta", None) or {})
    if bool(meta.get("reduce_only")):
        return False
    side = str(getattr(order, "side", "") or "").lower()
    intent = str(getattr(order, "intent", "") or "").upper()
    return side == "buy" or intent in {"OPEN_LONG", "OPEN", "REBALANCE"}


def _is_paper_or_shadow_order(order: Order) -> bool:
    meta = dict(getattr(order, "meta", None) or {})
    mode = str(meta.get("mode") or meta.get("tracking_mode") or meta.get("recommended_mode") or "").strip().lower()
    if mode in {"paper", "shadow", "research"}:
        return True
    for key in ("paper", "paper_only", "paper_strategy", "shadow", "shadow_only", "shadow_strategy"):
        if bool(meta.get(key)):
            return True
    strategy_text = " ".join(
        str(meta.get(key) or "")
        for key in ("strategy_id", "strategy_candidate", "entry_reason", "experiment_name")
    ).lower()
    return "_paper_" in strategy_text or strategy_text.endswith("_paper") or "shadow" in strategy_text


def _normalize_strategy_token(value: Any) -> str:
    text = str(value or "").strip().upper()
    out = []
    for ch in text:
        out.append(ch if ch.isalnum() else "_")
    return "_".join(part for part in "".join(out).split("_") if part)


def _order_strategy_aliases(order: Order) -> set[str]:
    meta = dict(getattr(order, "meta", None) or {})
    aliases: set[str] = set()
    for key in ("strategy_id", "strategy_candidate", "entry_reason", "probe_type", "experiment_name"):
        token = _normalize_strategy_token(meta.get(key))
        if token:
            aliases.add(token)
    if bool(meta.get("btc_leadership_probe")) or "BTC_LEADERSHIP_PROBE" in aliases:
        aliases.add("BTC_STRICT_PROBE")
        aliases.add("BTC_LEADERSHIP_PROBE")
    if bool(meta.get("market_impulse_probe")):
        aliases.add("MARKET_IMPULSE_PROBE")
    return aliases


def _order_strategy_candidate(order: Order) -> str:
    meta = dict(getattr(order, "meta", None) or {})
    for key in ("strategy_candidate", "strategy_id", "entry_reason", "probe_type", "experiment_name"):
        value = str(meta.get(key) or "").strip()
        if value:
            return value
    aliases = sorted(_order_strategy_aliases(order))
    return aliases[0] if aliases else ""


def _strategy_matches_whitelist(order: Order, whitelist: Iterable[Any]) -> bool:
    aliases = _order_strategy_aliases(order)
    wanted = {_normalize_strategy_token(item) for item in whitelist or []}
    wanted.discard("")
    return bool(aliases and wanted and aliases.intersection(wanted))


def _list_value(value: Any) -> Optional[List[Any]]:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass
        return [part.strip() for part in text.split(",") if part.strip()]
    return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _permission_would_block(permission: str) -> bool:
    normalized = normalize_permission(permission, allow_local=True)
    return normalized in {ABORT, SELL_ONLY}


def _parse_utc(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    text = str(ts).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _status_upper(status: Optional[str]) -> str:
    return str(status or "").strip().upper()


_ACTIVE_PERMISSION_STATUSES = {"ACTIVE_ALLOW", "ACTIVE_SELL_ONLY", "ACTIVE_ABORT"}


def _permission_not_fresh(
    status: Optional[str],
    expires_at: Optional[str],
    enforceable: Optional[bool],
) -> Tuple[bool, bool, Optional[str], str]:
    status_u = _status_upper(status)
    expired_by_time = False
    contract_violation = False
    reason = None
    expires_text = str(expires_at or "").strip()
    expires_dt = _parse_utc(expires_at)
    if status_u.startswith("STALE") or status_u.startswith("EXPIRED") or status_u == "NO_FRESH_PERMISSION":
        reason = "remote_permission_not_fresh"
        return True, contract_violation, reason, status_u
    if status_u not in _ACTIVE_PERMISSION_STATUSES:
        reason = "remote_permission_status_incomplete"
        return True, True, reason, status_u or "MISSING_PERMISSION_STATUS"
    if not expires_text:
        reason = "remote_permission_expiry_missing"
        return True, True, reason, status_u
    if expires_dt is None:
        reason = "remote_permission_expiry_invalid"
        return True, True, reason, status_u
    if enforceable is not True:
        reason = "remote_permission_not_enforceable"
        return True, True, reason, status_u
    if expires_dt <= datetime.now(timezone.utc):
        expired_by_time = True
        reason = "remote_permission_not_fresh"
        contract_violation = True
    if expired_by_time:
        return True, contract_violation, reason, f"EXPIRED_{status_u}" if status_u else "EXPIRED"
    return False, contract_violation, reason, status_u


def _effective_permission_for_mode(
    raw_permission: str,
    status: Optional[str],
    expires_at: Optional[str],
    enforceable: Optional[bool],
    apply_gate: bool,
) -> Tuple[str, bool, Optional[str], str]:
    raw = normalize_permission(raw_permission)
    if not apply_gate:
        return ALLOW, False, "quant_lab_shadow_mode", _status_upper(status)
    not_fresh, contract_violation, reason, effective_status = _permission_not_fresh(status, expires_at, enforceable)
    if not_fresh:
        if raw == ABORT and reason in {
            "remote_permission_status_incomplete",
            "remote_permission_expiry_missing",
            "remote_permission_expiry_invalid",
            "remote_permission_not_enforceable",
        }:
            return ABORT, contract_violation, reason, effective_status
        return SELL_ONLY, contract_violation, reason, effective_status
    return raw, contract_violation, None, effective_status


def _degraded_cost_model(estimate: CostEstimate) -> bool:
    source = str(estimate.source or "").strip().lower()
    fallback_level = str(estimate.fallback_level or "").strip().upper()
    cost_model_version = str(estimate.cost_model_version or "").strip().lower()
    raw_source = str(estimate.raw_response.get("cost_source") or "").strip().lower() if isinstance(estimate.raw_response, Mapping) else ""
    return source == "global_default" or raw_source == "global_default" or fallback_level == "GLOBAL_DEFAULT" or cost_model_version == "global_default_v0"


_COST_TRUST_LEVEL_RANK = {
    "BLOCK": 0,
    "PAPER_ONLY": 1,
    "CANARY": 2,
    "SCALE_READY": 3,
}


def _normalize_cost_trust_level(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "_")
    return text if text in _COST_TRUST_LEVEL_RANK else ""


def _cost_trust_level_at_least(level: str, required: str) -> bool:
    normalized_level = _normalize_cost_trust_level(level) or "BLOCK"
    normalized_required = _normalize_cost_trust_level(required) or "SCALE_READY"
    return _COST_TRUST_LEVEL_RANK[normalized_level] >= _COST_TRUST_LEVEL_RANK[normalized_required]


def _estimate_cost_trust_level(estimate: CostEstimate, gate: CostGateResult) -> str:
    raw_response = getattr(estimate, "raw_response", {}) or {}
    raw_level = _first_present(
        getattr(estimate, "cost_trust_level", None),
        raw_response.get("cost_trust_level") if isinstance(raw_response, Mapping) else None,
        getattr(gate, "cost_trust_level", None),
    )
    level = _normalize_cost_trust_level(raw_level)
    if level:
        return level
    if getattr(estimate, "cost_trusted_for_live_scale", None) is True or getattr(gate, "cost_trusted_for_live_scale", None) is True:
        return "SCALE_READY"
    if getattr(estimate, "cost_trusted_for_live_canary", None) is True or getattr(gate, "cost_trusted_for_live_canary", None) is True:
        return "CANARY"
    trusted_live = _first_present(
        getattr(estimate, "cost_trusted_for_live", None),
        getattr(gate, "cost_trusted_for_live", None),
    )
    if trusted_live is True:
        return "SCALE_READY"
    if trusted_live is False:
        return "PAPER_ONLY"
    return "BLOCK"


def _guard_float_value(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _cost_model_diagnosis(*, degraded: bool, fallback_used: bool, gate_reason: str) -> str:
    if degraded:
        return "global_default_cost"
    if fallback_used:
        return "cost_fallback"
    if gate_reason == "expected_edge_missing_no_filter":
        return "expected_edge_missing_not_verified"
    return "ok"


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
    live_guard_rows: List[Dict[str, Any]] = field(default_factory=list)
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

    def _apply_enforce_readiness(self, permission: Optional[RiskPermission] = None) -> None:
        requested = self.mode_resolution.requested_mode or self.mode_resolution.mode
        if requested != QuantLabMode.ENFORCE:
            return
        readiness = evaluate_enforce_readiness(self.cfg, permission=permission)
        self.mode_resolution.enforce_readiness = readiness
        if readiness.status == "READY":
            self.mode_resolution.mode = QuantLabMode.ENFORCE
            return
        self.mode_resolution.mode = QuantLabMode.SHADOW
        if "enforce_blocked" not in str(self.mode_resolution.mode_source):
            self.mode_resolution.mode_source = f"{self.mode_resolution.mode_source}_enforce_blocked"
        self.mode_resolution.warning = (
            "quant-lab enforce requested but readiness is BLOCKED; effective mode downgraded to shadow"
        )

    def _mode_fields(self, *, enforced: Optional[bool] = None, hypothetical: Optional[bool] = None) -> Dict[str, Any]:
        client = self.client
        requested_mode = (self.mode_resolution.requested_mode or self.mode).value
        readiness = self.mode_resolution.enforce_readiness
        return {
            "mode": self.mode.value,
            "local_mode": self.mode.value,
            "quant_lab_requested_mode": requested_mode,
            "quant_lab_effective_mode": self.mode.value,
            "mode_source": self.mode_resolution.mode_source,
            "mode_override_path": self.mode_resolution.override_path,
            "enforce_readiness_status": readiness.status if readiness is not None else None,
            "enforce_blocked_reasons": list(readiness.reasons) if readiness is not None else [],
            "enforce_blocked_reason": ";".join(readiness.reasons) if readiness is not None and readiness.reasons else None,
            "contract_version_match": readiness.contract_version_match if readiness is not None else None,
            "telemetry_schema_version_match": readiness.telemetry_schema_version_match if readiness is not None else None,
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
                raw_permission_status="DISABLED",
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

    def _live_cost_trust_guard_row(
        self,
        *,
        order: Order,
        estimate: CostEstimate,
        gate: CostGateResult,
        cost_filtered_before_guard: bool,
        cfg: Any,
    ) -> Dict[str, Any]:
        qcfg = _ql_cfg(cfg)
        guard_cfg = getattr(qcfg, "live_cost_trust_guard", None)
        enabled = bool(getattr(guard_cfg, "enabled", False)) if guard_cfg is not None else False
        mode = str(getattr(guard_cfg, "mode", "observe_only") if guard_cfg is not None else "observe_only")
        mode = mode.strip().lower().replace("-", "_") or "observe_only"
        never_block_exits = bool(getattr(guard_cfg, "never_block_exits", True)) if guard_cfg is not None else True
        block_only_new_open = bool(getattr(guard_cfg, "block_only_new_open", True)) if guard_cfg is not None else True
        required_canary = _normalize_cost_trust_level(
            getattr(guard_cfg, "required_level_for_canary", "CANARY")
            if guard_cfg is not None
            else "CANARY"
        ) or "CANARY"
        required_normal = _normalize_cost_trust_level(
            getattr(guard_cfg, "required_level_for_normal_live", "SCALE_READY")
            if guard_cfg is not None
            else "SCALE_READY"
        ) or "SCALE_READY"
        canary_max_notional = max(
            _guard_float_value(
                getattr(guard_cfg, "canary_max_notional_usdt", 20.0)
                if guard_cfg is not None
                else 20.0,
                20.0,
            ),
            0.0,
        )

        meta = dict(getattr(order, "meta", None) or {})
        is_exit = _is_sell_or_close(order)
        is_open = _is_live_open_candidate(order)
        paper_or_shadow = _is_paper_or_shadow_order(order)
        would_have_opened_live = bool(is_open and not paper_or_shadow and not cost_filtered_before_guard)
        whitelist = _get_cfg(qcfg, "quant_lab_shadow_live_canary_whitelist", ["BTC_STRICT_PROBE"]) or []
        whitelist_match = bool(_strategy_matches_whitelist(order, whitelist))
        trusted_live = getattr(estimate, "cost_trusted_for_live", None)
        if trusted_live is None:
            trusted_live = getattr(gate, "cost_trusted_for_live", None)
        raw_response = getattr(estimate, "raw_response", {}) or {}
        cost_trust_level = _estimate_cost_trust_level(estimate, gate)
        normal_live_allowed = _cost_trust_level_at_least(cost_trust_level, required_normal)
        canary_trust_allowed = _cost_trust_level_at_least(cost_trust_level, required_canary)
        order_notional = max(_guard_float_value(getattr(order, "notional_usdt", 0.0), 0.0), 0.0)
        canary_notional_ok = canary_max_notional <= 0.0 or order_notional <= canary_max_notional
        canary_live_allowed = bool(canary_trust_allowed and whitelist_match and canary_notional_ok)
        live_trust_allowed = bool(normal_live_allowed or canary_live_allowed)
        untrusted_live = not live_trust_allowed
        raw_allowed_live_modes = _first_present(
            raw_response.get("allowed_live_modes"),
            raw_response.get("live_modes"),
            self.permission_result.allowed_live_modes,
            dict(meta.get("quant_lab") or {}).get("allowed_live_modes"),
        )
        allowed_live_modes = _list_value(raw_allowed_live_modes)
        no_live_modes = allowed_live_modes == []
        raw_permission = self.permission_result.raw_permission_decision or self.permission_result.permission

        reasons: list[str] = []
        if trusted_live is False:
            reasons.append("cost_untrusted_for_live")
        elif trusted_live is None and not getattr(estimate, "cost_trust_level", None):
            reasons.append("cost_trust_missing_for_live")
        if cost_trust_level == "PAPER_ONLY":
            reasons.append("cost_trust_level_paper_only")
        if not normal_live_allowed:
            reasons.append(f"cost_trust_level_lt_{required_normal.lower()}")
        if not canary_trust_allowed:
            reasons.append(f"cost_trust_level_lt_{required_canary.lower()}")
        if canary_trust_allowed and whitelist_match and not canary_notional_ok:
            reasons.append("canary_notional_exceeds_limit")
        if no_live_modes:
            reasons.append("quant_lab_allowed_live_modes_empty")
        if not whitelist_match and not normal_live_allowed and (untrusted_live or no_live_modes):
            reasons.append("strategy_not_in_canary_whitelist")
        if mode == "block_all_untrusted_open" and untrusted_live:
            reasons.append("block_all_untrusted_open")
        if paper_or_shadow:
            reasons.append("paper_or_shadow_bypass")
        if is_exit and never_block_exits:
            reasons.append("exit_bypass")
        if block_only_new_open and not is_open:
            reasons.append("not_new_open_bypass")

        would_be_blocked_by_no_live_modes = bool(would_have_opened_live and no_live_modes)
        would_be_blocked_by_cost_trust = bool(would_have_opened_live and untrusted_live)
        would_be_blocked_by_whitelist = bool(
            would_have_opened_live
            and not whitelist_match
            and not normal_live_allowed
            and (untrusted_live or no_live_modes)
        )
        would_be_blocked_by_canary_notional = bool(
            would_have_opened_live
            and canary_trust_allowed
            and whitelist_match
            and not canary_notional_ok
            and not normal_live_allowed
        )
        if mode == "block_non_whitelist_only":
            guard_condition = bool(
                would_be_blocked_by_cost_trust
                or would_be_blocked_by_no_live_modes
                or would_be_blocked_by_whitelist
                or would_be_blocked_by_canary_notional
            )
        elif mode == "block_all_untrusted_open":
            guard_condition = bool(would_be_blocked_by_cost_trust or would_be_blocked_by_no_live_modes)
        else:
            guard_condition = bool(
                would_be_blocked_by_no_live_modes
                or would_be_blocked_by_cost_trust
                or would_be_blocked_by_whitelist
                or would_be_blocked_by_canary_notional
            )
        guard_enforced = bool(
            enabled
            and self.apply_cost_gate
            and mode in {"block_non_whitelist_only", "block_all_untrusted_open"}
        )
        blocked = bool(guard_enforced and guard_condition)
        before = "BLOCKED_COST_GATE" if cost_filtered_before_guard else "ALLOW"
        after = "BLOCKED_COST_TRUST_GUARD" if blocked else before
        cost_trust_exception = bool(
            would_have_opened_live
            and canary_live_allowed
            and not normal_live_allowed
            and not no_live_modes
        )
        return {
            "event_type": "live_guard_impact",
            "ts_utc": utc_now_iso(),
            "symbol": getattr(order, "symbol", None),
            "strategy_candidate": _order_strategy_candidate(order),
            "intent": getattr(order, "intent", None),
            "side": getattr(order, "side", None),
            "would_have_opened_live": would_have_opened_live,
            "would_block_by_cost_trust_guard": bool(guard_condition),
            "would_be_blocked_by_quant_lab_no_live_modes": would_be_blocked_by_no_live_modes,
            "would_be_blocked_by_cost_trust_guard": would_be_blocked_by_cost_trust,
            "would_be_blocked_by_shadow_live_whitelist": would_be_blocked_by_whitelist,
            "would_be_blocked_by_canary_notional": would_be_blocked_by_canary_notional,
            "blocked_by_quant_lab_no_live_modes": bool(blocked and would_be_blocked_by_no_live_modes),
            "blocked_by_cost_trust_guard": blocked,
            "blocked_by_shadow_live_whitelist": bool(blocked and would_be_blocked_by_whitelist),
            "blocked_by_canary_notional": bool(blocked and would_be_blocked_by_canary_notional),
            "whitelist_strategy_match": whitelist_match,
            "cost_trust_exception": cost_trust_exception,
            "paper_or_shadow_bypassed": bool(paper_or_shadow),
            "cost_quality": gate.cost_quality,
            "cost_trusted_for_live": trusted_live,
            "cost_trusted_for_live_canary": getattr(estimate, "cost_trusted_for_live_canary", None),
            "cost_trusted_for_live_scale": getattr(estimate, "cost_trusted_for_live_scale", None),
            "cost_trust_level": cost_trust_level,
            "required_level_for_canary": required_canary,
            "required_level_for_normal_live": required_normal,
            "canary_max_notional_usdt": canary_max_notional,
            "order_notional_usdt": order_notional,
            "normal_live_allowed_by_cost_trust": normal_live_allowed,
            "canary_live_allowed_by_cost_trust": canary_live_allowed,
            "cost_trust_block_reasons": ";".join(dict.fromkeys(reasons)),
            "raw_permission_decision": raw_permission,
            "allowed_live_modes": json.dumps(allowed_live_modes) if allowed_live_modes is not None else "",
            "final_decision_actual": after,
            "final_decision_before_guard": before,
            "final_decision_after_guard": after,
            "guard_mode": mode,
            "guard_enabled": enabled,
            "guard_enforced": guard_enforced,
        }

    def record_final_permission(self, *, local_preflight_permission: str, final_permission: str) -> None:
        self.local_preflight_permission = str(local_preflight_permission or "")
        self.final_permission = str(final_permission or "")
        raw_permission = self.permission_result.raw_permission_decision or self.permission_result.permission
        live_modes_empty = self.permission_result.allowed_live_modes == []
        would_block = _permission_would_block(raw_permission) or live_modes_empty
        self._emit_usage(
            {
                "event_type": "final_permission",
                "request_id": self.permission_result.request_id,
                "original_request_id": self.permission_result.request_id,
                "original_event_id": self.permission_result.original_event_id,
                "permission": self.permission_result.permission,
                "quant_lab_permission": raw_permission,
                "raw_permission_decision": raw_permission,
                "raw_permission_status": self.permission_result.raw_permission_status,
                "raw_permission_enforceable": self.permission_result.raw_permission_enforceable,
                "local_mode": self.mode.value,
                "local_preflight_permission": self.local_preflight_permission,
                "final_permission": self.final_permission,
                "effective_permission_decision": self.final_permission,
                "would_block_if_enforced": would_block,
                "shadow_override_reason": self.permission_result.shadow_override_reason,
                "fallback_used": bool(self.permission_result.fallback_used),
                "fallback_reason": self.permission_result.fallback_reason,
                "remote_permission_as_of_ts": self.permission_result.remote_permission_as_of_ts or self.permission_result.response_ts,
                "remote_permission_expires_at": self.permission_result.remote_permission_expires_at,
                "remote_permission_status": self.permission_result.remote_permission_status,
                "remote_permission_source_bundle_ts": self.permission_result.remote_permission_source_bundle_ts,
                "remote_permission_telemetry_latest_ts": self.permission_result.remote_permission_telemetry_latest_ts,
                "remote_permission_contract_version": self.permission_result.remote_permission_contract_version,
                "permission_contract_violation": self.permission_result.permission_contract_violation,
                "contract_version": self.permission_result.contract_version or CONTRACT_VERSION,
                "quant_lab_requested_mode": self.permission_result.quant_lab_requested_mode
                or (self.mode_resolution.requested_mode or self.mode).value,
                "quant_lab_effective_mode": self.permission_result.quant_lab_effective_mode or self.mode.value,
                "enforce_readiness_status": self.permission_result.enforce_readiness_status,
                "enforce_blocked_reasons": self.permission_result.enforce_blocked_reasons,
                "enforce_blocked_reason": self.permission_result.enforce_blocked_reason,
                "contract_version_match": self.permission_result.contract_version_match,
                "telemetry_schema_version_match": self.permission_result.telemetry_schema_version_match,
                "allowed_live_modes": self.permission_result.allowed_live_modes,
                "live_block_reasons": self.permission_result.live_block_reasons,
                "live_modes_empty": live_modes_empty,
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

        strategy_name = str(getattr(qcfg, "strategy_name", "v5") or "v5")
        strategy_version = str(getattr(qcfg, "strategy_version", "5.0.0") or "5.0.0")
        permission_request_id = f"{self.run_id or 'run'}:permission:{strategy_name}:{strategy_version}"
        permission_request_event = normalize_quant_lab_event(
            {
                "run_id": self.run_id,
                "event_type": EVENT_TYPE_REQUEST,
                "request_id": permission_request_id,
                "endpoint_path": "/v1/risk/live-permission",
            },
            default_event_type=EVENT_TYPE_REQUEST,
        )

        try:
            self.request_count += 1
            try:
                permission: RiskPermission = self.client.get_live_permission(
                    strategy=strategy_name,
                    version=strategy_version,
                    request_id=permission_request_id,
                    event_id=permission_request_event["event_id"],
                    ts_utc=permission_request_event["ts_utc"],
                )
            except TypeError:
                permission = self.client.get_live_permission(strategy=strategy_name, version=strategy_version)
            raw_permission = normalize_permission(permission.permission)
            self._apply_enforce_readiness(permission)
            effective_permission, permission_contract_violation, permission_reason, effective_status = _effective_permission_for_mode(
                raw_permission,
                permission.permission_status or permission.status,
                permission.expires_at,
                permission.enforceable,
                self.apply_permission_gate,
            )
            shadow_override_reason = "quant_lab_shadow_mode" if not self.apply_permission_gate else None
            reasons = list(permission.risk_reason_codes or permission.reasons or [])
            live_block_reasons = list(permission.live_block_reasons or [])
            live_modes_empty = raw_permission == ALLOW and permission.allowed_live_modes == []
            if live_modes_empty:
                permission_reason = "remote_permission_allowed_live_modes_empty"
                if self.apply_permission_gate:
                    effective_permission = SELL_ONLY
                if "quant_lab_allowed_live_modes_empty" not in live_block_reasons:
                    live_block_reasons.append("quant_lab_allowed_live_modes_empty")
            if permission_reason and permission_reason not in reasons:
                reasons.append(permission_reason)
            readiness = self.mode_resolution.enforce_readiness
            if readiness is not None and readiness.status != "READY":
                for reason in readiness.reasons:
                    if reason not in reasons:
                        reasons.append(reason)
            raw_status = permission.permission_status or permission.status
            remote_contract_version = permission.contract_version or CONTRACT_VERSION
            result = QuantLabGuardResult(
                enabled=True,
                permission=effective_permission if self.apply_permission_gate else raw_permission,
                allowed_modes=list(permission.allowed_modes or []),
                allowed_live_modes=permission.allowed_live_modes,
                reasons=reasons,
                live_block_reasons=live_block_reasons,
                cost_model_version=permission.cost_model_version,
                gate_version=permission.gate_version,
                fallback_used=False,
                response_ts=permission.as_of_ts or permission.created_at or utc_now_iso(),
                mode=self.mode.value,
                mode_source=self.mode_resolution.mode_source,
                mode_override_path=self.mode_resolution.override_path,
                called_api=True,
                apply_permission_gate=self.apply_permission_gate,
                apply_cost_gate=self.apply_cost_gate,
                permission_gate_enforced=self.apply_permission_gate,
                cost_gate_enforced=self.apply_cost_gate,
                raw_permission_decision=raw_permission,
                raw_permission_status=raw_status,
                raw_permission_enforceable=permission.enforceable,
                local_mode=self.mode.value,
                effective_permission_decision=effective_permission,
                would_block_if_enforced=_permission_would_block(raw_permission) or live_modes_empty,
                shadow_override_reason=shadow_override_reason,
                remote_permission_as_of_ts=permission.as_of_ts or permission.created_at,
                remote_permission_expires_at=permission.expires_at,
                remote_permission_status=effective_status or raw_status,
                remote_permission_source_bundle_ts=permission.source_bundle_ts,
                remote_permission_telemetry_latest_ts=permission.telemetry_latest_ts,
                remote_permission_contract_version=remote_contract_version,
                permission_contract_violation=permission_contract_violation,
                contract_version=remote_contract_version,
                request_id=permission_request_id,
                original_event_id=permission_request_event["event_id"],
                quant_lab_requested_mode=(self.mode_resolution.requested_mode or self.mode).value,
                quant_lab_effective_mode=self.mode.value,
                enforce_readiness_status=readiness.status if readiness is not None else None,
                enforce_blocked_reasons=list(readiness.reasons) if readiness is not None else [],
                enforce_blocked_reason=";".join(readiness.reasons) if readiness is not None and readiness.reasons else None,
                contract_version_match=readiness.contract_version_match if readiness is not None else None,
                telemetry_schema_version_match=readiness.telemetry_schema_version_match if readiness is not None else None,
            )
            self.permission_result = result
            self._emit_usage(
                {
                    "event_type": EVENT_TYPE_PERMISSION_AUDIT,
                    "legacy_event_type": "live_permission",
                    "request_id": permission_request_id,
                    "original_request_id": permission_request_id,
                    "original_event_id": permission_request_event["event_id"],
                    "strategy": strategy_name,
                    "strategy_version": strategy_version,
                    "endpoint": "/v1/risk/live-permission",
                    "endpoint_path": "/v1/risk/live-permission",
                    "status": "ok",
                    "success": True,
                    "permission": result.permission,
                    "quant_lab_permission": raw_permission,
                    "raw_permission_decision": raw_permission,
                    "raw_permission_status": raw_status,
                    "raw_permission_enforceable": permission.enforceable,
                    "local_mode": self.mode.value,
                    "final_permission": effective_permission,
                    "effective_permission_decision": effective_permission,
                    "would_block_if_enforced": result.would_block_if_enforced,
                    "shadow_override_reason": shadow_override_reason,
                    "fallback_used": False,
                    "fallback_reason": None,
                    "remote_permission_as_of_ts": result.remote_permission_as_of_ts or result.response_ts,
                    "remote_permission_expires_at": result.remote_permission_expires_at,
                    "remote_permission_status": result.remote_permission_status,
                    "remote_permission_source_bundle_ts": result.remote_permission_source_bundle_ts,
                    "remote_permission_telemetry_latest_ts": result.remote_permission_telemetry_latest_ts,
                    "remote_permission_contract_version": result.remote_permission_contract_version,
                    "permission_contract_violation": result.permission_contract_violation,
                    "contract_version": result.contract_version,
                    "quant_lab_requested_mode": result.quant_lab_requested_mode,
                    "quant_lab_effective_mode": result.quant_lab_effective_mode,
                    "enforce_readiness_status": result.enforce_readiness_status,
                    "enforce_blocked_reasons": result.enforce_blocked_reasons,
                    "enforce_blocked_reason": result.enforce_blocked_reason,
                    "contract_version_match": result.contract_version_match,
                    "telemetry_schema_version_match": result.telemetry_schema_version_match,
                    "allowed_modes": result.allowed_modes,
                    "allowed_live_modes": result.allowed_live_modes,
                    "live_block_reasons": result.live_block_reasons,
                    "max_gross_exposure_usdt": permission.max_gross_exposure_usdt,
                    "max_single_order_usdt": permission.max_single_order_usdt,
                    "risk_reason_codes": list(permission.risk_reason_codes or []),
                    "cost_model_version": result.cost_model_version,
                    "gate_version": result.gate_version,
                    "enforced": self.apply_permission_gate,
                    "hypothetical": not self.apply_permission_gate,
                }
            )
            return result
        except Exception as exc:
            self.request_error_count += 1
            self._apply_enforce_readiness(None)
            readiness = self.mode_resolution.enforce_readiness
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
                request_id=permission_request_id,
                original_event_id=permission_request_event["event_id"],
                quant_lab_requested_mode=(self.mode_resolution.requested_mode or self.mode).value,
                quant_lab_effective_mode=self.mode.value,
                enforce_readiness_status=readiness.status if readiness is not None else None,
                enforce_blocked_reasons=list(readiness.reasons) if readiness is not None else [],
                enforce_blocked_reason=";".join(readiness.reasons) if readiness is not None and readiness.reasons else None,
                contract_version_match=readiness.contract_version_match if readiness is not None else None,
                telemetry_schema_version_match=readiness.telemetry_schema_version_match if readiness is not None else None,
            )
            self.permission_result = result
            self._emit_usage(
                {
                    "event_type": EVENT_TYPE_FALLBACK,
                    "legacy_event_type": "fallback",
                    "request_id": permission_request_id,
                    "original_request_id": permission_request_id,
                    "original_event_id": permission_request_event["event_id"],
                    "endpoint": "/v1/risk/live-permission",
                    "endpoint_path": "/v1/risk/live-permission",
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
                    "quant_lab_requested_mode": result.quant_lab_requested_mode,
                    "quant_lab_effective_mode": result.quant_lab_effective_mode,
                    "enforce_readiness_status": result.enforce_readiness_status,
                    "enforce_blocked_reasons": result.enforce_blocked_reasons,
                    "enforce_blocked_reason": result.enforce_blocked_reason,
                    "contract_version_match": result.contract_version_match,
                    "telemetry_schema_version_match": result.telemetry_schema_version_match,
                    "error_type": result.error_type,
                    "error_message_sanitized": result.error_message_sanitized,
                }
            )
            return result

    def refresh_permission(self, *, include_health: bool = True) -> str:
        if include_health and self.client is not None and bool(_get_cfg(self.cfg, "enabled", True)):
            try:
                health = self.client.get_health()
                deep_health = None
                deep_health_getter = getattr(self.client, "get_deep_health", None)
                if callable(deep_health_getter):
                    deep_health = deep_health_getter()
                deep_cost_health = getattr(deep_health, "cost_health", {}) if deep_health is not None else {}
                if not isinstance(deep_cost_health, dict):
                    deep_cost_health = {}
                deep_data_health = getattr(deep_health, "data_health", {}) if deep_health is not None else {}
                if not isinstance(deep_data_health, dict):
                    deep_data_health = {}
                deep_risk_dependency = (
                    getattr(deep_health, "risk_permission_dependency_meta", {})
                    if deep_health is not None
                    else {}
                )
                if not isinstance(deep_risk_dependency, dict):
                    deep_risk_dependency = {}
                self._emit_usage(
                    {
                        "event_type": "health_check",
                        "legacy_event_type": "health",
                        "endpoint": "/v1/health",
                        "endpoint_path": "/v1/health",
                        "success": True,
                        "fallback_used": False,
                        "status": getattr(health, "status", "ok"),
                        "deep_health_status": getattr(deep_health, "status", None),
                        "deep_health_warnings": list(getattr(deep_health, "warnings", []) or []),
                        "deep_cost_health_status": deep_cost_health.get("status"),
                        "deep_cost_fallback_ratio": deep_cost_health.get("fallback_ratio"),
                        "deep_cost_hard_fallback_ratio": deep_cost_health.get("hard_fallback_ratio"),
                        "deep_cost_soft_fallback_ratio": deep_cost_health.get("soft_fallback_ratio"),
                        "deep_cost_actual_rows": deep_cost_health.get("actual_rows"),
                        "deep_cost_mixed_rows": deep_cost_health.get("mixed_rows"),
                        "deep_cost_proxy_rows": deep_cost_health.get("proxy_rows"),
                        "deep_cost_global_default_rows": deep_cost_health.get("global_default_rows"),
                        "deep_cost_proxy_only_count": deep_cost_health.get("proxy_only_count"),
                        "deep_cost_symbols_missing": deep_cost_health.get("symbols_missing_cost"),
                        "deep_cost_warnings": deep_cost_health.get("warnings"),
                        "deep_data_health_status": deep_data_health.get("status"),
                        "deep_risk_dependency_status": deep_risk_dependency.get("status"),
                    }
                )
            except Exception as exc:
                self.request_error_count += 1
                self._emit_usage(
                    {
                        "event_type": "health_check",
                        "legacy_event_type": "health",
                        "endpoint": "/v1/health",
                        "endpoint_path": "/v1/health",
                        "success": False,
                        "error_type": type(exc).__name__,
                        "error_message_sanitized": str(sanitize_quant_lab_obj(str(exc)[:300])),
                    }
                )
                qcfg = _ql_cfg(self.cfg)
                self._apply_enforce_readiness(None)
                readiness = self.mode_resolution.enforce_readiness
                fail_policy = str(getattr(qcfg, "fail_policy", "sell_only") or "sell_only").lower()
                action = _fail_policy_action(fail_policy)
                permission = ALLOW if action == ALLOW_LOCAL else action
                effective_permission = permission if self.apply_permission_gate else ALLOW
                request_id = f"{self.run_id or 'run'}:health"
                result = QuantLabGuardResult(
                    enabled=True,
                    permission=permission,
                    allowed_modes=["local"] if action == ALLOW_LOCAL else [permission.lower()],
                    reasons=["quant_lab_health_unavailable"],
                    fallback_used=True,
                    fallback_reason=f"quant_lab_health_unavailable_{fail_policy}",
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
                    request_id=request_id,
                    quant_lab_requested_mode=(self.mode_resolution.requested_mode or self.mode).value,
                    quant_lab_effective_mode=self.mode.value,
                    enforce_readiness_status=readiness.status if readiness is not None else None,
                    enforce_blocked_reasons=list(readiness.reasons) if readiness is not None else [],
                    enforce_blocked_reason=";".join(readiness.reasons) if readiness is not None and readiness.reasons else None,
                    contract_version_match=readiness.contract_version_match if readiness is not None else None,
                    telemetry_schema_version_match=readiness.telemetry_schema_version_match if readiness is not None else None,
                )
                self.permission_result = result
                self._emit_usage(
                    {
                        "event_type": EVENT_TYPE_FALLBACK,
                        "legacy_event_type": "fallback",
                        "request_id": request_id,
                        "original_request_id": request_id,
                        "endpoint": "/v1/health",
                        "endpoint_path": "/v1/health",
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
                        "quant_lab_requested_mode": result.quant_lab_requested_mode,
                        "quant_lab_effective_mode": result.quant_lab_effective_mode,
                        "enforce_readiness_status": result.enforce_readiness_status,
                        "enforce_blocked_reasons": result.enforce_blocked_reasons,
                        "enforce_blocked_reason": result.enforce_blocked_reason,
                        "contract_version_match": result.contract_version_match,
                        "telemetry_schema_version_match": result.telemetry_schema_version_match,
                        "error_type": result.error_type,
                        "error_message_sanitized": result.error_message_sanitized,
                    }
                )
                return result.permission
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
        final_permission = self.final_permission or self.permission_result.effective_permission_decision or permission
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
            elif self.permission_result.allowed_live_modes == [] and not _is_sell_or_close(order):
                would_filter = True
                reason = "quant_lab_allowed_live_modes_empty"
            if self.apply_permission_gate:
                actually_filtered = would_filter
            meta = dict(getattr(order, "meta", None) or {})
            qmeta = dict(meta.get("quant_lab") or {})
            qmeta.update(
                {
                    "permission": self.permission_result.permission,
                    "quant_lab_permission": raw_permission,
                    "raw_permission_decision": raw_permission,
                    "raw_permission_status": self.permission_result.raw_permission_status,
                    "raw_permission_enforceable": self.permission_result.raw_permission_enforceable,
                    "local_mode": self.mode.value,
                    "final_permission": final_permission,
                    "effective_permission_decision": final_permission,
                    "would_block_if_enforced": would_filter,
                    "shadow_override_reason": self.permission_result.shadow_override_reason,
                    "fallback_used": bool(self.permission_result.fallback_used),
                    "fallback_reason": self.permission_result.fallback_reason,
                    "remote_permission_as_of_ts": self.permission_result.remote_permission_as_of_ts or self.permission_result.response_ts,
                    "remote_permission_expires_at": self.permission_result.remote_permission_expires_at,
                    "remote_permission_status": self.permission_result.remote_permission_status,
                    "remote_permission_source_bundle_ts": self.permission_result.remote_permission_source_bundle_ts,
                    "remote_permission_telemetry_latest_ts": self.permission_result.remote_permission_telemetry_latest_ts,
                    "remote_permission_contract_version": self.permission_result.remote_permission_contract_version,
                    "permission_contract_violation": self.permission_result.permission_contract_violation,
                    "contract_version": self.permission_result.contract_version,
                    "allowed_live_modes": self.permission_result.allowed_live_modes,
                    "live_block_reasons": self.permission_result.live_block_reasons,
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
                "request_id": self.permission_result.request_id,
                "original_request_id": self.permission_result.request_id,
                "original_event_id": self.permission_result.original_event_id,
                "symbol": getattr(order, "symbol", None),
                "side": getattr(order, "side", None),
                "intent": getattr(order, "intent", None),
                "notional_usdt": float(getattr(order, "notional_usdt", 0.0) or 0.0),
                "permission": self.permission_result.permission,
                "quant_lab_permission": raw_permission,
                "raw_permission_decision": raw_permission,
                "raw_permission_status": self.permission_result.raw_permission_status,
                "raw_permission_enforceable": self.permission_result.raw_permission_enforceable,
                "local_mode": self.mode.value,
                "final_permission": final_permission,
                "effective_permission_decision": final_permission,
                "would_block_if_enforced": would_filter,
                "shadow_override_reason": self.permission_result.shadow_override_reason,
                "fallback_used": bool(self.permission_result.fallback_used),
                "fallback_reason": self.permission_result.fallback_reason,
                "remote_permission_as_of_ts": self.permission_result.remote_permission_as_of_ts or self.permission_result.response_ts,
                "remote_permission_expires_at": self.permission_result.remote_permission_expires_at,
                "remote_permission_status": self.permission_result.remote_permission_status,
                "remote_permission_source_bundle_ts": self.permission_result.remote_permission_source_bundle_ts,
                "remote_permission_telemetry_latest_ts": self.permission_result.remote_permission_telemetry_latest_ts,
                "remote_permission_contract_version": self.permission_result.remote_permission_contract_version,
                "permission_contract_violation": self.permission_result.permission_contract_violation,
                "contract_version": self.permission_result.contract_version,
                "allowed_live_modes": self.permission_result.allowed_live_modes,
                "live_block_reasons": self.permission_result.live_block_reasons,
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
            cost_request_event = normalize_quant_lab_event(
                {
                    "run_id": self.run_id,
                    "event_type": EVENT_TYPE_REQUEST,
                    "request_id": request_id,
                    "endpoint_path": "/v1/costs/estimate",
                    "symbol": original_symbol,
                    "normalized_symbol": normalized_symbol,
                },
                default_event_type=EVENT_TYPE_REQUEST,
            )
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
                    event_id=cost_request_event["event_id"],
                    ts_utc=cost_request_event["ts_utc"],
                    venue="OKX",
                    instrument_type="spot",
                )
            except Exception as exc:
                self.request_error_count += 1
                if bool(getattr(qcfg, "cost_fallback_to_local", True)):
                    estimate = _local_cost_estimate(order, cfg, regime=regime, quantile=quantile)
                    fallback_used = True
                    fallback_reason = "quant_lab_cost_unavailable_local_fallback"
                    self._emit_usage(
                        {
                            "event_type": EVENT_TYPE_FALLBACK,
                            "legacy_event_type": "fallback",
                            "request_id": request_id,
                            "original_request_id": request_id,
                            "original_event_id": cost_request_event["event_id"],
                            "endpoint_path": "/v1/costs/estimate",
                            "endpoint": "/v1/costs/estimate",
                            "symbol": original_symbol,
                            "normalized_symbol": normalized_symbol,
                            "side": side,
                            "success": False,
                            "fallback_used": True,
                            "fallback_reason": fallback_reason,
                            "fallback_policy": str(getattr(qcfg, "fail_policy", "sell_only") or "sell_only"),
                            "fallback_scope": "cost_usage",
                            "action_taken": "local_cost_model",
                            "error_type": type(exc).__name__,
                            "error_message_sanitized": str(sanitize_quant_lab_obj(str(exc)[:300])),
                        }
                    )
                else:
                    fail_policy = str(getattr(qcfg, "fail_policy", "sell_only") or "sell_only").lower()
                    if fail_policy == "abort" and self.apply_cost_gate:
                        self._emit_usage(
                            {
                                "event_type": EVENT_TYPE_FALLBACK,
                                "legacy_event_type": "fallback",
                                "request_id": request_id,
                                "original_request_id": request_id,
                                "original_event_id": cost_request_event["event_id"],
                                "endpoint_path": "/v1/costs/estimate",
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
                                "request_id": request_id,
                                "original_request_id": request_id,
                                "original_event_id": cost_request_event["event_id"],
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
                    self._emit_usage(
                        {
                            "event_type": EVENT_TYPE_FALLBACK,
                            "legacy_event_type": "fallback",
                            "request_id": request_id,
                            "original_request_id": request_id,
                            "original_event_id": cost_request_event["event_id"],
                            "endpoint_path": "/v1/costs/estimate",
                            "endpoint": "/v1/costs/estimate",
                            "symbol": original_symbol,
                            "normalized_symbol": normalized_symbol,
                            "side": side,
                            "success": False,
                            "fallback_used": True,
                            "fallback_reason": fallback_reason,
                            "fallback_policy": fail_policy,
                            "fallback_scope": "cost_usage",
                            "action_taken": "local_cost_model",
                            "error_type": type(exc).__name__,
                            "error_message_sanitized": str(sanitize_quant_lab_obj(str(exc)[:300])),
                        }
                    )

            gate: CostGateResult = apply_quant_lab_cost_gate(order, estimate, cfg, mode=self.mode.value)
            cost_filtered_before_guard = bool(gate.filtered and self.apply_cost_gate)
            live_guard_row = self._live_cost_trust_guard_row(
                order=order,
                estimate=estimate,
                gate=gate,
                cost_filtered_before_guard=cost_filtered_before_guard,
                cfg=cfg,
            )
            live_guard_blocked = bool(live_guard_row.get("blocked_by_cost_trust_guard"))
            actually_filtered = bool(cost_filtered_before_guard or live_guard_blocked)
            filter_reason = "cost_trust_guard_blocked" if live_guard_blocked and not cost_filtered_before_guard else gate.reason
            degraded_cost_model = _degraded_cost_model(estimate)
            fallback_used_for_cost_model = bool(fallback_used or degraded_cost_model)
            required_edge_bps = estimate.required_edge_bps if estimate.required_edge_bps is not None else gate.min_required_edge_bps
            warning = "expected_edge_missing_cost_gate_not_verified" if gate.reason == "expected_edge_missing_no_filter" else None
            diagnosis = _cost_model_diagnosis(
                degraded=degraded_cost_model,
                fallback_used=fallback_used,
                gate_reason=gate.reason,
            )
            cost_fallback_reason = fallback_reason or estimate.fallback_reason or ("global_default_cost" if degraded_cost_model else None)
            row = {
                "event_type": "cost_estimate",
                "contract_version": CONTRACT_VERSION,
                "symbol": original_symbol,
                "request_symbol": original_symbol,
                "normalized_symbol": normalized_symbol,
                "response_symbol": estimate.symbol,
                "venue": "OKX",
                "instrument_type": "spot",
                "side": side,
                "intent": getattr(order, "intent", None),
                "strategy_id": strategy_id,
                "request_id": request_id,
                "original_request_id": request_id,
                "original_event_id": cost_request_event["event_id"],
                "requested_regime": str(regime or "normal"),
                "requested_quantile": quantile,
                "matched_regime": estimate.matched_regime or estimate.regime or gate.regime,
                "regime": gate.regime,
                "notional_usdt": gate.notional_usdt,
                "quantile": gate.quantile,
                "fee_bps": gate.fee_bps,
                "slippage_bps": gate.slippage_bps,
                "spread_bps": gate.spread_bps,
                "total_cost_bps": gate.total_cost_bps,
                "effective_total_cost_bps": gate.effective_total_cost_bps,
                "one_way_all_in_cost_bps": gate.one_way_all_in_cost_bps,
                "roundtrip_all_in_cost_bps": gate.roundtrip_all_in_cost_bps,
                "selected_entry_gate_cost_bps": gate.selected_entry_gate_cost_bps,
                "local_cost_bps": gate.local_cost_bps,
                "local_cost_source": gate.local_cost_source,
                "fallback_level": gate.fallback_level,
                "source": gate.source,
                "cost_source": gate.source,
                "cost_quality": gate.cost_quality,
                "cost_trusted_for_paper": gate.cost_trusted_for_paper,
                "cost_trusted_for_live": gate.cost_trusted_for_live,
                "sample_count": gate.sample_count,
                "cost_model_version": gate.cost_model_version,
                "cost_contract_version": CONTRACT_VERSION,
                "as_of_ts": estimate.as_of_ts,
                "selected_total_cost_bps": estimate.total_cost_bps,
                "total_cost_bps_p50": estimate.total_cost_bps_p50,
                "total_cost_bps_p75": estimate.total_cost_bps_p75,
                "total_cost_bps_p90": estimate.total_cost_bps_p90,
                "expected_edge_bps": gate.expected_edge_bps,
                "expected_edge_source": gate.expected_edge_source or gate.proxy_source,
                "min_required_edge_bps": gate.min_required_edge_bps,
                "required_edge_bps": required_edge_bps,
                "proxy_source": gate.proxy_source,
                "passed": gate.passed,
                "filtered": gate.filtered,
                "filter_reason": filter_reason,
                "warning": warning,
                "cost_gate_verified": gate.reason != "expected_edge_missing_no_filter",
                "would_filter": bool(gate.filtered),
                "would_filter_by_cost": bool(gate.filtered),
                "would_block_by_cost": bool(gate.filtered),
                "actually_filtered": actually_filtered,
                "actually_filtered_by_cost": cost_filtered_before_guard,
                "actually_filtered_by_live_guard": live_guard_blocked,
                "order_filtered": actually_filtered,
                "cost_gate_enforced": self.apply_cost_gate,
                "enforced": self.apply_cost_gate,
                "hypothetical": bool(gate.filtered and not self.apply_cost_gate),
                "live_cost_trust_guard_mode": live_guard_row.get("guard_mode"),
                "live_cost_trust_guard_enforced": live_guard_row.get("guard_enforced"),
                "blocked_by_cost_trust_guard": live_guard_blocked,
                "fallback_used": fallback_used,
                "fallback_used_for_cost_model": fallback_used_for_cost_model,
                "fallback_reason": cost_fallback_reason,
                "degraded_cost_model": degraded_cost_model,
                "diagnosis": diagnosis,
                "success": True,
            }
            rows.append(dict(sanitize_quant_lab_obj(row)))
            self.cost_rows.append(dict(sanitize_quant_lab_obj(row)))
            self._emit_usage(row)
            live_guard_payload = dict(live_guard_row)
            live_guard_payload.update(
                {
                    "request_id": request_id,
                    "original_request_id": request_id,
                    "original_event_id": cost_request_event["event_id"],
                }
            )
            self.live_guard_rows.append(dict(sanitize_quant_lab_obj(live_guard_payload)))
            self._emit_usage(live_guard_payload)
            meta = dict(getattr(order, "meta", None) or {})
            qmeta = dict(meta.get("quant_lab") or {})
            qmeta.update(
                {
                    "permission": self.permission_result.permission,
                    "raw_permission_decision": self.permission_result.raw_permission_decision or self.permission_result.permission,
                    "raw_permission_status": self.permission_result.raw_permission_status,
                    "raw_permission_enforceable": self.permission_result.raw_permission_enforceable,
                    "local_mode": self.mode.value,
                    "final_permission": qmeta.get("final_permission")
                    or self.permission_result.effective_permission_decision
                    or self.permission_result.permission,
                    "effective_permission_decision": qmeta.get("effective_permission_decision")
                    or qmeta.get("final_permission")
                    or self.permission_result.effective_permission_decision
                    or self.permission_result.permission,
                    "would_block_if_enforced": qmeta.get("would_block_if_enforced", False),
                    "shadow_override_reason": self.permission_result.shadow_override_reason,
                    "remote_permission_as_of_ts": self.permission_result.remote_permission_as_of_ts
                    or self.permission_result.response_ts,
                    "remote_permission_expires_at": self.permission_result.remote_permission_expires_at,
                    "remote_permission_status": self.permission_result.remote_permission_status,
                    "remote_permission_source_bundle_ts": self.permission_result.remote_permission_source_bundle_ts,
                    "remote_permission_telemetry_latest_ts": self.permission_result.remote_permission_telemetry_latest_ts,
                    "remote_permission_contract_version": self.permission_result.remote_permission_contract_version,
                    "permission_contract_violation": self.permission_result.permission_contract_violation,
                    "allowed_live_modes": self.permission_result.allowed_live_modes,
                    "live_block_reasons": self.permission_result.live_block_reasons,
                    "request_symbol": original_symbol,
                    "normalized_symbol": normalized_symbol,
                    "response_symbol": estimate.symbol,
                    "venue": "OKX",
                    "instrument_type": "spot",
                    "strategy_id": strategy_id,
                    "request_id": request_id,
                    "requested_regime": str(regime or "normal"),
                    "requested_quantile": quantile,
                    "matched_regime": estimate.matched_regime or estimate.regime or gate.regime,
                    "cost_model_version": gate.cost_model_version,
                    "cost_contract_version": CONTRACT_VERSION,
                    "as_of_ts": estimate.as_of_ts,
                    "cost_quantile": gate.quantile,
                    "fee_bps": gate.fee_bps,
                    "slippage_bps": gate.slippage_bps,
                    "spread_bps": gate.spread_bps,
                    "total_cost_bps": gate.total_cost_bps,
                    "effective_total_cost_bps": gate.effective_total_cost_bps,
                    "one_way_all_in_cost_bps": gate.one_way_all_in_cost_bps,
                    "roundtrip_all_in_cost_bps": gate.roundtrip_all_in_cost_bps,
                    "selected_entry_gate_cost_bps": gate.selected_entry_gate_cost_bps,
                    "local_cost_bps": gate.local_cost_bps,
                    "local_cost_source": gate.local_cost_source,
                    "fallback_level": gate.fallback_level,
                    "source": gate.source,
                    "cost_source": gate.source,
                    "cost_quality": gate.cost_quality,
                    "cost_trusted_for_paper": gate.cost_trusted_for_paper,
                    "cost_trusted_for_live": gate.cost_trusted_for_live,
                    "sample_count": gate.sample_count,
                    "selected_total_cost_bps": estimate.total_cost_bps,
                    "total_cost_bps_p50": estimate.total_cost_bps_p50,
                    "total_cost_bps_p75": estimate.total_cost_bps_p75,
                    "total_cost_bps_p90": estimate.total_cost_bps_p90,
                    "expected_edge_bps": gate.expected_edge_bps,
                    "expected_edge_source": gate.expected_edge_source or gate.proxy_source,
                    "min_required_edge_bps": gate.min_required_edge_bps,
                    "required_edge_bps": required_edge_bps,
                    "proxy_source": gate.proxy_source,
                    "cost_gate_passed": gate.passed,
                    "cost_gate_verified": gate.reason != "expected_edge_missing_no_filter",
                    "cost_gate_enforced": self.apply_cost_gate,
                    "permission_gate_enforced": self.apply_permission_gate,
                    "would_filter_by_cost": bool(gate.filtered),
                    "would_block_by_cost": bool(gate.filtered),
                    "actually_filtered_by_cost": cost_filtered_before_guard,
                    "actually_filtered_by_live_guard": live_guard_blocked,
                    "blocked_by_cost_trust_guard": live_guard_blocked,
                    "cost_trust_exception": live_guard_row.get("cost_trust_exception"),
                    "cost_trust_block_reasons": live_guard_row.get("cost_trust_block_reasons"),
                    "live_cost_trust_guard_mode": live_guard_row.get("guard_mode"),
                    "live_cost_trust_guard_enforced": live_guard_row.get("guard_enforced"),
                    "fallback_used": bool(qmeta.get("fallback_used") or fallback_used),
                    "fallback_used_for_cost_model": fallback_used_for_cost_model,
                    "fallback_reason": cost_fallback_reason or qmeta.get("fallback_reason"),
                    "degraded_cost_model": degraded_cost_model,
                    "diagnosis": diagnosis,
                    "warning": warning,
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
                        "request_id": request_id,
                        "original_request_id": request_id,
                        "original_event_id": cost_request_event["event_id"],
                        "symbol": getattr(order, "symbol", None),
                        "side": getattr(order, "side", None),
                        "intent": getattr(order, "intent", None),
                        "order_filtered": True,
                        "filter_reason": filter_reason,
                    }
                )
                self._emit_usage(
                    {
                        "event_type": "filter_order",
                        "request_id": request_id,
                        "original_request_id": request_id,
                        "original_event_id": cost_request_event["event_id"],
                        "symbol": getattr(order, "symbol", None),
                        "side": getattr(order, "side", None),
                        "intent": getattr(order, "intent", None),
                        "order_filtered": True,
                        "filter_reason": filter_reason,
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
            if str(row.get("filter_reason", "")).startswith("quant_lab_sell")
            or row.get("filter_reason") == "quant_lab_abort"
            or row.get("filter_reason") == "quant_lab_allowed_live_modes_empty"
        ]
        would_filter_by_permission = len([row for row in permission_rows if row.get("would_filter") or row.get("would_filter_by_permission")])
        filtered_by_permission = len([row for row in permission_rows if row.get("actually_filtered") or row.get("order_filtered")])
        would_filter_by_cost = len([row for row in self.cost_rows if row.get("would_filter") or row.get("would_filter_by_cost")])
        filtered_by_cost = len([row for row in self.cost_rows if row.get("actually_filtered_by_cost")])
        cost_fallback = len([row for row in self.cost_rows if row.get("fallback_used")])
        live_guard_would_block = len([row for row in self.live_guard_rows if row.get("would_block_by_cost_trust_guard")])
        live_guard_actual_block = len([row for row in self.live_guard_rows if row.get("blocked_by_cost_trust_guard")])
        live_guard_whitelist_allowed = 0
        live_guard_paper_or_shadow = len([row for row in self.live_guard_rows if row.get("paper_or_shadow_bypassed")])
        final_permission = self.final_permission or (
            "LOCAL_ONLY" if self.mode == QuantLabMode.LOCAL_ONLY else permission.effective_permission_decision or permission.permission
        )
        raw_permission = permission.raw_permission_decision or permission.permission
        client = self.client
        return sanitize_quant_lab_obj(
            {
                "enabled": bool(permission.enabled),
                "mode": self.mode.value,
                "quant_lab_requested_mode": (self.mode_resolution.requested_mode or self.mode).value,
                "quant_lab_effective_mode": self.mode.value,
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
                "quant_lab_permission": raw_permission,
                "raw_permission_decision": raw_permission,
                "raw_permission_status": permission.raw_permission_status,
                "raw_permission_enforceable": permission.raw_permission_enforceable,
                "local_mode": self.mode.value,
                "local_preflight_permission": self.local_preflight_permission,
                "final_permission": final_permission,
                "effective_permission_decision": final_permission,
                "would_block_if_enforced": permission.would_block_if_enforced,
                "shadow_override_reason": permission.shadow_override_reason,
                "remote_permission_as_of_ts": permission.remote_permission_as_of_ts or permission.response_ts,
                "remote_permission_expires_at": permission.remote_permission_expires_at,
                "remote_permission_status": permission.remote_permission_status,
                "remote_permission_source_bundle_ts": permission.remote_permission_source_bundle_ts,
                "remote_permission_telemetry_latest_ts": permission.remote_permission_telemetry_latest_ts,
                "remote_permission_contract_version": permission.remote_permission_contract_version,
                "permission_contract_violation": permission.permission_contract_violation,
                "contract_version": permission.contract_version,
                "allowed_modes": permission.allowed_modes,
                "allowed_live_modes": permission.allowed_live_modes,
                "live_block_reasons": permission.live_block_reasons,
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
                "live_guard_would_block_count": live_guard_would_block,
                "live_guard_actual_block_count": live_guard_actual_block,
                "whitelist_allowed_count": live_guard_whitelist_allowed,
                "paper_or_shadow_redirected_count": live_guard_paper_or_shadow,
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
                "enforce_readiness_status": (
                    self.mode_resolution.enforce_readiness.status if self.mode_resolution.enforce_readiness is not None else None
                ),
                "enforce_blocked_reasons": (
                    list(self.mode_resolution.enforce_readiness.reasons)
                    if self.mode_resolution.enforce_readiness is not None
                    else []
                ),
                "enforce_blocked_reason": (
                    ";".join(self.mode_resolution.enforce_readiness.reasons)
                    if self.mode_resolution.enforce_readiness is not None and self.mode_resolution.enforce_readiness.reasons
                    else None
                ),
                "contract_version_match": (
                    self.mode_resolution.enforce_readiness.contract_version_match
                    if self.mode_resolution.enforce_readiness is not None
                    else None
                ),
                "telemetry_schema_version_match": (
                    self.mode_resolution.enforce_readiness.telemetry_schema_version_match
                    if self.mode_resolution.enforce_readiness is not None
                    else None
                ),
            }
        )

    def audit_payload(self) -> Dict[str, Any]:
        payload = self.summary_payload()
        payload["permission_result"] = self.permission_result.to_dict()
        payload["cost_estimates"] = list(self.cost_rows)
        payload["filtered_orders"] = list(self.filtered_orders)
        payload["events_tail"] = self.events[-50:]
        return sanitize_quant_lab_obj(payload)
