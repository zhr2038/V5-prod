from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PAPER_STRATEGY_CONTRACT_VERSION = "quant_lab.paper_strategy.v1"

SUPPORTED_MARKET_FIELDS = frozenset(
    {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "bid",
        "ask",
        "mid",
        "spread_bps",
        "return_1",
        "return_4",
        "return_8",
        "return_24",
        "momentum_4",
        "momentum_8",
        "momentum_24",
        "volatility_8",
        "volatility_24",
        "volume_zscore_24",
        "cross_sectional_rank",
        "cross_sectional_quantile",
        "market_regime",
        "holding_bars",
        "gross_pnl_bps",
        "net_pnl_bps",
        "peak_pnl_bps",
    }
)


class PaperRuntimeState(str, Enum):
    PROPOSAL_RECEIVED = "PROPOSAL_RECEIVED"
    VALIDATED = "VALIDATED"
    ACK_ACCEPTED = "ACK_ACCEPTED"
    WAITING_SIGNAL = "WAITING_SIGNAL"
    PAPER_OPEN = "PAPER_OPEN"
    PAPER_EXIT_PENDING = "PAPER_EXIT_PENDING"
    PAPER_CLOSED = "PAPER_CLOSED"
    COOLDOWN = "COOLDOWN"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    DISABLED = "DISABLED"


_LEGAL_RUNTIME_TRANSITIONS = {
    PaperRuntimeState.PROPOSAL_RECEIVED: {
        PaperRuntimeState.VALIDATED,
        PaperRuntimeState.REJECTED,
        PaperRuntimeState.EXPIRED,
        PaperRuntimeState.DISABLED,
    },
    PaperRuntimeState.VALIDATED: {
        PaperRuntimeState.ACK_ACCEPTED,
        PaperRuntimeState.REJECTED,
        PaperRuntimeState.DISABLED,
    },
    PaperRuntimeState.ACK_ACCEPTED: {
        PaperRuntimeState.WAITING_SIGNAL,
        PaperRuntimeState.EXPIRED,
        PaperRuntimeState.DISABLED,
    },
    PaperRuntimeState.WAITING_SIGNAL: {
        PaperRuntimeState.PAPER_OPEN,
        PaperRuntimeState.EXPIRED,
        PaperRuntimeState.DISABLED,
    },
    PaperRuntimeState.PAPER_OPEN: {
        PaperRuntimeState.PAPER_EXIT_PENDING,
        PaperRuntimeState.DISABLED,
    },
    PaperRuntimeState.PAPER_EXIT_PENDING: {
        PaperRuntimeState.PAPER_CLOSED,
        PaperRuntimeState.PAPER_OPEN,
        PaperRuntimeState.DISABLED,
    },
    PaperRuntimeState.PAPER_CLOSED: {
        PaperRuntimeState.COOLDOWN,
        PaperRuntimeState.WAITING_SIGNAL,
        PaperRuntimeState.EXPIRED,
        PaperRuntimeState.DISABLED,
    },
    PaperRuntimeState.COOLDOWN: {
        PaperRuntimeState.WAITING_SIGNAL,
        PaperRuntimeState.EXPIRED,
        PaperRuntimeState.DISABLED,
    },
    PaperRuntimeState.EXPIRED: set(),
    PaperRuntimeState.REJECTED: set(),
    PaperRuntimeState.DISABLED: {PaperRuntimeState.WAITING_SIGNAL},
}


def assert_runtime_transition(
    current: PaperRuntimeState | str,
    target: PaperRuntimeState | str,
) -> PaperRuntimeState:
    source = PaperRuntimeState(str(getattr(current, "value", current)))
    destination = PaperRuntimeState(str(getattr(target, "value", target)))
    if source == destination:
        return destination
    if destination not in _LEGAL_RUNTIME_TRANSITIONS[source]:
        raise ValueError(
            f"illegal paper runtime transition: {source.value}->{destination.value}"
        )
    return destination


RuleOperator = Literal[
    "all",
    "any",
    "not",
    "gt",
    "gte",
    "lt",
    "lte",
    "crosses_above",
    "crosses_below",
    "consecutive",
    "rank_gte",
    "rank_lte",
    "quantile_gte",
    "quantile_lte",
    "momentum_gt",
    "momentum_lt",
    "return_gt",
    "return_lt",
    "volatility_lt",
    "volatility_gt",
    "volume_zscore_gt",
    "volume_zscore_lt",
    "regime_in",
    "take_profit",
    "stop_loss",
    "trailing_exit",
    "max_holding_bars",
    "signal_invalid",
]


class PaperRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operator: RuleOperator
    field: str | None = None
    reference_field: str | None = None
    value: float | int | str | bool | None = None
    values: list[str | float | int] = Field(default_factory=list, max_length=64)
    window: int | None = Field(default=None, ge=1, le=512)
    periods: int | None = Field(default=None, ge=1, le=512)
    children: list["PaperRule"] = Field(default_factory=list, max_length=32)

    @field_validator("field", "reference_field")
    @classmethod
    def validate_field(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if normalized not in SUPPORTED_MARKET_FIELDS:
            raise ValueError(f"unsupported paper rule market field: {value}")
        return normalized

    @model_validator(mode="after")
    def validate_shape(self) -> "PaperRule":
        operator = self.operator
        if operator in {"all", "any"} and not self.children:
            raise ValueError(f"{operator} requires at least one child rule")
        if operator == "not" and len(self.children) != 1:
            raise ValueError("not requires exactly one child rule")
        if operator == "consecutive" and (
            len(self.children) != 1 or self.periods is None
        ):
            raise ValueError("consecutive requires one child rule and periods")
        if operator not in {"all", "any", "not", "consecutive"} and self.children:
            raise ValueError(f"{operator} cannot contain child rules")
        field_ops = {
            "gt",
            "gte",
            "lt",
            "lte",
            "crosses_above",
            "crosses_below",
            "rank_gte",
            "rank_lte",
            "quantile_gte",
            "quantile_lte",
            "momentum_gt",
            "momentum_lt",
            "return_gt",
            "return_lt",
            "volatility_lt",
            "volatility_gt",
            "volume_zscore_gt",
            "volume_zscore_lt",
        }
        if operator in field_ops and self.field is None:
            raise ValueError(f"{operator} requires field")
        if (
            operator in field_ops
            and self.reference_field is None
            and self.value is None
        ):
            raise ValueError(f"{operator} requires value or reference_field")
        if operator == "regime_in" and (
            self.field != "market_regime" or not self.values
        ):
            raise ValueError("regime_in requires field=market_regime and values")
        if (
            operator
            in {
                "take_profit",
                "stop_loss",
                "trailing_exit",
                "max_holding_bars",
            }
            and self.value is None
        ):
            raise ValueError(f"{operator} requires value")
        return self


class PaperStrategyProposal(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    contract_version: Literal[PAPER_STRATEGY_CONTRACT_VERSION]
    proposal_id: str = Field(min_length=1)
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_id: str = Field(min_length=1, max_length=160)
    strategy_version: str = Field(min_length=1, max_length=80)
    strategy_family: str = Field(min_length=1, max_length=120)
    symbol: str
    timeframe: str
    direction: Literal["long", "short"] = "long"
    entry_rule: PaperRule
    exit_rule: PaperRule
    max_holding_bars: int = Field(ge=1, le=10_000)
    min_holding_bars: int = Field(default=0, ge=0, le=10_000)
    cooldown_bars: int = Field(default=0, ge=0, le=10_000)
    signal_confirmation_bars: int = Field(default=1, ge=1, le=1_000)
    cost_quantile: Literal["p50", "p75", "p90", "p95"] = "p75"
    minimum_expected_edge_bps: float = Field(default=0.0, ge=-10_000, le=100_000)
    paper_notional_usdt: float = Field(gt=0, le=1_000_000)
    paper_only: Literal[True]
    live_order_effect: Literal["none"]
    max_live_notional_usdt: Literal[0.0] = 0.0
    created_at: datetime
    expires_at: datetime
    source_pack_sha256: str = ""
    source_dataset_versions: dict[str, str] = Field(default_factory=dict)
    required_market_fields: list[str] = Field(default_factory=list, max_length=64)
    required_cost_trust_level: Literal["BLOCK", "PAPER_ONLY", "CANARY", "SCALE_READY"]
    lifecycle_state: Literal["PAPER_PROPOSAL_READY"] = "PAPER_PROPOSAL_READY"
    lifecycle_reason: str = ""
    blocked_reasons: list[str] = Field(default_factory=list)
    next_required_actions: list[str] = Field(default_factory=list)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        text = value.strip().upper().replace("_", "/").replace("-", "/")
        parts = [part for part in text.split("/") if part]
        if len(parts) != 2 or not all(
            re.fullmatch(r"[A-Z0-9]+", part) for part in parts
        ):
            raise ValueError("invalid_symbol")
        return f"{parts[0]}/{parts[1]}"

    @field_validator("timeframe")
    @classmethod
    def normalize_timeframe(cls, value: str) -> str:
        text = value.strip().lower()
        if not re.fullmatch(r"[1-9][0-9]*(m|h|d)", text):
            raise ValueError("invalid_timeframe")
        return text

    @field_validator("required_market_fields")
    @classmethod
    def validate_required_fields(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for raw in values:
            value = raw.strip().lower()
            if value not in SUPPORTED_MARKET_FIELDS:
                raise ValueError(f"missing_market_field:{value}")
            if value not in normalized:
                normalized.append(value)
        return normalized

    @field_validator("created_at", "expires_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("proposal timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_contract(self) -> "PaperStrategyProposal":
        if self.min_holding_bars > self.max_holding_bars:
            raise ValueError("min_holding_bars cannot exceed max_holding_bars")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be later than created_at")
        if paper_proposal_hash(self) != self.proposal_hash:
            raise ValueError("proposal_hash does not match canonical proposal content")
        return self


class PaperStrategyAck(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    proposal_id: str
    proposal_hash: str
    accepted: bool
    reject_reason: str = ""
    tracker_id: str = ""
    contract_version: str = PAPER_STRATEGY_CONTRACT_VERSION
    strategy_version: str
    rules_locked: bool = False
    paper_only: bool = True
    live_order_effect: str = "none"
    accepted_at: datetime
    expires_at: datetime
    source_v5_commit: str = ""
    source_v5_bundle_sha256: str = ""


def paper_proposal_hash(value: PaperStrategyProposal | dict[str, Any]) -> str:
    if isinstance(value, BaseModel):
        payload = value.model_dump(mode="json")
    else:
        payload = _normalized_proposal_payload(dict(value))
    for key in (
        "proposal_id",
        "proposal_hash",
        "created_at",
        "expires_at",
        "source_pack_sha256",
        "lifecycle_reason",
        "blocked_reasons",
        "next_required_actions",
    ):
        payload.pop(key, None)
    material = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _normalized_proposal_payload(payload: dict[str, Any]) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "contract_version": PAPER_STRATEGY_CONTRACT_VERSION,
        "direction": "long",
        "min_holding_bars": 0,
        "cooldown_bars": 0,
        "signal_confirmation_bars": 1,
        "cost_quantile": "p75",
        "minimum_expected_edge_bps": 0.0,
        "max_live_notional_usdt": 0.0,
        "source_pack_sha256": "",
        "source_dataset_versions": {},
        "required_market_fields": [],
        "lifecycle_state": "PAPER_PROPOSAL_READY",
        "lifecycle_reason": "",
        "blocked_reasons": [],
        "next_required_actions": [],
    }
    normalized = {**defaults, **payload}
    normalized["symbol"] = PaperStrategyProposal.normalize_symbol(
        str(normalized["symbol"])
    )
    normalized["timeframe"] = PaperStrategyProposal.normalize_timeframe(
        str(normalized["timeframe"])
    )
    normalized["entry_rule"] = PaperRule.model_validate(
        normalized["entry_rule"]
    ).model_dump(mode="json")
    normalized["exit_rule"] = PaperRule.model_validate(
        normalized["exit_rule"]
    ).model_dump(mode="json")
    for field in ("created_at", "expires_at"):
        value = normalized[field]
        if not isinstance(value, datetime):
            value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        normalized[field] = value.astimezone(UTC).isoformat()
    normalized["required_market_fields"] = (
        PaperStrategyProposal.validate_required_fields(
            list(normalized.get("required_market_fields") or [])
        )
    )
    return normalized
