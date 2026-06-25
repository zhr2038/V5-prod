from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def canonical_expression(expression: dict[str, Any]) -> str:
    return json.dumps(expression, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def compute_expression_hash(expression: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_expression(expression).encode("utf-8")).hexdigest()


class FactorStatus(str, Enum):
    CANDIDATE = "candidate"
    VALIDATED = "validated"
    SHADOW = "shadow"
    PRODUCTION = "production"
    RETIRED = "retired"


class FactorSpec(BaseModel):
    factor_id: str
    name: str
    version: str = "v1"

    expression: dict[str, Any]
    inputs: list[str] = Field(default_factory=list)

    timeframe: str
    lookback_bars: int = Field(ge=1)
    availability_lag_bars: int = Field(default=0, ge=0)
    warmup_bars: int = Field(default=0, ge=0)

    causal: bool = True
    normalization: str | None = None

    status: FactorStatus = FactorStatus.CANDIDATE
    tags: list[str] = Field(default_factory=list)

    expression_hash: str | None = None
    owner: str = "quant"
    created_at: datetime = Field(default_factory=_utc_now)

    @field_validator("factor_id", "name", "version", "timeframe")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("value cannot be empty")
        return text

    @model_validator(mode="after")
    def _validate_expression_hash(self) -> "FactorSpec":
        expected = compute_expression_hash(self.expression)
        if self.expression_hash is None:
            self.expression_hash = expected
        elif str(self.expression_hash) != expected:
            raise ValueError("expression_hash does not match expression")
        return self

    @property
    def required_bars(self) -> int:
        return int(self.lookback_bars) + int(self.warmup_bars) + int(self.availability_lag_bars)


class FactorSnapshot(BaseModel):
    factor_id: str
    factor_version: str

    symbol: str
    timeframe: str
    value: float

    event_time: datetime
    available_time: datetime

    calculated_at: datetime = Field(default_factory=_utc_now)
    data_version: str

    quality_flags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_times(self) -> "FactorSnapshot":
        if self.available_time < self.event_time:
            raise ValueError("available_time cannot be earlier than event_time")
        return self

    def redis_latest_key(self) -> str:
        return f"factor:latest:{self.symbol}:{self.timeframe}:{self.factor_id}"

    def redis_history_key(self) -> str:
        event_ts = int(self.event_time.timestamp())
        return f"factor:history:{self.symbol}:{self.timeframe}:{self.factor_id}:{event_ts}"


class FactorMetrics(BaseModel):
    factor_id: str
    factor_version: str
    evaluation_id: str

    oos_return_delta: float
    oos_sharpe_delta: float
    max_drawdown_delta: float
    turnover_delta: float

    stability_score: float
    redundancy_score: float
    coverage: float
