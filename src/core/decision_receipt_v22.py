"""Default-off, append-only V5 decision receipts for Audit v2.2.

The recorder is intentionally not wired into the production pipeline.  Callers
must opt in explicitly.  ``try_record`` never raises, so a receipt failure cannot
block a protective exit that the caller has already decided to execute.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "v5_decision_receipt_v22.v1"
REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "receipt_id",
        "decision_ts",
        "recorded_at",
        "market_data_cutoff",
        "strategy_id",
        "strategy_version",
        "parameter_lock_hash",
        "strategy_code_hash",
        "manifest_hash",
        "current_positions",
        "available_cash",
        "target_weights",
        "risk_checks",
        "gate_result",
        "risk_permission",
        "order_intents",
        "execution_mode",
        "final_action",
        "error_codes",
    }
)
SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "api_secret",
    "passphrase",
    "password",
    "private_key",
    "authorization",
    "bearer",
    "access_sign",
    "secret",
    "token",
)
SENSITIVE_VALUE_MARKERS = (
    "BEGIN PRIVATE KEY",
    "OK-ACCESS-KEY",
    "OK-ACCESS-SIGN",
    "OK-ACCESS-PASSPHRASE",
    "Bearer ",
)
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")


class DecisionReceiptError(RuntimeError):
    pass


class DecisionReceiptValidationError(DecisionReceiptError):
    pass


class DecisionReceiptIntegrityError(DecisionReceiptError):
    pass


@dataclass(frozen=True)
class DecisionReceiptWriteResult:
    status: str
    receipt_id: str
    path: str
    written: bool
    protective_action_must_continue: bool
    error_code: str = ""


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _reject_sensitive(value: Any, *, path: str = "$.") -> None:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            lowered = key.lower().replace("-", "_")
            if any(marker in lowered for marker in SENSITIVE_KEY_PARTS):
                raise DecisionReceiptValidationError(
                    f"sensitive field is forbidden at {path}{key}"
                )
            _reject_sensitive(item, path=f"{path}{key}.")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_sensitive(item, path=f"{path}{index}.")
        return
    if isinstance(value, str) and any(
        marker.lower() in value.lower() for marker in SENSITIVE_VALUE_MARKERS
    ):
        raise DecisionReceiptValidationError("sensitive value marker is forbidden")


def validate_decision_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    receipt = dict(payload)
    missing = sorted(REQUIRED_FIELDS - set(receipt))
    extra = sorted(set(receipt) - REQUIRED_FIELDS)
    if missing:
        raise DecisionReceiptValidationError(f"missing receipt fields: {missing}")
    if extra:
        raise DecisionReceiptValidationError(f"unknown receipt fields: {extra}")
    if receipt["schema_version"] != SCHEMA_VERSION:
        raise DecisionReceiptValidationError("unsupported decision receipt schema")
    receipt_id = str(receipt["receipt_id"])
    if not SAFE_ID.fullmatch(receipt_id):
        raise DecisionReceiptValidationError("receipt_id is not path-safe")
    for field in ("parameter_lock_hash", "strategy_code_hash", "manifest_hash"):
        if not HEX64.fullmatch(str(receipt[field])):
            raise DecisionReceiptValidationError(f"{field} must be lowercase SHA256")
    if not isinstance(receipt["current_positions"], list):
        raise DecisionReceiptValidationError("current_positions must be a list")
    if not isinstance(receipt["target_weights"], Mapping):
        raise DecisionReceiptValidationError("target_weights must be an object")
    if not isinstance(receipt["risk_checks"], list):
        raise DecisionReceiptValidationError("risk_checks must be a list")
    if not isinstance(receipt["gate_result"], Mapping):
        raise DecisionReceiptValidationError("gate_result must be an object")
    if not isinstance(receipt["order_intents"], list):
        raise DecisionReceiptValidationError("order_intents must be a list")
    if not isinstance(receipt["error_codes"], list):
        raise DecisionReceiptValidationError("error_codes must be a list")
    _reject_sensitive(receipt)
    return receipt


def _atomic_append_only_create(target: Path, payload: bytes) -> bool:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.read_bytes() == payload:
            return False
        raise DecisionReceiptIntegrityError("receipt_id already exists with different content")
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".partial", dir=str(target.parent)
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.read_bytes() == payload:
                return False
            raise DecisionReceiptIntegrityError(
                "concurrent receipt_id collision with different content"
            )
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
        except OSError:
            directory_fd = -1
        if directory_fd >= 0:
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        return True
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


class DecisionReceiptRecorder:
    """Explicitly opt-in recorder; disabled by default and never places orders."""

    def __init__(self, root: str | Path, *, enabled: bool = False) -> None:
        self.root = Path(root)
        self.enabled = bool(enabled)

    def receipt_path(self, receipt_id: str) -> Path:
        if not SAFE_ID.fullmatch(receipt_id):
            raise DecisionReceiptValidationError("receipt_id is not path-safe")
        return self.root / f"{receipt_id}.json"

    def record(self, payload: Mapping[str, Any]) -> DecisionReceiptWriteResult:
        receipt_id = str(payload.get("receipt_id") or "")
        if not self.enabled:
            return DecisionReceiptWriteResult(
                status="DISABLED",
                receipt_id=receipt_id,
                path="",
                written=False,
                protective_action_must_continue=True,
            )
        receipt = validate_decision_receipt(payload)
        target = self.receipt_path(str(receipt["receipt_id"]))
        written = _atomic_append_only_create(target, _canonical_bytes(receipt))
        return DecisionReceiptWriteResult(
            status="WRITTEN" if written else "ALREADY_PRESENT",
            receipt_id=str(receipt["receipt_id"]),
            path=str(target),
            written=written,
            protective_action_must_continue=True,
        )

    def try_record(self, payload: Mapping[str, Any]) -> DecisionReceiptWriteResult:
        """Non-throwing boundary intended for post-decision observability hooks."""
        try:
            return self.record(payload)
        except Exception as exc:
            receipt_id = str(payload.get("receipt_id") or "")
            error_code = hashlib.sha256(type(exc).__name__.encode("utf-8")).hexdigest()[:16]
            return DecisionReceiptWriteResult(
                status="FAILED",
                receipt_id=receipt_id,
                path="",
                written=False,
                protective_action_must_continue=True,
                error_code=f"RECEIPT_WRITE_{error_code}",
            )
