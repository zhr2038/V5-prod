from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from src.core.decision_receipt_v22 import (
    DecisionReceiptIntegrityError,
    DecisionReceiptRecorder,
    DecisionReceiptValidationError,
    validate_decision_receipt,
)


def _receipt() -> dict:
    return {
        "schema_version": "v5_decision_receipt_v22.v1",
        "receipt_id": "receipt-20260719-0001",
        "decision_ts": "2026-07-19T00:00:00Z",
        "recorded_at": "2026-07-19T00:00:01Z",
        "market_data_cutoff": "2026-07-18T23:00:00Z",
        "strategy_id": "v5-production-state",
        "strategy_version": "snapshot-only",
        "parameter_lock_hash": "a" * 64,
        "strategy_code_hash": "b" * 64,
        "manifest_hash": "c" * 64,
        "current_positions": [{"symbol": "BTC-USDT", "quantity": 0.001}],
        "available_cash": 100.0,
        "target_weights": {"BTC-USDT": 0.0},
        "risk_checks": [{"name": "kill_switch", "result": "PASS"}],
        "gate_result": {"status": "ABORT"},
        "risk_permission": "ABORT",
        "order_intents": [],
        "execution_mode": "READ_ONLY",
        "final_action": "NO_ORDER",
        "error_codes": [],
    }


def test_recorder_is_default_off_and_writes_nothing(tmp_path: Path) -> None:
    result = DecisionReceiptRecorder(tmp_path).record(_receipt())
    assert result.status == "DISABLED"
    assert result.protective_action_must_continue is True
    assert list(tmp_path.iterdir()) == []


def test_append_only_atomic_receipt_is_idempotent(tmp_path: Path) -> None:
    recorder = DecisionReceiptRecorder(tmp_path, enabled=True)
    first = recorder.record(_receipt())
    second = recorder.record(_receipt())
    assert first.status == "WRITTEN"
    assert second.status == "ALREADY_PRESENT"
    assert json.loads(Path(first.path).read_text(encoding="utf-8"))["final_action"] == "NO_ORDER"
    changed = _receipt()
    changed["final_action"] = "DIFFERENT"
    with pytest.raises(DecisionReceiptIntegrityError):
        recorder.record(changed)


def test_sensitive_values_and_keys_never_reach_disk(tmp_path: Path) -> None:
    receipt = _receipt()
    receipt["order_intents"] = [{"nested": {"api_secret": "do-not-write"}}]
    recorder = DecisionReceiptRecorder(tmp_path, enabled=True)
    with pytest.raises(DecisionReceiptValidationError, match="sensitive field"):
        recorder.record(receipt)
    assert list(tmp_path.iterdir()) == []


def test_unknown_fields_are_rejected() -> None:
    receipt = _receipt()
    receipt["api_key"] = "forbidden"
    with pytest.raises(DecisionReceiptValidationError, match="unknown receipt fields"):
        validate_decision_receipt(receipt)


def test_json_schema_matches_runtime_contract() -> None:
    schema = json.loads(
        (Path(__file__).resolve().parents[1] / "schemas/v5_decision_receipt_v22.schema.json")
        .read_text(encoding="utf-8")
    )
    jsonschema.Draft202012Validator(schema).validate(_receipt())
    invalid = _receipt()
    invalid["unknown"] = True
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(schema).validate(invalid)


def test_nonthrowing_boundary_cannot_block_protective_action(tmp_path: Path, monkeypatch) -> None:
    recorder = DecisionReceiptRecorder(tmp_path, enabled=True)

    def fail(*_args, **_kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr("src.core.decision_receipt_v22._atomic_append_only_create", fail)
    result = recorder.try_record(_receipt())
    assert result.status == "FAILED"
    assert result.protective_action_must_continue is True
    assert result.error_code.startswith("RECEIPT_WRITE_")
