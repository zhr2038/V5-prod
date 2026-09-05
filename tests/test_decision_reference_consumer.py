import copy
import json
import sqlite3
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from configs.schema import DecisionReferenceConfig
from src.quant_lab_client.reference_consumer import record_cycle, fetch_reference

NOW = 1788652800


def iso(stamp):
    return datetime.fromtimestamp(stamp, timezone.utc).isoformat()


def payload():
    return {"schema_version": "qlab.decision.result.v2", "effective_status": "AVAILABLE",
            "publication": {"published_at": iso(NOW - 30)}, "advice": [{
                "advice_id": "advice-" + "a" * 64, "symbol": "BNBUSDT", "generated_at": iso(NOW - 60),
                "expires_at": iso(NOW + 180), "horizon_hours": 24, "cost": {"version": "current-cost-v1"},
                "experiment_version": "trend-reference-1h-v2", "live_order_effect": "none",
                "eligibility": {"live_execution_eligible": False}, "action": "REVIEW_ENTRY"}]}


def test_records_reference_then_links_later_original_decision_and_fill(tmp_path):
    value = payload()
    record_cycle(payload=value, contexts=[], now=NOW, output=tmp_path)
    context = {"run_id": "next-run", "decision_ts": NOW + 60, "audit_sha256": "b" * 64,
               "targets_post_risk": {"BNB/USDT": 0.1}, "router_decisions": [],
               "orders": [{"inst_id": "BNB-USDT", "state": "FILLED"}],
               "fills": [{"inst_id": "BNB-USDT", "trade_id": "123", "fee_ccy": "BNB"}],
               "execution_status": "fills_observed"}
    original = copy.deepcopy(context)
    record_cycle(payload=value, contexts=[context], now=NOW + 120, output=tmp_path)
    record_cycle(payload=value, contexts=[context], now=NOW + 240, output=tmp_path)
    assert context == original
    with sqlite3.connect(tmp_path / "receipts.sqlite") as con:
        assert con.execute("SELECT count(*),min(first_received) FROM advice").fetchone() == (1, NOW)
        rows = con.execute("SELECT evidence FROM associations").fetchall()
    assert len(rows) == 1
    event = json.loads(rows[0][0])
    assert event["adoption"] == "not_adopted" and event["live_order_effect"] == "none"
    assert event["reference_valid_at_v5_decision"]
    assert event["final_execution"]["fills"][0]["trade_id"] == "123"


@pytest.mark.parametrize("kind,reason", [("expired", "reference_expired"), ("legacy", "legacy_schema_never_restores_permission"), ("future", "reference_time_invalid")])
def test_invalid_reference_never_restores_permission(tmp_path, kind, reason):
    value = payload()
    if kind == "expired":
        value["advice"][0]["expires_at"] = iso(NOW)
    elif kind == "legacy":
        value["schema_version"] = "qlab.decision.result.v1"
    else:
        value["publication"]["published_at"] = iso(NOW + 30)
    result = record_cycle(payload=value, contexts=[], now=NOW, output=tmp_path)
    assert result["receipts"][0]["reason"] == reason
    assert result["live_order_effect"] == "none"


def test_missing_reference_records_unavailable_without_touching_live_files(tmp_path):
    live = tmp_path / "orders.sqlite"
    live.write_bytes(b"live-evidence")
    result = record_cycle(payload=None, contexts=[], now=NOW, output=tmp_path / "reference", error="REFERENCE_UNAVAILABLE")
    assert result["status"] == "REFERENCE_UNAVAILABLE"
    assert live.read_bytes() == b"live-evidence"


def test_consumer_rejects_enforce_and_order_endpoints():
    with pytest.raises(ValidationError):
        DecisionReferenceConfig(mode="enforce")
    with pytest.raises(ValueError, match="GET advice endpoint"):
        fetch_reference("https://qyun2.hrhome.top/api/v5/trade/order")
@pytest.mark.parametrize("payload", [[1], {"advice": [None]}, {"advice": [{}] * 9}])
def test_malformed_payload_publishes_explicit_status(tmp_path, payload):
    result = record_cycle(payload=payload, contexts=[], now=1788652800, output=tmp_path)
    assert result["status"] == "reference_payload_invalid"
    assert result["counts"] == {"advice": 0, "associations": 0}
