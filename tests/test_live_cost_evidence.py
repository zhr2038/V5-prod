from datetime import datetime, timezone
import hashlib
import json
import sqlite3

import pytest

from src.reporting.live_cost_evidence import build_live_cost_evidence, fill_evidence


def sample(*, side="buy", price="100.2", fee="-0.001", fee_ccy="BTC"):
    fill = dict(inst_id="BTC-USDT", trade_id="t1", ord_id="o1", cl_ord_id="c1", side=side,
                ts_ms=1_790_000_001_500, fill_px=price, fill_sz="1", fee=fee, fee_ccy=fee_ccy, source="fills")
    req = {"_meta": {"mid_px_at_submit": 100, "bid": 99.9, "ask": 100.1, "ts_ms": 1_790_000_000_000},
           "_v5_order_meta": {"quant_lab": {"one_way_all_in_cost_bps": 15}}}
    order = dict(inst_id="BTC-USDT", cl_ord_id="c1", ord_id="o1", side=side,
                 created_ts=1_790_000_001_000, ord_type="market", req_json=json.dumps(req))
    return fill, order


def test_real_fee_and_signed_cost_compare_to_frozen_order_estimate():
    row = fill_evidence(*sample())
    assert row["qualified_cost_observation"]
    assert row["fee_cost_usdt"] == pytest.approx(.1002)
    assert row["fee_bps"] == pytest.approx(10)
    assert row["signed_slippage_bps"] == pytest.approx(20)
    assert row["observed_one_way_cost_bps"] == pytest.approx(30)
    assert row["cost_error_bps"] == pytest.approx(15)


def test_pre_route_private_exchange_fill_is_real_evidence():
    fill, order = sample()
    fill["source"] = "fills_pre_route"
    assert fill_evidence(fill, order)["qualified_cost_observation"]


@pytest.mark.parametrize("side,price,slip", [("buy", "99.8", -20), ("sell", "100.2", -20), ("sell", "99.8", 20)])
def test_price_improvement_is_negative_cost(side, price, slip):
    row = fill_evidence(*sample(side=side, price=price, fee="-0.1", fee_ccy="USDT"))
    assert row["signed_slippage_bps"] == pytest.approx(slip)
    assert row["signed_slippage_usdt"] == pytest.approx(slip / 100)


@pytest.mark.parametrize("fee,ccy", [(None, "USDT"), ("", "USDT"), ("-0.1", "OKB"), ("nan", "USDT")])
def test_missing_or_unconvertible_fee_is_not_zero(fee, ccy):
    row = fill_evidence(*sample(fee=fee, fee_ccy=ccy))
    assert row["fee_cost_usdt"] is None
    assert row["observed_one_way_cost_bps"] is None
    assert "FEE_CONVERSION_UNAVAILABLE" in row["missing_reasons"]


def test_rebate_sign_is_retained():
    row = fill_evidence(*sample(fee="0.1", fee_ccy="USDT"))
    assert row["fee_cost_usdt"] == -.1


@pytest.mark.parametrize("key", ["one_way_all_in_cost_bps", "roundtrip_all_in_cost_bps", "local_cost_bps"])
def test_zero_estimate_is_known_and_does_not_fall_through(key):
    fill, order = sample()
    req = json.loads(order["req_json"])
    req["_v5_order_meta"]["quant_lab"] = {"local_cost_bps": 30, key: 0}
    order["req_json"] = json.dumps(req)
    row = fill_evidence(fill, order)
    assert row["expected_one_way_cost_bps"] == 0
    assert row["expected_cost_source"].startswith(f"order.quant_lab.{key}")
    assert row["cost_error_bps"] == pytest.approx(30)


def test_conflicting_client_order_id_cannot_qualify():
    fill, order = sample()
    order["cl_ord_id"] = "other-client-order"
    row = fill_evidence(fill, order)
    assert not row["qualified_cost_observation"]
    assert "ORDER_IDENTITY_MISMATCH" in row["missing_reasons"]


@pytest.mark.parametrize("age,reason", [(10_001, "SUBMIT_QUOTE_STALE_OR_FUTURE"), (-1, "SUBMIT_QUOTE_STALE_OR_FUTURE"), (None, "QUOTE_OR_ORDER_TIMESTAMP_MISSING")])
def test_stale_future_missing_quote_cannot_calibrate(age, reason):
    fill, order = sample()
    req = json.loads(order["req_json"])
    req["_meta"]["ts_ms"] = None if age is None else order["created_ts"] - age
    order["req_json"] = json.dumps(req)
    row = fill_evidence(fill, order)
    assert reason in row["missing_reasons"]
    assert row["signed_slippage_bps"] is None
    assert row["fee_bps"] == pytest.approx(10)


def test_long_resting_order_is_not_immediate_cost_calibration():
    fill, order = sample()
    fill["ts_ms"] += 70_000
    order["ord_type"] = "limit"
    row = fill_evidence(fill, order)
    assert set(row["missing_reasons"]) == {"NON_IMMEDIATE_ORDER_TYPE", "FILL_OUTSIDE_IMMEDIATE_EXECUTION_WINDOW"}


def test_order_mismatch_and_unknown_origin_fail_qualification():
    fill, order = sample()
    order["ord_id"] = "different"
    fill["source"] = "synthetic"
    row = fill_evidence(fill, order)
    assert "ORDER_IDENTITY_MISMATCH" in row["missing_reasons"]
    assert "UNVERIFIED_FILL_SOURCE" in row["missing_reasons"]
    assert not row["qualified_cost_observation"]


def databases(tmp_path, *, orphan=False):
    fill, order = sample()
    fills, orders = tmp_path / "fills.sqlite", tmp_path / "orders.sqlite"
    with sqlite3.connect(fills) as c:
        c.execute("CREATE TABLE fills (inst_id,trade_id,ord_id,cl_ord_id,side,ts_ms INTEGER,fill_px,fill_sz,fee,fee_ccy,source)")
        c.execute("INSERT INTO fills VALUES (?,?,?,?,?,?,?,?,?,?,?)", tuple(fill.values()))
        fill["trade_id"] = "t2"
        fill["ts_ms"] += 20
        c.execute("INSERT INTO fills VALUES (?,?,?,?,?,?,?,?,?,?,?)", tuple(fill.values()))
    with sqlite3.connect(orders) as c:
        c.execute("CREATE TABLE orders (inst_id,cl_ord_id,ord_id,side,created_ts INTEGER,ord_type,req_json)")
        if not orphan:
            c.execute("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", tuple(order.values()))
    return fills, orders


def test_read_only_report_counts_partial_fills_as_one_order(tmp_path):
    paths = databases(tmp_path)
    before = [hashlib.sha256(p.read_bytes()).hexdigest() for p in paths]
    now = datetime.fromtimestamp(1_790_000_050, timezone.utc)
    report = build_live_cost_evidence(*paths, now=now)
    assert report["fill_count"] == report["qualified_fill_count"] == 2
    assert report["qualified_order_count"] == report["groups"][0]["qualified_order_count"] == 1
    assert report["groups"][0]["mean_one_way_cost_bps"] == pytest.approx(30)
    assert report["updates_live_cost_model"] is False
    assert report["calibration_status"] == "OBSERVATIONS_NOT_CALIBRATED_MODEL"
    assert before == [hashlib.sha256(p.read_bytes()).hexdigest() for p in paths]


def test_orphan_fill_keeps_fee_and_missing_quote_reason(tmp_path):
    paths = databases(tmp_path, orphan=True)
    report = build_live_cost_evidence(*paths, now=datetime.fromtimestamp(1_790_000_050, timezone.utc))
    assert report["fill_count"] == report["fee_known_fill_count"] == 2
    assert report["qualified_fill_count"] == 0
    assert report["missing_reason_counts"]["ORDER_LINK_MISSING"] == 2
    assert report["groups"][0]["mean_one_way_cost_bps"] is None


def test_empty_window_and_missing_input_are_distinct(tmp_path):
    paths = databases(tmp_path)
    report = build_live_cost_evidence(*paths, now=datetime(2027, 1, 1, tzinfo=timezone.utc))
    assert report["status"] == "NO_FILLS" and report["quote_coverage_fraction"] is None
    with pytest.raises(FileNotFoundError):
        build_live_cost_evidence(tmp_path / "missing.sqlite", paths[1], now=datetime.now(timezone.utc))


def test_no_data_leak_from_future_fills(tmp_path):
    paths = databases(tmp_path)
    report = build_live_cost_evidence(*paths, now=datetime.fromtimestamp(1_789_999_999, timezone.utc))
    assert report["fill_count"] == 0
