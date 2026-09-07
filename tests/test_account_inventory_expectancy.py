"""Synthetic account history: external disposals must not become strategy gains."""
import json
from decimal import Decimal

import pytest

from src.execution.fill_store import FillRow, FillStore
from src.execution.bills_store import BillRow, BillsStore
from src.risk.negative_expectancy_cooldown import NegativeExpectancyConfig, NegativeExpectancyCooldown


START = 1_780_000_000_000


def history(tmp_path):
    fills = FillStore(str(tmp_path / "fills.sqlite"))
    bills = BillsStore(str(tmp_path / "bills.sqlite"))
    balance = Decimal(0)

    def trade(t, oid, side, qty, px, fee="0", fee_ccy="USDT", strategy=True, order_id=None):
        nonlocal balance
        qty, px, fee = map(Decimal, (str(qty), str(px), str(fee)))
        order_id = order_id or oid
        clid = "V5" + order_id if strategy else ""
        delta = (qty if side == "buy" else -qty) + (fee if fee_ccy == "BNB" else 0)
        cash = (-qty * px if side == "buy" else qty * px) + (fee if fee_ccy == "USDT" else 0)
        balance += delta
        raw = json.dumps({"tradeId": oid, "ordId": order_id, "fillTime": str(START + t)})
        fills.upsert_many([FillRow(inst_id="BNB-USDT", trade_id=oid, ord_id=order_id, cl_ord_id=clid,
                                  ts_ms=START + t, side=side, fill_px=str(px), fill_sz=str(qty),
                                  fee=str(fee), fee_ccy=fee_ccy, raw_json=raw)])
        bills.upsert_many([
            BillRow(bill_id=oid + "base", ts_ms=START + t, ccy="BNB", bal_chg=str(delta),
                    bal=str(balance), typ="2", inst_id="BNB-USDT", ord_id=order_id, raw_json=raw),
            BillRow(bill_id=oid + "quote", ts_ms=START + t, ccy="USDT", bal_chg=str(cash),
                    bal="100", typ="2", inst_id="BNB-USDT", ord_id=order_id, raw_json=raw),
        ])

    def convert(t):
        nonlocal balance
        bills.upsert_many([BillRow(bill_id="conversion", ts_ms=START + t, ccy="BNB",
                                  bal_chg=str(-balance), bal="0", typ="28", sub_type="237")])
        balance = Decimal(0)

    guard = NegativeExpectancyCooldown(NegativeExpectancyConfig(
        enabled=True, state_path=str(tmp_path / "state.json"), orders_db_path=str(tmp_path / "orders.sqlite"),
        fills_db_path=str(tmp_path / "fills.sqlite")))
    return trade, convert, guard


def scan(guard, since=500):
    return guard._scan_expectancy(since_ms=START + since, allowed_symbols={"BNB/USDT"})["BNB/USDT"]


def test_external_sell_and_conversion_invalidate_old_strategy_pairing(tmp_path, monkeypatch):
    trade, convert, guard = history(tmp_path)
    trade(1, "old", "buy", ".03", "600")
    trade(2, "manual", "sell", ".02", "610", strategy=False)
    convert(3)
    trade(1000, "new", "buy", ".020738", "771.5", "-.000020738", "BNB")
    trade(2000, "close", "sell", ".020717", "745.6", "-.0154465952")
    # Legacy CSV may contain a plausible but incorrect old-entry roundtrip.
    monkeypatch.setattr(guard, "_scan_expectancy_from_roundtrip_summary_csvs", lambda **kw: {
        "BNB/USDT": {"source": "strategy_roundtrip_canonical", "closed_cycles": 2,
                     "net_pnl_sum_usdt": 2.87, "net_expectancy_bps": 2289}})
    result = scan(guard)
    assert result["net_pnl_sum_usdt"] < 0
    assert result["closed_cycles"] == 1
    assert result["cycle_attributions"][0]["entry_order_id"] == "new"
    assert result["cycle_attributions"][0]["exit_order_id"] == "close"
    assert Decimal(result["inventory_remaining_qty"]) == Decimal(".000000262")
    expected_cost = Decimal("15.999367") * Decimal(".020717") / Decimal(".020717262")
    assert result["net_pnl_sum_usdt"] == pytest.approx(float(Decimal("15.4311486048") - expected_cost))
    assert result["source"] == "account_inventory_verified"
    assert result["evidence_valid"] is True


def test_base_fee_is_inventory_not_a_second_cash_charge(tmp_path):
    trade, _, guard = history(tmp_path)
    trade(1000, "buy", "buy", "1", "100", "-.01", "BNB")
    trade(2000, "sell", "sell", ".99", "101", "-.1")
    result = scan(guard)
    assert result["net_pnl_sum_usdt"] == pytest.approx(-.11)
    assert Decimal(result["inventory_remaining_qty"]) == 0
    assert result["closed_cycles"] == 1


def test_external_close_consumes_inventory_without_strategy_profit(tmp_path):
    trade, _, guard = history(tmp_path)
    trade(1, "buy", "buy", "1", "100")
    trade(1000, "external", "sell", "1", "200", strategy=False)
    assert scan(guard)["closed_cycles"] == 0
    assert scan(guard)["net_pnl_sum_usdt"] == 0


def test_missing_bill_cannot_fall_back_to_legacy_profit(tmp_path, monkeypatch):
    trade, _, guard = history(tmp_path)
    trade(1000, "buy", "buy", "1", "100")
    trade(2000, "sell", "sell", "1", "101")
    import sqlite3
    with sqlite3.connect(tmp_path / "bills.sqlite") as con:
        con.execute("DELETE FROM bills WHERE bill_id = 'sellbase'")
    monkeypatch.setattr(guard, "_scan_expectancy_from_roundtrip_summary_csvs", lambda **kw: {
        "BNB/USDT": {"closed_cycles": 1, "net_pnl_sum_usdt": 99}})
    result = scan(guard)
    assert result["evidence_valid"] is False
    assert result["degraded"] is True
    assert result["net_pnl_sum_usdt"] == 0
    assert "missing_trade_bill" in result["degraded_reason"]


def test_readonly_repeat_keeps_history_and_residual_identical(tmp_path):
    trade, convert, guard = history(tmp_path)
    trade(1, "old", "buy", "1", "100")
    convert(2)
    trade(1000, "new", "buy", "1", "100", "-.001", "BNB")
    trade(2000, "close", "sell", ".998", "99", "-.1")
    before = {p.name: p.read_bytes() for p in tmp_path.glob("*.sqlite")}
    first, second = scan(guard), scan(guard)
    assert first == second
    assert Decimal(first["inventory_remaining_qty"]) == Decimal(".001")
    assert before == {p.name: p.read_bytes() for p in tmp_path.glob("*.sqlite")}


def test_partial_fills_of_same_orders_count_once(tmp_path):
    trade, _, guard = history(tmp_path)
    trade(1000, "b1", "buy", ".4", "100", order_id="entry")
    trade(1100, "b2", "buy", ".6", "100", order_id="entry")
    trade(2000, "s1", "sell", ".3", "99", order_id="exit")
    trade(2100, "s2", "sell", ".7", "99", order_id="exit")
    result = scan(guard)
    assert result["closed_cycles"] == 1
    assert result["net_pnl_sum_usdt"] == -1
    assert result["evidence_valid"] is True


@pytest.mark.parametrize("column,value", [("bal_chg", "NaN"), ("bal_chg", "999"), ("ord_id", "different")])
def test_invalid_or_mismatched_cash_bill_is_not_verified(tmp_path, column, value):
    import sqlite3
    trade, _, guard = history(tmp_path)
    trade(1000, "buy", "buy", "1", "100")
    trade(2000, "sell", "sell", "1", "101")
    with sqlite3.connect(tmp_path / "bills.sqlite") as con:
        con.execute(f"UPDATE bills SET {column}=? WHERE bill_id='sellquote'", (value,))
    result = scan(guard)
    assert result["evidence_valid"] is False
    assert result["net_pnl_sum_usdt"] == 0


def test_excluded_probe_inventory_still_consumes_account_lots(tmp_path, monkeypatch):
    trade, _, guard = history(tmp_path)
    trade(1, "probe-buy", "buy", "1", "100")
    trade(2, "probe-sell", "sell", "1", "101")
    trade(1000, "strategy-buy", "buy", "1", "200")
    trade(2000, "strategy-sell", "sell", "1", "199")
    monkeypatch.setattr(guard, "_load_order_meta_by_id", lambda: ({}, {
        "probe-buy": {"execution_scope": "COST_PROBE"}, "probe-sell": {"execution_scope": "COST_PROBE"}}))
    result = scan(guard, since=0)
    assert result["closed_cycles"] == 1
    assert result["net_pnl_sum_usdt"] == -1
    assert result["cycle_attributions"][0]["entry_order_id"] == "strategy-buy"


def test_unknown_initial_inventory_is_explicit(tmp_path):
    import sqlite3
    trade, _, guard = history(tmp_path)
    trade(1, "external", "buy", "1", "100", strategy=False)
    trade(2000, "strategy-sell", "sell", "1", "199")
    with sqlite3.connect(tmp_path / "fills.sqlite") as con:
        con.execute("DELETE FROM fills WHERE trade_id='external'")
    result = scan(guard)
    assert result["evidence_valid"] is False
    assert result["net_pnl_sum_usdt"] == 0
    assert "unverified_or_non_strategy_entry" in result["degraded_reason"]


def test_fee_rebate_and_tiny_residual_are_retained(tmp_path):
    trade, _, guard = history(tmp_path)
    trade(1000, "buy", "buy", "1", "100", ".001", "BNB")
    trade(2000, "sell", "sell", "1.0009999999999", "100")
    result = scan(guard)
    assert Decimal(result["inventory_remaining_qty"]) == Decimal(".0000000000001")
    assert result["net_pnl_sum_usdt"] == pytest.approx(.1)


def test_record_generation_delay_is_not_a_trade_time_mismatch(tmp_path):
    import sqlite3
    trade, _, guard = history(tmp_path)
    trade(1000, "buy", "buy", "1", "100")
    trade(2000, "sell", "sell", "1", "99")
    with sqlite3.connect(tmp_path / "fills.sqlite") as con:
        con.execute("UPDATE fills SET ts_ms=ts_ms+60000")
    assert scan(guard)["evidence_valid"] is True
    assert scan(guard)["net_pnl_sum_usdt"] == -1
