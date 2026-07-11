from __future__ import annotations

import csv
import io
import json
import tarfile
from pathlib import Path

from src.execution.bills_store import BillRow, BillsStore
from src.execution.fill_store import FillRow, FillStore
from src.reporting.fill_bill_reconciliation import (
    FILL_BILL_RECONCILIATION_SCHEMA_VERSION,
    build_fill_bill_cost_reconciliation,
)
from src.reporting.v5_bundle_exporter import export_v5_bundle


def test_delayed_bill_recomputes_partial_fill_cost_as_actual(tmp_path: Path) -> None:
    fills_path = tmp_path / "reports" / "fills.sqlite"
    bills_path = tmp_path / "reports" / "bills.sqlite"
    fills = FillStore(str(fills_path))
    fills.upsert_many(
        [
            FillRow(
                inst_id="ETH-USDT",
                trade_id="trade-1",
                ts_ms=1_788_000_000_000,
                ord_id="order-1",
                cl_ord_id="client-1",
                side="buy",
                exec_type="T",
                fill_px="2500",
                fill_sz="0.002",
                fee="-0.005",
                fee_ccy="USDT",
            )
        ]
    )

    before = build_fill_bill_cost_reconciliation(fills_path, bills_path)
    assert before[0]["bill_match_status"] == "BILL_PENDING"
    assert before[0]["cost_evidence_status"] == "PARTIAL"
    assert before[0]["selected_fee_usdt"] == 0.005

    BillsStore(str(bills_path)).upsert_many(
        [
            BillRow(
                bill_id="bill-1",
                ts_ms=1_788_000_060_000,
                ccy="USDT",
                inst_id="ETH-USDT",
                ord_id="order-1",
                cl_ord_id="client-1",
                raw_json=json.dumps(
                    {
                        "billId": "bill-1",
                        "ordId": "order-1",
                        "clOrdId": "client-1",
                        "fee": "-0.005",
                        "feeCcy": "USDT",
                    }
                ),
            )
        ]
    )

    after = build_fill_bill_cost_reconciliation(fills_path, bills_path)
    row = after[0]
    assert row["bill_match_status"] == "PASS"
    assert row["cost_evidence_status"] == "ACTUAL"
    assert row["cost_source"] == "actual_fills_bills"
    assert row["liquidity_role"] == "taker"
    assert row["bill_delay_seconds"] == 60.0
    assert row["fee_complete"] is True


def test_missing_fee_is_partial_not_zero_and_rebate_keeps_sign(tmp_path: Path) -> None:
    missing_fills = tmp_path / "missing" / "fills.sqlite"
    FillStore(str(missing_fills)).upsert_many(
        [
            FillRow(
                inst_id="BNB-USDT",
                trade_id="missing-fee",
                ts_ms=1_788_000_000_000,
                ord_id="missing-order",
                side="sell",
                fill_px="600",
                fill_sz="0.01",
                fee=None,
                fee_ccy=None,
            )
        ]
    )
    missing = build_fill_bill_cost_reconciliation(
        missing_fills,
        tmp_path / "missing" / "bills.sqlite",
    )[0]
    assert missing["bill_match_status"] == "FEE_MISSING"
    assert missing["selected_fee_usdt"] is None
    assert missing["fee_complete"] is False

    rebate_fills = tmp_path / "rebate" / "fills.sqlite"
    rebate_bills = tmp_path / "rebate" / "bills.sqlite"
    FillStore(str(rebate_fills)).upsert_many(
        [
            FillRow(
                inst_id="BTC-USDT",
                trade_id="rebate-trade",
                ts_ms=1_788_100_000_000,
                ord_id="rebate-order",
                side="sell",
                exec_type="M",
                fill_px="100000",
                fill_sz="0.0001",
                fee="0.001",
                fee_ccy="USDT",
            )
        ]
    )
    BillsStore(str(rebate_bills)).upsert_many(
        [
            BillRow(
                bill_id="rebate-bill",
                ts_ms=1_788_100_000_500,
                ccy="USDT",
                ord_id="rebate-order",
                raw_json=json.dumps(
                    {
                        "billId": "rebate-bill",
                        "ordId": "rebate-order",
                        "fee": "0.001",
                        "feeCcy": "USDT",
                    }
                ),
            )
        ]
    )
    rebate = build_fill_bill_cost_reconciliation(rebate_fills, rebate_bills)[0]
    assert rebate["bill_match_status"] == "PASS"
    assert rebate["liquidity_role"] == "maker"
    assert rebate["selected_fee_usdt"] == -0.001
    assert rebate["rebate_usdt"] == 0.001


def test_bundle_contains_current_fill_bill_reconciliation(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    fills_path = reports / "fills.sqlite"
    bills_path = reports / "bills.sqlite"
    FillStore(str(fills_path)).upsert_many(
        [
            FillRow(
                inst_id="TRX-USDT",
                trade_id="trade-bundle",
                ts_ms=1_788_200_000_000,
                ord_id="order-bundle",
                side="buy",
                exec_type="T",
                fill_px="0.3",
                fill_sz="10",
                fee="-0.003",
                fee_ccy="USDT",
            )
        ]
    )
    BillsStore(str(bills_path)).upsert_many(
        [
            BillRow(
                bill_id="bill-bundle",
                ts_ms=1_788_200_001_000,
                ccy="USDT",
                ord_id="order-bundle",
                raw_json=json.dumps(
                    {
                        "billId": "bill-bundle",
                        "ordId": "order-bundle",
                        "fee": "-0.003",
                        "feeCcy": "USDT",
                    }
                ),
            )
        ]
    )

    bundle = export_v5_bundle(
        reports_dir=reports,
        out_dir=tmp_path / "exports",
        include_logs=False,
        include_config=False,
        refresh_cost_probe_preflight=False,
    )

    with tarfile.open(bundle, "r:gz") as archive:
        member = archive.extractfile("summaries/fill_bill_cost_reconciliation.csv")
        assert member is not None
        rows = list(csv.DictReader(io.StringIO(member.read().decode("utf-8"))))
        manifest_file = archive.extractfile("manifest.json")
        assert manifest_file is not None
        manifest = json.loads(manifest_file.read().decode("utf-8"))
    assert rows[0]["bill_match_status"] == "PASS"
    assert rows[0]["cost_evidence_status"] == "ACTUAL"
    assert manifest["fill_bill_reconciliation_schema_version"] == (
        FILL_BILL_RECONCILIATION_SCHEMA_VERSION
    )
    assert manifest["fill_bill_reconciliation_rows"] == 1
