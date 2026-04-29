from __future__ import annotations

import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

from configs.schema import ExecutionConfig
from scripts.trade_auditor_v3 import TradeAuditorV3
from src.execution.fill_reconciler import FillReconciler
from src.execution.fill_store import FillRow, FillStore
from src.execution.legacy_order_polling import split_whitelist_breach_records
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


class CountingOKX:
    def __init__(self) -> None:
        self.get_calls = 0
        self._orders: dict[str, dict[str, str]] = {}

    def get_order(self, *, inst_id, ord_id=None, cl_ord_id=None):
        self.get_calls += 1
        row = self._orders.get(str(cl_ord_id), None)
        if row is None:
            return SimpleNamespace(data={"code": "0", "data": []})
        return SimpleNamespace(data={"code": "0", "data": [row]})


def test_legacy_xaut_updated_after_release_is_not_current_whitelist_breach() -> None:
    release_start_ts = 1_000_000
    current, legacy = split_whitelist_breach_records(
        [
            {
                "symbol": "XAUT/USDT",
                "side": "buy",
                "qty": 0.0,
                "notional": 3.6,
                "created_at": "1970-01-01T00:16:39Z",
                "updated_at": "1970-01-01T00:16:41Z",
            }
        ],
        whitelist_symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"],
        release_start_ts=release_start_ts,
    )

    assert current == []
    assert len(legacy) == 1
    assert legacy[0]["symbol"] == "XAUT/USDT"


def test_post_release_xaut_is_current_whitelist_breach() -> None:
    release_start_ts = 1_000_000
    current, legacy = split_whitelist_breach_records(
        [
            {
                "inst_id": "XAUT-USDT",
                "side": "buy",
                "qty": 0.0,
                "notional": 3.6,
                "created_ts": 1_000_100,
                "updated_ts": 1_500_000,
            }
        ],
        whitelist_symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"],
        release_start_ts=release_start_ts,
    )

    assert legacy == []
    assert len(current) == 1
    assert current[0]["symbol"] == "XAUT/USDT"


def test_fully_labeled_terminal_order_no_longer_repolled() -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = CountingOKX()
        store = OrderStore(path=f"{td}/orders.sqlite")
        fills = FillStore(path=f"{td}/fills.sqlite")

        store.upsert_new(
            cl_ord_id="TERM1",
            run_id="r",
            inst_id="XAUT-USDT",
            side="buy",
            intent="REBALANCE",
            decision_hash="term-h",
            td_mode="cash",
            ord_type="market",
            notional_usdt=3.6,
        )
        store.update_state("TERM1", new_state="FILLED", ord_id="5001")
        fills.upsert_many(
            [
                FillRow(
                    inst_id="XAUT-USDT",
                    trade_id="xaut-fill-1",
                    ts_ms=1,
                    ord_id="5001",
                    cl_ord_id="TERM1",
                    side="buy",
                    fill_px="1800",
                    fill_sz="0.002",
                    fee="0",
                    fee_ccy="USDT",
                )
            ]
        )

        rec = FillReconciler(fill_store=fills, order_store=store, okx=okx, position_store=None)
        result = rec.reconcile(limit=100, max_get_order_per_run=10)

        assert okx.get_calls == 0
        assert result["fully_labeled_order_poll_skipped_count"] == 1
        assert result["legacy_order_poll_skipped_count"] == 1


def test_non_whitelist_legacy_unknown_order_not_polled() -> None:
    with tempfile.TemporaryDirectory() as td:
        okx = CountingOKX()
        store = OrderStore(path=f"{td}/orders.sqlite")
        pos = PositionStore(path=f"{td}/positions.sqlite")
        cfg = ExecutionConfig(order_store_path=f"{td}/orders.sqlite")
        eng = LiveExecutionEngine(cfg, okx=okx, order_store=store, position_store=pos, run_id="r")

        store.upsert_new(
            cl_ord_id="LEGACY1",
            run_id="r",
            inst_id="XAUT-USDT",
            side="buy",
            intent="REBALANCE",
            decision_hash="legacy-h",
            td_mode="cash",
            ord_type="market",
            notional_usdt=3.6,
        )
        store.update_state("LEGACY1", new_state="UNKNOWN")
        old_ts = int((time.time() - 96 * 3600) * 1000)
        with closing(sqlite3.connect(str(Path(td) / "orders.sqlite"))) as con:
            con.execute(
                "UPDATE orders SET created_ts=?, updated_ts=?, last_poll_ts=? WHERE cl_ord_id=?",
                (old_ts, int(time.time() * 1000), int(time.time() * 1000), "LEGACY1"),
            )
            con.commit()

        rows = eng.poll_open(limit=10)

        assert rows == []
        assert okx.get_calls == 0
        repaired = store.get("LEGACY1")
        assert repaired is not None
        assert repaired.state == "REJECTED"
        assert repaired.last_error_code == "EXPIRED"


def test_trade_auditor_v3_recent_orders_use_created_ts_not_updated_ts(tmp_path: Path) -> None:
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "reports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "live_prod.yaml").write_text(
        "\n".join(
            [
                "symbols:",
                "  - BTC/USDT",
                "  - ETH/USDT",
                "  - SOL/USDT",
                "  - BNB/USDT",
                "execution:",
                "  order_store_path: reports/orders.sqlite",
            ]
        ),
        encoding="utf-8",
    )

    now_ms = int(time.time() * 1000)
    stale_created_ts = now_ms - 6 * 3600 * 1000
    conn = sqlite3.connect(str(tmp_path / "reports" / "orders.sqlite"))
    conn.execute(
        """
        CREATE TABLE orders (
            cl_ord_id TEXT,
            inst_id TEXT,
            side TEXT,
            state TEXT,
            intent TEXT,
            ord_id TEXT,
            last_error_code TEXT,
            last_error_msg TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(cl_ord_id, inst_id, side, state, intent, ord_id, last_error_code, last_error_msg, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "LEGACY-XAUT",
            "XAUT-USDT",
            "buy",
            "UNKNOWN",
            "REBALANCE",
            None,
            None,
            None,
            stale_created_ts,
            now_ms,
        ),
    )
    conn.commit()
    conn.close()

    auditor = TradeAuditorV3(workspace=tmp_path)

    assert auditor.get_recent_orders(hours=2) == []
