import sqlite3
from contextlib import closing

from src.execution.fill_store import FillStore, _connect as connect_fill_store
from src.execution.order_store import OrderStore, _connect as connect_order_store


def _pragma(path, name: str):
    with sqlite3.connect(str(path)) as con:
        return con.execute(f"PRAGMA {name}").fetchone()[0]


def _connection_pragma(con, name: str):
    return con.execute(f"PRAGMA {name}").fetchone()[0]


def test_order_store_uses_wal_and_busy_timeout(tmp_path):
    db = tmp_path / "orders.sqlite"
    store = OrderStore(str(db))
    store.wal_checkpoint()

    assert str(_pragma(db, "journal_mode")).lower() == "wal"
    with closing(connect_order_store(db)) as con:
        assert int(_connection_pragma(con, "busy_timeout")) == 30000


def test_fill_store_uses_wal_and_busy_timeout(tmp_path):
    db = tmp_path / "fills.sqlite"
    store = FillStore(str(db))
    store.wal_checkpoint()

    assert str(_pragma(db, "journal_mode")).lower() == "wal"
    with closing(connect_fill_store(db)) as con:
        assert int(_connection_pragma(con, "busy_timeout")) == 30000
