from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.auto_sync_before_trade import _sync_local_store_to_okx_snapshot
from src.execution.position_store import PositionStore


def test_sync_preserves_entry_ts_for_existing_symbol(tmp_path):
    db = tmp_path / "positions.sqlite"
    store = PositionStore(path=str(db))
    original_entry_ts = "2026-03-10T08:00:00Z"
    store.upsert_buy("SUI/USDT", qty=10.0, px=1.0, now_ts=original_entry_ts)

    stats = _sync_local_store_to_okx_snapshot(
        store,
        {"SUI/USDT": 10.0},
        {"SUI/USDT": {"qty": 9.5, "eq_usd": 9.5}},
    )

    pos = store.get("SUI/USDT")
    assert pos is not None
    assert pos.qty == pytest.approx(9.5)
    assert pos.entry_ts == original_entry_ts
    assert stats == {"closed": 0, "updated": 1, "created": 0}


def test_sync_closes_missing_symbol_and_creates_new_symbol(tmp_path):
    db = tmp_path / "positions.sqlite"
    store = PositionStore(path=str(db))
    store.upsert_buy("OLD/USDT", qty=1.0, px=2.0, now_ts="2026-03-10T07:00:00Z")

    stats = _sync_local_store_to_okx_snapshot(
        store,
        {"OLD/USDT": 1.0},
        {"NEW/USDT": {"qty": 2.0, "eq_usd": 10.0}},
    )

    assert store.get("OLD/USDT") is None
    new_pos = store.get("NEW/USDT")
    assert new_pos is not None
    assert new_pos.qty == pytest.approx(2.0)
    assert new_pos.avg_px == pytest.approx(5.0)
    assert stats == {"closed": 1, "updated": 0, "created": 1}
