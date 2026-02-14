from datetime import datetime, timezone, timedelta

from src.execution.position_store import PositionStore, Position


def test_position_marking_updates_highest_and_pnl(tmp_path):
    db = tmp_path / "pos.sqlite"
    ps = PositionStore(path=str(db))
    now = datetime.now(timezone.utc)
    ts = now.isoformat().replace("+00:00", "Z")

    ps.upsert_buy("AAA/USDT", qty=1.0, px=100.0, now_ts=ts)
    p = ps.get("AAA/USDT")
    assert p is not None

    # simulate marking: highest goes up, mark price up -> pnl positive
    p2 = Position(
        symbol=p.symbol,
        qty=p.qty,
        avg_px=p.avg_px,
        entry_ts=p.entry_ts,
        highest_px=110.0,
        last_update_ts=ts,
        last_mark_px=110.0,
        unrealized_pnl_pct=0.10,
        tags_json=p.tags_json,
    )
    ps.upsert_position(p2)

    got = ps.get("AAA/USDT")
    assert got is not None
    assert got.highest_px == 110.0
    assert got.last_mark_px == 110.0
    assert got.unrealized_pnl_pct > 0
