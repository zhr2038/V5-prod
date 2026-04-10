from src.execution.position_store import PositionStore


def test_position_store_buy_close(tmp_path):
    db = tmp_path / "pos.sqlite"
    ps = PositionStore(path=str(db))

    assert ps.get("BTC/USDT") is None
    ps.upsert_buy("BTC/USDT", qty=0.1, px=100.0)
    p = ps.get("BTC/USDT")
    assert p is not None
    assert abs(p.qty - 0.1) < 1e-12

    # add more, avg price should update
    ps.upsert_buy("BTC/USDT", qty=0.1, px=200.0)
    p2 = ps.get("BTC/USDT")
    assert p2 is not None
    assert abs(p2.qty - 0.2) < 1e-12
    assert 100.0 < p2.avg_px < 200.0

    ps.close_long("BTC/USDT")
    assert ps.get("BTC/USDT") is None


def test_position_store_highest_tracker_follows_store_directory(tmp_path):
    db = tmp_path / "runtime" / "positions.sqlite"
    ps = PositionStore(path=str(db))

    ps.upsert_buy("ETH/USDT", qty=1.0, px=100.0)

    assert (db.parent / "highest_px_state.json").exists()


def test_position_store_highest_tracker_preserves_shadow_prefix(tmp_path):
    db = tmp_path / "runtime" / "shadow_positions.sqlite"
    ps = PositionStore(path=str(db))

    ps.upsert_buy("ETH/USDT", qty=1.0, px=100.0)

    assert (db.parent / "shadow_highest_px_state.json").exists()
    assert not (db.parent / "highest_px_state.json").exists()
