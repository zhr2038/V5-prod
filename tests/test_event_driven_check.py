from __future__ import annotations

from event_driven_check import _load_positions_snapshot
from src.execution.position_store import PositionStore


def test_load_positions_snapshot_reports_empty_position_store(tmp_path) -> None:
    positions_db = tmp_path / "positions.sqlite"
    PositionStore(path=str(positions_db))

    positions, symbols, source = _load_positions_snapshot(
        positions_db_path=positions_db,
        portfolio_path=tmp_path / "portfolio.json",
    )

    assert positions == {}
    assert symbols == set()
    assert source == "position_store_empty"
