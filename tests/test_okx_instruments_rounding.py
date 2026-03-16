from __future__ import annotations

from src.data.okx_instruments import round_down_to_lot


def test_round_down_to_lot() -> None:
    assert round_down_to_lot(0.1234, 0.01) == 0.12
    assert round_down_to_lot(1.0, 0.1) == 1.0
    assert round_down_to_lot(1.09, 0.1) == 1.0
    assert round_down_to_lot(0.0, 0.1) == 0.0
