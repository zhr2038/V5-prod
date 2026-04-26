from __future__ import annotations

from event_driven_check import _load_fused_signal_states, _load_positions_snapshot
from src.execution.event_driven_integration import EventDrivenConfig, EventDrivenTrader
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


def test_load_fused_signal_states_normalizes_zero_based_rank() -> None:
    signals = _load_fused_signal_states(
        {
            "fused": {
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "direction": "sell",
                    "score": 0.12,
                    "rank": 0,
                }
            }
        },
        {"ETH/USDT"},
    )

    assert signals["ETH/USDT"].rank == 1


def test_load_fused_signal_states_derives_ranks_for_duplicate_legacy_zero_ranks() -> None:
    signals = _load_fused_signal_states(
        {
            "fused": {
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "direction": "buy",
                    "score": 0.62,
                    "rank": 0,
                },
                "BNB/USDT": {
                    "symbol": "BNB/USDT",
                    "direction": "buy",
                    "score": 0.09,
                    "rank": 0,
                },
            }
        },
        {"ETH/USDT", "BNB/USDT"},
    )

    assert signals["ETH/USDT"].rank == 1
    assert signals["BNB/USDT"].rank == 2


def test_event_driven_history_normalizes_zero_based_rank(tmp_path) -> None:
    trader = EventDrivenTrader(
        EventDrivenConfig(
            monitor_state_path=str(tmp_path / "event_monitor_state.json"),
            cooldown_state_path=str(tmp_path / "cooldown_state.json"),
        )
    )

    state = trader._build_market_state(
        {
            "timestamp_ms": 1,
            "regime": "SIDEWAYS",
            "prices": {},
            "positions": {},
            "signals": {
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "direction": "sell",
                    "score": 0.12,
                    "rank": 0,
                    "timestamp_ms": 1,
                }
            },
        }
    )

    assert state.signals["ETH/USDT"].rank == 1
