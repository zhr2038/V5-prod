import json

from src.execution.highest_px_tracker import HighestPriceTracker


def test_highest_tracker_clear_symbol_preserves_external_records(tmp_path):
    state_path = tmp_path / "highest_px_state.json"
    tracker = HighestPriceTracker(str(state_path))
    tracker.update("BTC/USDT", 100.0, 100.0, source="new_position")

    state_path.write_text(
        json.dumps(
            {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "highest_px": 100.0,
                    "entry_px": 100.0,
                    "updated_at": "2026-04-10T00:00:00",
                    "source": "runtime",
                },
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "highest_px": 200.0,
                    "entry_px": 200.0,
                    "updated_at": "2026-04-10T00:00:00",
                    "source": "runtime-keep",
                },
            }
        ),
        encoding="utf-8",
    )

    tracker.clear_symbol("BTC/USDT")

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert "BTC/USDT" not in payload
    assert payload["ETH/USDT"]["source"] == "runtime-keep"
