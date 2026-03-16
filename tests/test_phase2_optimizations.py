from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.execution.multi_level_stop_loss import MultiLevelStopLoss, StopLossConfig
from src.execution.position_builder import PositionBuilder

POSITION_STATE_FILE = ROOT / "reports" / "position_builder_state.json"
STOP_LOSS_STATE_FILE = ROOT / "reports" / "stop_loss_state.json"


def _unlink_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def test_position_builder():
    _unlink_if_exists(POSITION_STATE_FILE)

    builder = PositionBuilder(
        stages=[0.3, 0.3, 0.4],
        price_drop_threshold=0.02,
        trend_confirmation_bars=2,
    )
    builder.position_states = {}

    symbol = "BTC/USDT"
    target_notional = 100.0

    notional_1 = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=50000,
        price_history=[48000, 49000, 50000],
    )
    assert abs(notional_1 - 30.0) < 0.01

    notional_2 = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=51000,
        price_history=[50000, 50500, 51000],
    )
    assert notional_2 == 0.0

    notional_2b = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=48500,
        price_history=[50000, 49000, 48500],
    )
    assert abs(notional_2b - 30.0) < 0.01

    notional_3 = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=48000,
        price_history=[48500, 48200, 48000],
    )
    assert notional_3 == 0.0

    notional_3b = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=49500,
        price_history=[48500, 49000, 49500],
    )
    assert abs(notional_3b - 40.0) < 0.01

    summary = builder.get_position_summary(symbol)
    assert summary["status"] == "completed"


def test_multi_level_stop_loss():
    _unlink_if_exists(STOP_LOSS_STATE_FILE)

    stop_loss = MultiLevelStopLoss(
        StopLossConfig(
            tight_pct=0.03,
            normal_pct=0.05,
            loose_pct=0.08,
        )
    )
    stop_loss.positions = {}

    symbol = "ETH/USDT"
    entry_price = 2000.0

    stop_price = stop_loss.initialize_position(symbol, entry_price, "Sideways")
    assert abs(stop_price - (entry_price * 0.95)) < 0.01

    new_stop, stop_type, triggered = stop_loss.update_stop_price(symbol, 1900)
    assert abs(new_stop - stop_price) < 0.01
    assert stop_type == "initial_normal"
    assert triggered is True

    stop_loss.remove_position(symbol)
    stop_loss.initialize_position(symbol, entry_price, "Trending")

    new_stop, stop_type, triggered = stop_loss.update_stop_price(symbol, 2200)
    assert abs(new_stop - (entry_price * 1.05)) < 0.01
    assert stop_type == "breakeven_plus_5pct"
    assert triggered is False

    new_stop, stop_type, triggered = stop_loss.update_stop_price(symbol, 2400)
    assert "trailing" in stop_type
    assert triggered is False

    _, _, triggered = stop_loss.update_stop_price(symbol, 2200)
    assert triggered is True

    stop_loss.remove_position(symbol)
    stop_price = stop_loss.initialize_position(symbol, entry_price, "Risk-Off")
    assert abs(stop_price - (entry_price * 0.97)) < 0.01


def main() -> bool:
    try:
        test_position_builder()
        test_multi_level_stop_loss()
    except Exception as exc:
        print(exc)
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
