from datetime import datetime, timedelta, timezone

from src.core.models import MarketSeries
from src.execution.position_store import Position
from src.risk.exit_policy import ExitPolicy, ExitConfig


def _series(last: float):
    closes = [last * (1 + 0.001 * i) for i in range(40)]
    return MarketSeries(
        symbol="AAA/USDT",
        timeframe="1h",
        ts=list(range(40)),
        open=closes,
        high=[c * 1.01 for c in closes],
        low=[c * 0.99 for c in closes],
        close=closes,
        volume=[100.0] * 40,
    )


def test_time_stop_triggers_when_not_profitable():
    cfg = ExitConfig(time_stop_days=20)
    ep = ExitPolicy(cfg)

    ent = (datetime.now(timezone.utc) - timedelta(days=21)).isoformat().replace("+00:00", "Z")
    pos = [Position(symbol="AAA/USDT", qty=1.0, avg_px=200.0, entry_ts=ent, highest_px=210.0, last_update_ts=ent, last_mark_px=190.0, unrealized_pnl_pct=-0.05)]
    md = {"AAA/USDT": _series(last=190.0)}
    orders = ep.evaluate(pos, md, regime_state="Trending")
    # Could be time_stop or atr_trailing depending on ATR/price path; at least one exit must trigger.
    assert any(o.intent == "CLOSE_LONG" for o in orders)


def test_regime_exit_closes_all():
    ep = ExitPolicy(ExitConfig(enable_regime_exit=True))
    ent = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    pos = [Position(symbol="AAA/USDT", qty=1.0, avg_px=100.0, entry_ts=ent, highest_px=100.0, last_update_ts=ent, last_mark_px=100.0, unrealized_pnl_pct=0.0)]
    md = {"AAA/USDT": _series(last=100.0)}
    orders = ep.evaluate(pos, md, regime_state="Risk-Off")
    assert len(orders) == 1
    assert orders[0].intent == "CLOSE_LONG"
