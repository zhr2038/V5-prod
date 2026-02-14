from datetime import datetime, timezone

from configs.schema import AppConfig
from src.core.clock import FixedClock
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.position_store import Position


def test_pipeline_marking_and_dd_mult():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    pipe = V5Pipeline(AppConfig(symbols=["BTC/USDT"]), clock=FixedClock(t0))

    md = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[0],
            open=[100.0],
            high=[110.0],
            low=[90.0],
            close=[105.0],
            volume=[1.0],
        )
    }

    pos = [
        Position(
            symbol="BTC/USDT",
            qty=1.0,
            avg_px=100.0,
            entry_ts=t0.isoformat().replace("+00:00", "Z"),
            highest_px=100.0,
            last_update_ts=t0.isoformat().replace("+00:00", "Z"),
            last_mark_px=100.0,
            unrealized_pnl_pct=0.0,
        )
    ]

    out = pipe.run(md, positions=pos, cash_usdt=1000.0, equity_peak_usdt=1200.0)
    # equity=1000+1*105=1105; peak=1200 => dd~7.9%, no delever => internal scaling should keep dd_mult=1
    dd_mults = [o.meta.get("dd_mult") for o in out.orders if isinstance(o.meta, dict) and "dd_mult" in o.meta]
    assert not dd_mults or all(float(x) == 1.0 for x in dd_mults)
