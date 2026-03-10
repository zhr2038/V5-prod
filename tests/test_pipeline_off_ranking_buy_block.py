from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(sym: str, close: float) -> MarketSeries:
    ts = [_ms(1700000000 + i * 3600) for i in range(30)]
    close_arr = [close for _ in range(30)]
    vol = [1000.0 for _ in range(30)]
    return MarketSeries(
        symbol=sym,
        timeframe="1h",
        ts=ts,
        open=close_arr,
        high=close_arr,
        low=close_arr,
        close=close_arr,
        volume=vol,
    )


def test_pipeline_blocks_buy_for_symbol_outside_entry_candidates():
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT"])
    pipe = V5Pipeline(cfg)
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0

    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={"ETH/USDT": 0.25},
        selected=["ETH/USDT"],
        entry_candidates=["SUI/USDT"],
        volatilities={},
        notes="",
    )

    market_data = {
        "BTC/USDT": _series("BTC/USDT", 50000.0),
        "ETH/USDT": _series("ETH/USDT", 2000.0),
    }
    alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"ETH/USDT": 0.5})
    regime = RegimeResult(
        state=RegimeState.SIDEWAYS,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.0,
    )
    audit = DecisionAudit(run_id="off-ranking-buy")

    out = pipe.run(
        market_data_1h=market_data,
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=alpha,
        precomputed_regime=regime,
    )

    assert out.orders == []
    assert any(
        d.get("symbol") == "ETH/USDT" and d.get("reason") == "off_ranking_buy_block"
        for d in audit.router_decisions
    )
