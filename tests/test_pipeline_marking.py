import json
from datetime import datetime, timezone

import src.core.pipeline as pipeline_module
from configs.schema import AppConfig
from configs.schema import RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.clock import FixedClock
from src.core.models import MarketSeries, Order
from src.core.pipeline import V5Pipeline
from src.execution.position_store import Position
from src.portfolio.portfolio_engine import PortfolioSnapshot
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit


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


def test_pipeline_ml_snapshot_timestamp_prefers_window_end():
    t0 = datetime(2026, 1, 1, 12, 34, tzinfo=timezone.utc)
    pipe = V5Pipeline(AppConfig(symbols=["BTC/USDT"]), clock=FixedClock(t0))
    audit = DecisionAudit(run_id="20260101_12", window_start_ts=1_704_110_400, window_end_ts=1_704_114_000)

    snapshot_ts = pipe._resolve_ml_snapshot_timestamp_ms(audit=audit)

    assert snapshot_ts == 1_704_114_000_000


def test_pipeline_ml_collection_uses_full_market_universe():
    t0 = datetime(2026, 1, 1, 12, 34, tzinfo=timezone.utc)
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT"])
    pipe = V5Pipeline(cfg, clock=FixedClock(t0))
    md = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[0, 1],
            open=[100.0, 100.2],
            high=[101.0, 101.2],
            low=[99.0, 99.2],
            close=[100.5, 100.8],
            volume=[1.0, 1.1],
        ),
        "ETH/USDT": MarketSeries(
            symbol="ETH/USDT",
            timeframe="1h",
            ts=[0, 1],
            open=[200.0, 200.4],
            high=[202.0, 202.4],
            low=[198.0, 198.3],
            close=[201.0, 201.5],
            volume=[1.0, 1.2],
        ),
    }
    audit = DecisionAudit(run_id="20260101_12", window_start_ts=1_704_110_400, window_end_ts=1_704_114_000)
    collected = []

    def fake_collect_features(*, timestamp, symbol, market_data, regime):
        collected.append((timestamp, symbol, regime, len(market_data["close"])))
        return True

    pipe.data_collector.collect_features = fake_collect_features
    pipe.data_collector.fill_labels = lambda current_timestamp: 0
    pipe.portfolio_engine.allocate = lambda **kwargs: PortfolioSnapshot(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        volatilities={},
        entry_candidates=["BTC/USDT"],
    )

    pipe.run(
        md,
        positions=[],
        cash_usdt=1000.0,
        equity_peak_usdt=1000.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0, "ETH/USDT": 0.5}),
        precomputed_regime=RegimeResult(
            state=RegimeState.SIDEWAYS,
            atr_pct=0.0,
            ma20=0.0,
            ma60=0.0,
            multiplier=1.0,
        ),
    )

    assert {(ts, sym) for ts, sym, _, _ in collected} == {
        (1_704_114_000_000, "BTC/USDT"),
        (1_704_114_000_000, "ETH/USDT"),
    }


def test_pipeline_ml_collection_uses_stable_research_universe(tmp_path, monkeypatch):
    t0 = datetime(2026, 1, 1, 12, 34, tzinfo=timezone.utc)
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "universe_cache.json").write_text(
        json.dumps({"symbols": ["BTC/USDT", "ETH/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(pipeline_module, "REPORTS_DIR", reports_dir)

    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.ml_research_use_stable_universe = True
    cfg.execution.ml_research_universe_path = "reports/universe_cache.json"

    pipe = V5Pipeline(cfg, clock=FixedClock(t0))
    md = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[0, 1],
            open=[100.0, 100.2],
            high=[101.0, 101.2],
            low=[99.0, 99.5],
            close=[100.5, 100.8],
            volume=[1.0, 1.1],
        ),
    }
    audit = DecisionAudit(run_id="20260101_12", window_start_ts=1_704_110_400, window_end_ts=1_704_114_000)
    collected = []

    def fake_collect_features(*, timestamp, symbol, market_data, regime):
        collected.append((timestamp, symbol, regime, len(market_data["close"])))
        return True

    pipe.data_collector.collect_features = fake_collect_features
    pipe.data_collector.fill_labels = lambda current_timestamp: 0
    pipe.data_collector.load_market_data_for_feature_snapshot = lambda symbol, end_timestamp, lookback_bars: {
        "symbol": symbol,
        "ts": [end_timestamp - 3600_000, end_timestamp],
        "open": [200.0, 201.0],
        "high": [202.0, 203.0],
        "low": [198.0, 199.0],
        "close": [201.0, 202.0],
        "volume": [2.0, 2.1],
    } if symbol == "ETH/USDT" else None
    pipe.portfolio_engine.allocate = lambda **kwargs: PortfolioSnapshot(
        target_weights={"BTC/USDT": 1.0},
        selected=["BTC/USDT"],
        volatilities={},
        entry_candidates=["BTC/USDT"],
    )

    pipe.run(
        md,
        positions=[],
        cash_usdt=1000.0,
        equity_peak_usdt=1000.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0, "ETH/USDT": 0.5}),
        precomputed_regime=RegimeResult(
            state=RegimeState.SIDEWAYS,
            atr_pct=0.0,
            ma20=0.0,
            ma60=0.0,
            multiplier=1.0,
        ),
    )

    assert {(ts, sym) for ts, sym, _, _ in collected} == {
        (1_704_114_000_000, "BTC/USDT"),
        (1_704_114_000_000, "ETH/USDT"),
    }


def test_rebalance_turnover_cap_uses_side_turnover_budget():
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.max_rebalance_turnover_per_cycle = 0.30
    pipe = V5Pipeline(cfg, clock=FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc)))
    orders = [
        Order(
            symbol="OLD/USDT",
            side="sell",
            intent="REBALANCE",
            notional_usdt=20.0,
            signal_price=1.0,
            meta={"drift": -0.20},
        ),
        Order(
            symbol="ADD/USDT",
            side="buy",
            intent="REBALANCE",
            notional_usdt=15.0,
            signal_price=1.0,
            meta={"drift": 0.30},
        ),
        Order(
            symbol="NEW/USDT",
            side="buy",
            intent="OPEN_LONG",
            notional_usdt=15.0,
            signal_price=1.0,
            meta={"drift": 0.35},
        ),
    ]

    kept, dropped, stats = pipe._apply_rebalance_turnover_cap(orders, equity_raw=100.0)

    assert not dropped
    assert [order.symbol for order in kept] == ["OLD/USDT", "ADD/USDT", "NEW/USDT"]
    assert stats["effective_turnover_notional"] == 30.0
    assert stats["cap_notional"] == 30.0


def test_rebalance_turnover_cap_prioritizes_existing_buys_before_new_opens():
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.max_rebalance_turnover_per_cycle = 0.20
    pipe = V5Pipeline(cfg, clock=FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc)))
    orders = [
        Order(
            symbol="OPEN_BIG/USDT",
            side="buy",
            intent="OPEN_LONG",
            notional_usdt=20.0,
            signal_price=1.0,
            meta={"drift": 0.60},
        ),
        Order(
            symbol="ADD_WINNER/USDT",
            side="buy",
            intent="REBALANCE",
            notional_usdt=15.0,
            signal_price=1.0,
            meta={"drift": 0.30},
        ),
        Order(
            symbol="OPEN_SMALL/USDT",
            side="buy",
            intent="OPEN_LONG",
            notional_usdt=5.0,
            signal_price=1.0,
            meta={"drift": 0.20},
        ),
    ]

    kept, dropped, stats = pipe._apply_rebalance_turnover_cap(orders, equity_raw=100.0)

    assert [order.symbol for order in kept] == ["ADD_WINNER/USDT", "OPEN_SMALL/USDT"]
    assert [order.symbol for order in dropped] == ["OPEN_BIG/USDT"]
    assert stats["kept_buy_notional"] == 20.0
