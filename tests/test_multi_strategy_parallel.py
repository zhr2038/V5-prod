from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from configs.schema import AlphaConfig, RiskConfig
from src.alpha.alpha_engine import AlphaEngine
from src.core.models import MarketSeries
from src.portfolio.portfolio_engine import PortfolioEngine
from src.strategy.multi_strategy_system import MultiStrategyAdapter, Signal, StrategyOrchestrator


def _series(symbol: str) -> MarketSeries:
    closes = [100.0 + i for i in range(30)]
    return MarketSeries(
        symbol=symbol,
        timeframe="1h",
        ts=list(range(30)),
        open=closes,
        high=[v + 1.0 for v in closes],
        low=[v - 1.0 for v in closes],
        close=closes,
        volume=[1000.0] * 30,
    )


class _StubAdapter:
    def __init__(self, targets):
        self.targets = targets

    def run_strategy_cycle(self, market_df):
        return list(self.targets)

    def set_run_id(self, run_id):
        self.run_id = run_id

    def strategy_signals_path(self):
        return None


def test_multi_strategy_scores_preserve_buy_sell_direction():
    engine = AlphaEngine(AlphaConfig())
    engine.use_multi_strategy = True
    engine.multi_strategy_adapter = _StubAdapter(
        [
            {
                "symbol": "BTC-USDT",
                "side": "buy",
                "target_position_usdt": 10.0,
                "signal_score": 0.8,
                "confidence": 1.0,
                "strategy_weight": 1.0,
            },
            {
                "symbol": "ETH-USDT",
                "side": "sell",
                "target_position_usdt": 10.0,
                "signal_score": 0.6,
                "confidence": 1.0,
                "strategy_weight": 1.0,
            },
        ]
    )

    scores = engine.compute_scores(
        {
            "BTC/USDT": _series("BTC/USDT"),
            "ETH/USDT": _series("ETH/USDT"),
        }
    )

    assert scores["BTC/USDT"] > 0
    assert scores["ETH/USDT"] < 0


def test_multi_strategy_same_symbol_merge_applies_weight_once():
    engine = AlphaEngine(AlphaConfig())
    engine.use_multi_strategy = True
    engine.multi_strategy_adapter = _StubAdapter(
        [
            {
                "symbol": "BTC-USDT",
                "side": "buy",
                "target_position_usdt": 16.0,
                "signal_score": 1.0,
                "confidence": 1.0,
                "strategy_weight": 0.8,
            },
            {
                "symbol": "BTC-USDT",
                "side": "buy",
                "target_position_usdt": 4.0,
                "signal_score": 1.0,
                "confidence": 1.0,
                "strategy_weight": 0.2,
            },
        ]
    )

    scores = engine.compute_scores({"BTC/USDT": _series("BTC/USDT")})

    assert scores["BTC/USDT"] == pytest.approx(1.0)


def test_portfolio_engine_loads_only_current_run_fused_signals(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    current = tmp_path / "reports" / "runs" / "current_run"
    stale = tmp_path / "reports" / "runs" / "stale_run"
    current.mkdir(parents=True)
    stale.mkdir(parents=True)

    (current / "strategy_signals.json").write_text(
        '{"fused": {"BTC/USDT": {"direction": "buy", "score": 0.7}}}',
        encoding="utf-8",
    )
    (stale / "strategy_signals.json").write_text(
        '{"fused": {"STALE/USDT": {"direction": "buy", "score": 9.9}}}',
        encoding="utf-8",
    )

    pe = PortfolioEngine(alpha_cfg=AlphaConfig(), risk_cfg=RiskConfig())
    pe.set_run_id("current_run")
    scores = pe._load_fused_signals()

    assert scores == {"BTC/USDT": 0.7}


def test_multi_strategy_adapter_normalizes_total_target_notional():
    orchestrator = StrategyOrchestrator(total_capital=Decimal("100"))
    orchestrator.strategy_allocations = {
        "TrendFollowing": Decimal("0.30"),
        "Alpha6": Decimal("0.25"),
    }
    adapter = MultiStrategyAdapter(orchestrator)
    now = datetime(2026, 3, 8)

    def _signals(_market_data):
        return [
            Signal(
                symbol="BTC/USDT",
                side="buy",
                score=1.0,
                confidence=1.0,
                strategy="FUSED",
                timestamp=now,
                metadata={"source_strategies": ["TrendFollowing", "Alpha6"]},
            ),
            Signal(
                symbol="ETH/USDT",
                side="buy",
                score=1.0,
                confidence=1.0,
                strategy="FUSED",
                timestamp=now,
                metadata={"source_strategies": ["TrendFollowing", "Alpha6"]},
            ),
            Signal(
                symbol="SOL/USDT",
                side="buy",
                score=1.0,
                confidence=1.0,
                strategy="FUSED",
                timestamp=now,
                metadata={"source_strategies": ["TrendFollowing", "Alpha6"]},
            ),
        ]

    orchestrator.generate_combined_signals = _signals
    targets = adapter.run_strategy_cycle(pd.DataFrame())

    total_target = sum(t["target_position_usdt"] for t in targets)
    assert total_target == pytest.approx(100.0, rel=1e-6)
    assert all(t["target_position_usdt"] == pytest.approx(100.0 / 3.0, rel=1e-6) for t in targets)
