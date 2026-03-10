from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from configs.schema import AlphaConfig, RiskConfig
from src.alpha.alpha_engine import AlphaEngine
from src.core.models import MarketSeries
from src.portfolio.portfolio_engine import PortfolioEngine
from src.strategy.multi_strategy_system import Alpha6FactorStrategy, MultiStrategyAdapter, Signal, StrategyOrchestrator


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


def test_multi_strategy_single_signal_keeps_raw_signal_scale():
    engine = AlphaEngine(AlphaConfig())
    engine.use_multi_strategy = True
    engine.multi_strategy_adapter = _StubAdapter(
        [
            {
                "symbol": "BTC-USDT",
                "side": "buy",
                "target_position_usdt": 11.0,
                "signal_score": 0.18,
                "confidence": 0.18,
                "strategy_weight": 0.55,
            },
        ]
    )

    scores = engine.compute_scores({"BTC/USDT": _series("BTC/USDT")})

    assert scores["BTC/USDT"] == pytest.approx(0.18)


def test_alpha6_strategy_compresses_display_score_but_preserves_raw_strength(monkeypatch):
    strategy = Alpha6FactorStrategy(
        config={
            "score_threshold": 0.05,
            "score_transform": "tanh",
            "score_transform_scale": 1.0,
            "use_sentiment": False,
            "alpha158_enabled": False,
        }
    )

    def _fake_calc_factors(_df, symbol):
        return {"synthetic_score": 4.0 if symbol == "FLOW/USDT" else -2.0}

    monkeypatch.setattr(strategy, "_calculate_factors", _fake_calc_factors)
    monkeypatch.setattr(strategy, "_zscore_factors", lambda factors: dict(factors))
    monkeypatch.setattr(
        strategy,
        "_calculate_score",
        lambda z_factors, _weights: float(z_factors["synthetic_score"]),
    )

    market_df = pd.DataFrame(
        {
            "symbol": ["FLOW/USDT"] * 60 + ["HYPE/USDT"] * 60,
            "close": [1.0] * 120,
            "high": [1.0] * 120,
            "low": [1.0] * 120,
            "volume": [1.0] * 120,
        }
    )

    signals = strategy.generate_signals(market_df)
    flow = next(s for s in signals if s.symbol == "FLOW/USDT")

    assert 0.99 < flow.score < 1.0
    assert flow.metadata["raw_score"] == pytest.approx(3.0)
    assert flow.metadata["display_score"] == pytest.approx(flow.score)
    assert flow.metadata["relative_score_raw"] == pytest.approx(3.0)


def test_strategy_orchestrator_keeps_raw_score_metadata_for_same_side_fusion():
    orchestrator = StrategyOrchestrator(total_capital=Decimal("100"))
    now = datetime(2026, 3, 11)
    signals = [
        Signal(
            symbol="FLOW/USDT",
            side="buy",
            score=0.999,
            confidence=1.0,
            strategy="Alpha6Factor",
            timestamp=now,
            metadata={"raw_score": 6.4456, "display_score": 0.999},
        ),
        Signal(
            symbol="FLOW/USDT",
            side="buy",
            score=0.82,
            confidence=0.92,
            strategy="TrendFollowing",
            timestamp=now,
            metadata={"raw_score": 0.82, "display_score": 0.82},
        ),
    ]

    fused = orchestrator._fuse_signals(signals)

    assert len(fused) == 1
    result = fused[0]
    assert result.side == "buy"
    assert 0.0 < result.score <= 1.0
    assert result.metadata["raw_score"] == pytest.approx((6.4456 + 0.82) / 2.0)
    assert result.metadata["display_score"] == pytest.approx(result.score)


def test_strategy_orchestrator_downweights_conflicting_signals():
    orchestrator = StrategyOrchestrator(
        total_capital=Decimal("100"),
        conflict_penalty_enabled=True,
        conflict_dominance_ratio=1.35,
        conflict_min_confidence=0.60,
        conflict_penalty_strength=0.65,
    )
    now = datetime(2026, 3, 10)
    signals = [
        Signal(
            symbol="OKB/USDT",
            side="buy",
            score=0.90,
            confidence=0.90,
            strategy="TrendFollowing",
            timestamp=now,
        ),
        Signal(
            symbol="OKB/USDT",
            side="sell",
            score=0.55,
            confidence=0.66,
            strategy="MeanReversion",
            timestamp=now,
        ),
    ]

    fused = orchestrator._fuse_signals(signals)

    assert len(fused) == 1
    result = fused[0]
    assert result.symbol == "OKB/USDT"
    assert result.side == "buy"
    assert result.strategy == "FUSED"
    assert 0.0 < result.score < 0.90
    assert 0.0 < result.confidence < 0.90
    assert result.metadata["conflict_detected"] is True
    assert result.metadata["conflict_penalty_factor"] < 1.0
    assert result.metadata["opposing_strategies"] == ["MeanReversion"]


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
