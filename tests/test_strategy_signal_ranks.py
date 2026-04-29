from __future__ import annotations

import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.strategy.multi_strategy_system import (
    BaseStrategy,
    Signal,
    StrategyOrchestrator,
    StrategyType,
)


class StaticSignalStrategy(BaseStrategy):
    def __init__(self, signals: list[Signal]):
        super().__init__("StaticSignal", StrategyType.MOMENTUM, {})
        self._signals = signals

    def generate_signals(self, market_data: pd.DataFrame) -> list[Signal]:
        return list(self._signals)

    def calculate_position_size(self, signal: Signal, available_capital: Decimal) -> Decimal:
        return Decimal("0")


def test_strategy_signal_payload_writes_one_based_fused_ranks(tmp_path):
    orchestrator = StrategyOrchestrator(audit_root=tmp_path)
    orchestrator.set_run_id("rank-test")
    orchestrator.register_strategy(
        StaticSignalStrategy(
            [
                Signal(
                    symbol="BTC/USDT",
                    side="buy",
                    score=0.25,
                    confidence=0.8,
                    strategy="StaticSignal",
                    timestamp=datetime(2026, 4, 26),
                ),
                Signal(
                    symbol="ETH/USDT",
                    side="buy",
                    score=0.75,
                    confidence=0.8,
                    strategy="StaticSignal",
                    timestamp=datetime(2026, 4, 26),
                ),
            ]
        ),
        allocation=Decimal("0.1"),
    )

    orchestrator.generate_combined_signals(pd.DataFrame({"symbol": ["BTC/USDT", "ETH/USDT"]}))

    payload = orchestrator.latest_strategy_signal_payload()
    fused = payload["fused"]
    assert fused["ETH/USDT"]["rank"] == 1
    assert fused["BTC/USDT"]["rank"] == 2

    path = tmp_path / "runs" / "rank-test" / "strategy_signals.json"
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert persisted["run_id"] == "rank-test"
    assert persisted["fused"]["ETH/USDT"]["rank"] == 1
    assert persisted["fused"]["BTC/USDT"]["rank"] == 2
