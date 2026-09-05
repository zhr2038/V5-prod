from pathlib import Path

import pytest

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.research.original_v5_adapter import OriginalV5Adapter
from src.research.quote_portfolio import QuotePortfolio


def test_original_pipeline_uses_its_own_account_without_execution(tmp_path):
    cfg = AppConfig()
    cfg.alpha.dynamic_ic_weighting.enabled = False
    cfg.regime.use_ensemble = False
    cfg.regime.use_hmm = False
    cfg.execution.collect_ml_training_data = False
    now = 1788652800
    data = {}
    rows = {}
    for index, symbol in enumerate(cfg.symbols):
        prices = [100 + i * (index + 1) / 10 for i in range(120)]
        data[symbol] = MarketSeries(symbol=symbol, timeframe="1h", ts=[(now-(119-i)*3600)*1000 for i in range(120)], open=prices, high=[p*1.01 for p in prices], low=[p*0.99 for p in prices], close=prices, volume=[100000.] * 120)
        rows[symbol] = {"close": prices[-1]}
    baseline = OriginalV5Adapter(cfg, sandbox=tmp_path / "baseline")
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=5)
    cwd = Path.cwd()
    orders, audit = baseline.decide(snapshot={"now_ts": now, "bar_ts": now, "symbols": rows}, market_data=data, portfolio=book)
    assert isinstance(orders, list) and audit["run_id"] == f"research-{now}"
    assert book.cash == 100 and book.fills == []
    assert Path.cwd() == cwd
    assert Path(baseline.cfg.execution.order_store_path).is_relative_to(tmp_path)
    assert baseline.cfg.execution.dry_run and not baseline.cfg.quant_lab.enabled
    with pytest.raises(ValueError, match="increase strictly"):
        baseline.decide(snapshot={"now_ts": now, "bar_ts": now, "symbols": rows}, market_data=data, portfolio=book)
