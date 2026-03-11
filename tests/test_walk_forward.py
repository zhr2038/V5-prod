from src.backtest.walk_forward import build_folds
from configs.schema import AppConfig
from src.backtest.walk_forward import run_walk_forward
from src.core.models import MarketSeries
from src.execution.ml_data_collector import MLDataCollector


def test_build_folds_basic():
    folds = build_folds(100, folds=4)
    assert len(folds) == 4
    assert folds[0][1] == (0, 25)
    assert folds[-1][1] == (75, 100)


def test_run_walk_forward_respects_disabled_ml_data_collection(monkeypatch):
    calls = {"collect": 0, "fill": 0}

    def _collect(self, *args, **kwargs):
        calls["collect"] += 1
        return True

    def _fill(self, *args, **kwargs):
        calls["fill"] += 1
        return 0

    monkeypatch.setattr(MLDataCollector, "collect_features", _collect)
    monkeypatch.setattr(MLDataCollector, "fill_labels", _fill)

    n = 120
    closes = [100.0 + i for i in range(n)]
    md = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[1_700_000_000_000 + i * 3600 * 1000 for i in range(n)],
            open=closes,
            high=closes,
            low=closes,
            close=closes,
            volume=[1e7] * n,
        )
    }

    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.collect_ml_training_data = False

    run_walk_forward(md, folds=1, cfg=cfg)

    assert calls == {"collect": 0, "fill": 0}
