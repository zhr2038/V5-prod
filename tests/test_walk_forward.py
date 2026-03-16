from concurrent.futures import Future

from src.backtest import walk_forward as walk_forward_module
from src.backtest.backtest_engine import BacktestResult
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


def test_run_walk_forward_parallel_preserves_fold_order(monkeypatch):
    submitted_workers = []
    fold_lengths = []

    class _FakeExecutor:
        def __init__(self, max_workers):
            submitted_workers.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, **kwargs):
            future = Future()
            future.set_result(fn(**kwargs))
            return future

    def _fake_fold_runner(**kwargs):
        fold_idx = int(kwargs["fold_idx"])
        train_range = kwargs["train_range"]
        test_range = kwargs["test_range"]
        fold_lengths.append(len(kwargs["market_data"]["BTC/USDT"].close))
        fold = walk_forward_module.WalkForwardFold(
            train_range=train_range,
            test_range=test_range,
            result=BacktestResult(
                sharpe=float(fold_idx),
                cagr=0.0,
                max_dd=0.0,
                profit_factor=0.0,
                turnover=0.0,
            ),
        )
        return fold_idx, fold

    monkeypatch.setattr(walk_forward_module, "ProcessPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(walk_forward_module, "_run_single_walk_forward_fold", _fake_fold_runner)

    n = 400
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

    folds = run_walk_forward(md, folds=4, cfg=AppConfig(symbols=["BTC/USDT"]), parallel_workers=2)

    assert submitted_workers == [2]
    assert fold_lengths == [100, 100, 100, 100]
    assert [fold.test_range for fold in folds] == [(0, 100), (100, 200), (200, 300), (300, 400)]
    assert [fold.result.sharpe for fold in folds] == [0.0, 1.0, 2.0, 3.0]
