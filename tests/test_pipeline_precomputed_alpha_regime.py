from pathlib import Path

from configs.schema import AppConfig, ExecutionConfig, RegimeState
import src.core.pipeline as pipeline_module
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.account_store import AccountState
from src.execution.fill_store import (
    derive_position_store_path,
    derive_runtime_auto_risk_guard_path,
    derive_runtime_named_artifact_path,
    derive_runtime_named_json_path,
)
from src.regime.regime_engine import RegimeResult


def test_pipeline_uses_precomputed_alpha_and_regime():
    pipe = V5Pipeline(AppConfig(symbols=["BTC/USDT"]))

    def _boom(*args, **kwargs):
        raise AssertionError("precomputed path should bypass recomputation")

    pipe.regime_engine.detect = _boom
    pipe.alpha_engine.compute_snapshot = _boom

    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[0],
            open=[100.0],
            high=[101.0],
            low=[99.0],
            close=[100.0],
            volume=[1.0],
        )
    }
    precomputed_alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 0.5})
    precomputed_regime = RegimeResult(
        state=RegimeState.TRENDING,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.2,
    )

    out = pipe.run(
        market_data,
        positions=[],
        cash_usdt=1000.0,
        equity_peak_usdt=1000.0,
        precomputed_alpha=precomputed_alpha,
        precomputed_regime=precomputed_regime,
    )

    assert out.alpha is precomputed_alpha
    assert out.regime is precomputed_regime
    assert pipe.alpha_engine.current_regime_key == "Trending"


def test_pipeline_uses_runtime_auto_risk_guard_path(monkeypatch):
    captured = {}
    sentinel = object()

    def fake_get_auto_risk_guard(state_path=None):
        captured["state_path"] = state_path
        return sentinel

    monkeypatch.setattr(pipeline_module, "get_auto_risk_guard", fake_get_auto_risk_guard)

    cfg = AppConfig(
        symbols=["BTC/USDT"],
        execution=ExecutionConfig(order_store_path="reports/shadow_runtime/orders.sqlite"),
    )

    pipe = pipeline_module.V5Pipeline(cfg)

    assert pipe.auto_risk_guard is sentinel
    assert Path(captured["state_path"]) == derive_runtime_auto_risk_guard_path(
        (pipeline_module.REPORTS_DIR.parent / "reports/shadow_runtime/orders.sqlite").resolve()
    ).resolve()


def test_pipeline_uses_runtime_ml_training_db_path(monkeypatch):
    captured = {}

    class FakeCollector:
        def __init__(self, db_path="reports/ml_training_data.db", data_provider=None):
            captured["db_path"] = Path(db_path)
            captured["data_provider"] = data_provider

    import src.execution.ml_data_collector as ml_data_collector_module

    monkeypatch.setattr(ml_data_collector_module, "MLDataCollector", FakeCollector)

    cfg = AppConfig(
        symbols=["BTC/USDT"],
        execution=ExecutionConfig(order_store_path="reports/shadow_orders.sqlite"),
    )

    pipe = pipeline_module.V5Pipeline(cfg)

    expected_db = derive_runtime_named_artifact_path(
        (pipeline_module.REPORTS_DIR.parent / "reports/shadow_orders.sqlite").resolve(),
        "ml_training_data",
        ".db",
    ).resolve()
    assert captured["db_path"] == expected_db
    assert pipe.data_collector is not None


def test_pipeline_uses_runtime_negative_expectancy_state_path(monkeypatch):
    cfg = AppConfig(
        symbols=["BTC/USDT"],
        execution=ExecutionConfig(
            order_store_path="reports/shadow_orders.sqlite",
            negative_expectancy_cooldown_enabled=True,
        ),
    )

    pipe = pipeline_module.V5Pipeline(cfg)

    expected_path = derive_runtime_named_json_path(
        (pipeline_module.REPORTS_DIR.parent / "reports/shadow_orders.sqlite").resolve(),
        "negative_expectancy_cooldown",
    ).resolve()
    assert Path(pipe.negative_expectancy_cooldown.cfg.state_path) == expected_path
    assert Path(pipe.negative_expectancy_cooldown.cfg.orders_db_path) == (
        pipeline_module.REPORTS_DIR.parent / "reports/shadow_orders.sqlite"
    ).resolve()


def test_pipeline_preserves_custom_negative_expectancy_state_path(monkeypatch):
    cfg = AppConfig(
        symbols=["BTC/USDT"],
        execution=ExecutionConfig(
            order_store_path="reports/shadow_orders.sqlite",
            negative_expectancy_cooldown_enabled=True,
            negative_expectancy_state_path="reports/custom_negexp_state.json",
        ),
    )

    pipe = pipeline_module.V5Pipeline(cfg)

    assert Path(pipe.negative_expectancy_cooldown.cfg.state_path) == (
        pipeline_module.REPORTS_DIR.parent / "reports/custom_negexp_state.json"
    ).resolve()


def test_pipeline_uses_runtime_state_files_for_trade_managers():
    cfg = AppConfig(
        symbols=["BTC/USDT"],
        execution=ExecutionConfig(order_store_path="reports/shadow_orders.sqlite"),
    )

    pipe = pipeline_module.V5Pipeline(cfg)
    runtime_order_store = (pipeline_module.REPORTS_DIR.parent / "reports/shadow_orders.sqlite").resolve()

    assert pipe.position_builder.state_file.resolve() == derive_runtime_named_json_path(
        runtime_order_store,
        "position_builder_state",
    ).resolve()
    assert pipe.stop_loss_manager.state_file.resolve() == derive_runtime_named_json_path(
        runtime_order_store,
        "stop_loss_state",
    ).resolve()
    assert pipe.fixed_stop_loss.state_file.resolve() == derive_runtime_named_json_path(
        runtime_order_store,
        "fixed_stop_loss_state",
    ).resolve()
    assert pipe.profit_taking.state_file.resolve() == derive_runtime_named_json_path(
        runtime_order_store,
        "profit_taking_state",
    ).resolve()


def test_pipeline_uses_runtime_position_store_for_scale_basis(monkeypatch):
    captured = {}

    class FakeAccountStore:
        def __init__(self, path):
            captured["path"] = Path(path)

        def get(self):
            return AccountState(cash_usdt=100.0, equity_peak_usdt=1000.0, scale_basis_usdt=0.0)

        def update_scale_basis(self, new_basis, propagate_to_peak=False):
            captured["updated_basis"] = float(new_basis)
            captured["propagate_to_peak"] = bool(propagate_to_peak)

    import src.execution.account_store as account_store_module

    monkeypatch.setattr(account_store_module, "AccountStore", FakeAccountStore)

    cfg = AppConfig(
        symbols=["BTC/USDT"],
        execution=ExecutionConfig(order_store_path="reports/shadow_runtime/orders.sqlite"),
    )
    pipe = V5Pipeline(cfg)

    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[0],
            open=[100.0],
            high=[101.0],
            low=[99.0],
            close=[100.0],
            volume=[1.0],
        )
    }
    precomputed_alpha = AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 0.5})
    precomputed_regime = RegimeResult(
        state=RegimeState.TRENDING,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.2,
    )

    pipe.run(
        market_data,
        positions=[],
        cash_usdt=1000.0,
        equity_peak_usdt=1000.0,
        precomputed_alpha=precomputed_alpha,
        precomputed_regime=precomputed_regime,
    )

    expected_path = derive_position_store_path(
        (pipeline_module.REPORTS_DIR.parent / "reports/shadow_runtime/orders.sqlite").resolve()
    ).resolve()
    assert captured["path"] == expected_path
    assert captured["updated_basis"] == 1000.0
    assert captured["propagate_to_peak"] is False
