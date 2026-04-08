from configs.schema import AppConfig, ExecutionConfig, RegimeState
import src.core.pipeline as pipeline_module
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.fill_store import derive_runtime_auto_risk_guard_path
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
    assert captured["state_path"] == derive_runtime_auto_risk_guard_path(
        "reports/shadow_runtime/orders.sqlite"
    )
