import pytest

from configs.schema import AppConfig
from src.core.pipeline import V5Pipeline
from src.quant_lab_client.return_contract import ForecastError, read_return_forecast


@pytest.mark.parametrize("strategy", ["Alpha6Factor", "TrendFollowing"])
@pytest.mark.parametrize("field", ["expected_gross_return_bps", "expected_net_return_bps", "expected_net_bps", "expected_edge_bps", "expected_net_edge_bps", "edge_bps"])
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf"), True, None, "bad"])
def test_all_signal_ingress_preserves_invalid_forecast_for_rejection(strategy, field, bad):
    pipeline = object.__new__(V5Pipeline)
    pipeline.cfg = AppConfig()
    contract = {field: bad, "roundtrip_cost_bps": 30, "horizon": "24h",
                "cost_basis": "roundtrip_all_in_quote_bps", "forecast_version": "regression-v1"}
    metadata = pipeline._expected_edge_metadata_for_buy(symbol="BNB/USDT", final_score=1,
        strategy_signal_lookup={strategy: {"BNB/USDT": {"metadata": contract}}})
    with pytest.raises(ForecastError):
        read_return_forecast(metadata)


def test_canonical_forecast_survives_production_metadata_path():
    pipeline = object.__new__(V5Pipeline)
    pipeline.cfg = AppConfig()
    metadata = pipeline._expected_edge_metadata_for_buy(symbol="BNB/USDT", final_score=1,
        strategy_signal_lookup={"Alpha6Factor": {"BNB/USDT": {"metadata": {
            "expected_gross_return_bps": 50, "roundtrip_cost_bps": 30, "horizon": "24h",
            "cost_basis": "roundtrip_all_in_quote_bps", "forecast_version": "regression-v1"}}}})
    assert read_return_forecast(metadata).net_at_cost(30) == 20


@pytest.mark.parametrize("alias", ["expected_edge_bps", "expected_net_edge_bps", "edge_bps"])
def test_legacy_signal_forecast_is_not_silently_replaced_by_score_proxy(alias):
    pipeline = object.__new__(V5Pipeline)
    pipeline.cfg = AppConfig()
    metadata = pipeline._expected_edge_metadata_for_buy(symbol="BNB/USDT", final_score=1,
        strategy_signal_lookup={"Alpha6Factor": {"BNB/USDT": {"metadata": {alias: 50}}}})
    assert metadata["expected_edge_source"] == "legacy_unbound"
    with pytest.raises(ForecastError, match="legacy_return_semantics_missing"):
        read_return_forecast(metadata)
