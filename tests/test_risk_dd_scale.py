from configs.schema import AlphaConfig, RiskConfig
from src.portfolio.portfolio_engine import PortfolioEngine
from src.risk.risk_engine import RiskEngine


def test_dd_scale_targets():
    risk_cfg = RiskConfig(max_single_weight=0.25, drawdown_trigger=0.08, drawdown_delever=0.5)
    pe = PortfolioEngine(alpha_cfg=AlphaConfig(long_top_pct=0.5), risk_cfg=risk_cfg)
    re = RiskEngine(risk_cfg)

    targets = {"AAA/USDT": 0.25, "BBB/USDT": 0.25, "CCC/USDT": 0.25, "DDD/USDT": 0.25}
    mult = re.exposure_multiplier(0.09)
    scaled = pe.scale_targets(targets, mult)
    assert abs(sum(scaled.values()) - 0.5) < 1e-9
    assert all(w >= 0 for w in scaled.values())
    assert all(w <= 0.25 + 1e-12 for w in scaled.values())
