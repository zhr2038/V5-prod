import json
from pathlib import Path

from configs.schema import AlphaConfig, RiskConfig
from src.portfolio.portfolio_engine import PortfolioEngine
from src.core.models import MarketSeries


def test_portfolio_caps_single_weight():
    pe = PortfolioEngine(alpha_cfg=AlphaConfig(long_top_pct=0.5), risk_cfg=RiskConfig(max_single_weight=0.25))
    scores = {"A/USDT": 10.0, "B/USDT": 9.0, "C/USDT": 1.0, "D/USDT": 0.0}

    md = {}
    for s in scores.keys():
        md[s] = MarketSeries(symbol=s, timeframe="1h", ts=list(range(200)), open=[1.0]*200, high=[1.0]*200, low=[1.0]*200, close=[1.0 + i*0.0001 for i in range(200)], volume=[1000.0]*200)

    snap = pe.allocate(scores=scores, market_data=md, regime_mult=1.0)
    assert snap.target_weights
    assert all(w <= 0.25 + 1e-9 for w in snap.target_weights.values())


def test_topk_dropout_reorders_before_cap_and_persists_final_selection(tmp_path: Path):
    alpha_cfg = AlphaConfig(long_top_pct=0.8, optimizer_enabled=False)
    alpha_cfg.dynamic_ic_weighting.enabled = False
    alpha_cfg.topk_dropout.state_path = str(tmp_path / "topk_dropout_state.json")
    pe = PortfolioEngine(alpha_cfg=alpha_cfg, risk_cfg=RiskConfig(max_single_weight=0.25, max_positions_override=3))

    state_path = Path(alpha_cfg.topk_dropout.state_path)
    state_path.write_text(
        json.dumps(
            {
                "selected": ["ETH/USDT", "BNB/USDT", "HYPE/USDT", "OKB/USDT"],
                "hold_cycles": {
                    "ETH/USDT": 3,
                    "BNB/USDT": 3,
                    "HYPE/USDT": 3,
                    "OKB/USDT": 3,
                },
                "updated_ts": 0,
            }
        ),
        encoding="utf-8",
    )

    scores = {
        "OKB/USDT": 1.57,
        "HYPE/USDT": 1.31,
        "SUI/USDT": 1.00,
        "BNB/USDT": 0.86,
        "ETH/USDT": 0.46,
    }
    md = {}
    for sym in scores:
        md[sym] = MarketSeries(
            symbol=sym,
            timeframe="1h",
            ts=list(range(200)),
            open=[1.0] * 200,
            high=[1.0] * 200,
            low=[1.0] * 200,
            close=[1.0 + i * 0.0001 for i in range(200)],
            volume=[1000.0] * 200,
        )

    snap = pe.allocate(scores=scores, market_data=md, regime_mult=1.0)

    assert snap.entry_candidates == ["OKB/USDT", "HYPE/USDT", "SUI/USDT"]
    assert snap.selected == ["OKB/USDT", "HYPE/USDT", "SUI/USDT"]
    assert "ETH/USDT" not in snap.target_weights

    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["selected"] == ["OKB/USDT", "HYPE/USDT", "SUI/USDT"]
