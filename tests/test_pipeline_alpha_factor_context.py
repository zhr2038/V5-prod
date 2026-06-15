from __future__ import annotations

import pytest

from src.alpha.alpha_engine import AlphaSnapshot
from src.core.pipeline import V5Pipeline


def test_alpha_factor_context_from_snapshot_exposes_canonical_candidate_fields() -> None:
    alpha = AlphaSnapshot(
        raw_factors={
            "BNB/USDT": {
                "f1_mom_5d": 0.01,
                "f2_mom_20d": 0.02,
                "f3_vol_adj_ret_20d": 0.03,
                "f4_volume_expansion": 0.04,
                "f5_rsi_trend_confirm": 0.05,
                "alpha6_relative_score": 0.41,
                "ml_pred_raw": 0.12,
            }
        },
        z_factors={
            "BNB/USDT": {
                "f1_mom_5d": 1.1,
                "f2_mom_20d": 1.2,
                "f3_vol_adj_ret_20d": 1.3,
                "f4_volume_expansion": 1.4,
                "f5_rsi_trend_confirm": 1.5,
                "alpha6_display_score": 0.67,
            }
        },
        scores={"BNB/USDT": 0.91},
        raw_scores={"BNB/USDT": 0.88},
        base_scores={"BNB/USDT": 0.74},
        ml_attribution_scores={"BNB/USDT": 0.82},
        ml_overlay_scores={"BNB/USDT": 0.08},
        ml_overlay_raw_scores={"BNB/USDT": 0.18},
    )

    context = V5Pipeline._alpha_factor_context_from_snapshot(
        alpha,
        base_rank_map={"BNB/USDT": 2},
        final_rank_map={"BNB/USDT": 1},
    )
    row = context["BNB/USDT"]

    assert row["final_score"] == pytest.approx(0.91)
    assert row["base_score"] == pytest.approx(0.74)
    assert row["rank"] == 1
    assert row["base_rank"] == 2
    assert row["rank_delta"] == 1
    assert row["f1_mom_5d"] == pytest.approx(1.1)
    assert row["f3_vol_adj_ret"] == pytest.approx(1.3)
    assert row["alpha6_score"] == pytest.approx(0.67)
    assert row["ml_score"] == pytest.approx(0.82)
    assert row["ml_pred_raw"] == pytest.approx(0.12)
    assert row["raw_factors"]["f3_vol_adj_ret_20d"] == pytest.approx(0.03)
    assert row["z_factors"]["f3_vol_adj_ret_20d"] == pytest.approx(1.3)
