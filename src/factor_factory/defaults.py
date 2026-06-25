from __future__ import annotations

from src.factor_factory.models import FactorSpec, FactorStatus


ALPHA6_PRIMITIVE_LOOKBACKS: dict[str, int] = {
    "f1_mom_5d": 120,
    "f2_mom_20d": 480,
    "f3_vol_adj_ret": 481,
    "f4_volume_expansion": 192,
    "f5_rsi_trend_confirm": 14,
    "f6_sentiment": 1,
    "f6_corr_pv_10": 10,
    "f7_cord_10": 10,
    "f8_rsqr_10": 10,
    "f9_rank_20": 20,
    "f10_imax_14": 14,
    "f11_imin_14": 14,
    "f12_imxd_14": 14,
}


def build_alpha6_factor_specs(
    *,
    timeframe: str = "1h",
    status: FactorStatus = FactorStatus.SHADOW,
    version: str = "legacy-alpha6-v1",
) -> list[FactorSpec]:
    specs: list[FactorSpec] = []
    for name, lookback in ALPHA6_PRIMITIVE_LOOKBACKS.items():
        specs.append(
            FactorSpec(
                factor_id=name,
                name=name,
                version=version,
                expression={"feature": name},
                inputs=[name],
                timeframe=timeframe,
                lookback_bars=int(lookback),
                status=status,
                tags=["alpha6", "legacy-compatible", "primitive-proxy"],
            )
        )
    return specs
