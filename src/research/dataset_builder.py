from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd

from .feature_registry import (
    build_feature_frame_from_market_data,
    build_inference_frame_from_market_data,
    resolve_feature_names,
)
from .processors import cross_sectional_rank


@dataclass
class DatasetBuildConfig:
    feature_groups: tuple[str, ...] = ("classic",)
    include_time_features: bool = False
    target_mode: str = "raw"
    min_symbol_samples: int = 100
    min_symbol_target_std: float = 1e-6
    min_cross_sectional_group_size: int = 2
    min_group_coverage_ratio: float = 0.9


class ResearchDatasetBuilder:
    def __init__(self, config: DatasetBuildConfig | None = None):
        self.config = config or DatasetBuildConfig()

    def build_feature_frame_from_market_data(self, market_data: Dict[str, Any]) -> pd.DataFrame:
        return build_feature_frame_from_market_data(
            market_data,
            feature_groups=self.config.feature_groups,
            include_time_features=self.config.include_time_features,
        )

    def build_inference_frame(
        self,
        market_data: Dict[str, Any],
        *,
        feature_names: Sequence[str],
    ) -> pd.DataFrame:
        return build_inference_frame_from_market_data(
            market_data,
            feature_names=feature_names,
            feature_groups=self.config.feature_groups,
            include_time_features=self.config.include_time_features,
        )

    @staticmethod
    def prepare_target(features: pd.DataFrame, horizon: int = 6) -> pd.DataFrame:
        work = features.copy()
        if "returns_1h" not in work.columns:
            raise ValueError("features must contain returns_1h")

        for symbol in work["symbol"].unique():
            mask = work["symbol"] == symbol
            vals = work.loc[mask, "returns_1h"].astype(float).to_numpy()
            out = np.full(len(vals), np.nan, dtype=float)
            for i in range(len(vals)):
                end = i + int(horizon)
                if end >= len(vals):
                    break
                window = vals[i + 1:end + 1]
                if len(window) != int(horizon) or not np.isfinite(window).all():
                    continue
                out[i] = float(np.prod(1.0 + window) - 1.0)
            work.loc[mask, "target"] = out
        return work

    @staticmethod
    def _target_col_for_horizon(target_col: str, hours: int) -> str | None:
        for suffix in ("6h", "12h", "24h"):
            if target_col.endswith(suffix):
                return f"{target_col[:-len(suffix)]}{hours}h"
        return target_col if hours == 6 else None

    def _available_target_horizons(
        self,
        work: pd.DataFrame,
        *,
        target_col: str,
    ) -> list[tuple[int, str]]:
        out: list[tuple[int, str]] = []
        for hours in (6, 12, 24):
            col = self._target_col_for_horizon(target_col, hours)
            if col and col in work.columns and work[col].notna().any():
                out.append((hours, col))
        return out

    @staticmethod
    def _scaled_forward_volatility(work: pd.DataFrame, hours: int) -> pd.Series:
        if "volatility_24h" not in work.columns:
            return pd.Series(np.ones(len(work), dtype=float), index=work.index)

        vol = work["volatility_24h"].astype(float).abs().replace([np.inf, -np.inf], np.nan)
        finite_vol = vol[np.isfinite(vol)]
        floor = float(finite_vol.quantile(0.25)) if not finite_vol.empty else 1e-6
        floor = max(floor, 1e-6)
        horizon_scale = float(np.sqrt(max(float(hours), 1.0) / 24.0))
        return vol.clip(lower=floor) * horizon_scale

    def _build_forward_edge_rank_target(
        self,
        work: pd.DataFrame,
        *,
        target_col: str,
    ) -> tuple[pd.Series, list[str]]:
        horizon_weights = {6: 0.5, 12: 0.3, 24: 0.2}
        used_horizons = self._available_target_horizons(work, target_col=target_col)
        if not used_horizons:
            raise ValueError(f"missing target column: {target_col}")

        components = []
        weights = []
        used_cols: list[str] = []
        for hours, col in used_horizons:
            raw = work[col].astype(float)
            raw_rank = raw.groupby(work["timestamp"]).transform(cross_sectional_rank)
            vol = self._scaled_forward_volatility(work, hours)
            edge = raw / vol
            edge_rank = edge.groupby(work["timestamp"]).transform(cross_sectional_rank)
            components.append(0.6 * raw_rank + 0.4 * edge_rank)
            weights.append(float(horizon_weights.get(hours, 0.0)))
            used_cols.append(col)

        if len(components) == 1:
            return components[0], used_cols

        weights_s = pd.Series(weights, dtype=float)
        weights_s = weights_s / weights_s.sum()
        comp_df = pd.concat(components, axis=1)
        comp_df.columns = used_cols
        complete = comp_df.notna().all(axis=1)
        score = pd.Series(np.nan, index=work.index, dtype=float)
        score.loc[complete] = comp_df.loc[complete].mul(weights_s.to_numpy(), axis=1).sum(axis=1)
        return score, used_cols

    def build_training_frame(
        self,
        df: pd.DataFrame,
        *,
        target_col: str,
        explicit_feature_names: Sequence[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.Series, dict[str, object]]:
        work = df.copy()
        meta: dict[str, object] = {
            "target_col": target_col,
            "target_mode": self.config.target_mode,
            "feature_groups": list(self.config.feature_groups),
        }

        if target_col not in work.columns:
            raise ValueError(f"missing target column: {target_col}")

        sort_cols = [c for c in ["timestamp", "symbol"] if c in work.columns]
        if sort_cols:
            work = work.sort_values(sort_cols).reset_index(drop=True)

        if "timestamp" in work.columns and self.config.include_time_features:
            ts = pd.to_datetime(work["timestamp"], unit="ms", errors="coerce")
            if ts.isna().all():
                ts = pd.to_datetime(work["timestamp"], errors="coerce")
            work["hour_of_day"] = ts.dt.hour.astype(float)
            work["day_of_week"] = ts.dt.dayofweek.astype(float)

        if "symbol" in work.columns:
            sym_stats = work.groupby("symbol")[target_col].agg(["size", "std"]).fillna(0.0)
            keep_mask = (
                (sym_stats["size"] >= int(self.config.min_symbol_samples))
                & (sym_stats["std"] >= float(self.config.min_symbol_target_std))
            )
            keep_symbols = [str(x) for x in sym_stats.index[keep_mask].tolist()]
            meta["kept_symbols"] = keep_symbols
            meta["dropped_symbols"] = [str(x) for x in sym_stats.index[~keep_mask].tolist()]
            if keep_symbols:
                work = work[work["symbol"].isin(keep_symbols)].copy()
        else:
            meta["kept_symbols"] = []
            meta["dropped_symbols"] = []

        work = work.replace([np.inf, -np.inf], np.nan)
        work = work.dropna(subset=[target_col]).copy()

        if self.config.target_mode == "cross_sectional_demean":
            if "timestamp" not in work.columns:
                raise ValueError("cross_sectional_demean requires timestamp")
            work[target_col] = work[target_col] - work.groupby("timestamp")[target_col].transform("mean")
            meta["horizon_target_cols"] = [target_col]
        elif self.config.target_mode == "cross_sectional_rank":
            if "timestamp" not in work.columns:
                raise ValueError("cross_sectional_rank requires timestamp")
            work[target_col] = work.groupby("timestamp")[target_col].transform(cross_sectional_rank)
            meta["horizon_target_cols"] = [target_col]
        elif self.config.target_mode == "forward_edge_rank":
            if "timestamp" not in work.columns:
                raise ValueError("forward_edge_rank requires timestamp")
            work[target_col], used_target_cols = self._build_forward_edge_rank_target(work, target_col=target_col)
            meta["horizon_target_cols"] = used_target_cols
        elif self.config.target_mode != "raw":
            raise ValueError(f"unknown target_mode: {self.config.target_mode}")
        else:
            meta["horizon_target_cols"] = [target_col]

        feature_cols = resolve_feature_names(
            self.config.feature_groups,
            include_time_features=self.config.include_time_features,
            explicit_feature_names=explicit_feature_names,
        )
        if not feature_cols:
            excluded = {target_col, "target", "timestamp", "symbol", "regime"}
            feature_cols = [
                c for c in work.columns
                if c not in excluded and pd.api.types.is_numeric_dtype(work[c])
            ]

        for col in feature_cols:
            if col not in work.columns:
                work[col] = 0.0

        X = work[feature_cols].replace([np.inf, -np.inf], np.nan)
        valid = X.notna().all(axis=1) & work[target_col].notna()
        X = X.loc[valid].reset_index(drop=True)
        y = work.loc[valid, target_col].reset_index(drop=True)
        timestamps = None
        if "timestamp" in work.columns:
            timestamps = work.loc[valid, "timestamp"].reset_index(drop=True)

        if timestamps is not None and self.config.target_mode != "raw" and len(X) > 0:
            group_sizes = timestamps.value_counts()
            max_group_size = int(group_sizes.max()) if not group_sizes.empty else 0
            required_group_size = max(
                int(self.config.min_cross_sectional_group_size),
                int(math.ceil(max_group_size * float(self.config.min_group_coverage_ratio))),
            )
            keep_groups = group_sizes[group_sizes >= required_group_size].index
            keep_mask = timestamps.isin(keep_groups)
            X = X.loc[keep_mask].reset_index(drop=True)
            y = y.loc[keep_mask].reset_index(drop=True)
            timestamps = timestamps.loc[keep_mask].reset_index(drop=True)
            meta["group_filter"] = {
                "enabled": True,
                "max_group_size": max_group_size,
                "required_group_size": required_group_size,
                "groups_before": int(group_sizes.size),
                "groups_after": int(pd.Series(timestamps).nunique()) if len(timestamps) else 0,
            }
        else:
            meta["group_filter"] = {"enabled": False}

        meta["feature_cols"] = feature_cols
        meta["rows_after_clean"] = int(len(X))
        if timestamps is not None:
            meta["timestamps"] = timestamps.tolist()
        return X, y, meta
