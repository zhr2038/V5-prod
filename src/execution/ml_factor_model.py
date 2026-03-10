"""
Phase 3: Machine Learning Factor Model.

This module keeps the public API stable while fixing a few structural issues:
- target generation now matches the collector's 6h forward-return semantics
- training can build a cleaner cross-sectional target from exported snapshots
- ridge models persist and reload correctly with their scaler
"""

from __future__ import annotations

import json
import math
import pickle
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

try:
    import lightgbm as lgb

    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    lgb = None


def _safe_corr(a, b) -> float:
    try:
        a_s = pd.Series(a)
        b_s = pd.Series(b)
        if float(a_s.std(ddof=0)) <= 0.0 or float(b_s.std(ddof=0)) <= 0.0:
            return 0.0
        v = float(np.corrcoef(a_s, b_s)[0, 1])
        return 0.0 if not np.isfinite(v) else v
    except Exception:
        return 0.0


@dataclass
class MLFactorConfig:
    model_type: str = "ridge"  # ridge | hist_gbm | lightgbm

    alpha: float = 10.0

    n_estimators: int = 50
    max_depth: int = 4
    learning_rate: float = 0.05
    hgb_max_iter: int = 120
    hgb_min_samples_leaf: int = 120
    subsample: float = 0.6
    colsample_bytree: float = 0.6
    num_leaves: int = 7
    min_data_in_leaf: int = 50
    min_child_samples: int = 30
    reg_alpha: float = 2.0
    reg_lambda: float = 5.0
    random_state: int = 42

    train_lookback_days: int = 60
    prediction_horizon: int = 6
    min_train_samples: int = 200
    early_stopping_rounds: int = 10

    # Better aligned with alpha ranking than raw absolute future return.
    target_mode: str = "cross_sectional_rank"  # raw | cross_sectional_demean | cross_sectional_rank | forward_edge_rank
    include_time_features: bool = True
    min_symbol_samples: int = 100
    min_symbol_target_std: float = 1e-6
    min_cross_sectional_group_size: int = 2
    min_group_coverage_ratio: float = 0.9


class MLFactorModel:
    def __init__(self, config: MLFactorConfig | None = None):
        self.config = config or MLFactorConfig()
        self.model = None
        self.scaler = None
        self.feature_names: List[str] = []
        self.is_trained = False

        if self.config.model_type == "lightgbm" and not LIGHTGBM_AVAILABLE:
            raise ImportError("lightgbm is required. Install with: pip install lightgbm")

    def _value_from_data(self, data, key: str):
        if isinstance(data, dict):
            return data.get(key)
        return getattr(data, key, None)

    def feature_engineering(self, market_data: Dict) -> pd.DataFrame:
        features = pd.DataFrame()

        for symbol, data in (market_data or {}).items():
            close_raw = self._value_from_data(data, "close")
            if close_raw is None or len(close_raw) < 30:
                continue

            close = pd.Series(close_raw, dtype=float)
            volume = pd.Series(self._value_from_data(data, "volume") or [0] * len(close), dtype=float)
            high = pd.Series(self._value_from_data(data, "high") or close, dtype=float)
            low = pd.Series(self._value_from_data(data, "low") or close, dtype=float)
            ts_raw = self._value_from_data(data, "ts")

            returns_1h = close.pct_change(1)
            volume_sma = volume.rolling(24).mean()
            bb_middle = close.rolling(20).mean()
            bb_std = close.rolling(20).std()
            high_20d = high.rolling(20 * 24).max()
            low_20d = low.rolling(20 * 24).min()

            delta = close.diff()
            gain = delta.where(delta > 0, 0.0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
            rs = (gain / loss).replace([np.inf, -np.inf], np.nan)
            rsi = (100 - (100 / (1 + rs))).fillna(50.0)

            exp1 = close.ewm(span=12).mean()
            exp2 = close.ewm(span=26).mean()
            macd_line = exp1 - exp2
            macd_signal_line = macd_line.ewm(span=9).mean()

            symbol_features = pd.DataFrame(
                {
                    "symbol": [symbol] * len(close),
                    "timestamp": list(ts_raw) if ts_raw is not None and len(ts_raw) == len(close) else [np.nan] * len(close),
                    "returns_1h": returns_1h,
                    "returns_6h": close.pct_change(6),
                    "returns_24h": close.pct_change(24),
                    "momentum_5d": (close - close.shift(5 * 24)) / close.shift(5 * 24),
                    "momentum_20d": (close - close.shift(20 * 24)) / close.shift(20 * 24),
                    "volatility_6h": returns_1h.rolling(6).std(),
                    "volatility_24h": returns_1h.rolling(24).std(),
                    "volatility_ratio": (
                        returns_1h.rolling(6).std() / returns_1h.rolling(24).std()
                    ).replace([np.inf, -np.inf], np.nan),
                    "volume_ratio": (volume / volume_sma).replace([np.inf, -np.inf], np.nan),
                    "obv": (np.sign(returns_1h.fillna(0.0)) * volume).cumsum(),
                    "rsi": rsi,
                    "macd": macd_line,
                    "macd_signal": macd_signal_line,
                    "bb_position": ((close - bb_middle) / (2 * bb_std)).replace([np.inf, -np.inf], np.nan),
                    "price_position": ((close - low_20d) / (high_20d - low_20d)).replace([np.inf, -np.inf], np.nan),
                }
            )

            features = pd.concat([features, symbol_features], ignore_index=True)

        self.feature_names = [c for c in features.columns if c not in {"symbol", "timestamp", "target"}]
        return features

    def prepare_target(self, features: pd.DataFrame, horizon: int = 6) -> pd.DataFrame:
        features = features.copy()
        if "returns_1h" not in features.columns:
            raise ValueError("features must contain returns_1h")

        for symbol in features["symbol"].unique():
            mask = features["symbol"] == symbol
            vals = features.loc[mask, "returns_1h"].astype(float).to_numpy()
            out = np.full(len(vals), np.nan, dtype=float)
            for i in range(len(vals)):
                end = i + int(horizon)
                if end >= len(vals):
                    break
                window = vals[i + 1:end + 1]
                if len(window) != int(horizon) or not np.isfinite(window).all():
                    continue
                out[i] = float(np.prod(1.0 + window) - 1.0)
            features.loc[mask, "target"] = out

        return features

    def _rank_target_within_timestamp(self, s: pd.Series) -> pd.Series:
        if len(s) <= 1:
            return pd.Series(np.zeros(len(s), dtype=float), index=s.index)
        return s.rank(pct=True) - 0.5

    def _target_col_for_horizon(self, target_col: str, hours: int) -> str | None:
        for suffix in ("6h", "12h", "24h"):
            if target_col.endswith(suffix):
                return f"{target_col[:-len(suffix)]}{hours}h"
        return target_col if hours == 6 else None

    def _available_target_horizons(
        self,
        work: pd.DataFrame,
        *,
        target_col: str,
    ) -> List[Tuple[int, str]]:
        out: List[Tuple[int, str]] = []
        for hours in (6, 12, 24):
            col = self._target_col_for_horizon(target_col, hours)
            if col and col in work.columns:
                series = work[col]
                if series.notna().any():
                    out.append((hours, col))
        return out

    def _scaled_forward_volatility(self, work: pd.DataFrame, hours: int) -> pd.Series:
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
    ) -> Tuple[pd.Series, List[str]]:
        horizon_weights = {6: 0.5, 12: 0.3, 24: 0.2}
        used_horizons = self._available_target_horizons(work, target_col=target_col)
        if not used_horizons:
            raise ValueError(f"missing target column: {target_col}")

        components = []
        weights = []
        used_cols = []
        for hours, col in used_horizons:
            raw = work[col].astype(float)
            raw_rank = raw.groupby(work["timestamp"]).transform(self._rank_target_within_timestamp)
            vol = self._scaled_forward_volatility(work, hours)
            edge = raw / vol
            edge_rank = edge.groupby(work["timestamp"]).transform(self._rank_target_within_timestamp)
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

    def _build_training_frame(
        self,
        df: pd.DataFrame,
        *,
        target_col: str,
    ) -> Tuple[pd.DataFrame, pd.Series, Dict[str, object]]:
        work = df.copy()
        meta: Dict[str, object] = {
            "target_col": target_col,
            "target_mode": self.config.target_mode,
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
            work[target_col] = work.groupby("timestamp")[target_col].transform(self._rank_target_within_timestamp)
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

        preferred = [
            "returns_24h",
            "momentum_5d",
            "momentum_20d",
            "volatility_24h",
            "volatility_ratio",
            "volume_ratio",
            "obv",
            "rsi",
            "macd",
            "macd_signal",
            "bb_position",
            "price_position",
            "hour_of_day",
            "day_of_week",
        ]
        feature_cols = [c for c in preferred if c in work.columns]
        if not feature_cols:
            excluded = {target_col, "target", "timestamp", "symbol", "regime"}
            feature_cols = [
                c for c in work.columns
                if c not in excluded and pd.api.types.is_numeric_dtype(work[c])
            ]

        X = work[feature_cols].replace([np.inf, -np.inf], np.nan)
        valid = X.notna().all(axis=1) & work[target_col].notna()
        X = X.loc[valid].reset_index(drop=True)
        y = work.loc[valid, target_col].reset_index(drop=True)
        timestamps = None
        if "timestamp" in work.columns:
            timestamps = work.loc[valid, "timestamp"].reset_index(drop=True)

        if (
            timestamps is not None
            and self.config.target_mode != "raw"
            and len(X) > 0
        ):
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
            meta["group_filter"] = {
                "enabled": False,
            }

        meta["feature_cols"] = feature_cols
        meta["rows_after_clean"] = int(len(X))
        if timestamps is not None:
            meta["timestamps"] = timestamps.tolist()
        return X, y, meta

    def build_training_frame(
        self,
        df: pd.DataFrame,
        *,
        target_col: str,
    ) -> Tuple[pd.DataFrame, pd.Series, Dict[str, object]]:
        return self._build_training_frame(df, target_col=target_col)

    def train(
        self,
        X_train=None,
        y_train=None,
        X_valid=None,
        y_valid=None,
        sample_weight=None,
        market_data: Dict = None,
        force_retrain: bool = False,
    ):
        if self.is_trained and not force_retrain:
            print("Model already trained. Use force_retrain=True to retrain.")
            return

        if X_train is not None and y_train is not None:
            print(
                f"Training with provided data: {len(X_train)} train, "
                f"{len(X_valid) if X_valid is not None else 0} valid"
            )
            self._train_with_data(X_train, y_train, X_valid, y_valid, sample_weight=sample_weight)
            return

        if market_data is None:
            raise ValueError("Must provide either (X_train, y_train) or market_data")

        print("Building features from market_data...")
        features = self.feature_engineering(market_data)
        features = self.prepare_target(features, self.config.prediction_horizon)
        X, y, meta = self._build_training_frame(features, target_col="target")

        if len(X) < self.config.min_train_samples:
            raise ValueError(f"Insufficient samples: {len(X)} < {self.config.min_train_samples}")

        split_idx = int(len(X) * 0.8)
        X_train, X_valid = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_valid = y.iloc[:split_idx], y.iloc[split_idx:]

        print(f"Training samples: {len(X_train)}, Validation samples: {len(X_valid)}")
        print(f"Features used: {meta.get('feature_cols')}")
        self._train_with_data(X_train, y_train, X_valid, y_valid, sample_weight=sample_weight)

    def _train_with_data(self, X_train, y_train, X_valid, y_valid, *, sample_weight=None):
        if X_valid is None or y_valid is None:
            raise ValueError("X_valid and y_valid are required")

        if not self.feature_names:
            self.feature_names = [c for c in X_train.columns if c not in {"symbol", "target"}]

        from sklearn.preprocessing import StandardScaler

        if self.config.model_type == "ridge":
            from sklearn.linear_model import Ridge

            self.scaler = StandardScaler()
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_valid_scaled = self.scaler.transform(X_valid)

            self.model = Ridge(alpha=self.config.alpha)
            fit_kwargs = {}
            if sample_weight is not None:
                fit_kwargs["sample_weight"] = np.asarray(sample_weight, dtype=float)
            self.model.fit(X_train_scaled, y_train, **fit_kwargs)

            train_pred = self.model.predict(X_train_scaled)
            valid_pred = self.model.predict(X_valid_scaled)
            train_ic = _safe_corr(y_train, train_pred)
            valid_ic = _safe_corr(y_valid, valid_pred)

            print("\nRidge Model Performance:")
            print(f"  Train IC: {train_ic:.4f}")
            print(f"  Valid IC: {valid_ic:.4f}")

            coef_df = pd.DataFrame(
                {"feature": self.feature_names, "coef": self.model.coef_}
            ).sort_values("coef", key=abs, ascending=False)
            print("\nTop Coefficients:")
            for _, row in coef_df.head(5).iterrows():
                print(f"  {row['feature']}: {row['coef']:.6f}")

        elif self.config.model_type == "hist_gbm":
            from sklearn.ensemble import HistGradientBoostingRegressor

            self.scaler = None
            self.model = HistGradientBoostingRegressor(
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                max_iter=self.config.hgb_max_iter,
                min_samples_leaf=self.config.hgb_min_samples_leaf,
                random_state=self.config.random_state,
            )
            fit_kwargs = {}
            if sample_weight is not None:
                fit_kwargs["sample_weight"] = np.asarray(sample_weight, dtype=float)
            self.model.fit(X_train, y_train, **fit_kwargs)

            train_pred = self.model.predict(X_train)
            valid_pred = self.model.predict(X_valid)
            train_ic = _safe_corr(y_train, train_pred)
            valid_ic = _safe_corr(y_valid, valid_pred)

            print("\nHistGradientBoosting Model Performance:")
            print(f"  Train IC: {train_ic:.4f}")
            print(f"  Valid IC: {valid_ic:.4f}")

        elif self.config.model_type == "lightgbm":
            self.model = lgb.LGBMRegressor(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                subsample=self.config.subsample,
                colsample_bytree=self.config.colsample_bytree,
                num_leaves=self.config.num_leaves,
                min_data_in_leaf=self.config.min_data_in_leaf,
                min_child_samples=self.config.min_child_samples,
                reg_alpha=self.config.reg_alpha,
                reg_lambda=self.config.reg_lambda,
                random_state=self.config.random_state,
                verbose=-1,
            )
            self.model.fit(
                X_train,
                y_train,
                sample_weight=np.asarray(sample_weight, dtype=float) if sample_weight is not None else None,
                eval_set=[(X_valid, y_valid)],
                callbacks=[
                    lgb.early_stopping(stopping_rounds=self.config.early_stopping_rounds),
                    lgb.log_evaluation(period=0),
                ],
            )
            train_pred = self.model.predict(X_train)
            valid_pred = self.model.predict(X_valid)
            train_ic = _safe_corr(y_train, train_pred)
            valid_ic = _safe_corr(y_valid, valid_pred)

            print("\nLightGBM Model Performance:")
            print(f"  Train IC: {train_ic:.4f}")
            print(f"  Valid IC: {valid_ic:.4f}")
            self.print_feature_importance()
        else:
            raise ValueError(f"Unknown model_type: {self.config.model_type}")

        self.is_trained = True

    def predict(self, symbol_features: Dict[str, float]) -> float:
        if not self.is_trained:
            raise RuntimeError("Model not trained. Call train() first.")

        X = pd.DataFrame([symbol_features])
        X = X[self.feature_names]
        if self.config.model_type == "ridge":
            X = self.scaler.transform(X)
        return float(self.model.predict(X)[0])

    def predict_batch(self, features_df: pd.DataFrame) -> pd.Series:
        if not self.is_trained:
            raise RuntimeError("Model not trained. Call train() first.")

        X = features_df[self.feature_names]
        if self.config.model_type == "ridge":
            X = self.scaler.transform(X)
        return pd.Series(self.model.predict(X), index=features_df.index)

    def print_feature_importance(self, top_n: int = 10):
        if self.model is None:
            return
        if hasattr(self.model, "feature_importances_"):
            importance = self.model.feature_importances_
        elif hasattr(self.model, "feature_importance"):
            importance = self.model.feature_importance()
        else:
            return
        imp_df = pd.DataFrame({"feature": self.feature_names, "importance": importance}).sort_values(
            "importance", ascending=False
        )
        print(f"\nTop {top_n} Important Features:")
        for _, row in imp_df.head(top_n).iterrows():
            print(f"  {row['feature']}: {row['importance']:.4f}")

    def save_model(self, path: str):
        if not self.is_trained:
            raise RuntimeError("Model not trained.")

        model_data = {
            "config": self.config.__dict__,
            "feature_names": self.feature_names,
            "model_type": self.config.model_type,
        }

        if self.config.model_type == "lightgbm":
            self.model.booster_.save_model(f"{path}.txt")
        elif self.config.model_type in {"ridge", "hist_gbm"}:
            artifact = {
                "model": self.model,
                "scaler": self.scaler,
            }
            with open(f"{path}.pkl", "wb") as f:
                pickle.dump(artifact, f)

        with open(f"{path}_config.json", "w", encoding="utf-8") as f:
            json.dump(model_data, f, indent=2)

        print(f"Model saved to {path}")

    def load_model(self, path: str):
        with open(f"{path}_config.json", "r", encoding="utf-8") as f:
            model_data = json.load(f)

        self.config = MLFactorConfig(**model_data["config"])
        self.feature_names = list(model_data["feature_names"])

        if self.config.model_type == "lightgbm":
            if not LIGHTGBM_AVAILABLE:
                raise ImportError("lightgbm is required to load this model")
            self.model = lgb.Booster(model_file=f"{path}.txt")
            self.scaler = None
        elif self.config.model_type in {"ridge", "hist_gbm"}:
            with open(f"{path}.pkl", "rb") as f:
                artifact = pickle.load(f)
            self.model = artifact.get("model")
            self.scaler = artifact.get("scaler")
            if self.model is None:
                raise RuntimeError("Invalid model artifact")
        else:
            raise ValueError(f"Unknown model_type: {self.config.model_type}")

        self.is_trained = True
        print(f"Model loaded from {path}")
