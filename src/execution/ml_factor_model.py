"""
Phase 3: Machine Learning Factor Model.

This module keeps the public API stable while fixing a few structural issues:
- target generation now matches the collector's 6h forward-return semantics
- training can build a cleaner cross-sectional target from exported snapshots
- ridge models persist and reload correctly with their scaler
"""

from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd
from src.research.dataset_builder import DatasetBuildConfig, ResearchDatasetBuilder

try:
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler

    SKLEARN_AVAILABLE = True
except Exception:
    HistGradientBoostingRegressor = None
    Ridge = None
    StandardScaler = None
    SKLEARN_AVAILABLE = False

try:
    import lightgbm as lgb

    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    lgb = None

try:
    import xgboost as xgb

    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
    xgb = None


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


def _to_2d_float_array(values) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    if arr.ndim == 1:
        arr = arr.reshape(-1, 1)
    return arr


class _FallbackStandardScaler:
    def __init__(self):
        self.mean_ = None
        self.scale_ = None

    def fit(self, X):
        arr = _to_2d_float_array(X)
        self.mean_ = arr.mean(axis=0)
        scale = arr.std(axis=0, ddof=0)
        scale[scale <= 1e-12] = 1.0
        self.scale_ = scale
        return self

    def transform(self, X):
        arr = _to_2d_float_array(X)
        if self.mean_ is None or self.scale_ is None:
            raise RuntimeError("StandardScaler is not fitted")
        return (arr - self.mean_) / self.scale_

    def fit_transform(self, X):
        return self.fit(X).transform(X)


class _FallbackRidge:
    def __init__(self, alpha: float = 1.0):
        self.alpha = float(alpha)
        self.coef_ = None
        self.intercept_ = 0.0

    def fit(self, X, y, sample_weight=None):
        X_arr = _to_2d_float_array(X)
        y_arr = np.asarray(y, dtype=float).reshape(-1)
        if sample_weight is not None:
            w = np.sqrt(np.asarray(sample_weight, dtype=float).reshape(-1, 1))
            X_arr = X_arr * w
            y_arr = y_arr * w.reshape(-1)

        design = np.column_stack([np.ones(len(X_arr), dtype=float), X_arr])
        reg = np.eye(design.shape[1], dtype=float) * self.alpha
        reg[0, 0] = 0.0
        beta = np.linalg.pinv(design.T @ design + reg) @ design.T @ y_arr
        self.intercept_ = float(beta[0])
        self.coef_ = beta[1:]
        return self

    def predict(self, X):
        X_arr = _to_2d_float_array(X)
        if self.coef_ is None:
            raise RuntimeError("Ridge model is not fitted")
        return X_arr @ self.coef_ + self.intercept_


class _FallbackHistGradientBoostingRegressor:
    def __init__(
        self,
        *,
        max_depth: int | None = None,
        learning_rate: float = 0.05,
        max_iter: int = 120,
        min_samples_leaf: int = 20,
        random_state: int | None = None,
    ):
        del max_depth, learning_rate, max_iter, min_samples_leaf, random_state
        self._ridge = _FallbackRidge(alpha=1.0)
        self.feature_importances_ = None

    def fit(self, X, y, sample_weight=None):
        self._ridge.fit(X, y, sample_weight=sample_weight)
        coef = np.abs(np.asarray(self._ridge.coef_, dtype=float))
        total = float(coef.sum())
        if total <= 0.0:
            coef = np.zeros_like(coef)
        else:
            coef = coef / total
        self.feature_importances_ = coef
        return self

    def predict(self, X):
        return self._ridge.predict(X)


@dataclass
class MLFactorConfig:
    model_type: str = "ridge"  # ridge | hist_gbm | lightgbm | xgboost
    feature_groups: tuple[str, ...] = ("classic",)

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
    compute_device: str = "auto"  # auto | cpu | cuda
    max_bin: int = 256
    n_jobs: int = -1
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
        self.training_device = "cpu"

        if self.config.model_type == "lightgbm" and not LIGHTGBM_AVAILABLE:
            raise ImportError("lightgbm is required. Install with: pip install lightgbm")
        if self.config.model_type == "xgboost" and not XGBOOST_AVAILABLE:
            raise ImportError("xgboost is required. Install with: pip install xgboost")

    def _value_from_data(self, data, key: str):
        if isinstance(data, dict):
            return data.get(key)
        return getattr(data, key, None)

    def _dataset_builder(self) -> ResearchDatasetBuilder:
        return ResearchDatasetBuilder(
            DatasetBuildConfig(
                feature_groups=tuple(getattr(self.config, "feature_groups", ("classic",)) or ("classic",)),
                include_time_features=bool(self.config.include_time_features),
                target_mode=str(self.config.target_mode),
                min_symbol_samples=int(self.config.min_symbol_samples),
                min_symbol_target_std=float(self.config.min_symbol_target_std),
                min_cross_sectional_group_size=int(self.config.min_cross_sectional_group_size),
                min_group_coverage_ratio=float(self.config.min_group_coverage_ratio),
            )
        )

    def feature_engineering(self, market_data: Dict) -> pd.DataFrame:
        features = self._dataset_builder().build_feature_frame_from_market_data(market_data)
        self.feature_names = [c for c in features.columns if c not in {"symbol", "timestamp", "target"}]
        return features

    def prepare_target(self, features: pd.DataFrame, horizon: int = 6) -> pd.DataFrame:
        return self._dataset_builder().prepare_target(features, horizon=horizon)

    def build_training_frame(
        self,
        df: pd.DataFrame,
        *,
        target_col: str,
    ):
        return self._dataset_builder().build_training_frame(df, target_col=target_col)

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
        X, y, meta = self.build_training_frame(features, target_col="target")

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

        if self.config.model_type == "ridge":
            scaler_cls = StandardScaler if SKLEARN_AVAILABLE and StandardScaler is not None else _FallbackStandardScaler
            ridge_cls = Ridge if SKLEARN_AVAILABLE and Ridge is not None else _FallbackRidge

            self.scaler = scaler_cls()
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_valid_scaled = self.scaler.transform(X_valid)

            self.model = ridge_cls(alpha=self.config.alpha)
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
            self.training_device = "cpu"

            coef_df = pd.DataFrame(
                {"feature": self.feature_names, "coef": self.model.coef_}
            ).sort_values("coef", key=abs, ascending=False)
            print("\nTop Coefficients:")
            for _, row in coef_df.head(5).iterrows():
                print(f"  {row['feature']}: {row['coef']:.6f}")

        elif self.config.model_type == "hist_gbm":
            self.scaler = None
            hgb_cls = (
                HistGradientBoostingRegressor
                if SKLEARN_AVAILABLE and HistGradientBoostingRegressor is not None
                else _FallbackHistGradientBoostingRegressor
            )
            self.model = hgb_cls(
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
            self.training_device = "cpu"

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
                n_jobs=self.config.n_jobs,
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
            self.training_device = "cpu"
            self.print_feature_importance()
        elif self.config.model_type == "xgboost":
            self.scaler = None
            preferred_device = self._preferred_xgboost_device()
            self.model = self._fit_xgboost_model(
                X_train,
                y_train,
                X_valid,
                y_valid,
                sample_weight=sample_weight,
                device=preferred_device,
            )

            train_pred = self.model.predict(X_train)
            valid_pred = self.model.predict(X_valid)
            train_ic = _safe_corr(y_train, train_pred)
            valid_ic = _safe_corr(y_valid, valid_pred)

            print("\nXGBoost Model Performance:")
            print(f"  Device: {self.training_device}")
            print(f"  Train IC: {train_ic:.4f}")
            print(f"  Valid IC: {valid_ic:.4f}")
            self.print_feature_importance()
        else:
            raise ValueError(f"Unknown model_type: {self.config.model_type}")

        self.is_trained = True

    def _preferred_xgboost_device(self) -> str:
        desired = str(self.config.compute_device or "auto").strip().lower()
        if desired not in {"auto", "cpu", "cuda"}:
            raise ValueError("compute_device must be one of: auto, cpu, cuda")
        if desired == "auto":
            return "cuda"
        return desired

    def _build_xgboost_model(self, *, device: str):
        return xgb.XGBRegressor(
            n_estimators=self.config.n_estimators,
            max_depth=self.config.max_depth,
            learning_rate=self.config.learning_rate,
            subsample=self.config.subsample,
            colsample_bytree=self.config.colsample_bytree,
            reg_alpha=self.config.reg_alpha,
            reg_lambda=self.config.reg_lambda,
            max_bin=self.config.max_bin,
            n_jobs=self.config.n_jobs,
            random_state=self.config.random_state,
            objective="reg:squarederror",
            tree_method="hist",
            device=device,
            early_stopping_rounds=self.config.early_stopping_rounds,
            verbosity=0,
        )

    def _fit_xgboost_model(
        self,
        X_train,
        y_train,
        X_valid,
        y_valid,
        *,
        sample_weight=None,
        device: str,
    ):
        fit_kwargs = {
            "eval_set": [(X_valid, y_valid)],
            "verbose": False,
        }
        if sample_weight is not None:
            fit_kwargs["sample_weight"] = np.asarray(sample_weight, dtype=float)

        model = self._build_xgboost_model(device=device)
        try:
            model.fit(X_train, y_train, **fit_kwargs)
            self.training_device = str(device)
            return model
        except Exception:
            if device != "cuda" or str(self.config.compute_device or "auto").strip().lower() != "auto":
                raise
            model = self._build_xgboost_model(device="cpu")
            model.fit(X_train, y_train, **fit_kwargs)
            self.training_device = "cpu"
            return model

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
            "training_device": self.training_device,
        }

        if self.config.model_type == "lightgbm":
            self.model.booster_.save_model(f"{path}.txt")
        elif self.config.model_type == "xgboost":
            self.model.save_model(f"{path}.json")
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
            self.training_device = "cpu"
        elif self.config.model_type == "xgboost":
            if not XGBOOST_AVAILABLE:
                raise ImportError("xgboost is required to load this model")
            self.model = xgb.XGBRegressor()
            self.model.load_model(f"{path}.json")
            self.scaler = None
            self.training_device = str(model_data.get("training_device") or self.config.compute_device or "cpu")
        elif self.config.model_type in {"ridge", "hist_gbm"}:
            with open(f"{path}.pkl", "rb") as f:
                artifact = pickle.load(f)
            self.model = artifact.get("model")
            self.scaler = artifact.get("scaler")
            if self.model is None:
                raise RuntimeError("Invalid model artifact")
            self.training_device = "cpu"
        else:
            raise ValueError(f"Unknown model_type: {self.config.model_type}")

        self.is_trained = True
        print(f"Model loaded from {path}")
