from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from pathlib import Path
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
import os
import json
import numpy as np
import pandas as pd

from src.core.models import MarketSeries
from src.utils.math import safe_pct_change, zscore_cross_section
from configs.schema import AlphaConfig
from src.reporting.alpha_evaluation import robust_zscore_cross_section, compute_quote_volume
from src.alpha.qlib_factors import compute_alpha158_style_factors
from src.utils.features import calculate_all_features

# 多策略集成
try:
    from src.strategy.multi_strategy_system import (
        StrategyOrchestrator,
        TrendFollowingStrategy,
        MeanReversionStrategy,
        MultiStrategyAdapter
    )
    MULTI_STRATEGY_AVAILABLE = True
except ImportError:
    MULTI_STRATEGY_AVAILABLE = False


def _rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    deltas = np.diff(np.array(closes[-(period + 1) :], dtype=float))
    gains = np.clip(deltas, 0, None)
    losses = -np.clip(deltas, None, 0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


@dataclass
class AlphaSnapshot:
    """Alpha因子快照
    
    包含原始因子、标准化因子和最终评分
    """
    raw_factors: Dict[str, Dict[str, float]]  # symbol -> factor -> value
    z_factors: Dict[str, Dict[str, float]]
    scores: Dict[str, float]


class AlphaEngine:
    """Alpha因子引擎
    
    计算多因子Alpha评分，支持：
    - 传统5因子Alpha (动量、波动率、成交量、RSI等)
    - 多策略模式 (趋势跟踪 + 均值回归 + 6因子Alpha)
    """
    
    def __init__(self, cfg: AlphaConfig):
        """初始化Alpha引擎
        
        Args:
            cfg: Alpha配置
        """
        self.cfg = cfg

        # 初始化多策略系统（如果启用）
        self.use_multi_strategy = getattr(cfg, 'use_multi_strategy', False)
        self.multi_strategy_adapter = None
        self.run_id = ""
        self.current_regime_key: Optional[str] = None
        self.alpha6_strategy = None
        self._alpha6_static_weights: Dict[str, float] = {}
        self.repo_root = Path(__file__).resolve().parents[2]
        self._ml_model = None
        self._ml_model_base_path: Optional[Path] = None
        self._ml_model_signature: Optional[str] = None
        self._ml_model_error: Optional[str] = None

        if self.use_multi_strategy and MULTI_STRATEGY_AVAILABLE:
            self._init_multi_strategy()

    @staticmethod
    def _normalize_regime_key(regime_key: Optional[Any]) -> Optional[str]:
        raw = str(regime_key or "").strip()
        if not raw:
            return None
        mapping = {
            "TRENDING": "Trending",
            "Trending": "Trending",
            "SIDEWAYS": "Sideways",
            "Sideways": "Sideways",
            "RISK_OFF": "Risk-Off",
            "Risk-Off": "Risk-Off",
            "Risk_Off": "Risk-Off",
            "RiskOff": "Risk-Off",
        }
        return mapping.get(raw, raw)

    def set_regime_context(self, regime_key: Optional[Any]) -> None:
        self.current_regime_key = self._normalize_regime_key(regime_key)

    def _load_regime_weight_override(self) -> Dict[str, float]:
        if not bool(getattr(self.cfg, "dynamic_weights_by_regime_enabled", False)):
            return {}

        regime_key = self._normalize_regime_key(self.current_regime_key)
        if not regime_key:
            return {}

        try:
            p = Path(str(getattr(self.cfg, "dynamic_weights_by_regime_path", "") or ""))
            if not p.exists():
                return {}
            data = json.loads(p.read_text(encoding="utf-8"))
            weights = (((data.get("regimes") or {}).get(regime_key) or {}).get("weights"))
            if not isinstance(weights, dict):
                return {}
            return {str(k): float(v) for k, v in weights.items()}
        except Exception:
            return {}

    def _resolve_classic_base_weights(self, static_base_w: Dict[str, float]) -> Dict[str, float]:
        weights = dict(static_base_w)
        regime_override = self._load_regime_weight_override()
        for key in static_base_w.keys():
            if key in regime_override:
                weights[key] = float(regime_override[key])
        return self._load_dynamic_ic_weights(weights)

    def _resolve_multi_strategy_alpha6_weights(self) -> Dict[str, float]:
        weights = dict(self._alpha6_static_weights)
        regime_override = self._load_regime_weight_override()
        remap = {
            "f1_mom_5d": "f1_mom_5d",
            "f2_mom_20d": "f2_mom_20d",
            "f3_vol_adj_ret_20d": "f3_vol_adj_ret",
            "f4_volume_expansion": "f4_volume_expansion",
            "f5_rsi_trend_confirm": "f5_rsi_trend_confirm",
        }
        for src_key, dst_key in remap.items():
            if src_key in regime_override:
                weights[dst_key] = float(regime_override[src_key])
        return weights

    def _apply_multi_strategy_regime_weights(self) -> None:
        if self.alpha6_strategy is None:
            return
        self.alpha6_strategy.set_factor_weights(self._resolve_multi_strategy_alpha6_weights())

    def _load_dynamic_ic_weights(self, default_weights: Dict[str, float]) -> Dict[str, float]:
        """Load dynamic factor weights from IC monitor summary.

        规则：
        - 若启用 dynamic_ic_weighting 且有可用IC，按 sign(IC) * abs(IC) 生成新权重
        - 再缩放到与静态权重同等 L1 强度，避免分值尺度漂移
        - 失败时回退静态权重
        """
        try:
            ic_cfg = getattr(self.cfg, 'dynamic_ic_weighting', None)
            if not ic_cfg or not bool(getattr(ic_cfg, 'enabled', False)):
                return dict(default_weights)

            p = Path(str(getattr(ic_cfg, 'ic_monitor_path', 'reports/alpha_ic_monitor.json')))
            if not p.exists():
                return dict(default_weights)

            obj = json.loads(p.read_text(encoding='utf-8'))
            factor_ic = obj.get('factor_ic') if isinstance(obj, dict) else None
            if not isinstance(factor_ic, dict):
                return dict(default_weights)

            min_abs_ic = float(getattr(ic_cfg, 'min_abs_ic', 0.003) or 0.003)
            dyn = {}
            has_any = False
            for k, w in (default_weights or {}).items():
                rec = factor_ic.get(k) or {}
                # 优先 short，再 long
                ic_mean = None
                try:
                    ic_mean = float((rec.get('rank_ic_short') or {}).get('mean'))
                except Exception:
                    ic_mean = None
                if ic_mean is None:
                    try:
                        ic_mean = float((rec.get('ic_short') or {}).get('mean'))
                    except Exception:
                        ic_mean = None
                if ic_mean is None:
                    continue

                if abs(ic_mean) < min_abs_ic:
                    continue

                sign = 1.0 if ic_mean >= 0 else -1.0
                dyn[k] = sign * abs(ic_mean)
                has_any = True

            if not has_any:
                return dict(default_weights)

            # scale L1 norm to static weights
            l1_static = float(sum(abs(float(v)) for v in default_weights.values()))
            l1_dyn = float(sum(abs(float(v)) for v in dyn.values()))
            if l1_dyn <= 1e-12:
                return dict(default_weights)
            scale = l1_static / l1_dyn if l1_static > 0 else 1.0
            out = dict(default_weights)
            for k in out.keys():
                if k in dyn:
                    out[k] = float(dyn[k]) * scale
            return out
        except Exception:
            return dict(default_weights)

    def _resolve_total_capital_usdt(self) -> float:
        """Resolve dynamic capital base for multi-strategy sizing.

        Priority:
        1) ENV override: V5_MULTI_STRATEGY_CAP_USDT
        2) reports/equity_validation.json (okx_total_eq / calculated_total_eq)
        3) legacy config fields (for compatibility)
        4) fallback 20.0
        """
        # 1) ENV override
        env_cap = os.getenv('V5_MULTI_STRATEGY_CAP_USDT', '').strip()
        if env_cap:
            try:
                v = float(env_cap)
                if v > 0:
                    return v
            except Exception:
                pass

        # 2) live equity snapshot
        try:
            p = Path('reports/equity_validation.json')
            if p.exists():
                obj = json.loads(p.read_text(encoding='utf-8'))
                for key in ('okx_total_eq', 'calculated_total_eq'):
                    v = obj.get(key)
                    if v is not None:
                        v = float(v)
                        if v > 0:
                            return v
        except Exception:
            pass

        # 3) compatibility paths
        try:
            if hasattr(self.cfg, 'live_equity_cap_usdt') and self.cfg.live_equity_cap_usdt:
                v = float(self.cfg.live_equity_cap_usdt)
                if v > 0:
                    return v
        except Exception:
            pass

        try:
            if hasattr(self.cfg, 'budget') and hasattr(self.cfg.budget, 'live_equity_cap_usdt'):
                v = getattr(self.cfg.budget, 'live_equity_cap_usdt', None)
                if v is not None and float(v) > 0:
                    return float(v)
        except Exception:
            pass

        try:
            if hasattr(self.cfg, 'account') and hasattr(self.cfg.account, 'live_equity_cap_usdt'):
                v = getattr(self.cfg.account, 'live_equity_cap_usdt', None)
                if v is not None and float(v) > 0:
                    return float(v)
        except Exception:
            pass

        return 20.0

    def _init_multi_strategy(self):
        """初始化多策略系统 (趋势跟踪 + 均值回归 + 6因子Alpha)"""
        from decimal import Decimal
        from src.strategy.multi_strategy_system import Alpha6FactorStrategy

        # 动态资金基数：优先实时权益，不再固定20U
        cap_usdt = self._resolve_total_capital_usdt()
        total_capital = Decimal(str(cap_usdt))

        # 创建策略编排器
        orchestrator = StrategyOrchestrator(
            total_capital=total_capital,
            conflict_penalty_enabled=bool(
                getattr(self.cfg, "multi_strategy_conflict_penalty_enabled", True)
            ),
            conflict_dominance_ratio=float(
                getattr(self.cfg, "multi_strategy_conflict_dominance_ratio", 1.35) or 1.35
            ),
            conflict_min_confidence=float(
                getattr(self.cfg, "multi_strategy_conflict_min_confidence", 0.60) or 0.60
            ),
            conflict_penalty_strength=float(
                getattr(self.cfg, "multi_strategy_conflict_penalty_strength", 0.65) or 0.65
            ),
        )
        print(f"[AlphaEngine] 多策略资金基数: {float(total_capital):.4f} USDT (dynamic)")

        # 注册趋势跟踪策略 (20%资金)
        trend_strategy = TrendFollowingStrategy(config={
            'fast_ma': 20,
            'slow_ma': 60,
            'adx_threshold': 28,
            'position_size_pct': 0.35,
            'trailing_stop': 0.04
        })
        orchestrator.register_strategy(trend_strategy, allocation=Decimal('0.20'))

        # 注册均值回归策略 (25%资金)
        mean_revert_strategy = MeanReversionStrategy(config={
            'rsi_period': 14,
            'rsi_oversold': 28,
            'rsi_overbought': 72,
            'bb_period': 20,
            'bb_std': 2,
            'position_size_pct': 0.25,
            'mean_rev_threshold': 0.025
        })
        orchestrator.register_strategy(mean_revert_strategy, allocation=Decimal('0.25'))

        # 注册6因子Alpha策略 (55%资金，主策略)
        # 根治：不再硬编码权重，统一读取 live 配置，避免“改了配置但策略不生效”
        cfg_weights = getattr(self.cfg, 'weights', None)
        alpha_weights = {
            'f1_mom_5d': float(getattr(cfg_weights, 'f1_mom_5d', 0.15)) if cfg_weights else 0.15,
            'f2_mom_20d': float(getattr(cfg_weights, 'f2_mom_20d', 0.25)) if cfg_weights else 0.25,
            'f3_vol_adj_ret': float(getattr(cfg_weights, 'f3_vol_adj_ret_20d', 0.15)) if cfg_weights else 0.15,
            'f4_volume_expansion': float(getattr(cfg_weights, 'f4_volume_expansion', 0.15)) if cfg_weights else 0.15,
            'f5_rsi_trend_confirm': float(getattr(cfg_weights, 'f5_rsi_trend_confirm', 0.15)) if cfg_weights else 0.15,
            'f6_sentiment': 0.15,
        }

        # Qlib Alpha158 overlay 权重并入 Alpha6 策略
        ov = getattr(self.cfg, 'alpha158_overlay', None)
        if ov and bool(getattr(ov, 'enabled', False)):
            ow = getattr(ov, 'weights', None)
            if ow is not None:
                alpha_weights.update(
                    {
                        'f6_corr_pv_10': float(getattr(ow, 'f6_corr_pv_10', 0.15)),
                        'f7_cord_10': float(getattr(ow, 'f7_cord_10', 0.15)),
                        'f8_rsqr_10': float(getattr(ow, 'f8_rsqr_10', 0.20)),
                        'f9_rank_20': float(getattr(ow, 'f9_rank_20', 0.15)),
                        'f10_imax_14': float(getattr(ow, 'f10_imax_14', -0.05)),
                        'f11_imin_14': float(getattr(ow, 'f11_imin_14', 0.05)),
                        'f12_imxd_14': float(getattr(ow, 'f12_imxd_14', 0.35)),
                    }
                )

        alpha6_strategy = Alpha6FactorStrategy(config={
            'weights': alpha_weights,
            'position_size_pct': 0.30,
            # 与组合层最低门槛联动，避免二次门槛叠加导致长期0买入
            'score_threshold': float(max(0.03, min(0.10, getattr(self.cfg, 'min_score_threshold', 0.05)))),
            'alpha158_enabled': bool(getattr(getattr(self.cfg, 'alpha158_overlay', None), 'enabled', False)),
            'alpha158_blend_weight': float(getattr(getattr(self.cfg, 'alpha158_overlay', None), 'blend_weight', 0.35) or 0.35),
            'dynamic_ic_weighting': {
                'enabled': bool(getattr(getattr(self.cfg, 'dynamic_ic_weighting', None), 'enabled', False)),
                'ic_monitor_path': str(getattr(getattr(self.cfg, 'dynamic_ic_weighting', None), 'ic_monitor_path', 'reports/alpha_ic_monitor.json')),
                'min_abs_ic': float(getattr(getattr(self.cfg, 'dynamic_ic_weighting', None), 'min_abs_ic', 0.003) or 0.003),
                'fallback_to_static': bool(getattr(getattr(self.cfg, 'dynamic_ic_weighting', None), 'fallback_to_static', True)),
            },
        })
        self.alpha6_strategy = alpha6_strategy
        self._alpha6_static_weights = dict(alpha_weights)
        orchestrator.register_strategy(alpha6_strategy, allocation=Decimal('0.55'))

        # 创建适配器
        self.multi_strategy_adapter = MultiStrategyAdapter(orchestrator)
        print(f"[AlphaEngine] 多策略融合已启用:")
        print(f"              - 趋势跟踪: 20%")
        print(f"              - 均值回归: 25%")
        print(f"              - 6因子Alpha: 55%")

    def set_run_id(self, run_id: Optional[str]) -> None:
        self.run_id = str(run_id or "").strip()
        if self.multi_strategy_adapter:
            self.multi_strategy_adapter.set_run_id(self.run_id)

    def strategy_signals_path(self) -> Optional[Path]:
        if self.multi_strategy_adapter:
            return self.multi_strategy_adapter.strategy_signals_path()
        return None

    def _resolve_repo_path(self, raw_path: Optional[str], default: str) -> Path:
        path = Path(str(raw_path or default))
        if not path.is_absolute():
            path = self.repo_root / path
        return path

    @staticmethod
    def _normalize_ml_base_path(path: Path) -> Path:
        p = Path(path)
        if p.name.endswith("_config.json"):
            return p.with_name(p.name[: -len("_config.json")])
        if p.suffix in {".txt", ".pkl"}:
            return p.with_suffix("")
        return p

    @staticmethod
    def _ml_artifact_candidates(base_path: Path) -> List[Path]:
        return [
            Path(f"{base_path}.txt"),
            Path(f"{base_path}.pkl"),
            Path(f"{base_path}_config.json"),
        ]

    @classmethod
    def _ml_artifact_exists(cls, base_path: Path) -> bool:
        return any(p.exists() for p in cls._ml_artifact_candidates(base_path))

    @classmethod
    def _ml_artifact_signature(cls, base_path: Path) -> Optional[str]:
        existing = [p for p in cls._ml_artifact_candidates(base_path) if p.exists()]
        if not existing:
            return None
        parts = [f"{p.name}:{p.stat().st_mtime_ns}" for p in existing]
        return "|".join(sorted(parts))

    def _write_ml_runtime_status(self, payload: Dict[str, Any]) -> None:
        ml_cfg = getattr(self.cfg, "ml_factor", None)
        if ml_cfg is None or not bool(getattr(ml_cfg, "enabled", False)):
            return
        try:
            status_path = self._resolve_repo_path(
                getattr(ml_cfg, "runtime_status_path", "reports/ml_runtime_status.json"),
                "reports/ml_runtime_status.json",
            )
            status_path.parent.mkdir(parents=True, exist_ok=True)
            status_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _build_ml_inference_frame(self, market_data: Dict[str, MarketSeries], feature_names: List[str]) -> pd.DataFrame:
        rows: List[Dict[str, float]] = []
        include_time = any(name in {"hour_of_day", "day_of_week"} for name in feature_names)
        for sym, series in (market_data or {}).items():
            close = pd.Series(list(getattr(series, "close", []) or []), dtype=float)
            if len(close) < 2:
                continue
            volume = pd.Series(list(getattr(series, "volume", []) or [0.0] * len(close)), dtype=float)
            high = pd.Series(list(getattr(series, "high", []) or list(close)), dtype=float)
            low = pd.Series(list(getattr(series, "low", []) or list(close)), dtype=float)
            features = calculate_all_features(close, volume, high, low)
            row: Dict[str, float] = {"symbol": sym}
            row.update({str(k): float(v) for k, v in features.items()})
            if include_time:
                ts_list = list(getattr(series, "ts", []) or [])
                latest_ts = ts_list[-1] if ts_list else None
                dt = pd.to_datetime(latest_ts, unit="ms", errors="coerce")
                if pd.isna(dt):
                    dt = datetime.now(timezone.utc)
                row["hour_of_day"] = float(dt.hour)
                row["day_of_week"] = float(dt.dayofweek)
            rows.append(row)

        if not rows:
            return pd.DataFrame(columns=["symbol", *feature_names])

        df = pd.DataFrame(rows)
        for col in feature_names:
            if col not in df.columns:
                df[col] = 0.0
        df = df[["symbol", *feature_names]].replace([np.inf, -np.inf], np.nan)
        valid = df[feature_names].notna().all(axis=1)
        return df.loc[valid].reset_index(drop=True)

    def _compute_ml_overlay_scores(self, market_data: Dict[str, MarketSeries]) -> tuple[Dict[str, float], Dict[str, float], Dict[str, Any]]:
        ml_cfg = getattr(self.cfg, "ml_factor", None)
        status: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "configured_enabled": bool(getattr(ml_cfg, "enabled", False)) if ml_cfg is not None else False,
            "promotion_passed": False,
            "trained": False,
            "used_in_latest_snapshot": False,
            "reason": "disabled",
            "model_path": None,
            "prediction_count": 0,
            "ml_weight": float(getattr(ml_cfg, "ml_weight", 0.0) or 0.0) if ml_cfg is not None else 0.0,
        }
        if ml_cfg is None or not status["configured_enabled"]:
            return {}, {}, status

        if len(market_data or {}) < int(getattr(ml_cfg, "min_symbols", 3) or 3):
            status["reason"] = "insufficient_symbols"
            self._write_ml_runtime_status(status)
            return {}, {}, status

        pointer_path = self._resolve_repo_path(
            getattr(ml_cfg, "active_model_pointer_path", "models/ml_factor_model_active.txt"),
            "models/ml_factor_model_active.txt",
        )
        decision_path = self._resolve_repo_path(
            getattr(ml_cfg, "promotion_decision_path", "reports/model_promotion_decision.json"),
            "reports/model_promotion_decision.json",
        )
        model_base_path = self._normalize_ml_base_path(
            self._resolve_repo_path(getattr(ml_cfg, "model_path", "models/ml_factor_model"), "models/ml_factor_model")
        )

        if decision_path.exists():
            try:
                decision = json.loads(decision_path.read_text(encoding="utf-8"))
                status["promotion_passed"] = bool(decision.get("passed"))
                if decision.get("fail_reasons"):
                    status["promotion_fail_reasons"] = [str(x) for x in decision.get("fail_reasons") or []]
            except Exception:
                status["reason"] = "promotion_decision_unreadable"

        if pointer_path.exists():
            try:
                pointer_value = pointer_path.read_text(encoding="utf-8").strip()
                if pointer_value:
                    pointer_base = self._normalize_ml_base_path(self._resolve_repo_path(pointer_value, pointer_value))
                    if self._ml_artifact_exists(pointer_base):
                        model_base_path = pointer_base
            except Exception:
                status["reason"] = "active_pointer_unreadable"

        status["model_path"] = str(model_base_path)
        status["trained"] = self._ml_artifact_exists(model_base_path)
        if not status["trained"]:
            status["reason"] = "model_artifact_missing"
            self._write_ml_runtime_status(status)
            return {}, {}, status

        if bool(getattr(ml_cfg, "require_promotion_passed", True)):
            if not status["promotion_passed"]:
                status["reason"] = "promotion_not_passed"
                self._write_ml_runtime_status(status)
                return {}, {}, status
            if not pointer_path.exists():
                status["reason"] = "active_pointer_missing"
                self._write_ml_runtime_status(status)
                return {}, {}, status

        latest_mtime_ns = max(
            p.stat().st_mtime_ns for p in self._ml_artifact_candidates(model_base_path) if p.exists()
        )
        max_age_hours = float(getattr(ml_cfg, "max_model_age_hours", 72) or 72)
        model_age_hours = max(0.0, (datetime.now().timestamp() - (latest_mtime_ns / 1_000_000_000.0)) / 3600.0)
        status["model_age_hours"] = model_age_hours
        if model_age_hours > max_age_hours:
            status["reason"] = "model_too_old"
            self._write_ml_runtime_status(status)
            return {}, {}, status

        signature = self._ml_artifact_signature(model_base_path)
        if self._ml_model is None or self._ml_model_base_path != model_base_path or self._ml_model_signature != signature:
            try:
                from src.execution.ml_factor_model import MLFactorModel

                model = MLFactorModel()
                with redirect_stdout(StringIO()):
                    model.load_model(str(model_base_path))
                self._ml_model = model
                self._ml_model_base_path = model_base_path
                self._ml_model_signature = signature
                self._ml_model_error = None
            except Exception as exc:
                self._ml_model = None
                self._ml_model_base_path = None
                self._ml_model_signature = None
                self._ml_model_error = str(exc)
                status["reason"] = "model_load_failed"
                status["error"] = str(exc)
                self._write_ml_runtime_status(status)
                return {}, {}, status

        inference_df = self._build_ml_inference_frame(market_data, list(self._ml_model.feature_names))
        if inference_df.empty:
            status["reason"] = "inference_frame_empty"
            self._write_ml_runtime_status(status)
            return {}, {}, status

        try:
            preds = self._ml_model.predict_batch(inference_df)
        except Exception as exc:
            status["reason"] = "prediction_failed"
            status["error"] = str(exc)
            self._write_ml_runtime_status(status)
            return {}, {}, status

        raw_preds = {
            str(sym): float(pred)
            for sym, pred in zip(inference_df["symbol"].tolist(), preds.tolist())
            if np.isfinite(float(pred))
        }
        if not raw_preds:
            status["reason"] = "prediction_empty"
            self._write_ml_runtime_status(status)
            return {}, {}, status

        if bool(getattr(ml_cfg, "use_robust_zscore", True)):
            overlay_scores = robust_zscore_cross_section(raw_preds, winsorize_pct=0.05)
        else:
            overlay_scores = zscore_cross_section(raw_preds)

        status["used_in_latest_snapshot"] = bool(overlay_scores)
        status["prediction_count"] = int(len(overlay_scores))
        status["reason"] = "ok" if overlay_scores else "prediction_empty"
        self._write_ml_runtime_status(status)
        return (
            {str(k): float(v) for k, v in overlay_scores.items()},
            raw_preds,
            status,
        )

    def compute_scores(self, market_data: Dict[str, MarketSeries]) -> Dict[str, float]:
        """计算Alpha评分
        
        Args:
            market_data: 市场数据 {symbol: MarketSeries}
            
        Returns:
            评分字典 {symbol: score}
        """
        # 如果使用多策略，返回多策略信号
        if self.use_multi_strategy and self.multi_strategy_adapter:
            return self._compute_multi_strategy_scores(market_data)

        # 否则使用原有的6因子Alpha
        snap = self.compute_snapshot(market_data)
        return snap.scores

    def _compute_multi_strategy_scores(self, market_data: Dict[str, MarketSeries]) -> Dict[str, float]:
        """
        使用多策略系统计算评分
        """
        import pandas as pd
        from datetime import datetime

        self._apply_multi_strategy_regime_weights()

        # 将 MarketSeries 转换为 DataFrame
        all_data = []
        for sym, series in market_data.items():
            if len(series.close) < 25:
                continue

            # 构建DataFrame
            df = pd.DataFrame({
                'symbol': sym,
                'close': list(series.close),
                'high': list(series.high) if hasattr(series, 'high') else list(series.close),
                'low': list(series.low) if hasattr(series, 'low') else list(series.close),
                'volume': list(series.volume) if hasattr(series, 'volume') else [0] * len(series.close)
            })
            all_data.append(df)

        if not all_data:
            return {}

        market_df = pd.concat(all_data, ignore_index=True)

        # 运行多策略
        targets = self.multi_strategy_adapter.run_strategy_cycle(market_df)
        
        # 转换为评分格式 (0-1之间的分数)
        # 同一symbol可能出现多个信号（多策略/多阶段），使用加权平均而非简单覆盖
        from collections import defaultdict
        
        symbol_signals = defaultdict(list)  # symbol -> [(score, weight, side), ...]
        total_capital = max(float(self._resolve_total_capital_usdt() or 0.0), 1e-9)

        for target in targets:
            sym = target['symbol'].replace('-', '/')

            signal_score = float(target.get('signal_score', 0.0) or 0.0)
            confidence = max(0.0, float(target.get('confidence', 0.0) or 0.0))

            # 让“策略分配权重”真正进入打分链路
            strategy_weight = float(target.get('strategy_weight', confidence) or 0.0)
            position_usdt = float(target.get('target_position_usdt', 0.0) or 0.0)
            position_weight = max(0.0, position_usdt) / total_capital

            # 组合权重：优先策略权重，兼顾目标仓位占比
            if strategy_weight > 0 and position_weight > 0:
                effective_weight = 0.7 * strategy_weight + 0.3 * position_weight
            else:
                effective_weight = max(strategy_weight, position_weight, confidence)

            effective_weight = max(effective_weight, 1e-6)
            merge_weight = max(effective_weight * max(confidence, 0.05), 1e-6)

            # 买入信号为正分，卖出为负分
            score = abs(signal_score)
            if target['side'] == 'sell':
                score = -score

            symbol_signals[sym].append({
                'score': score,
                'merge_weight': merge_weight,
                'side': target['side']
            })
        
        # Merge multi-strategy signals while preserving buy-positive / sell-negative semantics.
        scores = {}
        for sym, signals in symbol_signals.items():
            if len(signals) == 1:
                scores[sym] = signals[0]['score']
            else:
                total_weight = sum(s['merge_weight'] for s in signals)
                weighted_score = sum(s['score'] * s['merge_weight'] for s in signals) / max(total_weight, 1e-9)
                scores[sym] = weighted_score
        
        return scores

    def compute_snapshot(self, market_data: Dict[str, MarketSeries], use_robust_zscore: bool = True) -> AlphaSnapshot:
        """计算完整的Alpha快照（包含原始因子、标准化因子和评分）
        
        Args:
            market_data: 市场数据 {symbol: MarketSeries}
            use_robust_zscore: 是否使用稳健标准化（去极值）
            
        Returns:
            Alpha快照
        """
        # 如果使用多策略，直接返回多策略结果
        if self.use_multi_strategy and self.multi_strategy_adapter:
            scores = self._compute_multi_strategy_scores(market_data)
            ml_overlay_scores, ml_raw_preds, ml_runtime = self._compute_ml_overlay_scores(market_data)
            ml_weight = float(getattr(getattr(self.cfg, "ml_factor", None), "ml_weight", 0.0) or 0.0)
            if ml_overlay_scores and ml_weight > 0:
                blended_scores: Dict[str, float] = {}
                for sym in sorted(set(scores.keys()) | set(ml_overlay_scores.keys())):
                    base_score = float(scores.get(sym, 0.0))
                    if sym in ml_overlay_scores:
                        blended_scores[sym] = float(
                            (1.0 - ml_weight) * base_score + ml_weight * float(ml_overlay_scores[sym])
                        )
                    else:
                        blended_scores[sym] = base_score
                scores = blended_scores
            if ml_runtime and ml_runtime.get("used_in_latest_snapshot"):
                ml_runtime["symbols_used"] = sorted(ml_overlay_scores.keys())
                self._write_ml_runtime_status(ml_runtime)
            symbols = sorted(set(scores.keys()) | set(ml_raw_preds.keys()))
            # 构建简化的AlphaSnapshot（多策略模式下部分字段为空）
            return AlphaSnapshot(
                raw_factors={sym: {"ml_pred_raw": float(ml_raw_preds.get(sym, 0.0))} for sym in symbols},
                z_factors={sym: {"ml_pred_zscore": float(ml_overlay_scores.get(sym, 0.0))} for sym in symbols},
                scores=scores
            )

        # 否则使用传统5因子 + Alpha158 overlay（可选）
        f1: Dict[str, float] = {}
        f2: Dict[str, float] = {}
        f3: Dict[str, float] = {}
        f4: Dict[str, float] = {}
        f5: Dict[str, float] = {}

        # Alpha158 overlay factors
        ov_names = [
            "f6_corr_pv_10",
            "f7_cord_10",
            "f8_rsqr_10",
            "f9_rank_20",
            "f10_imax_14",
            "f11_imin_14",
            "f12_imxd_14",
        ]
        ov_vals: Dict[str, Dict[str, float]] = {k: {} for k in ov_names}

        for sym, s in (market_data or {}).items():
            c = list(s.close)
            v = list(s.volume)
            h = list(s.high) if hasattr(s, "high") else list(s.close)
            l = list(s.low) if hasattr(s, "low") else list(s.close)
            if len(c) < 25:
                continue

            # base 5 factors
            mom_5d = safe_pct_change(c[-1 - 24 * 5], c[-1]) if len(c) > 24 * 5 else safe_pct_change(c[0], c[-1])
            mom_20d = safe_pct_change(c[-1 - 24 * 20], c[-1]) if len(c) > 24 * 20 else safe_pct_change(c[0], c[-1])

            rets = np.diff(np.array(c[-(24 * 20 + 1) :], dtype=float)) / np.array(c[-(24 * 20 + 1) : -1], dtype=float)
            vol = float(np.std(rets)) if len(rets) > 10 else 0.0
            vol_adj = mom_20d / (vol + 1e-12)

            if len(v) >= 24 and len(c) >= 24:
                vol_1d = compute_quote_volume(v[-24:], c[-24:])
                daily_quote = []
                if len(v) >= 24 * 8 and len(c) >= 24 * 8:
                    for k in range(1, 8):
                        start = -24 * (k + 1)
                        end = -24 * k
                        daily_quote.append(compute_quote_volume(v[start:end], c[start:end]))
                avg_7d = float(np.mean(daily_quote)) if daily_quote else vol_1d
                vol_exp = (vol_1d / (avg_7d + 1e-12)) - 1.0
            else:
                vol_exp = 0.0

            rsi = _rsi(c, 14)
            rsi_trend = (rsi - 50.0) / 50.0

            f1[sym] = float(mom_5d)
            f2[sym] = float(mom_20d)
            f3[sym] = float(vol_adj)
            f4[sym] = float(vol_exp)
            f5[sym] = float(rsi_trend)

            # Alpha158 overlay
            qf = compute_alpha158_style_factors(c, h, l, v)
            for k in ov_names:
                ov_vals[k][sym] = float(qf.get(k, 0.0))

        # z-score
        if use_robust_zscore:
            z_map = {
                "f1_mom_5d": robust_zscore_cross_section(f1, winsorize_pct=0.05),
                "f2_mom_20d": robust_zscore_cross_section(f2, winsorize_pct=0.05),
                "f3_vol_adj_ret_20d": robust_zscore_cross_section(f3, winsorize_pct=0.05),
                "f4_volume_expansion": robust_zscore_cross_section(f4, winsorize_pct=0.05),
                "f5_rsi_trend_confirm": robust_zscore_cross_section(f5, winsorize_pct=0.05),
            }
            for k in ov_names:
                z_map[k] = robust_zscore_cross_section(ov_vals.get(k, {}), winsorize_pct=0.05)
        else:
            z_map = {
                "f1_mom_5d": zscore_cross_section(f1),
                "f2_mom_20d": zscore_cross_section(f2),
                "f3_vol_adj_ret_20d": zscore_cross_section(f3),
                "f4_volume_expansion": zscore_cross_section(f4),
                "f5_rsi_trend_confirm": zscore_cross_section(f5),
            }
            for k in ov_names:
                z_map[k] = zscore_cross_section(ov_vals.get(k, {}))

        static_base_w = {
            "f1_mom_5d": float(self.cfg.weights.f1_mom_5d),
            "f2_mom_20d": float(self.cfg.weights.f2_mom_20d),
            "f3_vol_adj_ret_20d": float(self.cfg.weights.f3_vol_adj_ret_20d),
            "f4_volume_expansion": float(self.cfg.weights.f4_volume_expansion),
            "f5_rsi_trend_confirm": float(self.cfg.weights.f5_rsi_trend_confirm),
        }
        base_w = self._resolve_classic_base_weights(static_base_w)

        ov_cfg = getattr(self.cfg, "alpha158_overlay", None)
        ov_enabled = bool(getattr(ov_cfg, "enabled", False))
        ov_blend = float(getattr(ov_cfg, "blend_weight", 0.35) or 0.35)

        static_ov_w: Dict[str, float] = {}
        if ov_enabled and ov_cfg is not None and getattr(ov_cfg, "weights", None) is not None:
            ow = ov_cfg.weights
            static_ov_w = {
                "f6_corr_pv_10": float(getattr(ow, "f6_corr_pv_10", 0.15)),
                "f7_cord_10": float(getattr(ow, "f7_cord_10", 0.15)),
                "f8_rsqr_10": float(getattr(ow, "f8_rsqr_10", 0.20)),
                "f9_rank_20": float(getattr(ow, "f9_rank_20", 0.15)),
                "f10_imax_14": float(getattr(ow, "f10_imax_14", -0.05)),
                "f11_imin_14": float(getattr(ow, "f11_imin_14", 0.05)),
                "f12_imxd_14": float(getattr(ow, "f12_imxd_14", 0.35)),
            }
        ov_w = self._load_dynamic_ic_weights(static_ov_w) if static_ov_w else {}
        ml_overlay_scores, ml_raw_preds, ml_runtime = self._compute_ml_overlay_scores(market_data)
        ml_weight = float(getattr(getattr(self.cfg, "ml_factor", None), "ml_weight", 0.0) or 0.0)

        raw_factors: Dict[str, Dict[str, float]] = {}
        z_factors: Dict[str, Dict[str, float]] = {}
        scores: Dict[str, float] = {}

        symbols = sorted(set(f1.keys()))
        for sym in symbols:
            raw_factors[sym] = {
                "f1_mom_5d": f1.get(sym, 0.0),
                "f2_mom_20d": f2.get(sym, 0.0),
                "f3_vol_adj_ret_20d": f3.get(sym, 0.0),
                "f4_volume_expansion": f4.get(sym, 0.0),
                "f5_rsi_trend_confirm": f5.get(sym, 0.0),
            }
            for k in ov_names:
                raw_factors[sym][k] = float(ov_vals.get(k, {}).get(sym, 0.0))
            raw_factors[sym]["ml_pred_raw"] = float(ml_raw_preds.get(sym, 0.0))

            z_factors[sym] = {}
            for k, zv in z_map.items():
                z_factors[sym][k] = float(zv.get(sym, 0.0))
            z_factors[sym]["ml_pred_zscore"] = float(ml_overlay_scores.get(sym, 0.0))

            base_score = float(sum(float(base_w.get(k, 0.0)) * float(z_factors[sym].get(k, 0.0)) for k in static_base_w.keys()))
            if ov_enabled and ov_w:
                ov_score = float(sum(float(ov_w.get(k, 0.0)) * float(z_factors[sym].get(k, 0.0)) for k in ov_w.keys()))
                raw_score = (1.0 - ov_blend) * base_score + ov_blend * ov_score
            else:
                raw_score = base_score

            classic_score = float(-raw_score)
            if sym in ml_overlay_scores and ml_weight > 0:
                scores[sym] = float((1.0 - ml_weight) * classic_score + ml_weight * float(ml_overlay_scores[sym]))
            else:
                scores[sym] = classic_score

        if ml_runtime and ml_runtime.get("used_in_latest_snapshot"):
            ml_runtime["symbols_used"] = sorted(ml_overlay_scores.keys())
            self._write_ml_runtime_status(ml_runtime)

        return AlphaSnapshot(raw_factors=raw_factors, z_factors=z_factors, scores=scores)
