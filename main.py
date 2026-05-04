from __future__ import annotations

import logging
import sys
import time
import os
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure repo-local imports work even when the entrypoint is launched outside the repo root.
PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT_STR = str(PROJECT_ROOT)
if PROJECT_ROOT_STR not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_STR)

from configs.loader import load_config
from configs.schema import AppConfig, normalize_alpha_base_factor_mapping
from src.data.mock_provider import MockProvider
from src.data.okx_ccxt_provider import OKXCCXTProvider
from src.execution.account_store import AccountStore
from src.execution.execution_engine import ExecutionEngine
from src.execution.event_action_bridge import consume_event_actions_for_run
from src.execution.fill_store import (
    derive_position_store_path,
    derive_runtime_named_artifact_path,
    derive_runtime_named_json_path,
    derive_runtime_reports_dir,
    derive_runtime_runs_dir,
    derive_runtime_spread_snapshots_dir,
)
from src.execution.position_store import PositionStore
from src.reporting.reporting import dump_run_artifacts
from src.core.models import MarketSeries, Order, PositionState
from src.risk.negative_expectancy_cooldown import negative_expectancy_config_fingerprint
from src.risk.risk_engine import RiskEngine

# 棰勭畻闄愬埗锛?0 USDT纭檺鍒讹級
try:
    from src.risk.budget_guard import BudgetGuard
    from src.risk.live_equity_fetcher import get_live_equity_from_okx, check_budget_limit
    BUDGET_GUARD_ENABLED = True
except ImportError:
    BUDGET_GUARD_ENABLED = False
    BudgetGuard = None

# Alpha 鍘嗗彶鏁版嵁鏀堕泦锛堝彲閫夛級
try:
    from scripts.collect_alpha_history import AlphaHistoryCollector
    ALPHA_HISTORY_ENABLED = True
except ImportError:
    ALPHA_HISTORY_ENABLED = False
    AlphaHistoryCollector = None


def setup_logging(level: str = "INFO") -> None:
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[
            logging.FileHandler("logs/v5_runtime.log"),
            logging.StreamHandler(),
        ],
    )


# ========== 瓒嬪娍缂撳瓨鍔熻兘 ==========
TREND_CACHE_PATH = Path("reports/trend_cache.json")


def _resolve_trend_cache_path(order_store_path: str | os.PathLike[str] | None = None) -> Path:
    if order_store_path is None:
        return TREND_CACHE_PATH
    return derive_runtime_named_json_path(str(order_store_path), "trend_cache")


def _resolve_order_state_machine_path(cfg) -> Path:
    order_store_path = str(
        getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite")
        or "reports/orders.sqlite"
    )
    raw_path = str(
        getattr(getattr(cfg, "execution", None), "order_state_machine_path", "reports/order_state_machine.json")
        or ""
    ).strip()
    if not raw_path or raw_path == "reports/order_state_machine.json":
        return derive_runtime_named_json_path(order_store_path, "order_state_machine")
    return Path(raw_path)


def _resolve_dynamic_alpha_weights_path(cfg) -> Path:
    order_store_path = str(
        getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite")
        or "reports/orders.sqlite"
    )
    return derive_runtime_named_json_path(order_store_path, "alpha_dynamic_weights")


def _resolve_universe_cache_path(cfg) -> Path:
    order_store_path = str(
        getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite")
        or "reports/orders.sqlite"
    )
    raw_path = str(
        getattr(getattr(cfg, "universe", None), "cache_path", "reports/universe_cache.json")
        or ""
    ).strip()
    if not raw_path or raw_path == "reports/universe_cache.json":
        return derive_runtime_reports_dir(order_store_path) / "universe_cache.json"
    return Path(raw_path)


def _prime_pipeline_run_context(pipe: Any, run_id: str) -> None:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return
    for attr_name in ("alpha_engine", "portfolio_engine"):
        engine = getattr(pipe, attr_name, None)
        setter = getattr(engine, "set_run_id", None)
        if callable(setter):
            setter(normalized_run_id)


def _is_close_only_risk_off(cfg: Any, regime_state: Any) -> bool:
    name = str(getattr(regime_state, "name", "") or "").strip().upper()
    value = str(getattr(regime_state, "value", regime_state) or "").strip()
    normalized = value.lower().replace("_", "-").replace(" ", "-")
    is_risk_off = (
        name == "RISK_OFF"
        or normalized in {"risk-off", "riskoff", "regimestate.risk-off"}
        or normalized.endswith(".risk-off")
    )
    try:
        risk_off_mult = float(getattr(getattr(cfg, "regime", None), "pos_mult_risk_off", 0.0) or 0.0)
    except Exception:
        risk_off_mult = 0.0
    return bool(is_risk_off and risk_off_mult <= 0.0)


def _should_update_skipped_candidate_tracker(cfg: Any, regime_state: Any, positions: Any) -> bool:
    if not _is_close_only_risk_off(cfg, regime_state):
        return True
    return any(float(getattr(pos, "qty", 0.0) or 0.0) > 0.0 for pos in (positions or []))


def save_trend_cache(
    alpha_snapshot,
    regime_result,
    symbols: list,
    timestamp: float = None,
    *,
    order_store_path: str | os.PathLike[str] | None = None,
) -> None:
    """淇濆瓨瓒嬪娍璁＄畻缁撴灉鍒扮紦瀛樻枃浠?

    鐢ㄤ簬瓒嬪娍鏇存柊绋嬪簭鍦?:57 璁＄畻锛屼氦鏄撶▼搴忓湪 :00 璇诲彇
    """
    if timestamp is None:
        timestamp = time.time()

    cache_data = {
        "timestamp": timestamp,
        "timestamp_iso": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "symbols": symbols,
        "alpha": {
            "scores": alpha_snapshot.scores if alpha_snapshot else {},
            "ranks": alpha_snapshot.ranks if alpha_snapshot else {},
            "raw": alpha_snapshot.raw if alpha_snapshot else {},
        },
        "regime": {
            "state": regime_result.state.value if regime_result else "UNKNOWN",
            "multiplier": regime_result.multiplier if regime_result else 1.0,
            "atr_pct": regime_result.atr_pct if regime_result else 0.0,
            "ma20": regime_result.ma20 if regime_result else 0.0,
            "ma60": regime_result.ma60 if regime_result else 0.0,
        }
    }

    trend_cache_path = _resolve_trend_cache_path(order_store_path)
    trend_cache_path.parent.mkdir(parents=True, exist_ok=True)
    trend_cache_path.write_text(json.dumps(cache_data, indent=2, default=str), encoding="utf-8")
    logging.getLogger("v5").info(f"[TrendCache] Saved to {trend_cache_path}")


def load_trend_cache(
    max_age_sec: int = 300,
    *,
    order_store_path: str | os.PathLike[str] | None = None,
) -> Optional[dict]:
    """浠庣紦瀛樻枃浠惰鍙栬秼鍔胯绠楃粨鏋?

    Args:
        max_age_sec: 缂撳瓨鏈€澶ф湁鏁堟椂闂达紙绉掞級锛岄粯璁?鍒嗛挓

    Returns:
        缂撳瓨鏁版嵁鎴朜one锛堝鏋滅紦瀛樹笉瀛樺湪鎴栧凡杩囨湡锛?
    """
    trend_cache_path = _resolve_trend_cache_path(order_store_path)
    if not trend_cache_path.exists():
        return None

    try:
        data = json.loads(trend_cache_path.read_text(encoding="utf-8"))
        cache_time = data.get("timestamp", 0)
        age_sec = time.time() - cache_time

        if age_sec > max_age_sec:
            logging.getLogger("v5").warning(
                f"[TrendCache] Cache expired: {age_sec:.0f}s > {max_age_sec}s"
            )
            return None

        logging.getLogger("v5").info(
            f"[TrendCache] Loaded from {trend_cache_path}, age={age_sec:.0f}s"
        )
        return data
    except Exception as e:
        logging.getLogger("v5").warning(f"[TrendCache] Load failed: {e}")
        return None


class TrendCacheAlphaSnapshot:
    """浠庣紦瀛樺垱寤虹殑 Alpha 蹇収瀵硅薄"""
    def __init__(self, cache_data: dict):
        self.scores = cache_data.get("alpha", {}).get("scores", {})
        self.ranks = cache_data.get("alpha", {}).get("ranks", {})
        self.raw = cache_data.get("alpha", {}).get("raw", {})


class TrendCacheRegimeResult:
    """浠庣紦瀛樺垱寤虹殑 Regime 缁撴灉瀵硅薄"""
    def __init__(self, cache_data: dict, cfg):
        from configs.schema import RegimeState
        regime_data = cache_data.get("regime", {})
        state_str = regime_data.get("state", "SIDEWAYS")
        try:
            self.state = RegimeState[state_str]
        except KeyError:
            self.state = RegimeState.SIDEWAYS
        self.multiplier = regime_data.get("multiplier", 1.0)
        self.atr_pct = regime_data.get("atr_pct", 0.0)
        self.ma20 = regime_data.get("ma20", 0.0)
        self.ma60 = regime_data.get("ma60", 0.0)
        self.hmm_state = None
        self.hmm_probability = None
        self.hmm_probs = None


# ========== 瓒嬪娍缂撳瓨鍔熻兘缁撴潫 ==========


def _get_env_epoch_sec(name: str) -> Optional[int]:
    """浠庣幆澧冨彉閲忚鍙栨椂闂存埑锛堢/姣/寰鍏煎锛?

    瑙勫垯锛?
    - 10浣嶅強浠ヤ笅锛氱
    - 13浣嶏細姣
    - 16浣嶅強浠ヤ笂锛氬井绉?
    - 鍏朵粬浣嶆暟锛氭寜鏁板€煎厹搴曞垽瀹氾紙>1e14 褰撳井绉掞紝>1e11 褰撴绉掞級
    """
    v = os.getenv(name)
    if not v:
        return None
    try:
        x = int(v)
        digits = len(str(abs(x)))

        if digits <= 10:
            pass  # seconds
        elif digits == 13:
            x //= 1000  # milliseconds -> seconds
        elif digits >= 16:
            x //= 1_000_000  # microseconds -> seconds
        else:
            # 11/12/14/15 浣嶏細闈炲父瑙勮緭鍏ワ紝鍋氫繚瀹堝厹搴?
            if abs(x) > 100_000_000_000_000:
                x //= 1_000_000
            elif abs(x) > 100_000_000_000:
                x //= 1000
            logging.getLogger(__name__).warning(
                "Unusual timestamp digits for %s: %s (%d digits)", name, v, digits
            )

        # 鏀惧鍒?2000-2100锛岄伩鍏嶆湭鏉ュ勾浠借鍒?
        if x < 946684800 or x > 4102444800:  # 2000-01-01 to 2100-01-01
            logging.getLogger(__name__).warning(
                "Timestamp %s out of reasonable range (2000-2100): %s", name, x
            )
            return None

        return x
    except (ValueError, TypeError) as e:
        logging.getLogger(__name__).warning("Invalid timestamp for %s: %s - %s", name, v, e)
        return None


def build_provider(cfg: AppConfig):
    """Build market data provider.

    Safety rule:
    - dry-run: default to MockProvider
    - live: MUST use a real provider (OKX public) to avoid trading on fake data
    """

    which = (os.getenv("V5_DATA_PROVIDER") or "mock").lower()
    mode = str(getattr(cfg.execution, "mode", "dry_run") or "dry_run").lower()

    if mode == "live" and which != "okx":
        raise RuntimeError("Live mode requires V5_DATA_PROVIDER=okx (refuse to trade on mock data)")

    if which == "okx":
        return OKXCCXTProvider(rate_limit=True)

    return MockProvider(seed=7)


def _validate_market_data_snapshot(
    symbols: list[str],
    market_data: Dict[str, MarketSeries],
    *,
    require_symbol: Optional[str],
    min_coverage_ratio: float,
) -> tuple[bool, str, Dict[str, MarketSeries]]:
    valid = {
        str(sym): series
        for sym, series in (market_data or {}).items()
        if getattr(series, "ts", None) and len(getattr(series, "ts", []) or []) > 0
    }

    requested = [str(s) for s in (symbols or []) if str(s).strip()]
    if not valid:
        return False, "No market data returned from provider", {}

    if require_symbol and require_symbol in requested and require_symbol not in valid:
        return False, f"Required benchmark symbol missing market data: {require_symbol}", valid

    coverage = (float(len(valid)) / float(len(requested))) if requested else 1.0
    if coverage < float(min_coverage_ratio):
        return (
            False,
            f"Market data coverage too low: {len(valid)}/{len(requested)} ({coverage:.1%}) < {float(min_coverage_ratio):.1%}",
            valid,
        )

    return True, "", valid


def compute_orders(current_weights: Dict[str, float], target_weights: Dict[str, float], prices: Dict[str, float], equity_usdt: float):
    orders = []
    for sym, tw in target_weights.items():
        cw = float(current_weights.get(sym, 0.0))
        delta = float(tw) - cw
        if abs(delta) < 1e-6:
            continue
        side = "buy" if delta > 0 else "sell"
        notional = abs(delta) * float(equity_usdt)
        px = float(prices.get(sym, 0.0) or 0.0)
        if px <= 0:
            continue
        intent = "REBALANCE"
        if cw <= 0 and delta > 0:
            intent = "OPEN_LONG"
        elif tw <= 0 and delta < 0:
            intent = "CLOSE_LONG"
        orders.append(Order(symbol=sym, side=side, intent=intent, notional_usdt=float(notional), signal_price=px, meta={"target_w": tw, "current_w": cw}))
    return orders


def _merge_managed_symbols(base_symbols: list[str], held_symbols: list[str]) -> list[str]:
    base = [str(s) for s in (base_symbols or []) if str(s).strip()]
    held = [str(s) for s in (held_symbols or []) if str(s).strip()]
    return list(dict.fromkeys(base + held))


def _merge_event_close_override_orders(
    *,
    orders: list[Order],
    positions,
    prices: dict[str, float],
    run_id: str,
    order_store_path: str | os.PathLike[str] | None = None,
    audit=None,
) -> list[Order]:
    override_actions = consume_event_actions_for_run(run_id=run_id, order_store_path=order_store_path)
    if not override_actions:
        return list(orders or [])

    held_map = {
        str(getattr(p, "symbol", "") or ""): p
        for p in (positions or [])
        if float(getattr(p, "qty", 0.0) or 0.0) > 0.0
    }
    existing_close_symbols = {
        str(getattr(o, "symbol", "") or "")
        for o in (orders or [])
        if str(getattr(o, "side", "")).lower() == "sell"
        and str(getattr(o, "intent", "")).upper() == "CLOSE_LONG"
    }

    appended: list[Order] = []
    skipped: list[str] = []
    for action in override_actions:
        symbol = str(action.get("symbol") or "").strip()
        if not symbol or symbol in existing_close_symbols:
            continue
        pos = held_map.get(symbol)
        qty = float(getattr(pos, "qty", 0.0) or 0.0) if pos is not None else 0.0
        px = float(prices.get(symbol, 0.0) or 0.0)
        if pos is None or qty <= 0.0 or px <= 0.0:
            skipped.append(symbol)
            continue

        appended.append(
            Order(
                symbol=symbol,
                side="sell",
                intent="CLOSE_LONG",
                notional_usdt=float(qty * px),
                signal_price=float(px),
                meta={
                    "source": "event_driven_override",
                    "reason": str(action.get("reason") or "event_close"),
                    "event_type": str(action.get("event_type") or ""),
                    "priority": 0,
                },
            )
        )
        existing_close_symbols.add(symbol)

    if audit is not None:
        audit.add_note(
            f"event close override: loaded={len(override_actions)} appended={len(appended)} skipped={skipped}"
        )

    return list(orders or []) + appended


def _apply_live_preflight_order_restrictions(
    *,
    orders: list[Order],
    live_preflight_result,
    audit=None,
    log=None,
) -> list[Order]:
    decision = str(getattr(live_preflight_result, "decision", "") or "").upper()
    if decision != "SELL_ONLY":
        return list(orders or [])

    filtered = [
        order
        for order in (orders or [])
        if not (
            str(getattr(order, "side", "")).lower() == "buy"
            and str(getattr(order, "intent", "")).upper() in {"OPEN_LONG", "REBALANCE"}
        )
    ]
    removed = len(list(orders or [])) - len(filtered)
    if removed > 0:
        reason = str(getattr(live_preflight_result, "reason", "") or "sell_only")
        message = f"live preflight sell-only filtered buy orders: removed={removed} reason={reason}"
        if log is not None:
            log.warning(message)
        if audit is not None:
            audit.add_note(message)
    return filtered


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _coalesce(value, default):
    return default if value is None else value


def _log_order_arbitration_summary(log, cfg: AppConfig, message: str) -> None:
    mode = str(getattr(getattr(cfg, "execution", None), "mode", "dry_run") or "dry_run").lower()
    if mode == "live":
        log.warning(message)
    else:
        log.info(message)


def _resolve_runtime_run_paths(cfg: AppConfig, run_id: str) -> dict[str, Path]:
    order_store_path = str(
        getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite")
        or "reports/orders.sqlite"
    )
    reports_dir = derive_runtime_reports_dir(order_store_path)
    runs_dir = derive_runtime_runs_dir(order_store_path)
    run_dir = runs_dir / str(run_id)
    return {
        "reports_dir": reports_dir,
        "runs_dir": runs_dir,
        "run_dir": run_dir,
        "spread_snapshots_dir": derive_runtime_spread_snapshots_dir(order_store_path),
    }


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return path


def _live_symbol_whitelist(cfg: AppConfig) -> list[str]:
    mode = str(getattr(getattr(cfg, "execution", None), "mode", "dry_run") or "dry_run").lower()
    if mode != "live":
        return []
    return list(dict.fromkeys(str(sym).strip() for sym in (cfg.symbols or []) if str(sym).strip()))


BTC_LEADERSHIP_PROBE_CONFIG_KEYS = (
    "btc_leadership_probe_enabled",
    "btc_leadership_probe_only_in_protect",
    "btc_leadership_probe_target_w",
    "btc_leadership_probe_dynamic_sizing_enabled",
    "btc_leadership_probe_max_target_w",
    "btc_leadership_probe_cooldown_hours",
    "btc_leadership_probe_lookback_hours",
    "btc_leadership_probe_breakout_buffer_bps",
    "btc_leadership_probe_min_alpha6_score",
    "btc_leadership_probe_min_f5_rsi",
    "btc_leadership_probe_min_f4_volume",
    "btc_leadership_probe_require_regime_not_risk_off",
    "btc_leadership_probe_allow_single_negative_cycle_bypass",
    "btc_leadership_probe_max_negative_cycles_to_bypass",
    "btc_leadership_probe_min_net_expectancy_bps_to_bypass",
    "btc_leadership_probe_time_stop_hours",
)


PROBE_EXIT_CONFIG_KEYS = (
    "probe_exit_enabled",
    "probe_take_profit_net_bps",
    "probe_stop_loss_net_bps",
    "probe_trailing_enable_after_net_bps",
    "probe_trailing_gap_bps",
    "probe_time_stop_hours",
    "probe_time_stop_min_net_bps",
    "probe_ignore_normal_zero_target_close",
    "probe_min_hold_hours_before_zero_target_close",
    "probe_allow_zero_target_close_on_risk_off",
    "probe_allow_zero_target_close_on_hard_negative_expectancy",
)


PROTECT_PROFIT_LOCK_CONFIG_KEYS = (
    "protect_profit_lock_enabled",
    "protect_profit_lock_min_net_bps",
    "protect_profit_lock_breakeven_plus_bps",
    "protect_profit_lock_trailing_start_net_bps",
    "protect_profit_lock_trailing_gap_bps",
    "protect_profit_lock_strong_start_net_bps",
    "protect_profit_lock_strong_trailing_gap_bps",
)


SAME_SYMBOL_REENTRY_GUARD_CONFIG_KEYS = (
    "same_symbol_reentry_guard_enabled",
    "same_symbol_reentry_cooldown_hours_after_profit_lock",
    "same_symbol_reentry_cooldown_hours_after_probe_stop",
    "same_symbol_reentry_cooldown_hours_after_probe_take_profit",
    "same_symbol_reentry_allow_breakout",
    "same_symbol_reentry_breakout_above_last_high_bps",
    "same_symbol_reentry_breakout_above_exit_bps",
    "same_symbol_reentry_apply_to_market_impulse_probe",
    "same_symbol_reentry_apply_to_btc_leadership_probe",
    "same_symbol_reentry_apply_to_normal_entry",
)


def _btc_leadership_probe_effective_config(cfg: AppConfig) -> Dict[str, Any]:
    execution = getattr(cfg, "execution", None)
    return {
        key: getattr(execution, key)
        for key in BTC_LEADERSHIP_PROBE_CONFIG_KEYS
        if hasattr(execution, key)
    }


def _probe_exit_effective_config(cfg: AppConfig) -> Dict[str, Any]:
    execution = getattr(cfg, "execution", None)
    return {
        key: getattr(execution, key)
        for key in PROBE_EXIT_CONFIG_KEYS
        if hasattr(execution, key)
    }


def _protect_profit_lock_effective_config(cfg: AppConfig) -> Dict[str, Any]:
    execution = getattr(cfg, "execution", None)
    return {
        key: getattr(execution, key)
        for key in PROTECT_PROFIT_LOCK_CONFIG_KEYS
        if hasattr(execution, key)
    }


def _same_symbol_reentry_guard_effective_config(cfg: AppConfig) -> Dict[str, Any]:
    execution = getattr(cfg, "execution", None)
    return {
        key: getattr(execution, key)
        for key in SAME_SYMBOL_REENTRY_GUARD_CONFIG_KEYS
        if hasattr(execution, key)
    }


def _negative_expectancy_state_path_for_config(cfg: AppConfig) -> Path:
    execution = getattr(cfg, "execution", None)
    order_store_path = str(getattr(execution, "order_store_path", "reports/orders.sqlite") or "reports/orders.sqlite")
    raw_state_path = str(
        getattr(execution, "negative_expectancy_state_path", "reports/negative_expectancy_cooldown.json")
        or ""
    ).strip()
    if not raw_state_path or raw_state_path == "reports/negative_expectancy_cooldown.json":
        return derive_runtime_named_json_path(order_store_path, "negative_expectancy_cooldown").resolve()
    state_path = Path(raw_state_path)
    if not state_path.is_absolute():
        state_path = (PROJECT_ROOT / state_path).resolve()
    return state_path


def _coerce_release_start_ts(raw: Any) -> Optional[int]:
    if raw is None or raw == "" or str(raw).strip() == "not_observable":
        return None
    try:
        value = int(float(raw))
    except Exception:
        return None
    return value if value > 0 else None


def _negative_expectancy_feedback_enabled(cfg: AppConfig) -> bool:
    execution = getattr(cfg, "execution", None)
    return any(
        [
            bool(getattr(execution, "negative_expectancy_cooldown_enabled", False)),
            bool(getattr(execution, "negative_expectancy_score_penalty_enabled", False)),
            bool(getattr(execution, "negative_expectancy_open_block_enabled", False)),
            bool(getattr(execution, "negative_expectancy_fast_fail_open_block_enabled", False)),
        ]
    )


def _ensure_negative_expectancy_release_start_ts(cfg: AppConfig, state_path: Path) -> Dict[str, Any]:
    if not _negative_expectancy_feedback_enabled(cfg):
        return {}

    current_fp = negative_expectancy_config_fingerprint(cfg)
    if not current_fp:
        return {
            "warnings": [
                "negative_expectancy_release_start_ts_not_observable: config_fingerprint is missing"
            ]
        }

    payload: Dict[str, Any] = {}
    if state_path.exists():
        try:
            loaded = json.loads(state_path.read_text(encoding="utf-8"))
            payload = loaded if isinstance(loaded, dict) else {}
        except Exception as exc:
            return {"warnings": [f"negative_expectancy_state_read_error: {exc}"]}

    cached_fp = str(payload.get("config_fingerprint") or "").strip()
    release_start_ts = _coerce_release_start_ts(payload.get("release_start_ts"))
    warnings = [
        str(item)
        for item in (payload.get("warnings") or [])
        if str(item).strip()
        and "negative_expectancy_release_start_ts_not_observable" not in str(item)
    ]
    now_ms = int(time.time() * 1000)
    changed = False

    if cached_fp != current_fp:
        payload["config_fingerprint"] = current_fp
        payload["release_start_ts"] = now_ms
        payload["release_start_ts_status"] = "ok"
        payload["symbols"] = {}
        payload["stats"] = {}
        changed = True
    elif release_start_ts is None:
        warnings.append(
            "negative_expectancy_release_start_ts_recovered: "
            "cached release_start_ts is missing, zero, or not_observable; initialized before effective config write"
        )
        payload["release_start_ts"] = now_ms
        payload["release_start_ts_status"] = "recovered"
        payload["symbols"] = {}
        payload["stats"] = {}
        changed = True

    if changed:
        payload["updated_ts_ms"] = now_ms
        payload["lookback_hours"] = int(getattr(cfg.execution, "negative_expectancy_lookback_hours", 24) or 24)
        payload["cooldown_hours"] = int(getattr(cfg.execution, "negative_expectancy_cooldown_hours", 24) or 24)
        payload["warnings"] = warnings
        payload.setdefault("symbols", {})
        payload.setdefault("stats", {})
        _atomic_write_json(state_path, payload)

    return payload


def _negative_expectancy_release_effective_config(cfg: AppConfig) -> Dict[str, Any]:
    state_path = _negative_expectancy_state_path_for_config(cfg)
    ensured_payload = _ensure_negative_expectancy_release_start_ts(cfg, state_path)
    base: Dict[str, Any] = {
        "negative_expectancy_state_path": str(state_path),
        "negative_expectancy_release_start_ts": "not_observable",
        "negative_expectancy_release_start_ts_status": "not_observable",
        "negative_expectancy_release_start_ts_warning": "negative_expectancy_state_missing",
    }
    if ensured_payload and not state_path.exists():
        base["negative_expectancy_release_start_ts_warning"] = "; ".join(
            [str(item) for item in (ensured_payload.get("warnings") or []) if str(item).strip()]
        ) or "negative_expectancy_state_missing"
        return base
    if not state_path.exists():
        return base
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        base["negative_expectancy_release_start_ts_warning"] = f"negative_expectancy_state_read_error: {exc}"
        return base
    if not isinstance(payload, dict):
        base["negative_expectancy_release_start_ts_warning"] = "negative_expectancy_state_not_object"
        return base

    release_start_ts = payload.get("release_start_ts")
    if isinstance(release_start_ts, str) and release_start_ts.strip() == "not_observable":
        base["negative_expectancy_release_start_ts_warning"] = "; ".join(
            [str(item) for item in (payload.get("warnings") or []) if str(item).strip()]
        ) or "negative_expectancy_release_start_ts_not_observable"
        return base
    try:
        release_start_ts_int = int(float(release_start_ts))
    except Exception:
        release_start_ts_int = 0
    if release_start_ts_int > 0:
        base["negative_expectancy_release_start_ts"] = release_start_ts_int
        base["negative_expectancy_release_start_ts_status"] = str(payload.get("release_start_ts_status") or "ok")
        base["negative_expectancy_release_start_ts_warning"] = "; ".join(
            [str(item) for item in (payload.get("warnings") or []) if str(item).strip()]
        )
        return base

    base["negative_expectancy_release_start_ts_warning"] = "negative_expectancy_release_start_ts_missing_or_zero"
    return base


def _effective_live_config_payload(cfg: AppConfig) -> Dict[str, Any]:
    return {
        "symbols": list(cfg.symbols or []),
        "universe": {
            "enabled": bool(getattr(cfg.universe, "enabled", False)),
            "use_universe_symbols": bool(getattr(cfg.universe, "use_universe_symbols", False)),
        },
        "alpha": {
            "alpha158_overlay": {
                "enabled": bool(getattr(getattr(cfg.alpha, "alpha158_overlay", None), "enabled", False)),
            },
            "long_top_pct": float(getattr(cfg.alpha, "long_top_pct", 0.0) or 0.0),
            "min_score_threshold": float(getattr(cfg.alpha, "min_score_threshold", 0.0) or 0.0),
        },
        "execution": {
            "fee_bps": float(getattr(cfg.execution, "fee_bps", 0.0) or 0.0),
            "cost_aware_roundtrip_cost_bps": (
                None
                if getattr(cfg.execution, "cost_aware_roundtrip_cost_bps", None) is None
                else float(getattr(cfg.execution, "cost_aware_roundtrip_cost_bps", 0.0) or 0.0)
            ),
            "rank_exit_max_rank": int(getattr(cfg.execution, "rank_exit_max_rank", 0) or 0),
            "rank_exit_confirm_rounds": int(getattr(cfg.execution, "rank_exit_confirm_rounds", 0) or 0),
            "rank_exit_strict_mode": bool(getattr(cfg.execution, "rank_exit_strict_mode", False)),
            "min_hold_minutes_before_rank_exit": int(
                getattr(cfg.execution, "min_hold_minutes_before_rank_exit", 0) or 0
            ),
            "min_hold_minutes_before_regime_exit": int(
                getattr(cfg.execution, "min_hold_minutes_before_regime_exit", 0) or 0
            ),
            "max_rebalance_turnover_per_cycle": (
                None
                if getattr(cfg.execution, "max_rebalance_turnover_per_cycle", None) is None
                else float(getattr(cfg.execution, "max_rebalance_turnover_per_cycle", 0.0) or 0.0)
            ),
            "protect_entry_require_alpha6_confirmation": bool(
                getattr(cfg.execution, "protect_entry_require_alpha6_confirmation", True)
            ),
            "protect_entry_block_trend_only": bool(
                getattr(cfg.execution, "protect_entry_block_trend_only", True)
            ),
            "protect_entry_require_alpha6_rsi_confirm_positive": bool(
                getattr(cfg.execution, "protect_entry_require_alpha6_rsi_confirm_positive", True)
            ),
            "protect_entry_alpha6_min_score": float(
                getattr(cfg.execution, "protect_entry_alpha6_min_score", 0.10) or 0.0
            ),
            **_btc_leadership_probe_effective_config(cfg),
            **_probe_exit_effective_config(cfg),
            **_protect_profit_lock_effective_config(cfg),
            **_same_symbol_reentry_guard_effective_config(cfg),
            **_negative_expectancy_release_effective_config(cfg),
        },
    }


def _write_effective_live_config(cfg: AppConfig) -> Path:
    order_store_path = str(
        getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite")
        or "reports/orders.sqlite"
    )
    out_path = derive_runtime_named_json_path(order_store_path, "effective_live_config")
    return _atomic_write_json(out_path, _effective_live_config_payload(cfg))


def _audit_and_raise(audit, runtime_run_dir: Path, reject_reason: str, message: str) -> None:
    if audit is not None:
        audit.reject(reject_reason)
        audit.add_note(message)
        audit.save(str(runtime_run_dir))
    raise RuntimeError(message)


def _validate_live_contract(cfg: AppConfig) -> list[str]:
    whitelist = _live_symbol_whitelist(cfg)
    if not whitelist:
        return []

    errors: list[str] = []
    if bool(getattr(cfg.universe, "use_universe_symbols", False)):
        errors.append(
            "universe.use_universe_symbols must be false in live mode when top-level symbols whitelist is enforced"
        )
    if not bool(getattr(cfg.execution, "preflight_enabled", True)):
        errors.append("execution.preflight_enabled must be true in live mode")
    return errors


def _assert_live_symbol_subset(
    symbols: list[str],
    allowed_symbols: list[str],
    *,
    stage: str,
    audit,
    runtime_run_dir: Path,
) -> None:
    allowed = {str(sym) for sym in (allowed_symbols or []) if str(sym).strip()}
    unexpected = sorted({str(sym) for sym in (symbols or []) if str(sym).strip() and str(sym) not in allowed})
    if not unexpected:
        return
    _audit_and_raise(
        audit,
        runtime_run_dir,
        "live_whitelist_violation",
        f"live whitelist violation at {stage}: unexpected={unexpected} allowed={sorted(allowed)}",
    )


def _run_live_preflight_or_raise(
    cfg: AppConfig,
    *,
    store,
    acc_store,
    audit,
    runtime_run_dir: Path,
):
    mode = str(getattr(getattr(cfg, "execution", None), "mode", "dry_run") or "dry_run").lower()
    if mode != "live":
        return None, None

    from src.execution.live_preflight import LivePreflight
    from src.execution.okx_private_client import OKXPrivateClient

    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        runtime_order_store_path = str(
            getattr(cfg.execution, "order_store_path", "reports/orders.sqlite") or "reports/orders.sqlite"
        )
        pf = LivePreflight(
            cfg.execution,
            okx=client,
            position_store=store,
            account_store=acc_store,
            bills_db_path=str(
                derive_runtime_named_artifact_path(runtime_order_store_path, "bills", ".sqlite")
            ),
            ledger_state_path=str(
                derive_runtime_named_json_path(runtime_order_store_path, "ledger_state")
            ),
            ledger_status_path=str(
                derive_runtime_named_json_path(runtime_order_store_path, "ledger_status")
            ),
            reconcile_status_path=str(
                derive_runtime_named_json_path(runtime_order_store_path, "reconcile_status")
                if str(getattr(cfg.execution, "reconcile_status_path", "") or "").strip()
                in {"", "reports/reconcile_status.json"}
                else getattr(cfg.execution, "reconcile_status_path")
            ),
        )
        res = pf.run(
            max_pages=int(getattr(cfg.execution, "preflight_max_pages", 5)),
            max_status_age_sec=int(getattr(cfg.execution, "max_status_age_sec", 180)),
        )
        if audit is not None:
            audit.add_note(f"LIVE_PREFLIGHT decision={res.decision} reason={res.reason}")
        decision = str(res.decision).upper()
        fail_action = str(getattr(cfg.execution, "preflight_fail_action", "sell_only") or "sell_only").lower()
        if decision == "ALLOW":
            return client, res
        if decision == "SELL_ONLY" and fail_action in {"sell_only", "allow"}:
            if audit is not None:
                audit.add_note(f"LIVE_PREFLIGHT continue decision={decision} fail_action={fail_action}")
            return client, res
        if decision != "ALLOW":
            _audit_and_raise(
                audit,
                runtime_run_dir,
                "live_preflight_blocked",
                f"live preflight blocked before routing: decision={res.decision} reason={res.reason}",
            )
        return client, res
    except RuntimeError:
        raise
    except Exception as e:
        _audit_and_raise(audit, runtime_run_dir, "live_preflight_failed", f"live preflight failed: {e}")
    return None, None


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    cfg_path = os.getenv("V5_CONFIG")
    if not cfg_path:
        for candidate in ("configs/live_prod.yaml", "configs/config.yaml"):
            candidate_path = repo_root / candidate
            if candidate_path.exists():
                cfg_path = str(candidate_path)
                break
        else:
            cfg_path = str(repo_root / "configs/live_prod.yaml")
    cfg = load_config(cfg_path, env_path=str(repo_root / ".env"))
    setup_logging()
    log = logging.getLogger("v5")

    # Optional: dynamic alpha weights (computed offline from alpha_history.db)
    # Enable with env: V5_DYNAMIC_ALPHA_WEIGHTS=YES
    try:
        if str(os.getenv("V5_DYNAMIC_ALPHA_WEIGHTS") or "").upper() == "YES":
            p = _resolve_dynamic_alpha_weights_path(cfg)
            if p.exists():
                obj = json.loads(p.read_text(encoding="utf-8"))
                w = (obj.get("weights") or {}) if isinstance(obj, dict) else {}
                if isinstance(w, dict) and w:
                    normalized_w = normalize_alpha_base_factor_mapping(
                        w,
                        context=f"dynamic alpha weights override ({p.name})",
                        output="schema",
                    )
                    for k, v in normalized_w.items():
                        if hasattr(cfg.alpha.weights, k):
                            setattr(cfg.alpha.weights, k, float(v))
                    log.info(f"Dynamic alpha weights loaded: {normalized_w}")
    except ValueError:
        raise
    except Exception as e:
        log.warning(f"Dynamic alpha weights load failed: {e}")

    run_id = os.getenv("V5_RUN_ID")
    if not run_id:
        run_id = _utc_now().strftime("%Y%m%d_%H%M%S")
    runtime_paths = _resolve_runtime_run_paths(cfg, run_id)
    runtime_reports_dir = runtime_paths["reports_dir"]
    runtime_run_dir = runtime_paths["run_dir"]
    runtime_spread_snapshots_dir = runtime_paths["spread_snapshots_dir"]
    runtime_budget_state_dir = runtime_reports_dir / "budget_state"
    runtime_reports_dir.mkdir(parents=True, exist_ok=True)

    # 鍒涘缓DecisionAudit锛堥渶瑕佸厛瀹氫箟run_id锛?
    from src.reporting.decision_audit import DecisionAudit
    
    window_start_ts = _get_env_epoch_sec("V5_WINDOW_START_TS")
    window_end_ts = _get_env_epoch_sec("V5_WINDOW_END_TS")
    
    audit = DecisionAudit(
        run_id=run_id,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
    )

    live_whitelist = _live_symbol_whitelist(cfg)
    if live_whitelist:
        effective_cfg_path = _write_effective_live_config(cfg)
        audit.add_note(f"effective live config written: {effective_cfg_path}")
        contract_errors = _validate_live_contract(cfg)
        if contract_errors:
            _audit_and_raise(
                audit,
                runtime_run_dir,
                "live_config_contract",
                "live config contract violation: " + "; ".join(contract_errors),
            )

    # load persisted positions/account early so current holdings always stay in managed market-data scope
    order_store_path = str(getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite"))
    positions_db_path = str(derive_position_store_path(order_store_path))
    store = PositionStore(path=positions_db_path)
    acc_store = AccountStore(path=positions_db_path)
    held = store.list()
    held_symbols = [
        str(getattr(p, "symbol", "") or "")
        for p in held
        if float(getattr(p, "qty", 0.0) or 0.0) > 0.0
    ]
    if live_whitelist:
        _assert_live_symbol_subset(
            held_symbols,
            live_whitelist,
            stage="held_symbols",
            audit=audit,
            runtime_run_dir=runtime_run_dir,
        )

    live_okx_client = None
    live_preflight_result = None
    if live_whitelist:
        live_okx_client, live_preflight_result = _run_live_preflight_or_raise(
            cfg,
            store=store,
            acc_store=acc_store,
            audit=audit,
            runtime_run_dir=runtime_run_dir,
        )

    provider = build_provider(cfg)

    symbols = list(live_whitelist or cfg.symbols)
    # Optional: dynamic universe
    if cfg.universe.enabled and not live_whitelist:
        try:
            from src.data.universe.okx_universe import OKXUniverseProvider

            up = OKXUniverseProvider(
                cache_path=str(_resolve_universe_cache_path(cfg)),
                cache_ttl_sec=cfg.universe.cache_ttl_sec,
                top_n=int(getattr(cfg.universe, "top_n_market_cap", 30) or 30),
                min_24h_quote_volume_usdt=cfg.universe.min_24h_quote_volume_usdt,
                max_spread_bps=getattr(cfg.universe, "max_spread_bps", None),
                blacklist_path=cfg.universe.blacklist_path,
                exclude_stablecoins=cfg.universe.exclude_stablecoins,
                exclude_symbols=list(getattr(cfg.universe, "exclude_symbols", []) or []),
                refine_with_single_ticker=bool(getattr(cfg.universe, "refine_with_single_ticker", False)),
                refine_single_ticker_max_candidates=int(getattr(cfg.universe, "refine_single_ticker_max_candidates", 200) or 200),
                refine_single_ticker_sleep_sec=float(getattr(cfg.universe, "refine_single_ticker_sleep_sec", 0.02) or 0.0),
            )
            uni = up.get_universe()
            if cfg.universe.use_universe_symbols and uni:
                inc = [str(s) for s in (getattr(cfg.universe, "include_symbols", []) or [])]
                merged = list(dict.fromkeys(inc + list(uni)))
                symbols = merged
                log.info(f"Universe enabled: using {len(symbols)} symbols (include={len(inc)})")
        except Exception as e:
            log.warning(f"Universe fetch failed, fallback to config symbols: {e}")
    elif live_whitelist and cfg.universe.enabled:
        audit.add_note("live whitelist active: dynamic universe fetch skipped")

    scored_symbols = [str(s) for s in (live_whitelist or symbols or []) if str(s).strip()]
    managed_symbols = _merge_managed_symbols(scored_symbols, held_symbols)
    if live_whitelist:
        _assert_live_symbol_subset(
            managed_symbols,
            live_whitelist,
            stage="managed_symbols",
            audit=audit,
            runtime_run_dir=runtime_run_dir,
        )
    if len(managed_symbols) > len(scored_symbols):
        added = [sym for sym in managed_symbols if sym not in set(scored_symbols)]
        log.info(
            "Managed universe expanded with held positions: scored=%d managed=%d added=%s",
            len(scored_symbols),
            len(managed_symbols),
            added,
        )
        audit.add_note(
            f"managed universe expanded with held positions: scored={len(scored_symbols)} "
            f"managed={len(managed_symbols)} added={added}"
        )
    
    # 璁板綍universe閰嶇疆鍒癮udit
    audit.universe_config = {
        "enabled": cfg.universe.enabled,
        "use_universe_symbols": cfg.universe.use_universe_symbols,
        "config_symbols_count": len(cfg.symbols),
        "actual_symbols_count": len(scored_symbols),
        "actual_symbols_sample": scored_symbols[:10] if scored_symbols else [],
        "managed_symbols_count": len(managed_symbols),
        "managed_symbols_sample": managed_symbols[:10] if managed_symbols else [],
        "held_symbols_count": len(held_symbols),
        "held_symbols_sample": held_symbols[:10] if held_symbols else [],
        "whitelist_enforced": bool(live_whitelist),
        "whitelist_symbols": list(live_whitelist),
    }

    # fetch 1h bars for alpha/regime and 4h for auxiliary (placeholder)
    # 浣跨敤绐楀彛鏃堕棿杩囨护锛屽彧鍙栧凡鏀剁洏bar
    window_start_ts = _get_env_epoch_sec("V5_WINDOW_START_TS")
    window_end_ts = _get_env_epoch_sec("V5_WINDOW_END_TS")
    
    end_ts_ms = None
    if window_end_ts is not None:
        end_ts_ms = window_end_ts * 1000  # 杞崲涓烘绉?
    
    md_1h = provider.fetch_ohlcv(
        managed_symbols,
        timeframe=cfg.timeframe_main,
        limit=24 * 60,
        end_ts_ms=end_ts_ms,
    )

    ok_md, md_reason, md_1h = _validate_market_data_snapshot(
        symbols=scored_symbols,
        market_data=md_1h,
        require_symbol="BTC/USDT" if bool(getattr(cfg.universe, "require_btc_benchmark", True)) else None,
        min_coverage_ratio=float(_coalesce(getattr(cfg.universe, "min_data_coverage_ratio", None), 0.80)),
    )
    if not ok_md:
        log.error(md_reason)
        audit.reject("market_data_coverage_insufficient")
        audit.add_note(md_reason)
        audit.save(str(runtime_run_dir))
        return
    scored_available = len([sym for sym in scored_symbols if sym in md_1h])
    if scored_available < len(scored_symbols):
        log.warning(
            "Market data partial coverage: %d/%d scored symbols available",
            scored_available,
            len(scored_symbols),
        )
        audit.add_note(f"market data partial coverage: {scored_available}/{len(scored_symbols)}")
    missing_held_symbols = [sym for sym in held_symbols if sym not in md_1h]
    if missing_held_symbols:
        log.warning("Held symbols missing managed market data: %s", missing_held_symbols)
        audit.add_note(f"held symbols missing managed market data: {missing_held_symbols}")

    from src.core.pipeline import V5Pipeline

    pipe = V5Pipeline(cfg, data_provider=provider)
    _prime_pipeline_run_context(pipe, run_id)

    alpha_market_data = {sym: md_1h[sym] for sym in scored_symbols if sym in md_1h}

    # regime from BTC (market data validated above)
    btc = alpha_market_data.get("BTC/USDT")
    if btc is None:
        if bool(getattr(cfg.universe, "require_btc_benchmark", True)):
            raise RuntimeError("BTC/USDT missing after market-data validation")
        btc = next(iter(alpha_market_data.values()))

    # ========== 瓒嬪娍缂撳瓨锛氫繚瀛樻垨璇诲彇 ==========
    is_trend_update_only = str(os.getenv("V5_TREND_UPDATE_ONLY") or "").upper() == "1"
    use_cached_trend = str(os.getenv("V5_USE_CACHED_TREND") or "").upper() == "1"
    trend_cache_order_store_path = str(
        getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite")
    )

    cached = None
    if use_cached_trend and not is_trend_update_only:
        cached = load_trend_cache(max_age_sec=300, order_store_path=trend_cache_order_store_path)

    if cached:
        log.info("[TrendCache] Using cached trend data")
        alpha_snap = TrendCacheAlphaSnapshot(cached)
        regime = TrendCacheRegimeResult(cached, cfg)
    else:
        regime = pipe.regime_engine.detect(btc)
        pipe.alpha_engine.set_regime_context(
            regime.state.value if hasattr(regime.state, "value") else regime.state
        )
        alpha_snap = pipe.alpha_engine.compute_snapshot(alpha_market_data)

    if is_trend_update_only:
        save_trend_cache(
            alpha_snap,
            regime,
            scored_symbols,
            order_store_path=trend_cache_order_store_path,
        )
        log.info("[TrendUpdate] Trend cache saved, exiting (V5_TREND_UPDATE_ONLY=1)")
        return

    if use_cached_trend and not cached:
        log.warning("[TrendCache] No valid cache found, using freshly computed trend")
    # ========== 瓒嬪娍缂撳瓨缁撴潫 ==========

    acc = acc_store.get()
    held = store.list()
    # Mark-to-market at cycle start
    now_ts = _utc_now().isoformat().replace("+00:00", "Z")
    prices = {s: float(md_1h[s].close[-1]) for s in md_1h.keys() if md_1h[s].close}
    for p in held:
        s = md_1h.get(p.symbol)
        if not s or not s.close:
            continue
        store.mark_position(p.symbol, now_ts=now_ts, mark_px=float(s.close[-1]), high_px=float(s.high[-1]) if s.high else float(s.close[-1]))

    held = store.list()
    try:
        pruned = store.prune_orphan_risk_state()
        if pruned:
            log.info("PRUNED_ORPHAN_RISK_STATE: %s", pruned)
            audit.add_note(f"Orphan risk state pruned: {pruned}")
    except Exception as e:
        log.warning("orphan risk state prune failed: %s", e)

    # Run unified pipeline with equity/drawdown scaling
    from src.core.run_logger import RunLogger

    run_logger = RunLogger(run_dir=str(runtime_run_dir))

    # F3.1 pre-run budget action input: load today's budget state (UTC) and set audit.budget
    try:
        from src.reporting.budget_state import _utc_yyyymmdd_from_epoch_sec, load_budget_state

        ts_for_day = window_end_ts or window_start_ts
        if ts_for_day is not None:
            ymd = _utc_yyyymmdd_from_epoch_sec(int(ts_for_day))
            st = load_budget_state(str(runtime_budget_state_dir / f"{ymd}.json"))
            if st is not None:
                audit.budget = {
                    "ymd_utc": ymd,
                    "avg_equity_est": st.avg_equity_est,
                    "turnover_used": st.turnover_used,
                    "turnover_used_usdt": st.turnover_used_usdt(),
                    "turnover_used_ratio": st.turnover_used_ratio(),
                    "turnover_budget_per_day": st.turnover_budget_per_day,
                    "turnover_budget_ratio": st.turnover_budget_ratio(),
                    "turnover_budget_usdt": st.turnover_budget_usdt(),
                    "turnover_exceeded": st.turnover_exceeded(),
                    "cost_used_usdt": st.cost_used_usdt,
                    "cost_used_bps": st.cost_used_bps(),
                    "cost_budget_bps_per_day": st.cost_budget_bps_per_day,
                    "cost_exceeded": st.cost_exceeded(),
                    "exceeded": st.exceeded(),
                    "reason": st.reason(),
                    "fills_count_today": st.fills_count_today,
                    "median_notional_usdt_today": st.median_notional_usdt_today,
                    "small_trade_ratio_today": st.small_trade_ratio_today,
                    "small_trade_notional_cutoff": st.small_trade_notional_cutoff,
                }
    except Exception:
        pass
    
    # 璁板綍universe鏁伴噺
    audit.counts["universe"] = len(scored_symbols)

    # Sanity-check equity peak: if an old corrupted peak is orders-of-magnitude above current equity,
    # it will permanently trigger drawdown throttle (DD multiplier). Clamp it.
    try:
        eq_now = float(acc.cash_usdt)
        for p in held:
            s = md_1h.get(p.symbol)
            if s and s.close:
                eq_now += float(p.qty) * float(s.close[-1])
        peak = float(acc.equity_peak_usdt or 0.0)
        
        # 鍔ㄦ€侀槇鍊硷細鍩轰簬閰嶇疆鐨勮祫閲戣妯?
        # 灏忚祫閲戣处鎴凤紙<100U锛変娇鐢ㄦ洿鏁忔劅鐨勯槇鍊?
        equity_cap = float(getattr(cfg.budget, 'live_equity_cap_usdt', 0) or eq_now)
        min_equity_threshold = min(100.0, equity_cap * 0.5) if equity_cap > 0 else 100.0
        ratio_threshold = 3.0 if equity_cap < 100 else 5.0  # 灏忚祫閲戠敤3鍊嶏紝澶ц祫閲戠敤5鍊?
        
        if peak > min_equity_threshold and peak > eq_now * ratio_threshold and eq_now > 0:
            log.warning(f"equity_peak_usdt seems corrupted: peak={peak} >> equity={eq_now} (cap={equity_cap}); clamping peak to equity")
            acc.equity_peak_usdt = float(eq_now)
            acc_store.set(acc)
    except Exception as e:
        log.debug("equity peak check skipped: %s", e)

    # ========== 棰勭畻闄愬埗妫€鏌ワ紙鍙紑鍏筹級==========
    try:
        budget_enabled = bool(getattr(cfg.budget, 'action_enabled', True))
        cap_raw = getattr(cfg.budget, 'live_equity_cap_usdt', None)
        equity_cap = float(cap_raw) if cap_raw is not None else None

        # 鑾峰彇瀹炴椂鏉冪泭锛堢洿鎺ヤ粠OKX API锛屼笉渚濊禆鏈湴缂撳瓨锛?
        eq_now = get_live_equity_from_okx()

        if eq_now is None:
            log.error("Failed to fetch live equity from OKX; skipping budget check")
        elif not budget_enabled:
            cap_txt = f"{equity_cap:.2f}" if (equity_cap is not None) else "None"
            log.warning(f"鈿狅笍 Budget action disabled: equity={eq_now:.2f} cap={cap_txt}, skip budget blocking")
            audit.budget = getattr(audit, 'budget', {}) or {}
            audit.budget['equity_cap_usdt'] = equity_cap
            audit.budget['current_equity_usdt'] = eq_now
            audit.budget['utilization_pct'] = (eq_now / equity_cap * 100.0) if (equity_cap is not None and equity_cap > 0) else None
            audit.budget['action_enabled'] = False
        else:
            # If cap is not configured, skip blocking but keep observability
            if equity_cap is None or equity_cap <= 0:
                log.info(
                    f"Budget equity cap not configured; skip cap blocking and keep daily turnover/cost budget actions "
                    f"(equity={eq_now:.2f})"
                )
                audit.budget = getattr(audit, 'budget', {}) or {}
                audit.budget['equity_cap_usdt'] = equity_cap
                audit.budget['current_equity_usdt'] = eq_now
                audit.budget['utilization_pct'] = None
                audit.budget['action_enabled'] = True
                audit.budget['equity_cap_enforced'] = False
            else:
                log.info(f"馃挵 瀹炴椂鏉冪泭妫€娴? {eq_now:.2f} USDT (涓婇檺: {equity_cap:.2f} USDT)")

                budget_result = check_budget_limit(equity_cap)

                if not budget_result['ok']:
                    log.error(f"馃毃 BUDGET EXCEEDED: {eq_now:.2f} USDT > {equity_cap:.2f} USDT. STOPPING ALL TRADING.")
                    # 棰勭畻瓒呴檺鏃剁洿鎺ョ粓姝㈡湰杞紝閬垮厤缁х画杩涘叆浜ゆ槗娴佺▼
                    audit.add_note(f"BUDGET_LIMIT_EXCEEDED: {eq_now:.2f} > {equity_cap:.2f}")
                    audit.budget = getattr(audit, 'budget', {}) or {}
                    audit.budget['equity_cap_usdt'] = equity_cap
                    audit.budget['current_equity_usdt'] = eq_now
                    audit.budget['utilization_pct'] = budget_result.get('utilization')
                    audit.budget['action_enabled'] = True
                    audit.save(str(runtime_run_dir))
                    log.info("V5 live run completed (BUDGET LIMITED)")
                    return
                elif budget_result['utilization'] > 90:
                    log.warning(f"鈿狅笍 BUDGET WARNING: {eq_now:.2f} / {equity_cap:.2f} USDT ({budget_result['utilization']:.0f}%)")
                else:
                    log.info(f"鉁?BUDGET OK: {eq_now:.2f} / {equity_cap:.2f} USDT ({budget_result['utilization']:.0f}%)")

                audit.budget = getattr(audit, 'budget', {}) or {}
                audit.budget['equity_cap_usdt'] = equity_cap
                audit.budget['current_equity_usdt'] = eq_now
                audit.budget['utilization_pct'] = budget_result['utilization']
                audit.budget['action_enabled'] = True

    except Exception as e:
        log.warning(f"Budget check skipped: {e}")
    # ========== 棰勭畻闄愬埗妫€鏌ョ粨鏉?==========

    out = pipe.run(
        market_data_1h=md_1h,
        positions=held,
        cash_usdt=float(acc.cash_usdt),
        equity_peak_usdt=float(acc.equity_peak_usdt),
        run_logger=run_logger,
        audit=audit,
        precomputed_alpha=alpha_snap,
        precomputed_regime=regime,
    )
    if live_whitelist:
        _assert_live_symbol_subset(
            list((out.portfolio.target_weights or {}).keys()),
            live_whitelist,
            stage="targets",
            audit=audit,
            runtime_run_dir=runtime_run_dir,
        )
        _assert_live_symbol_subset(
            [
                str(d.get("symbol") or "").strip()
                for d in (audit.router_decisions or [])
                if str(d.get("symbol") or "").strip()
            ],
            live_whitelist,
            stage="router_decisions",
            audit=audit,
            runtime_run_dir=runtime_run_dir,
        )

    collector = None
    log.info(f"ALPHA_HISTORY_ENABLED={ALPHA_HISTORY_ENABLED}, AlphaHistoryCollector={AlphaHistoryCollector}")
    if ALPHA_HISTORY_ENABLED and AlphaHistoryCollector:
        try:
            collector = AlphaHistoryCollector()
            collector.save_snapshot(
                run_id=run_id,
                ts=int(_utc_now().timestamp()),
                snapshot=out.alpha,
                regime=str(out.regime.state.value if hasattr(out.regime.state, "value") else out.regime.state),
                regime_multiplier=float(getattr(out.regime, "multiplier", 1.0) or 1.0),
                selected_symbols=list(getattr(out.portfolio, "selected", []) or []),
                traded_symbols=[],
            )
            log.info(f"Alpha snapshot saved to history database (run_id={run_id})")
        except Exception as e:
            log.warning(f"Failed to save alpha history: {e}")

    # Qlib-style IC monitor update (score/rankIC + factor IC when available)
    try:
        from src.alpha.ic_monitor import AlphaICMonitor

        icm = AlphaICMonitor()
        closes = {s: float(md_1h[s].close[-1]) for s in md_1h.keys() if getattr(md_1h[s], 'close', None)}
        ic_summary = icm.update(
            now_ts_ms=int(_utc_now().timestamp() * 1000),
            alpha_snapshot=out.alpha,
            closes=closes,
        )
        if ic_summary:
            ic_mean = float(((ic_summary.get('score_rank_ic_short') or {}).get('mean') or 0.0))
            if audit:
                audit.add_note(f"IC monitor updated: short_rank_ic_mean={ic_mean:.4f}")
    except Exception as e:
        log.warning(f"ic monitor update failed: {e}")

    # F1.2 spread snapshot (records even when no orders/fills)
    try:
        from src.reporting.spread_snapshots import append_spread_snapshot

        tob = {}
        if hasattr(provider, "fetch_top_of_book"):
            tob = provider.fetch_top_of_book(scored_symbols)

        selected = set(getattr(out.portfolio, "selected", []) or [])
        rows = []
        for sym in scored_symbols:
            ba = tob.get(sym) or {}
            bid = ba.get("bid")
            ask = ba.get("ask")
            if bid is None or ask is None:
                continue
            bid = float(bid)
            ask = float(ask)
            if bid <= 0 or ask <= 0:
                continue
            mid = (bid + ask) / 2.0
            spread_bps = (ask - bid) / mid * 10_000.0 if mid > 0 else None
            rows.append(
                {
                    "symbol": sym,
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "spread_bps": spread_bps,
                    "selected": sym in selected,
                }
            )

        if window_end_ts is not None:
            evt = {
                "ts": _utc_now().isoformat().replace("+00:00", "Z"),
                "run_id": run_id,
                "window_start_ts": window_start_ts,
                "window_end_ts": window_end_ts,
                "symbols": rows,
            }
            append_spread_snapshot(evt, base_dir=str(runtime_spread_snapshots_dir))
            # also keep a per-run copy for easy inspection
            (runtime_run_dir / "spread_snapshot.json").write_text(
                json.dumps(evt, ensure_ascii=False, indent=2), encoding="utf-8"
            )
    except Exception as e:
        log.warning(f"spread snapshot failed: {e}")
    
    # 淇濆瓨DecisionAudit
    audit.save(str(runtime_run_dir))

    # Read-only skipped-candidate outcome tracker.
    try:
        from src.reporting.skipped_candidate_tracker import update_skipped_candidate_tracker

        tracker_result = {"enabled": False, "new_records": 0, "total_records": 0}
        if _should_update_skipped_candidate_tracker(cfg, out.regime.state, held):
            tracker_result = update_skipped_candidate_tracker(
                run_dir=str(runtime_run_dir),
                audit=audit,
                market_data_1h=md_1h,
                cfg=cfg,
                current_level=pipe._load_current_auto_risk_level(),
                cache_dir=PROJECT_ROOT / "data" / "cache",
            )
        if tracker_result.get("enabled"):
            log.info(
                "SKIPPED_CANDIDATE_TRACKER new_records=%s total_records=%s",
                int(tracker_result.get("new_records", 0) or 0),
                int(tracker_result.get("total_records", 0) or 0),
            )
    except Exception as e:
        log.warning(f"skipped candidate tracker failed: {e}")

    # Read-only ALT impulse shadow evaluator. It records forward labels only.
    try:
        from src.reporting.alt_impulse_shadow import update_alt_impulse_shadow_evaluator

        shadow_result = update_alt_impulse_shadow_evaluator(
            run_dir=str(runtime_run_dir),
            audit=audit,
            market_data_1h=md_1h,
            cfg=cfg,
            current_level=pipe._load_current_auto_risk_level(),
            cache_dir=PROJECT_ROOT / "data" / "cache",
        )
        if shadow_result.get("enabled"):
            log.info(
                "ALT_IMPULSE_SHADOW new_records=%s total_records=%s",
                int(shadow_result.get("new_records", 0) or 0),
                int(shadow_result.get("total_records", 0) or 0),
            )
    except Exception as e:
        log.warning(f"alt impulse shadow evaluator failed: {e}")

    # Update account peak equity
    # (equity is logged inside pipeline; recompute here quickly)
    eq = float(acc.cash_usdt)
    for p in held:
        s = md_1h.get(p.symbol)
        if s and s.close:
            eq += float(p.qty) * float(s.close[-1])
    acc.equity_peak_usdt = max(float(acc.equity_peak_usdt), float(eq))
    acc_store.set(acc)

    orders = _merge_event_close_override_orders(
        orders=list(out.orders or []),
        positions=store.list(),
        prices=prices,
        run_id=run_id,
        order_store_path=str(getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite") or "reports/orders.sqlite"),
        audit=audit,
    )
    if live_whitelist:
        _assert_live_symbol_subset(
            [str(getattr(order, "symbol", "") or "").strip() for order in (orders or []) if str(getattr(order, "symbol", "") or "").strip()],
            live_whitelist,
            stage="order_intents_pre_arbitration",
            audit=audit,
            runtime_run_dir=runtime_run_dir,
        )

    # Order arbitration layer: unified priority + per-symbol state machine
    # to avoid cross-module conflicts (close vs rebalance/open in same run, cooldown churn, etc.).
    try:
        from src.execution.order_arbitrator import arbitrate_orders

        orders_before = len(orders or [])
        sm_path = str(_resolve_order_state_machine_path(cfg))
        runtime_order_store_path = str(
            getattr(getattr(cfg, "execution", None), "order_store_path", "reports/orders.sqlite")
            or "reports/orders.sqlite"
        )
        cooldown_min = int(_coalesce(getattr(cfg.execution, "open_long_cooldown_minutes", None), 10))
        take_profit_cooldown_min = int(
            _coalesce(getattr(cfg.execution, "take_profit_reentry_cooldown_minutes", None), 0)
        )
        orders, arb_decisions = arbitrate_orders(
            orders=(orders or []),
            positions=store.list(),
            run_id=run_id,
            cooldown_minutes=cooldown_min,
            state_path=sm_path,
            take_profit_cooldown_minutes=take_profit_cooldown_min,
            take_profit_cooldown_state_path=str(
                derive_runtime_named_json_path(runtime_order_store_path, "take_profit_cooldown_state")
            ),
        )
        blocked_n = len([d for d in (arb_decisions or []) if d.get("action") == "blocked"])
        if blocked_n > 0:
            msg = f"ORDER_ARBITRATION: before={orders_before} after={len(orders)} blocked={blocked_n}"
            _log_order_arbitration_summary(log, cfg, msg)
            try:
                audit.add_note(msg)
                # Keep only first N decision details to control artifact size
                audit.add_note("ORDER_ARBITRATION_DETAILS: " + json.dumps(arb_decisions[:20], ensure_ascii=False))
            except Exception:
                pass
    except Exception as e:
        log.warning(f"order arbitration skipped: {e}")

    from src.reporting.trade_log import TradeLogWriter

    trade_log = TradeLogWriter(run_dir=str(runtime_run_dir))

    def make_executor():
        mode = getattr(cfg.execution, "mode", "dry_run")
        if str(mode).lower() == "live":
            # Last-arm safety
            arm_env = str(getattr(cfg.execution, "live_arm_env", "V5_LIVE_ARM"))
            arm_val = str(getattr(cfg.execution, "live_arm_value", "YES"))
            if os.getenv(arm_env) != arm_val:
                raise RuntimeError(f"Live mode requested but not armed: set {arm_env}={arm_val}")

            # Key completeness
            if not (cfg.exchange.api_key and cfg.exchange.api_secret and cfg.exchange.passphrase):
                raise RuntimeError("Live mode requested but exchange API credentials are missing")

            from src.execution.okx_private_client import OKXPrivateClient
            from src.execution.live_execution_engine import LiveExecutionEngine

            from src.execution.order_store import OrderStore
            
            client = live_okx_client or OKXPrivateClient(exchange=cfg.exchange)
            order_store = OrderStore(cfg.execution.order_store_path)
            live = LiveExecutionEngine(
                cfg.execution,
                okx=client,
                order_store=order_store,
                position_store=store,
                run_id=run_id,
                exp_time_ms=getattr(cfg.execution, "okx_exp_time_ms", None),
            )
            return live, True

        # default dry-run
        return ExecutionEngine(cfg.execution, position_store=store, account_store=acc_store, trade_log=trade_log, run_id=run_id), False

    exec_engine, is_live = make_executor()

    # pre-trade equity point (so instant runs reflect costs)
    try:
        pre_acc = acc_store.get()
        pre_cash = float(pre_acc.cash_usdt)
        pre_eq = float(pre_cash)
        for p in store.list():
            s = md_1h.get(p.symbol)
            if s and s.close:
                pre_eq += float(p.qty) * float(s.close[-1])
        run_logger.log_equity(
            {
                "ts": _utc_now().isoformat().replace("+00:00", "Z"),
                "phase": "pre_trade",
                "cash": pre_cash,
                "equity": pre_eq,
            }
        )
    except Exception:
        pass

    if is_live and hasattr(exec_engine, "poll_open"):
        try:
            exec_engine.poll_open(limit=200)
        except Exception as e:
            log.warning(f"live poll_open (pre) failed: {e}")

    if is_live and live_preflight_result is not None:
        borrow_blocked = {
            str(sym)
            for sym in (((live_preflight_result.details or {}).get("borrow_check") or {}).get("blocked_symbols") or [])
            if str(sym)
        }
        if borrow_blocked:
            before_n = len(orders)
            orders = [o for o in orders if str(getattr(o, "symbol", "")) not in borrow_blocked]
            after_n = len(orders)
            if before_n != after_n:
                log.warning(
                    "live preflight filtered borrow-blocked symbols: blocked=%s removed=%d",
                    sorted(borrow_blocked),
                    before_n - after_n,
                )
                audit.add_note(
                    f"live preflight filtered borrow-blocked symbols: blocked={sorted(borrow_blocked)} removed={before_n - after_n}"
                )

        orders = _apply_live_preflight_order_restrictions(
            orders=list(orders or []),
            live_preflight_result=live_preflight_result,
            audit=audit,
            log=log,
        )

    if live_whitelist:
        _assert_live_symbol_subset(
            [str(getattr(order, "symbol", "") or "").strip() for order in (orders or []) if str(getattr(order, "symbol", "") or "").strip()],
            live_whitelist,
            stage="order_intents",
            audit=audit,
            runtime_run_dir=runtime_run_dir,
        )

    report = exec_engine.execute(orders)
    report.notes = f"regime={out.regime.state} selected={out.portfolio.selected} orders={len(orders)}"

    if is_live and hasattr(exec_engine, "poll_open"):
        try:
            exec_engine.poll_open(limit=200)
        except Exception as e:
            log.warning(f"live poll_open (post) failed: {e}")

    # Live fills sync + export (F2): pull OKX fills into FillStore then reconcile/export to trades.csv + cost_events.
    # This is needed because get_order polling alone doesn't guarantee fills are exported.
    if is_live:
        try:
            from src.execution.fill_store import FillStore, derive_fill_store_path, parse_okx_fills
            from src.execution.fill_reconciler import FillReconciler

            client = getattr(exec_engine, "okx", None)
            if client is not None:
                live_order_store = getattr(exec_engine, "order_store", None)
                if live_order_store is None:
                    raise RuntimeError("live execution engine missing order_store")
                fs = FillStore(path=str(derive_fill_store_path(live_order_store.path)))
                # page newest fills backward; keep small to limit API usage
                after = None
                total_new = 0
                for _ in range(5):
                    r = client.get_fills(after=after, limit=100)
                    rows = parse_okx_fills(r.data, source="fills")
                    ins, _ = fs.upsert_many(rows)
                    total_new += int(ins)

                    data = (r.data or {}).get("data") or []
                    if not isinstance(data, list) or not data:
                        break
                    last = data[-1] if isinstance(data[-1], dict) else {}
                    after = last.get("billId") or last.get("tradeId")
                    if ins == 0:
                        break
                    time.sleep(0.05)

                # reconcile + export
                try:
                    rec = FillReconciler(
                        fill_store=fs,
                        order_store=live_order_store,
                        okx=client,
                        position_store=getattr(exec_engine, "position_store", None),
                    )
                    rec.reconcile(limit=2000, max_get_order_per_run=10)
                except Exception as e:
                    log.warning(f"live fill reconciliation failed after fills sync: {e}", exc_info=True)

                if total_new > 0:
                    log.info(f"FILLS_SYNC new_fills={total_new} total={fs.count()}")
        except Exception as e:
            log.warning(f"fills sync/export failed: {e}")

        # Keep local cash state aligned with exchange after live execution.
        # Position qty is synced on fills; cash must also be refreshed or reconcile drifts
        # will accumulate across sell cycles.
        try:
            live_client = getattr(exec_engine, "okx", None)
            if live_client is not None:
                bal = live_client.get_balance(ccy="USDT")
                rows = (bal.data or {}).get("data") if isinstance(bal.data, dict) else None
                details = ((rows[0] if isinstance(rows, list) and rows else {}) or {}).get("details")
                usdt_cash = None
                if isinstance(details, list):
                    for d in details:
                        if isinstance(d, dict) and str(d.get("ccy") or "").upper() == "USDT":
                            try:
                                usdt_cash = float(d.get("cashBal"))
                            except Exception:
                                usdt_cash = None
                            break
                if usdt_cash is not None:
                    post_state = acc_store.get()
                    post_state.cash_usdt = float(usdt_cash)
                    acc_store.set(post_state)
                    log.info(f"LIVE_CASH_SYNC usdt={usdt_cash:.8f}")
        except Exception as e:
            log.warning(f"live cash sync failed: {e}")

    # After execution/fill reconciliation, reread local positions and clear stale
    # active lifecycle state when a close left only exchange dust.
    try:
        post_positions = store.list()
        _, cleanup_decisions, _ = pipe.cleanup_stale_position_state_for_dust_positions(
            post_positions,
            prices=prices,
            audit=audit,
        )
        if cleanup_decisions:
            audit.router_decisions = list(audit.router_decisions or []) + list(cleanup_decisions)
            audit.save(str(runtime_run_dir))
            log.info(
                "POSITION_STATE_CLEANUP_AFTER_EXECUTION cleared=%d symbols=%s",
                len(cleanup_decisions),
                [str(d.get("symbol") or "") for d in cleanup_decisions],
            )
    except Exception as e:
        log.warning(f"post-close position lifecycle cleanup failed: {e}")
    
    # 鏇存柊 alpha 鍘嗗彶鏁版嵁涓殑浜ゆ槗淇℃伅
    if collector and hasattr(report, 'orders') and report.orders:
        traded_symbols = set()
        for order in report.orders:
            if hasattr(order, 'symbol'):
                traded_symbols.add(order.symbol)
                # 濡傛灉鏈夌泩浜忎俊鎭紝鍙互鏇存柊
                # if hasattr(order, 'pnl'):
                #     collector.update_trade_pnl(
                #         symbol=order.symbol,
                #         ts=int(_utc_now().timestamp()),
                #         pnl=order.pnl
                #     )
        
        # 鏇存柊 traded_symbols锛堢畝鍖栵細鍦ㄤ笅娆¤繍琛屾椂鏇存柊锛?
        log.info(f"Traded symbols: {list(traded_symbols)}")

    # post-trade equity point (after applying fees/slippage)
    try:
        post_acc = acc_store.get()
        post_cash = float(post_acc.cash_usdt)
        post_eq = float(post_cash)
        for p in store.list():
            s = md_1h.get(p.symbol)
            if s and s.close:
                post_eq += float(p.qty) * float(s.close[-1])
        run_logger.log_equity(
            {
                "ts": _utc_now().isoformat().replace("+00:00", "Z"),
                "phase": "post_trade",
                "cash": post_cash,
                "equity": post_eq,
            }
        )
    except Exception:
        pass

    # write/update summary
    try:
        from src.reporting.summary_writer import write_summary

        window_start_ts = _get_env_epoch_sec("V5_WINDOW_START_TS")
        window_end_ts = _get_env_epoch_sec("V5_WINDOW_END_TS")

        # 绐楀彛闀垮害鏍￠獙锛堥伩鍏?silent bug锛?
        if window_start_ts is not None and window_end_ts is not None:
            if window_end_ts <= window_start_ts:
                raise ValueError(f"Invalid window: {window_start_ts} -> {window_end_ts}")

        summ = write_summary(
            str(runtime_run_dir),
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
        )

        # Live finalize: refresh summary metrics after fills/trades export may have appended rows.
        try:
            if is_live:
                from src.reporting.summary_writer import refresh_summary_metrics

                summ = refresh_summary_metrics(str(runtime_run_dir))
        except Exception:
            pass

        # F3.0 budget monitoring (monitor + tagging only, no behavior change)
        try:
            from src.reporting.budget_state import derive_ymd_utc_from_summary, update_daily_budget_state
            from src.reporting.summary_writer import attach_budget
            from src.reporting.decision_audit import load_decision_audit
            from src.reporting.metrics import read_trades_csv

            trades = read_trades_csv(str(runtime_run_dir / "trades.csv"))
            notionals = [abs(float(t.get("notional_usdt") or 0.0)) for t in trades]
            turnover_inc = float(sum(notionals))
            cost_inc = float(sum(float(t.get("fee_usdt") or 0.0) + float(t.get("slippage_usdt") or 0.0) for t in trades))
            fills_count_inc = int(len([x for x in notionals if x > 0]))

            ymd = derive_ymd_utc_from_summary(summ)
            st = update_daily_budget_state(
                base_dir=str(runtime_budget_state_dir),
                ymd_utc=ymd,
                run_id=run_id,
                turnover_inc=turnover_inc,
                cost_inc_usdt=cost_inc,
                fills_count_inc=fills_count_inc,
                notionals_inc=notionals,
                avg_equity=summ.get("avg_equity"),
                turnover_budget_per_day=cfg.budget.turnover_budget_per_day,
                cost_budget_bps_per_day=cfg.budget.cost_budget_bps_per_day,
                small_trade_notional_cutoff=float(cfg.budget.min_trade_notional_base),
            )
            budget_dict = {
                "ymd_utc": ymd,
                "avg_equity_est": st.avg_equity_est,
                "turnover_used": st.turnover_used,
                "turnover_used_usdt": st.turnover_used_usdt(),
                "turnover_used_ratio": st.turnover_used_ratio(),
                "turnover_budget_per_day": st.turnover_budget_per_day,
                "turnover_budget_ratio": st.turnover_budget_ratio(),
                "turnover_budget_usdt": st.turnover_budget_usdt(),
                "turnover_exceeded": st.turnover_exceeded(),
                "cost_used_usdt": st.cost_used_usdt,
                "cost_used_bps": st.cost_used_bps(),
                "cost_budget_bps_per_day": st.cost_budget_bps_per_day,
                "cost_exceeded": st.cost_exceeded(),
                "exceeded": st.exceeded(),
                "reason": st.reason(),
                "fills_count_today": st.fills_count_today,
                "median_notional_usdt_today": st.median_notional_usdt_today,
                "small_trade_ratio_today": st.small_trade_ratio_today,
                "small_trade_notional_cutoff": st.small_trade_notional_cutoff,
            }
            attach_budget(str(runtime_run_dir), budget_dict)

            audit = load_decision_audit(str(runtime_run_dir))
            if audit is not None:
                audit.budget = budget_dict
                audit.save(str(runtime_run_dir))

                # Also attach exit signals into summary for explainability
                try:
                    from src.reporting.summary_writer import attach_exit_signals

                    attach_exit_signals(str(runtime_run_dir), audit.exit_signals or [])
                except Exception:
                    pass
        except Exception:
            pass

    except Exception:
        pass

    dump_run_artifacts(
        reports_dir=str(runtime_reports_dir),
        alpha=alpha_snap,
        regime=regime,
        portfolio=out.portfolio,
        execution=report,
    )

    if is_live:
        log.info("V5 live run completed")
    else:
        log.info("V5 dry-run completed")
    log.info(report.notes)
    log.info(f"orders={len(report.orders)}")


if __name__ == "__main__":
    main()


