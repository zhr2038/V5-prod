from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class RegimeState(str, Enum):
    TRENDING = "Trending"
    SIDEWAYS = "Sideways"
    RISK_OFF = "Risk-Off"


class ExchangeConfig(BaseModel):
    name: str = Field(default="okx", description="Exchange name")
    api_key: Optional[str] = Field(default=None)
    api_secret: Optional[str] = Field(default=None)
    passphrase: Optional[str] = Field(default=None)
    testnet: bool = False


class UniverseConfig(BaseModel):
    enabled: bool = Field(default=False, description="Enable dynamic universe selection")
    use_universe_symbols: bool = Field(default=False, description="Use universe output as trading symbols")
    include_symbols: List[str] = Field(default_factory=list, description="Always include these symbols when use_universe_symbols=true (e.g. BTC/USDT).")
    cache_path: str = Field(default="reports/universe_cache.json")
    cache_ttl_sec: int = Field(default=3600, ge=0)

    top_n_market_cap: int = Field(default=30, ge=1)
    min_24h_quote_volume_usdt: float = Field(default=5_000_000.0, ge=0)
    blacklist_path: str = Field(default="configs/blacklist.json")
    exclude_stablecoins: bool = True

    # Step-2: refine liquidity ranking using per-instrument ticker (more reliable than batch tickers on some mirrors).
    refine_with_single_ticker: bool = Field(default=False)
    refine_single_ticker_max_candidates: int = Field(default=200, ge=1)
    refine_single_ticker_sleep_sec: float = Field(default=0.02, ge=0)


class AlphaWeights(BaseModel):
    f1_mom_5d: float = 0.25
    f2_mom_20d: float = 0.25
    f3_vol_adj_ret_20d: float = 0.20
    f4_volume_expansion: float = 0.15
    f5_rsi_trend_confirm: float = 0.15

    @field_validator("f1_mom_5d", "f2_mom_20d", "f3_vol_adj_ret_20d", "f4_volume_expansion", "f5_rsi_trend_confirm")
    @classmethod
    def _finite(cls, v: float) -> float:
        v = float(v)
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("weight must be finite")
        return v


class AlphaConfig(BaseModel):
    weights: AlphaWeights = Field(default_factory=AlphaWeights)
    long_top_pct: float = Field(default=0.20, gt=0, le=1)


class RegimeConfig(BaseModel):
    atr_threshold: float = Field(default=0.02, gt=0, description="ATR% threshold above which trend regime allowed")
    atr_very_low: float = Field(default=0.008, gt=0, description="ATR% below which sideways")
    pos_mult_trending: float = 1.2
    pos_mult_sideways: float = 0.6
    pos_mult_risk_off: float = 0.3


class RiskConfig(BaseModel):
    max_single_weight: float = Field(default=0.25, gt=0, le=1)
    max_gross_exposure: float = Field(default=1.0, gt=0, le=1.0)
    drawdown_trigger: float = Field(default=0.08, gt=0, le=1)
    drawdown_delever: float = Field(default=0.50, gt=0, le=1)


class RebalanceConfig(BaseModel):
    # no-trade region (deadband) by regime
    deadband_sideways: float = Field(default=0.05, ge=0, le=1)
    deadband_trending: float = Field(default=0.03, ge=0, le=1)
    deadband_riskoff: float = Field(default=0.05, ge=0, le=1)


class ExecutionConfig(BaseModel):
    # Mode selector (preferred). Keep dry_run for backward compatibility.
    mode: str = Field(default="dry_run", description="dry_run|live")
    dry_run: bool = True

    # Stores / safety files
    order_store_path: str = Field(default="reports/orders.sqlite")
    kill_switch_path: str = Field(default="reports/kill_switch.json")
    reconcile_status_path: str = Field(default="reports/reconcile_status.json")

    # Reconcile behavior (G1)
    reconcile_dust_usdt_ignore: float = Field(default=1.0, ge=0, description="Ignore base mismatches whose USDT value is below this (best-effort using mid).")
    reconcile_ccy_mode: str = Field(default="universe_only", description="universe_only|all")

    # Live preflight catch-up (A)
    preflight_enabled: bool = Field(default=True)
    preflight_max_pages: int = Field(default=5, ge=1)
    max_status_age_sec: int = Field(default=180, ge=1)
    preflight_fail_action: str = Field(default="sell_only", description="sell_only|abort")

    # Optional: controlled exchange->local bootstrap patch (live-only)
    preflight_bootstrap_patch_enabled: bool = Field(default=False, description="When reconcile fails (base/usdt mismatch), patch local cash/qty from exchange as a state-alignment step.")
    preflight_bootstrap_patch_max_total_usdt: float = Field(default=50.0, ge=0, description="Safety cap: if estimated total drift exceeds this, do not patch.")
    preflight_bootstrap_patch_min_interval_sec: int = Field(default=300, ge=0, description="Min seconds between patches to avoid thrash.")

    # OKX request expiration (ms) for trading endpoints (optional).
    # Note: OKX expects expTime as an epoch-millisecond timestamp.
    # We treat values < 1e12 as a delta-ms from now for convenience.
    okx_exp_time_ms: Optional[int] = Field(default=1500, ge=1)

    # Borrow safety (live only)
    abort_on_borrow: bool = Field(default=True, description="If OKX balance shows any liabilities/negative eq, abort preflight")
    borrow_liab_eps: float = Field(default=1e-6, ge=0)
    borrow_neg_eq_eps: float = Field(default=1e-6, ge=0)

    # (Optional / future) borrow prevention knobs (kept for config compatibility)
    borrow_prevention: bool = Field(default=False)
    check_fee_currency_balance: bool = Field(default=False)
    high_risk_blacklist_path: str = Field(default="configs/borrow_prevention_rules.json")

    # Last-arm safety env var (required for live)
    live_arm_env: str = Field(default="V5_LIVE_ARM")
    live_arm_value: str = Field(default="YES")

    split_orders: int = Field(default=3, ge=1, le=10)
    split_interval_sec: float = Field(default=3.0, ge=0)
    max_hourly_volume_pct: float = Field(default=0.05, gt=0, le=1)
    slippage_db_path: str = Field(default="reports/slippage.sqlite")

    # dry-run cost model (bps)
    fee_bps: float = Field(default=6.0, ge=0)
    slippage_bps: float = Field(default=5.0, ge=0)

    @field_validator("mode")
    @classmethod
    def _mode_norm(cls, v: str) -> str:
        vv = str(v or "dry_run").strip().lower()
        if vv not in {"dry_run", "live"}:
            raise ValueError("execution.mode must be 'dry_run' or 'live'")
        return vv

    @field_validator("reconcile_ccy_mode")
    @classmethod
    def _reconcile_ccy_mode(cls, v: str) -> str:
        vv = str(v or "universe_only").strip().lower()
        if vv not in {"universe_only", "all"}:
            raise ValueError("execution.reconcile_ccy_mode must be 'universe_only' or 'all'")
        return vv

    @field_validator("preflight_fail_action")
    @classmethod
    def _preflight_fail_action(cls, v: str) -> str:
        vv = str(v or "sell_only").strip().lower()
        if vv not in {"sell_only", "abort"}:
            raise ValueError("execution.preflight_fail_action must be 'sell_only' or 'abort'")
        return vv


    @model_validator(mode="before")
    @classmethod
    def _compat_pre(cls, data: object) -> object:
        # Backward-compat: if mode not present, derive from dry_run.
        if isinstance(data, dict) and "mode" not in data and "dry_run" in data:
            d = dict(data)
            d["mode"] = "dry_run" if bool(d.get("dry_run", True)) else "live"
            return d
        return data


class BacktestConfig(BaseModel):
    fee_bps: float = Field(default=6.0, ge=0)
    slippage_bps: float = Field(default=5.0, ge=0)
    one_bar_delay: bool = True
    walk_forward_folds: int = Field(default=4, ge=1)

    # cost calibration (F2)
    cost_model: str = Field(default="default", description="default|calibrated")
    cost_stats_dir: str = Field(default="reports/cost_stats")
    fee_quantile: str = Field(default="p75")
    slippage_quantile: str = Field(default="p90")
    min_fills_global: int = Field(default=30, ge=0)
    min_fills_bucket: int = Field(default=10, ge=0)
    max_stats_age_days: int = Field(default=7, ge=0)


class BudgetConfig(BaseModel):
    # F3.0: monitoring
    turnover_budget_per_day: Optional[float] = Field(default=None, ge=0)
    cost_budget_bps_per_day: Optional[float] = Field(default=None, ge=0)

    # F3.1/F3.2: action (only takes effect when budget exceeded)
    action_enabled: bool = Field(default=True)

    # Stage-1: widen deadband
    deadband_multiplier_exceeded: float = Field(default=1.5, ge=1.0)
    deadband_cap: float = Field(default=0.15, ge=0, le=1)

    # Stage-2: raise min_trade_notional to suppress small noisy rebalances
    min_fills_for_second_stage: int = Field(default=5, ge=0)
    min_trade_notional_base: float = Field(default=25.0, ge=0)
    min_trade_notional_multiplier_exceeded: float = Field(default=2.0, ge=1.0)
    min_trade_notional_cap_abs: float = Field(default=200.0, ge=0)
    min_trade_notional_cap_equity_ratio: float = Field(default=0.01, ge=0, le=1)

    # Optional: for live small-budget sampling, cap the equity used by sizing logic.
    # This does NOT change reconcile/accounting; it only caps order sizing.
    live_equity_cap_usdt: Optional[float] = Field(default=None, ge=0)

    # Trigger metrics (computed from daily trades)
    small_trade_ratio_threshold: float = Field(default=0.6, ge=0, le=1)
    small_trade_median_threshold_abs: float = Field(default=10.0, ge=0)
    small_trade_median_threshold_equity_ratio: float = Field(default=0.0025, ge=0, le=1)


class AppConfig(BaseModel):
    symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"])
    timeframe_main: str = "1h"
    timeframe_aux: str = "4h"
    exchange: ExchangeConfig = Field(default_factory=ExchangeConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    alpha: AlphaConfig = Field(default_factory=AlphaConfig)
    regime: RegimeConfig = Field(default_factory=RegimeConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    rebalance: RebalanceConfig = Field(default_factory=RebalanceConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)
    budget: BudgetConfig = Field(default_factory=BudgetConfig)

    @field_validator("symbols")
    @classmethod
    def _symbols_format(cls, v: List[str]) -> List[str]:
        out = []
        for s in v or []:
            s = str(s)
            if "/" not in s:
                raise ValueError(f"invalid symbol format: {s}")
            out.append(s)
        if not out:
            raise ValueError("symbols cannot be empty")
        return out
