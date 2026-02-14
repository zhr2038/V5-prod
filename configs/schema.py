from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


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
    cache_path: str = Field(default="reports/universe_cache.json")
    cache_ttl_sec: int = Field(default=3600, ge=0)

    top_n_market_cap: int = Field(default=30, ge=1)
    min_24h_quote_volume_usdt: float = Field(default=5_000_000.0, ge=0)
    blacklist_path: str = Field(default="configs/blacklist.json")
    exclude_stablecoins: bool = True


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


class ExecutionConfig(BaseModel):
    dry_run: bool = True
    split_orders: int = Field(default=3, ge=1, le=10)
    split_interval_sec: float = Field(default=3.0, ge=0)
    max_hourly_volume_pct: float = Field(default=0.05, gt=0, le=1)
    slippage_db_path: str = Field(default="reports/slippage.sqlite")


class BacktestConfig(BaseModel):
    fee_bps: float = Field(default=6.0, ge=0)
    slippage_bps: float = Field(default=5.0, ge=0)
    one_bar_delay: bool = True
    walk_forward_folds: int = Field(default=4, ge=1)


class AppConfig(BaseModel):
    symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"])
    timeframe_main: str = "1h"
    timeframe_aux: str = "4h"
    exchange: ExchangeConfig = Field(default_factory=ExchangeConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    alpha: AlphaConfig = Field(default_factory=AlphaConfig)
    regime: RegimeConfig = Field(default_factory=RegimeConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)

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
