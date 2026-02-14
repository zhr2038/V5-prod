from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

from configs.loader import load_config
from configs.schema import AppConfig
from src.alpha.alpha_engine import AlphaEngine
from src.data.mock_provider import MockProvider
from src.data.okx_ccxt_provider import OKXCCXTProvider
from src.execution.execution_engine import ExecutionEngine
from src.portfolio.portfolio_engine import PortfolioEngine
from src.regime.regime_engine import RegimeEngine
from src.reporting.reporting import dump_run_artifacts
from src.core.models import Order, PositionState
from src.risk.risk_engine import RiskEngine


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


def build_provider(cfg: AppConfig):
    # dry-run defaults to Mock; you can set V5_DATA_PROVIDER=okx to use public ccxt.
    import os

    which = (os.getenv("V5_DATA_PROVIDER") or "mock").lower()
    if which == "okx":
        return OKXCCXTProvider(rate_limit=True)
    return MockProvider(seed=7)


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
        orders.append(Order(symbol=sym, side=side, notional_usdt=float(notional), signal_price=px, meta={"target_w": tw, "current_w": cw}))
    return orders


def main() -> None:
    cfg = load_config("configs/config.yaml", env_path=".env")
    setup_logging()
    log = logging.getLogger("v5")

    Path("reports").mkdir(exist_ok=True)

    provider = build_provider(cfg)

    symbols = list(cfg.symbols)
    # Optional: dynamic universe
    if cfg.universe.enabled:
        try:
            from src.data.universe.okx_universe import OKXUniverseProvider

            up = OKXUniverseProvider(
                cache_path=cfg.universe.cache_path,
                cache_ttl_sec=cfg.universe.cache_ttl_sec,
                min_24h_quote_volume_usdt=cfg.universe.min_24h_quote_volume_usdt,
                blacklist_path=cfg.universe.blacklist_path,
                exclude_stablecoins=cfg.universe.exclude_stablecoins,
            )
            uni = up.get_universe()
            if cfg.universe.use_universe_symbols and uni:
                symbols = uni
                log.info(f"Universe enabled: using {len(symbols)} symbols")
        except Exception as e:
            log.warning(f"Universe fetch failed, fallback to config symbols: {e}")

    # fetch 1h bars for alpha/regime and 4h for auxiliary (placeholder)
    md_1h = provider.fetch_ohlcv(symbols, timeframe=cfg.timeframe_main, limit=24 * 60)

    alpha_engine = AlphaEngine(cfg.alpha)
    alpha_snap = alpha_engine.compute_snapshot(md_1h)

    # regime from BTC
    btc = md_1h.get("BTC/USDT") or next(iter(md_1h.values()))
    regime_engine = RegimeEngine(cfg.regime)
    regime = regime_engine.detect(btc)

    portfolio_engine = PortfolioEngine(alpha_cfg=cfg.alpha, risk_cfg=cfg.risk)
    portfolio = portfolio_engine.allocate(scores=alpha_snap.scores, market_data=md_1h, regime_mult=regime.multiplier)

    # risk state (scaffold: start flat with equity=100)
    equity = 100.0
    ps = PositionState(
        equity_usdt=equity,
        equity_peak_usdt=equity,
        positions={},
        entry_prices={},
        highest_prices={},
        days_held={},
    )
    risk_engine = RiskEngine(cfg.risk)
    rd = risk_engine.apply(ps)

    # apply delever multiplier to target weights
    target = {s: float(w) * float(rd.delever_mult) for s, w in (portfolio.target_weights or {}).items()}

    prices = {s: float(md_1h[s].close[-1]) for s in md_1h.keys() if md_1h[s].close}
    orders = compute_orders(ps.positions, target, prices, equity)

    exec_engine = ExecutionEngine(cfg.execution)
    report = exec_engine.execute(orders)
    report.notes = f"regime={regime.state} delever={rd.delever_mult} selected={portfolio.selected}"

    dump_run_artifacts(
        reports_dir="reports",
        alpha=alpha_snap,
        regime=regime,
        portfolio=portfolio,
        execution=report,
    )

    log.info("V5 dry-run completed")
    log.info(report.notes)
    log.info(f"orders={len(report.orders)}")


if __name__ == "__main__":
    main()
