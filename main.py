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
from src.execution.position_store import PositionStore
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
        intent = "REBALANCE"
        if cw <= 0 and delta > 0:
            intent = "OPEN_LONG"
        elif tw <= 0 and delta < 0:
            intent = "CLOSE_LONG"
        orders.append(Order(symbol=sym, side=side, intent=intent, notional_usdt=float(notional), signal_price=px, meta={"target_w": tw, "current_w": cw}))
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

    # load persisted positions/account
    store = PositionStore(path="reports/positions.sqlite")
    from src.execution.account_store import AccountStore
    acc_store = AccountStore(path="reports/positions.sqlite")
    acc = acc_store.get()
    held = store.list()
    # Mark-to-market at cycle start
    now_ts = datetime.utcnow().isoformat() + "Z"
    prices = {s: float(md_1h[s].close[-1]) for s in md_1h.keys() if md_1h[s].close}
    for p in held:
        s = md_1h.get(p.symbol)
        if not s or not s.close:
            continue
        store.mark_position(p.symbol, now_ts=now_ts, mark_px=float(s.close[-1]), high_px=float(s.high[-1]) if s.high else float(s.close[-1]))

    held = store.list()

    # Run unified pipeline with equity/drawdown scaling
    from src.core.pipeline import V5Pipeline
    from src.core.run_logger import RunLogger

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_logger = RunLogger(run_dir=f"reports/runs/{run_id}")

    pipe = V5Pipeline(cfg)
    out = pipe.run(
        market_data_1h=md_1h,
        positions=held,
        cash_usdt=float(acc.cash_usdt),
        equity_peak_usdt=float(acc.equity_peak_usdt),
        run_logger=run_logger,
    )

    # Update account peak equity
    # (equity is logged inside pipeline; recompute here quickly)
    eq = float(acc.cash_usdt)
    for p in held:
        s = md_1h.get(p.symbol)
        if s and s.close:
            eq += float(p.qty) * float(s.close[-1])
    acc.equity_peak_usdt = max(float(acc.equity_peak_usdt), float(eq))
    acc_store.set(acc)

    orders = out.orders

    exec_engine = ExecutionEngine(cfg.execution, position_store=store, account_store=acc_store)
    report = exec_engine.execute(orders)
    report.notes = f"regime={out.regime.state} selected={out.portfolio.selected} orders={len(orders)}"

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
