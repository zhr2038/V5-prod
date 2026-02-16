from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

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


def _get_env_epoch_sec(name: str) -> Optional[int]:
    """从环境变量读取时间戳（秒/毫秒兼容）"""
    v = os.getenv(name)
    if not v:
        return None
    x = int(v)
    # 兼容毫秒 epoch
    if x > 10_000_000_000:  # ~2286-11-20 in seconds
        x //= 1000
    return x


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
    cfg_path = os.getenv("V5_CONFIG") or "configs/config.yaml"
    cfg = load_config(cfg_path, env_path=".env")
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
                top_n=int(getattr(cfg.universe, "top_n_market_cap", 30) or 30),
                min_24h_quote_volume_usdt=cfg.universe.min_24h_quote_volume_usdt,
                blacklist_path=cfg.universe.blacklist_path,
                exclude_stablecoins=cfg.universe.exclude_stablecoins,
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

    # fetch 1h bars for alpha/regime and 4h for auxiliary (placeholder)
    # 使用窗口时间过滤，只取已收盘bar
    window_start_ts = _get_env_epoch_sec("V5_WINDOW_START_TS")
    window_end_ts = _get_env_epoch_sec("V5_WINDOW_END_TS")
    
    end_ts_ms = None
    if window_end_ts is not None:
        end_ts_ms = window_end_ts * 1000  # 转换为毫秒
    
    md_1h = provider.fetch_ohlcv(
        symbols,
        timeframe=cfg.timeframe_main,
        limit=24 * 60,
        end_ts_ms=end_ts_ms,
    )

    alpha_engine = AlphaEngine(cfg.alpha)
    alpha_snap = alpha_engine.compute_snapshot(md_1h)

    # regime from BTC (handle empty market data explicitly)
    if not md_1h:
        log.error("No market data returned from provider (md_1h is empty); aborting run")
        return

    btc = md_1h.get("BTC/USDT")
    if btc is None:
        # fallback to any available symbol
        btc = next(iter(md_1h.values()))

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
    from src.reporting.decision_audit import DecisionAudit

    import os

    run_id = os.getenv("V5_RUN_ID")
    if not run_id:
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_logger = RunLogger(run_dir=f"reports/runs/{run_id}")
    
    # 创建DecisionAudit
    audit = DecisionAudit(
        run_id=run_id,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
    )

    # F3.1 pre-run budget action input: load today's budget state (UTC) and set audit.budget
    try:
        from src.reporting.budget_state import _utc_yyyymmdd_from_epoch_sec, load_budget_state

        ts_for_day = window_end_ts or window_start_ts
        if ts_for_day is not None:
            ymd = _utc_yyyymmdd_from_epoch_sec(int(ts_for_day))
            st = load_budget_state(f"reports/budget_state/{ymd}.json")
            if st is not None:
                audit.budget = {
                    "ymd_utc": ymd,
                    "avg_equity_est": st.avg_equity_est,
                    "turnover_used": st.turnover_used,
                    "turnover_budget_per_day": st.turnover_budget_per_day,
                    "cost_used_usdt": st.cost_used_usdt,
                    "cost_used_bps": st.cost_used_bps(),
                    "cost_budget_bps_per_day": st.cost_budget_bps_per_day,
                    "exceeded": st.exceeded(),
                    "reason": st.reason(),
                    "fills_count_today": st.fills_count_today,
                    "median_notional_usdt_today": st.median_notional_usdt_today,
                    "small_trade_ratio_today": st.small_trade_ratio_today,
                    "small_trade_notional_cutoff": st.small_trade_notional_cutoff,
                }
    except Exception:
        pass
    
    # 记录universe数量
    audit.counts["universe"] = len(symbols)

    pipe = V5Pipeline(cfg)
    out = pipe.run(
        market_data_1h=md_1h,
        positions=held,
        cash_usdt=float(acc.cash_usdt),
        equity_peak_usdt=float(acc.equity_peak_usdt),
        run_logger=run_logger,
        audit=audit,
    )

    # F1.2 spread snapshot (records even when no orders/fills)
    try:
        from src.reporting.spread_snapshots import append_spread_snapshot

        tob = {}
        if hasattr(provider, "fetch_top_of_book"):
            tob = provider.fetch_top_of_book(symbols)

        selected = set(getattr(out.portfolio, "selected", []) or [])
        rows = []
        for sym in symbols:
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
                "ts": datetime.utcnow().isoformat() + "Z",
                "run_id": run_id,
                "window_start_ts": window_start_ts,
                "window_end_ts": window_end_ts,
                "symbols": rows,
            }
            append_spread_snapshot(evt)
            # also keep a per-run copy for easy inspection
            Path(f"reports/runs/{run_id}/spread_snapshot.json").write_text(
                json.dumps(evt, ensure_ascii=False, indent=2), encoding="utf-8"
            )
    except Exception as e:
        log.warning(f"spread snapshot failed: {e}")
    
    # 保存DecisionAudit
    audit.save(f"reports/runs/{run_id}")

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

    from src.reporting.trade_log import TradeLogWriter

    trade_log = TradeLogWriter(run_dir=f"reports/runs/{run_id}")

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

            client = OKXPrivateClient(exchange=cfg.exchange)
            live = LiveExecutionEngine(
                cfg.execution,
                okx=client,
                order_store=None,
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
                "ts": datetime.utcnow().isoformat() + "Z",
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

    # Live preflight catch-up (Commit A): bills -> ledger -> reconcile/guard -> decision
    if is_live and bool(getattr(cfg.execution, "preflight_enabled", True)):
        try:
            from src.execution.live_preflight import LivePreflight

            client = getattr(exec_engine, "okx", None)
            if client is not None:
                pf = LivePreflight(
                    cfg.execution,
                    okx=client,
                    position_store=store,
                    account_store=acc_store,
                    bills_db_path="reports/bills.sqlite",
                    ledger_state_path="reports/ledger_state.json",
                    ledger_status_path="reports/ledger_status.json",
                    reconcile_status_path=str(getattr(cfg.execution, "reconcile_status_path", "reports/reconcile_status.json")),
                )
                res = pf.run(
                    max_pages=int(getattr(cfg.execution, "preflight_max_pages", 5)),
                    max_status_age_sec=int(getattr(cfg.execution, "max_status_age_sec", 180)),
                )
                log.info(f"LIVE_PREFLIGHT decision={res.decision} reason={res.reason}")

                fail_action = str(getattr(cfg.execution, "preflight_fail_action", "sell_only") or "sell_only").lower()
                if res.decision == "ABORT" or (res.decision == "SELL_ONLY" and fail_action == "abort"):
                    raise RuntimeError(f"live preflight blocked: {res.decision} {res.reason}")

                if res.decision == "SELL_ONLY":
                    orders = [o for o in orders if str(o.side).lower() == "sell" or str(o.intent) == "CLOSE_LONG"]
        except Exception as e:
            log.warning(f"live preflight failed: {e}")

    report = exec_engine.execute(orders)
    report.notes = f"regime={out.regime.state} selected={out.portfolio.selected} orders={len(orders)}"

    if is_live and hasattr(exec_engine, "poll_open"):
        try:
            exec_engine.poll_open(limit=200)
        except Exception as e:
            log.warning(f"live poll_open (post) failed: {e}")

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
                "ts": datetime.utcnow().isoformat() + "Z",
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

        # 窗口长度校验（避免 silent bug）
        if window_start_ts is not None and window_end_ts is not None:
            if window_end_ts <= window_start_ts:
                raise ValueError(f"Invalid window: {window_start_ts} -> {window_end_ts}")

        summ = write_summary(
            f"reports/runs/{run_id}",
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
        )

        # Live finalize: refresh summary metrics after fills/trades export may have appended rows.
        try:
            if is_live:
                from src.reporting.summary_writer import refresh_summary_metrics

                summ = refresh_summary_metrics(f"reports/runs/{run_id}")
        except Exception:
            pass

        # F3.0 budget monitoring (monitor + tagging only, no behavior change)
        try:
            from src.reporting.budget_state import derive_ymd_utc_from_summary, update_daily_budget_state
            from src.reporting.summary_writer import attach_budget
            from src.reporting.decision_audit import load_decision_audit
            from src.reporting.metrics import read_trades_csv

            trades = read_trades_csv(f"reports/runs/{run_id}/trades.csv")
            notionals = [abs(float(t.get("notional_usdt") or 0.0)) for t in trades]
            turnover_inc = float(sum(notionals))
            cost_inc = float(sum(float(t.get("fee_usdt") or 0.0) + float(t.get("slippage_usdt") or 0.0) for t in trades))
            fills_count_inc = int(len([x for x in notionals if x > 0]))

            ymd = derive_ymd_utc_from_summary(summ)
            st = update_daily_budget_state(
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
                "turnover_budget_per_day": st.turnover_budget_per_day,
                "cost_used_usdt": st.cost_used_usdt,
                "cost_used_bps": st.cost_used_bps(),
                "cost_budget_bps_per_day": st.cost_budget_bps_per_day,
                "exceeded": st.exceeded(),
                "reason": st.reason(),
                "fills_count_today": st.fills_count_today,
                "median_notional_usdt_today": st.median_notional_usdt_today,
                "small_trade_ratio_today": st.small_trade_ratio_today,
                "small_trade_notional_cutoff": st.small_trade_notional_cutoff,
            }
            attach_budget(f"reports/runs/{run_id}", budget_dict)

            audit = load_decision_audit(f"reports/runs/{run_id}")
            if audit is not None:
                audit.budget = budget_dict
                audit.save(f"reports/runs/{run_id}")
        except Exception:
            pass

    except Exception:
        pass

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
