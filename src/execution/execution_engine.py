from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from configs.schema import ExecutionConfig
from src.core.models import ExecutionReport, Order
from src.execution.position_store import PositionStore
from src.execution.account_store import AccountStore, AccountState


class ExecutionEngine:
    def __init__(
        self,
        cfg: ExecutionConfig,
        position_store: Optional[PositionStore] = None,
        account_store: Optional[AccountStore] = None,
        trade_log=None,
        run_id: str = "",
    ):
        self.cfg = cfg
        self.db_path = Path(cfg.slippage_db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self.position_store = position_store
        self.account_store = account_store
        self.trade_log = trade_log
        self.run_id = str(run_id or "")

    def _init_db(self) -> None:
        try:
            con = sqlite3.connect(str(self.db_path))
            cur = con.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS slippage (
                    ts TEXT,
                    symbol TEXT,
                    side TEXT,
                    signal_price REAL,
                    execution_price REAL,
                    slippage_bps REAL
                )
                """
            )
            con.commit()
            con.close()
        except Exception:
            pass

    def execute(self, order_batch: List[Order]) -> ExecutionReport:
        ts = datetime.utcnow().isoformat() + "Z"

        fee_bps = float(getattr(self.cfg, 'fee_bps', 6.0) or 6.0)
        slp_bps = float(getattr(self.cfg, 'slippage_bps', 5.0) or 5.0)

        # dry-run: assume execution at signal_price
        for o in order_batch or []:
            self._record(o.symbol, o.side, o.signal_price, o.signal_price)

            # update cash + position store (spot long-only semantics)
            acc = self.account_store.get() if self.account_store else None

            px = float(o.signal_price)
            notional = float(o.notional_usdt)
            qty = (notional / px) if px else 0.0
            fee = abs(notional) * fee_bps / 10_000.0
            slp = abs(notional) * slp_bps / 10_000.0

            realized_usdt = None
            realized_pct = None

            if self.position_store and o.intent in {"OPEN_LONG", "REBALANCE"} and o.side == "buy":
                if acc is not None:
                    acc.cash_usdt = float(acc.cash_usdt) - notional - fee - slp
                if qty > 0:
                    self.position_store.upsert_buy(o.symbol, qty=qty, px=px)

            elif self.position_store and o.intent in {"CLOSE_LONG", "REBALANCE"} and o.side == "sell":
                # compute realized pnl using stored avg_px and qty (close full)
                p = self.position_store.get(o.symbol) if self.position_store else None
                close_qty = float(p.qty) if p else qty
                entry_px = float(p.avg_px) if p else px
                gross = (px - entry_px) * close_qty
                realized_usdt = float(gross) - fee - slp
                realized_pct = (gross / (entry_px * close_qty)) if (entry_px > 0 and close_qty > 0) else 0.0

                if acc is not None:
                    acc.cash_usdt = float(acc.cash_usdt) + (close_qty * px) - fee - slp
                if self.position_store:
                    self.position_store.close_long(o.symbol)

            if acc is not None and self.account_store:
                self.account_store.set(acc)

            # trade log
            if self.trade_log is not None:
                try:
                    from src.reporting.trade_log import Fill

                    self.trade_log.append_fill(
                        Fill(
                            ts=ts,
                            run_id=self.run_id,
                            symbol=o.symbol,
                            intent=o.intent,
                            side=o.side,
                            qty=float(qty if o.side == 'buy' else (close_qty if 'close_qty' in locals() else qty)),
                            price=px,
                            notional_usdt=float(notional),
                            fee_usdt=float(fee),
                            slippage_usdt=float(slp),
                            realized_pnl_usdt=realized_usdt,
                            realized_pnl_pct=realized_pct,
                        )
                    )
                except Exception:
                    pass

            # cost event log (fills only)
            try:
                from src.reporting.cost_events import append_cost_event

                # best-effort extract window/regime from order meta
                meta = o.meta or {}
                window_start_ts = meta.get("window_start_ts")
                window_end_ts = meta.get("window_end_ts")
                
                # 如果window时间戳缺失，使用合理的时间范围
                current_ts = int(datetime.utcnow().timestamp())
                if window_start_ts is None:
                    window_start_ts = current_ts - 3600  # 默认1小时前
                if window_end_ts is None:
                    window_end_ts = current_ts

                # 确保时间戳有效
                window_start_ts = max(0, int(window_start_ts))
                window_end_ts = max(window_start_ts + 1, int(window_end_ts))

                # 计算成本指标
                fee_bps_eff = (float(fee) / float(notional) * 10_000.0) if float(notional) else None
                slp_bps_eff = (float(slp) / float(notional) * 10_000.0) if float(notional) else None
                cost_usdt_total = float(fee) + float(slp)
                cost_bps_total = (cost_usdt_total / float(notional) * 10_000.0) if float(notional) else None
                
                # 在dry-run模式下，如果成本为0，使用配置的默认值生成有意义的模拟数据
                if self.cfg.dry_run and (fee_bps_eff is None or fee_bps_eff == 0):
                    fee_bps_eff = fee_bps
                if self.cfg.dry_run and (slp_bps_eff is None or slp_bps_eff == 0):
                    slp_bps_eff = slp_bps
                if self.cfg.dry_run and cost_bps_total is None:
                    cost_bps_total = fee_bps + slp_bps

                event = {
                    "schema_version": 1,
                    "event_type": "fill",
                    "ts": current_ts,
                    "run_id": self.run_id,
                    "window_start_ts": window_start_ts,
                    "window_end_ts": window_end_ts,
                    "symbol": o.symbol,
                    "side": o.side,
                    "intent": o.intent,
                    "regime": meta.get("regime") or "Sideways",  # 默认市场状态
                    "router_action": "fill",
                    "notional_usdt": float(notional),
                    "mid_px": float(o.signal_price),
                    "bid": None,
                    "ask": None,
                    "spread_bps": None,
                    "fill_px": float(o.signal_price),
                    "slippage_bps": slp_bps_eff,
                    "fee_usdt": float(fee),
                    "fee_bps": fee_bps_eff,
                    "cost_usdt_total": cost_usdt_total,
                    "cost_bps_total": cost_bps_total,
                    "deadband_pct": meta.get("deadband_pct"),
                    "drift": meta.get("drift"),
                }
                append_cost_event(event)
            except Exception as e:
                # 记录错误但不中断执行
                import logging
                logging.getLogger(__name__).warning(f"Failed to append cost event: {e}")
                pass

        return ExecutionReport(timestamp=ts, dry_run=bool(self.cfg.dry_run), orders=list(order_batch or []))

    def _record(self, symbol: str, side: str, signal_price: float, execution_price: float) -> None:
        try:
            sp = float(signal_price)
            ep = float(execution_price)
            bps = ((ep - sp) / sp) * 10_000.0 if sp else 0.0
            con = sqlite3.connect(str(self.db_path))
            cur = con.cursor()
            cur.execute(
                "INSERT INTO slippage(ts, symbol, side, signal_price, execution_price, slippage_bps) VALUES (?,?,?,?,?,?)",
                (datetime.utcnow().isoformat() + "Z", symbol, side, sp, ep, float(bps)),
            )
            con.commit()
            con.close()
        except Exception:
            pass
