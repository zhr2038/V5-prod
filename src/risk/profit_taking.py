#!/usr/bin/env python3
"""
Profit-taking and rank-exit state manager.
"""

from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class ProfitLevel:
    profit_pct: float
    action: str
    stop_pct: float
    sell_pct: float = 0.0
    trail_buffer: float = 0.0


@dataclass
class PeakDrawdownLevel:
    profit_pct: float
    retrace_pct: float
    sell_pct: float


@dataclass
class PositionProfitState:
    symbol: str
    entry_price: float
    entry_time: datetime
    highest_price: float
    profit_high: float = 0.0
    current_stop: float = 0.0
    current_action: str = "hold"
    partial_sold: bool = False
    partial_sell_time: Optional[datetime] = None
    triggered_actions: List[str] = field(default_factory=list)
    rank_exit_streak: int = 0
    last_rank: Optional[int] = None
    last_rank_exit_time: Optional[datetime] = None


class ProfitTakingManager:
    """
    State machine for profit management.

    Legacy behavior:
    - +10%: move stop to breakeven
    - +20%: partial sell 30%
    - +30%: trailing stop
    - +50%: partial sell 50%

    New behavior:
    - optional peak-drawdown exits after profit retraces from the high-water mark
    - optional strict rank-exit mode with no profit-based rank relaxation
    """

    def __init__(
        self,
        *,
        rank_exit_strict_mode: bool = False,
        take_profit_sell_all_pct: float = 0.0,
        peak_drawdown_levels: Optional[List[PeakDrawdownLevel]] = None,
        state_path: str = "reports/profit_taking_state.json",
    ):
        self.profit_levels = [
            ProfitLevel(profit_pct=0.10, action="breakeven", stop_pct=0.0),
            ProfitLevel(profit_pct=0.20, action="partial_sell", stop_pct=0.10, sell_pct=0.30, trail_buffer=0.05),
            ProfitLevel(profit_pct=0.30, action="trailing", stop_pct=0.20, trail_buffer=0.08),
            ProfitLevel(profit_pct=0.50, action="partial_sell", stop_pct=0.35, sell_pct=0.50, trail_buffer=0.10),
        ]
        self.rank_exit_strict_mode = bool(rank_exit_strict_mode)
        self.take_profit_sell_all_pct = max(0.0, float(take_profit_sell_all_pct or 0.0))
        self.peak_drawdown_levels = sorted(
            list(peak_drawdown_levels or []),
            key=lambda level: float(level.profit_pct),
        )
        self.positions: Dict[str, PositionProfitState] = {}
        self.state_file = Path(state_path)
        self._load_state()

    @staticmethod
    def _pct_token(value: float) -> str:
        pct = f"{float(value) * 100.0:.2f}".rstrip("0").rstrip(".")
        return pct.replace(".", "_") + "pct"

    def _partial_action_key(self, level: ProfitLevel) -> str:
        return f"partial_sell_{self._pct_token(level.profit_pct)}"

    def _peak_drawdown_key(self, level: PeakDrawdownLevel) -> str:
        return f"peak_drawdown_{self._pct_token(level.profit_pct)}"

    def _take_profit_sell_all_key(self) -> Optional[str]:
        if self.take_profit_sell_all_pct <= 0:
            return None
        return f"take_profit_{self._pct_token(self.take_profit_sell_all_pct)}"

    def _legacy_partial_action_key(self) -> Optional[str]:
        for level in self.profit_levels:
            if level.action == "partial_sell":
                return self._partial_action_key(level)
        return None

    def _load_state(self):
        if not self.state_file.exists():
            return
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for sym, raw in (data or {}).items():
                if not isinstance(raw, dict):
                    continue
                triggered = [str(x) for x in (raw.get("triggered_actions") or []) if str(x)]
                state = PositionProfitState(
                    symbol=str(raw.get("symbol") or sym),
                    entry_price=float(raw["entry_price"]),
                    entry_time=datetime.fromisoformat(raw["entry_time"]),
                    highest_price=float(raw.get("highest_price") or raw["entry_price"]),
                    profit_high=float(raw.get("profit_high", 0.0) or 0.0),
                    current_stop=float(raw.get("current_stop", 0.0) or 0.0),
                    current_action=str(raw.get("current_action", "hold") or "hold"),
                    partial_sold=bool(raw.get("partial_sold", False)),
                    partial_sell_time=datetime.fromisoformat(raw["partial_sell_time"]) if raw.get("partial_sell_time") else None,
                    triggered_actions=triggered,
                    rank_exit_streak=int(raw.get("rank_exit_streak", 0) or 0),
                    last_rank=int(raw["last_rank"]) if raw.get("last_rank") is not None else None,
                    last_rank_exit_time=datetime.fromisoformat(raw["last_rank_exit_time"]) if raw.get("last_rank_exit_time") else None,
                )
                if state.partial_sold and not state.triggered_actions:
                    legacy_key = self._legacy_partial_action_key()
                    if legacy_key:
                        state.triggered_actions.append(legacy_key)
                self.positions[state.symbol] = state
        except Exception as e:
            print(f"[ProfitTaking] failed to load state: {e}")

    def _save_state(self):
        try:
            payload = {}
            for sym, state in self.positions.items():
                payload[sym] = {
                    "symbol": state.symbol,
                    "entry_price": state.entry_price,
                    "entry_time": state.entry_time.isoformat(),
                    "highest_price": state.highest_price,
                    "profit_high": state.profit_high,
                    "current_stop": state.current_stop,
                    "current_action": state.current_action,
                    "partial_sold": state.partial_sold,
                    "partial_sell_time": state.partial_sell_time.isoformat() if state.partial_sell_time else None,
                    "triggered_actions": list(state.triggered_actions or []),
                    "rank_exit_streak": int(state.rank_exit_streak),
                    "last_rank": state.last_rank,
                    "last_rank_exit_time": state.last_rank_exit_time.isoformat() if state.last_rank_exit_time else None,
                }
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[ProfitTaking] failed to save state: {e}")

    def register_position(
        self,
        symbol: str,
        entry_price: float,
        current_price: float = None,
        highest_price_hint: float | None = None,
    ):
        if symbol in self.positions:
            old_entry = float(self.positions[symbol].entry_price or 0.0)
            price_diff_pct = 1.0 if old_entry <= 0 else abs(entry_price - old_entry) / old_entry
            if price_diff_pct > 0.01:
                print(
                    f"[ProfitTaking] reset entry for {symbol}: "
                    f"{old_entry:.4f} -> {entry_price:.4f} ({price_diff_pct:.2%})"
                )
                state = self.positions[symbol]
                state.entry_price = entry_price
                state.entry_time = datetime.now()
                state.highest_price = current_price or entry_price
                state.profit_high = 0.0
                state.current_stop = entry_price * 0.95
                state.current_action = "hold"
                state.partial_sold = False
                state.partial_sell_time = None
                state.triggered_actions = []
                state.rank_exit_streak = 0
                state.last_rank = None
                state.last_rank_exit_time = None
                if highest_price_hint is not None:
                    state.highest_price = max(float(highest_price_hint), state.highest_price)
                    if state.entry_price > 0:
                        state.profit_high = max(
                            float(state.profit_high or 0.0),
                            (float(state.highest_price) - float(state.entry_price)) / float(state.entry_price),
                        )
                self._save_state()
            else:
                state = self.positions[symbol]
                changed = False
                if highest_price_hint is not None:
                    synced_high = max(float(state.highest_price or 0.0), float(highest_price_hint or 0.0))
                    if synced_high > float(state.highest_price or 0.0):
                        state.highest_price = synced_high
                        changed = True
                elif current_price is not None and float(current_price or 0.0) > float(state.highest_price or 0.0):
                    state.highest_price = float(current_price or 0.0)
                    changed = True
                if float(state.entry_price or 0.0) > 0:
                    synced_profit_high = (float(state.highest_price or 0.0) - float(state.entry_price)) / float(state.entry_price)
                    if synced_profit_high > float(state.profit_high or 0.0):
                        state.profit_high = synced_profit_high
                        changed = True
                if changed:
                    self._save_state()
            return

        highest_seed = current_price or entry_price
        if highest_price_hint is not None:
            highest_seed = max(float(highest_seed or entry_price), float(highest_price_hint))
        self.positions[symbol] = PositionProfitState(
            symbol=symbol,
            entry_price=entry_price,
            entry_time=datetime.now(),
            highest_price=highest_seed,
            current_stop=entry_price * 0.95,
            profit_high=max(0.0, (float(highest_seed) - float(entry_price)) / float(entry_price)) if float(entry_price or 0.0) > 0 else 0.0,
            triggered_actions=[],
        )
        self._save_state()
        print(
            f"[ProfitTaking] registered {symbol} @ {entry_price:.4f}, "
            f"initial stop {self.positions[symbol].current_stop:.4f}"
        )

    def evaluate(
        self,
        symbol: str,
        current_price: float,
        *,
        observed_low_price: float | None = None,
        observed_high_price: float | None = None,
    ) -> Tuple[str, float, str]:
        if symbol not in self.positions:
            return "hold", 0, "not_registered"

        state = self.positions[symbol]
        entry = float(state.entry_price or 0.0)
        if entry <= 0:
            return "hold", 0, "invalid_entry"

        changed = False
        eval_price = float(current_price)
        if observed_low_price is not None:
            eval_price = min(eval_price, float(observed_low_price))
        observed_peak_price = float(current_price)
        if observed_high_price is not None:
            observed_peak_price = max(observed_peak_price, float(observed_high_price))

        if observed_peak_price > float(state.highest_price or 0.0):
            state.highest_price = observed_peak_price
            changed = True
        profit_pct = (float(state.highest_price or observed_peak_price) - entry) / entry
        if profit_pct > float(state.profit_high or 0.0):
            state.profit_high = profit_pct
            changed = True

        take_profit_key = self._take_profit_sell_all_key()
        if take_profit_key and float(state.profit_high or 0.0) + 1e-12 >= self.take_profit_sell_all_pct:
            if take_profit_key not in state.triggered_actions:
                state.triggered_actions.append(take_profit_key)
                changed = True
            if state.current_action != take_profit_key:
                state.current_action = take_profit_key
                changed = True
            if changed:
                self._save_state()
            return "sell_all", current_price, take_profit_key

        if eval_price <= state.current_stop:
            if changed:
                self._save_state()
            return "sell_all", current_price, f"stop_loss_hit_{state.current_action}"

        if self.peak_drawdown_levels and state.highest_price > 0:
            retrace_from_peak = max(0.0, (state.highest_price - eval_price) / state.highest_price)
            for level in reversed(self.peak_drawdown_levels):
                action_key = self._peak_drawdown_key(level)
                if action_key in state.triggered_actions:
                    continue
                if state.profit_high + 1e-12 < float(level.profit_pct):
                    continue
                if retrace_from_peak + 1e-12 < float(level.retrace_pct):
                    continue
                state.triggered_actions.append(action_key)
                state.current_action = action_key
                if float(level.sell_pct) < 0.999:
                    state.partial_sold = True
                    state.partial_sell_time = datetime.now()
                self._save_state()
                reason = (
                    f"peak_drawdown_{self._pct_token(level.profit_pct)}"
                    f"_retrace_{self._pct_token(level.retrace_pct)}"
                )
                if float(level.sell_pct) >= 0.999:
                    return "sell_all", current_price, reason
                return "sell_partial", float(level.sell_pct), reason

        for level in self.profit_levels:
            if profit_pct + 1e-12 < float(level.profit_pct):
                continue
            if state.profit_high + 1e-12 < float(level.profit_pct):
                continue

            if level.action == "breakeven" and state.current_action == "hold":
                new_stop = entry * 1.01
                if new_stop > state.current_stop:
                    state.current_stop = new_stop
                    state.current_action = "breakeven"
                    self._save_state()
                    return "move_stop", new_stop, f"breakeven_at_{level.profit_pct:.0%}"

            elif level.action == "partial_sell":
                action_key = self._partial_action_key(level)
                if action_key in state.triggered_actions:
                    continue
                state.triggered_actions.append(action_key)
                state.partial_sold = True
                state.partial_sell_time = datetime.now()
                new_stop = entry * (1 + level.stop_pct)
                if new_stop > state.current_stop:
                    state.current_stop = new_stop
                state.current_action = f"partial_{level.profit_pct:.0%}"
                self._save_state()
                return "sell_partial", float(level.sell_pct), f"profit_{level.profit_pct:.0%}_take_{level.sell_pct:.0%}"

            elif level.action == "trailing":
                trail_stop = state.highest_price * (1 - level.trail_buffer)
                min_stop = entry * (1 + level.stop_pct)
                new_stop = max(trail_stop, min_stop, state.current_stop)
                if new_stop > state.current_stop:
                    state.current_stop = new_stop
                    state.current_action = f"trailing_{level.profit_pct:.0%}"
                    self._save_state()
                    return "move_stop", new_stop, f"trail_at_{level.profit_pct:.0%}"

        if changed:
            self._save_state()
        return "hold", 0, f"profit_{profit_pct:.1%}_holding"

    def should_exit_by_rank(
        self,
        symbol: str,
        current_rank: int,
        max_rank: int = 3,
        confirm_rounds: int = 2,
    ) -> Tuple[bool, str]:
        if symbol not in self.positions:
            return False, "not_in_positions"

        state = self.positions[symbol]
        current_rank_i = int(current_rank if current_rank is not None else 999)
        confirm_rounds_i = max(1, int(confirm_rounds or 1))
        effective_max_rank = int(max_rank)
        if not self.rank_exit_strict_mode and state.profit_high > 0.20:
            effective_max_rank = 5

        changed = False
        if current_rank_i > effective_max_rank:
            state.rank_exit_streak = int(state.rank_exit_streak or 0) + 1
            state.last_rank = current_rank_i
            changed = True

            if state.rank_exit_streak >= confirm_rounds_i:
                state.last_rank_exit_time = datetime.now()
                self._save_state()
                if state.profit_high > 0.20 and not self.rank_exit_strict_mode:
                    return True, f"rank_{current_rank_i}_exceeds_{effective_max_rank}_with_profit_streak_{state.rank_exit_streak}"
                return True, f"rank_{current_rank_i}_exceeds_{effective_max_rank}_streak_{state.rank_exit_streak}"

            if changed:
                self._save_state()
            return False, f"rank_exit_pending_{state.rank_exit_streak}/{confirm_rounds_i}"

        if int(state.rank_exit_streak or 0) != 0 or state.last_rank != current_rank_i:
            state.rank_exit_streak = 0
            state.last_rank = current_rank_i
            changed = True

        if changed:
            self._save_state()

        if state.profit_high > 0.20 and not self.rank_exit_strict_mode:
            return False, f"rank_{current_rank_i}_ok_high_profit"
        return False, f"rank_{current_rank_i}_ok"

    def get_position_summary(self, symbol: str, current_price: float) -> Optional[dict]:
        if symbol not in self.positions:
            return None

        state = self.positions[symbol]
        entry = float(state.entry_price or 0.0)
        if entry <= 0:
            return None
        profit_pct = (current_price - entry) / entry

        return {
            "symbol": symbol,
            "entry": entry,
            "current": current_price,
            "profit_pct": profit_pct,
            "profit_high": state.profit_high,
            "stop_price": state.current_stop,
            "stop_distance": (current_price - state.current_stop) / current_price if current_price > 0 else 0.0,
            "action": state.current_action,
            "triggered_actions": list(state.triggered_actions or []),
            "days_held": (datetime.now() - state.entry_time).days,
        }

    def clear_position(self, symbol: str):
        if symbol in self.positions:
            del self.positions[symbol]
            self._save_state()


if __name__ == "__main__":
    pm = ProfitTakingManager()
    pm.register_position("DOT/USDT", 1.30, 1.63)
    action, value, reason = pm.evaluate("DOT/USDT", 1.63)
    print(f"DOT @ 1.63: {action}, {value}, {reason}")
    action, value, reason = pm.evaluate("DOT/USDT", 1.31)
    print(f"DOT @ 1.31: {action}, {value}, {reason}")
    should_exit, reason = pm.should_exit_by_rank("DOT/USDT", 5)
    print(f"DOT rank 5: exit={should_exit}, {reason}")
