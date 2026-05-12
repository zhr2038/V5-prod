#!/usr/bin/env python3
import csv
import datetime as dt
import json
import os
import pathlib
import shutil
import subprocess
import tarfile
import tempfile


SCRIPT = pathlib.Path(__file__).with_name("generate_v5_bundle_remote.sh")


def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def iso(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fixture_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    label_ts = window_end - 3600
    run_id = now.strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "btc_leadership_probe_enabled: true\nprobe_time_stop_hours: 4\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_json(root / "reports/effective_live_config.json", {"btc_leadership_probe_enabled": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    decisions = [
        {
            "symbol": "BTC/USDT",
            "action": "skip",
            "reason": "btc_leadership_probe_alpha6_score_too_low",
            "btc_leadership_probe": True,
            "entry_px": 100.0,
            "alpha6_score": 0.1,
        },
        {
            "symbol": "BTC/USDT",
            "action": "skip",
            "reason": "btc_leadership_probe_alpha6_score_too_low",
            "btc_leadership_probe": True,
            "entry_px": 100.0,
            "alpha6_score": 0.1,
        },
        {
            "symbol": "ETH/USDT",
            "action": "skip",
            "reason": "btc_leadership_probe_no_alpha6_buy",
            "btc_leadership_probe": True,
            "entry_px": 200.0,
        },
        {
            "symbol": "BTC/USDT",
            "action": "skip",
            "reason": "btc_leadership_probe_not_flat",
            "btc_leadership_probe": True,
        },
        {
            "symbol": "BTC/USDT",
            "action": "skip",
            "reason": "btc_leadership_probe_cooldown",
            "btc_leadership_probe": True,
        },
        {
            "symbol": "SOL/USDT",
            "action": "skip",
            "reason": "btc_leadership_probe_risk_off",
            "btc_leadership_probe": True,
            "entry_px": 50.0,
        },
    ]
    audit = {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "counts": {
            "btc_leadership_probe_candidate_count": 6,
            "btc_leadership_probe_blocked_count": 6,
        },
        "router_decisions": decisions,
    }
    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", audit)
    write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})

    labels = [
        {
            "run_id": run_id,
            "ts_utc": iso(label_ts),
            "symbol": "BTC/USDT",
            "skip_reason": "btc_leadership_probe_alpha6_score_too_low",
            "entry_px": 100.0,
            "label_status": "complete",
            "label_4h_net_bps": 1,
            "label_8h_net_bps": 2,
            "label_12h_net_bps": 3,
            "label_24h_net_bps": 4,
        },
        {
            "run_id": run_id,
            "ts_utc": iso(label_ts),
            "symbol": "BTC/USDT",
            "skip_reason": "btc_leadership_probe_alpha6_score_too_low",
            "entry_px": 100.0,
            "label_status": "complete",
            "label_4h_net_bps": 1,
            "label_8h_net_bps": 2,
            "label_12h_net_bps": 3,
            "label_24h_net_bps": 4,
        },
        {
            "run_id": run_id,
            "ts_utc": iso(label_ts),
            "symbol": "ETH/USDT",
            "skip_reason": "btc_leadership_probe_no_alpha6_buy",
            "entry_px": 200.0,
            "label_status": "complete",
            "label_4h_net_bps": 5,
            "label_8h_net_bps": 6,
            "label_12h_net_bps": 7,
            "label_24h_net_bps": 8,
        },
    ]
    write_text(root / "reports/skipped_candidate_labels.jsonl", "\n".join(json.dumps(row) for row in labels) + "\n")

    return run_id


def fixture_open_position_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    run_id = now.strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "probe_time_stop_hours: 4\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_json(root / "reports/ledger_state.json", {"balances": {"USDT": "90", "BTC": "1.0"}})
    write_json(root / "reports/highest_px_state.json", {"BTC/USDT": {"symbol": "BTC/USDT", "highest_px": 112.0, "entry_px": 100.0}})
    write_json(root / "reports/profit_taking_state.json", {"BTC/USDT": {"symbol": "BTC/USDT", "entry_px": 100.0, "highest_price": 112.0, "current_stop": 95.0}})
    write_json(root / "reports/stop_loss_state.json", {"BTC/USDT": {"symbol": "BTC/USDT", "entry_price": 100.0, "highest_price": 112.0, "current_stop_price": 95.0, "current_stop_type": "initial_normal", "is_trailing": False}})
    write_json(root / "reports/fixed_stop_loss_state.json", {"BTC/USDT": {"entry_price": 100.0}})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")
    write_json(root / "reports/event_candidates.json", {"regime": "TRENDING", "candidates": [{"symbol": "BTC/USDT", "price": 112.0}]})

    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "regime": "Trending",
        "router_decisions": [
            {"symbol": "BTC/USDT", "action": "create", "intent": "OPEN_LONG", "side": "buy", "reason": "ok", "notional": 100.0}
        ],
    })
    write_text(
        run_dir / "trades.csv",
        f"ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n{iso(window_end + 20)},{run_id},BTC/USDT,OPEN_LONG,buy,1,100,100,0.1\n",
    )
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    return run_id


def fixture_dust_residual_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)

    write_text(
        root / "configs/live_prod.yaml",
        "execution:\n  dust_usdt_ignore: 1.0\n  min_trade_value_usdt: 10.0\n  probe_time_stop_hours: 4\n",
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_json(
        root / "reports/ledger_state.json",
        {"assets": [{"asset": "BTC", "qty": "0.000000002", "eqUsd": "0.00015"}, {"asset": "USDT", "eqUsd": "100"}]},
    )
    write_json(root / "reports/effective_live_config.json", {"execution": {"dust_usdt_ignore": 1.0, "min_trade_value_usdt": 10.0}})
    write_json(root / "reports/event_candidates.json", {"regime": "Trending", "candidates": [{"symbol": "BTC/USDT", "price": 78277.4}]})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    def run_dir_for(hours_ago):
        run_dt = now - dt.timedelta(hours=hours_ago)
        run_id = run_dt.strftime("%Y%m%d_%H")
        run_dir = root / "reports/runs/prod" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        write_text(run_dir / "equity.jsonl", "{}\n")
        write_json(run_dir / "summary.json", {"run_id": run_id})
        return run_id, run_dir, int(run_dt.timestamp())

    run_id, run_dir, ts = run_dir_for(5)
    write_json(run_dir / "decision_audit.json", {
        "window_end_ts": ts,
        "router_decisions": [
            {"symbol": "BTC/USDT", "action": "create", "intent": "OPEN_LONG", "side": "buy", "reason": "normal_entry"}
        ],
    })
    write_text(
        run_dir / "trades.csv",
        f"ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n{iso(ts + 20)},{run_id},BTC/USDT,OPEN_LONG,buy,0.00020939,76412.1,15.999751719,0.016\n",
    )

    run_id, run_dir, ts = run_dir_for(4)
    write_json(run_dir / "decision_audit.json", {
        "window_end_ts": ts,
        "router_decisions": [
            {"symbol": "BTC/USDT", "action": "create", "intent": "CLOSE_LONG", "side": "sell", "reason": "close", "source_reason": "rank_exit"}
        ],
    })
    write_text(
        run_dir / "trades.csv",
        f"ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n{iso(ts + 20)},{run_id},BTC/USDT,CLOSE_LONG,sell,0.00020905,78100,16.332805,0.0163\n",
    )

    run_id, run_dir, ts = run_dir_for(3)
    write_json(run_dir / "decision_audit.json", {
        "window_end_ts": ts,
        "router_decisions": [
            {"symbol": "BTC/USDT", "action": "create", "intent": "CLOSE_LONG", "side": "sell", "reason": "close", "source_reason": "probe_stop_loss"}
        ],
    })
    write_text(
        run_dir / "trades.csv",
        f"ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n{iso(ts + 20)},{run_id},BTC/USDT,CLOSE_LONG,sell,0.00000021,78277.4,0.016438254,0.00002\n",
    )
    return run_id


def fixture_last_72h_trade_no_24h_root(root):
    fixture_root(root)
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)

    def add_trade_run(hours_ago, intent, side, qty, price, fee, reason, source_reason=""):
        run_dt = now - dt.timedelta(hours=hours_ago)
        run_id = run_dt.strftime("%Y%m%d_%H")
        ts = int(run_dt.timestamp())
        run_dir = root / "reports/runs/prod" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        write_json(run_dir / "decision_audit.json", {
            "window_end_ts": ts,
            "router_decisions": [
                {
                    "symbol": "BTC/USDT",
                    "action": "create",
                    "intent": intent,
                    "side": side,
                    "reason": reason,
                    "source_reason": source_reason,
                }
            ],
        })
        write_text(
            run_dir / "trades.csv",
            "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n"
            f"{iso(ts + 20)},{run_id},BTC/USDT,{intent},{side},{qty},{price},{qty * price},{fee}\n",
        )
        write_text(run_dir / "equity.jsonl", "{}\n")
        write_json(run_dir / "summary.json", {"run_id": run_id})

    add_trade_run(48, "OPEN_LONG", "buy", 1.0, 100.0, 0.1, "ok")
    add_trade_run(47, "CLOSE_LONG", "sell", 1.0, 110.0, 0.11, "exit_signal_priority", "rank_exit")
    write_json(root / "reports/negative_expectancy_cooldown.json", {
        "stats": {
            "BTC/USDT": {
                "closed_cycles": 1,
                "net_pnl_sum_usdt": -0.1,
                "net_expectancy_bps": -10.0,
                "fast_fail_net_expectancy_bps": -12.0,
            }
        }
    })


def fixture_negative_expectancy_consistent_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    write_text(root / "configs/live_prod.yaml", "probe_time_stop_hours: 4\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    current_run_id = now.strftime("%Y%m%d_%H")
    current_run_dir = root / "reports/runs/prod" / current_run_id
    write_json(current_run_dir / "decision_audit.json", {"window_end_ts": int(now.timestamp()), "router_decisions": []})
    write_text(current_run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(current_run_dir / "equity.jsonl", "{}\n")
    write_json(current_run_dir / "summary.json", {"run_id": current_run_id})

    def add_trade_run(hours_ago, intent, side, qty, price, fee, reason):
        run_dt = now - dt.timedelta(hours=hours_ago)
        run_id = run_dt.strftime("%Y%m%d_%H")
        ts = int(run_dt.timestamp())
        run_dir = root / "reports/runs/prod" / run_id
        write_json(run_dir / "decision_audit.json", {
            "window_end_ts": ts,
            "router_decisions": [
                {"symbol": "BTC/USDT", "action": "create", "intent": intent, "side": side, "reason": reason}
            ],
        })
        write_text(
            run_dir / "trades.csv",
            "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n"
            f"{iso(ts + 20)},{run_id},BTC/USDT,{intent},{side},{qty},{price},{qty * price},{fee}\n",
        )
        write_text(run_dir / "equity.jsonl", "{}\n")
        write_json(run_dir / "summary.json", {"run_id": run_id})

    add_trade_run(6, "OPEN_LONG", "buy", 1.0, 100.0, 0.1, "ok")
    add_trade_run(5, "CLOSE_LONG", "sell", 1.0, 110.0, 0.11, "exit_signal_priority")
    write_json(root / "reports/negative_expectancy_cooldown.json", {
        "stats": {
            "BTC/USDT": {
                "closed_cycles": 1,
                "net_pnl_sum_usdt": 9.78,
                "net_expectancy_bps": 978.0,
                "fast_fail_net_expectancy_bps": 977.0,
            }
        }
    })


def fixture_negative_expectancy_missing_root(root):
    fixture_negative_expectancy_consistent_root(root)
    write_json(root / "reports/negative_expectancy_cooldown.json", {"stats": {}})


def fixture_config_runtime_consumption_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    run_id = now.strftime("%Y%m%d_%H")

    write_text(
        root / "configs/live_prod.yaml",
        "\n".join(
            [
                "execution:",
                "  split_orders: 3",
                "  split_interval_sec: 3.0",
                "  same_symbol_reentry_enabled: true",
                "  btc_leadership_probe_enabled: true",
                "  protect_profit_lock_enabled: true",
                "  swing_hold_enabled: true",
                "  swing_min_hold_hours: 24",
                "  protect_recovery_multi_position_enabled: false",
                "  protect_negative_expectancy_short_cycle_guard_enabled: true",
                "  open_long_entry_guard_fail_open_buy: false",
                "  open_long_entry_guard_fail_open_sell: true",
                "quant_lab:",
                "  enabled: true",
                "  mode: shadow",
                "diagnostics:",
                "  multi_position_swing_shadow_enabled: true",
                "  alt_impulse_shadow_enabled: true",
                "",
            ]
        ),
    )
    write_text(
        root / "configs/schema.py",
        "\n".join(
            [
                "split_orders: int = 1",
                "split_interval_sec: float = 0.0",
                "same_symbol_reentry_enabled: bool = False",
                "btc_leadership_probe_enabled: bool = False",
                "protect_profit_lock_enabled: bool = False",
                "swing_hold_enabled: bool = True",
                "swing_min_hold_hours: int = 24",
                "protect_recovery_multi_position_enabled: bool = False",
                "protect_negative_expectancy_short_cycle_guard_enabled: bool = True",
                "open_long_entry_guard_fail_open_buy: bool = False",
                "open_long_entry_guard_fail_open_sell: bool = True",
                "multi_position_swing_shadow_enabled: bool = True",
                "alt_impulse_shadow_enabled: bool = True",
                "probe_exit_enabled: bool = False",
                "",
            ]
        ),
    )
    write_text(
        root / "main.py",
        "\n".join(
            [
                "def boot(cfg):",
                "    getattr(cfg.execution, 'swing_min_hold_hours', 24)",
                "",
            ]
        ),
    )
    write_text(
        root / "src/core/pipeline.py",
        "\n".join(
            [
                "def consume(cfg):",
                "    getattr(cfg.execution, 'same_symbol_reentry_enabled', False)",
                "    getattr(cfg.execution, 'btc_leadership_probe_enabled', False)",
                "    getattr(cfg.execution, 'protect_profit_lock_enabled', False)",
                "    getattr(cfg.execution, 'swing_hold_enabled', True)",
                "    getattr(cfg.execution, 'protect_recovery_multi_position_enabled', False)",
                "    getattr(cfg.execution, 'protect_negative_expectancy_short_cycle_guard_enabled', False)",
                "",
            ]
        ),
    )
    write_text(
        root / "src/execution/live_execution_engine.py",
        "\n".join(
            [
                "def guard(cfg):",
                "    getattr(cfg.execution, 'open_long_entry_guard_fail_open_buy', False)",
                "    getattr(cfg.execution, 'open_long_entry_guard_fail_open_sell', True)",
                "",
            ]
        ),
    )
    write_text(
        root / "src/reporting/decision_audit.py",
        "CONFIG_KEYS = ['split_orders', 'split_interval_sec']\n",
    )
    write_text(
        root / "src/reporting/multi_position_swing_shadow.py",
        "def enabled(cfg):\n    return getattr(cfg.diagnostics, 'multi_position_swing_shadow_enabled', True)\n",
    )
    write_text(
        root / "src/reporting/alt_impulse_shadow.py",
        "def enabled(cfg):\n    return getattr(cfg.diagnostics, 'alt_impulse_shadow_enabled', True)\n",
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_json(
        root / "reports/effective_live_config.json",
        {
            "execution": {
                "split_orders": 3,
                "split_interval_sec": 3.0,
                "same_symbol_reentry_enabled": True,
                "btc_leadership_probe_enabled": True,
                "protect_profit_lock_enabled": True,
                "swing_hold_enabled": True,
                "swing_min_hold_hours": 24,
                "protect_recovery_multi_position_enabled": False,
                "protect_negative_expectancy_short_cycle_guard_enabled": True,
                "open_long_entry_guard_fail_open_buy": False,
                "open_long_entry_guard_fail_open_sell": True,
            },
            "diagnostics": {
                "multi_position_swing_shadow_enabled": True,
                "alt_impulse_shadow_enabled": True,
            }
        },
    )
    write_text(root / "logs/v5_runtime.log", "fixture log\n")
    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {"now_ts": window_end + 15, "window_end_ts": window_end})
    write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    return run_id


def fixture_rank_exit_consistency_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    open_run_dt = now - dt.timedelta(hours=1)
    open_run_id = open_run_dt.strftime("%Y%m%d_%H")
    close_run_id = now.strftime("%Y%m%d_%H")
    open_ts = int(open_run_dt.timestamp())
    close_ts = int(now.timestamp())

    write_text(root / "configs/live_prod.yaml", "close_only_weight_eps: 0.001\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "rank_exit_target_still_positive: BNB/USDT target_w=0.1500 > eps=0.0010, rank=4, source=fused\n")

    open_run_dir = root / "reports/runs/prod" / open_run_id
    write_json(open_run_dir / "decision_audit.json", {"window_end_ts": open_ts, "router_decisions": []})
    write_text(
        open_run_dir / "trades.csv",
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,entry_reason\n"
        f"{iso(open_ts + 20)},{open_run_id},BNB/USDT,OPEN_LONG,buy,1.0,600.0,600.0,0.6,ok\n",
    )
    write_text(open_run_dir / "equity.jsonl", "{}\n")
    write_json(open_run_dir / "summary.json", {"run_id": open_run_id})

    close_run_dir = root / "reports/runs/prod" / close_run_id
    write_json(close_run_dir / "decision_audit.json", {
        "now_ts": close_ts + 15,
        "window_end_ts": close_ts,
        "notes": [
            "rank_exit_target_still_positive: BNB/USDT target_w=0.1500 > eps=0.0010, rank=4, source=fused"
        ],
        "exit_signals": [],
        "router_decisions": [
            {"symbol": "BNB/USDT", "action": "skip", "reason": "target_still_positive"}
        ],
        "targets_post_risk": {"BNB/USDT": 0.15},
        "target_execution_explain": [
            {"symbol": "BNB/USDT", "target_w": 0.15, "selected_rank": 2}
        ],
    })
    write_text(
        close_run_dir / "trades.csv",
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,exit_reason\n"
        f"{iso(close_ts + 20)},{close_run_id},BNB/USDT,CLOSE_LONG,sell,1.0,610.0,610.0,0.61,rank_exit_4\n",
    )
    write_text(close_run_dir / "equity.jsonl", "{}\n")
    write_json(close_run_dir / "summary.json", {"run_id": close_run_id})
    return close_run_id


def fixture_rank_exit_log_only_consistency_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    open_run_dt = now - dt.timedelta(hours=1)
    open_run_id = open_run_dt.strftime("%Y%m%d_%H")
    close_run_id = now.strftime("%Y%m%d_%H")
    open_ts = int(open_run_dt.timestamp())
    close_ts = int(now.timestamp())
    log_ts = close_ts + 20

    write_text(root / "configs/live_prod.yaml", "close_only_weight_eps: 0.001\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(
        root / "logs/v5_runtime.log",
        f"{iso(log_ts)} INFO TRADE_SAFETY: sell BNB-USDT, tdMode=cash, intent=CLOSE_LONG, reason=rank_exit_4, notional=12.3400\n"
        f"{iso(log_ts)} WARNING rank_exit_target_still_positive: BNB/USDT target_w=0.1500 > eps=0.0010, rank=2, source=fused\n",
    )

    open_run_dir = root / "reports/runs/prod" / open_run_id
    write_json(open_run_dir / "decision_audit.json", {"window_end_ts": open_ts, "router_decisions": []})
    write_text(
        open_run_dir / "trades.csv",
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,entry_reason\n"
        f"{iso(open_ts + 20)},{open_run_id},BNB/USDT,OPEN_LONG,buy,1.0,600.0,600.0,0.6,ok\n",
    )
    write_text(open_run_dir / "equity.jsonl", "{}\n")
    write_json(open_run_dir / "summary.json", {"run_id": open_run_id})

    close_run_dir = root / "reports/runs/prod" / close_run_id
    write_json(close_run_dir / "decision_audit.json", {
        "now_ts": close_ts + 15,
        "window_end_ts": close_ts,
        "notes": [
            "rank_exit_target_still_positive: BNB/USDT target_w=0.1500 > eps=0.0010, rank=2, source=fused"
        ],
        "exit_signals": [],
        "router_decisions": [
            {"symbol": "BNB/USDT", "action": "skip", "reason": "deadband"}
        ],
        "targets_post_risk": {"BNB/USDT": 0.15},
        "target_execution_explain": [
            {"symbol": "BNB/USDT", "target_w": 0.15, "selected_rank": 2}
        ],
    })
    write_text(
        close_run_dir / "trades.csv",
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n"
        f"{iso(log_ts)},{close_run_id},BNB/USDT,CLOSE_LONG,sell,1.0,610.0,610.0,0.61\n",
    )
    write_text(close_run_dir / "equity.jsonl", "{}\n")
    write_json(close_run_dir / "summary.json", {"run_id": close_run_id})
    return close_run_id


def fixture_legacy_rank_exit_log_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    run_id = now.strftime("%Y%m%d_%H")
    window_end = int(now.timestamp())
    old_ts = dt.datetime(2026, 3, 17, 8, 0, 49, tzinfo=dt.timezone.utc)
    old_iso = old_ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    write_text(root / "configs/live_prod.yaml", "close_only_weight_eps: 0.001\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(
        root / "logs/v5_runtime.log",
        f"{old_iso} INFO TRADE_SAFETY: sell ETH-USDT, tdMode=cash, intent=CLOSE_LONG, reason=rank_exit_4, notional=12.3400\n"
        f"{old_iso} WARNING rank_exit_target_still_positive: ETH/USDT target_w=0.1500 > eps=0.0010, rank=2, source=fused\n",
    )

    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "notes": [
            "rank_exit_target_still_positive: ETH/USDT target_w=0.1500 > eps=0.0010, rank=2, source=fused"
        ],
        "exit_signals": [],
        "router_decisions": [
            {"symbol": "ETH/USDT", "action": "skip", "reason": "deadband"}
        ],
        "targets_post_risk": {"ETH/USDT": 0.15},
        "target_execution_explain": [
            {"symbol": "ETH/USDT", "target_w": 0.15, "selected_rank": 2}
        ],
    })
    write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    return run_id


def fixture_protect_sideways_normal_entry_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    open_run_dt = now - dt.timedelta(hours=6)
    close_run_dt = now
    open_run_id = open_run_dt.strftime("%Y%m%d_%H")
    close_run_id = close_run_dt.strftime("%Y%m%d_%H")
    open_ts = int(open_run_dt.timestamp())
    close_ts = int(close_run_dt.timestamp())

    write_text(root / "configs/live_prod.yaml", "protect_entry_alpha6_min_score: 0.40\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    open_run_dir = root / "reports/runs/prod" / open_run_id
    write_json(open_run_dir / "decision_audit.json", {
        "now_ts": open_ts + 15,
        "window_end_ts": open_ts,
        "regime": "Sideways",
        "current_level": "PROTECT",
        "router_decisions": [
            {"symbol": "BNB/USDT", "action": "create", "intent": "OPEN_LONG", "side": "buy", "reason": "ok"}
        ],
        "target_execution_explain": [
            {
                "symbol": "BNB/USDT",
                "target_w": 0.15,
                "router_action": "create",
                "router_reason": "ok",
                "final_score": 0.74,
                "alpha6_score": 0.572,
                "f4_volume_expansion": 1.925,
                "f5_rsi_trend_confirm": 0.302,
                "trend_score": 0.81,
                "current_level": "PROTECT",
                "regime": "Sideways",
            }
        ],
    })
    open_rows = ["ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,entry_reason\n"]
    for idx in range(5):
        open_rows.append(
            f"{iso(open_ts + 20 + idx)},{open_run_id},BNB/USDT,OPEN_LONG,buy,1.0,628.4,628.4,0.6284,ok\n"
        )
    write_text(open_run_dir / "trades.csv", "".join(open_rows))
    write_text(open_run_dir / "equity.jsonl", "{}\n")
    write_json(open_run_dir / "summary.json", {"run_id": open_run_id})

    close_run_dir = root / "reports/runs/prod" / close_run_id
    write_json(close_run_dir / "decision_audit.json", {
        "now_ts": close_ts + 15,
        "window_end_ts": close_ts,
        "regime": "Sideways",
        "current_level": "PROTECT",
        "router_decisions": [
            {"symbol": "BNB/USDT", "action": "create", "intent": "CLOSE_LONG", "side": "sell", "reason": "stop_loss", "source_reason": "stop_loss"}
        ],
    })
    close_rows = ["ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,exit_reason\n"]
    for idx in range(5):
        close_rows.append(
            f"{iso(close_ts + 20 + idx)},{close_run_id},BNB/USDT,CLOSE_LONG,sell,1.0,621.5,621.5,0.6215,stop_loss\n"
        )
    write_text(close_run_dir / "trades.csv", "".join(close_rows))
    write_text(close_run_dir / "equity.jsonl", "{}\n")
    write_json(close_run_dir / "summary.json", {"run_id": close_run_id})
    return close_run_id


def fixture_high_score_blocked_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    run_id = now.strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "protect_entry_alpha6_min_score: 0.40\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "regime": "Trending",
        "target_execution_explain": [
            {
                "symbol": "ETH/USDT",
                "target_w": 0.15,
                "final_score": 1.0,
                "selected_rank": 1,
                "router_action": "skip",
                "router_reason": "protect_entry_trend_only",
                "high_score_but_not_executed": True,
                "high_score_block_category": "trend_only",
                "trend_score": 1.0,
                "trend_side": "buy",
                "alpha6_score": None,
                "alpha6_side": None,
                "f4_volume_expansion": None,
                "f5_rsi_trend_confirm": None,
                "current_level": "PROTECT",
                "regime": "Trending",
            },
            {
                "symbol": "ETH/USDT",
                "target_w": 0.12,
                "final_score": 0.823,
                "selected_rank": 1,
                "router_action": "skip",
                "router_reason": "protect_entry_no_alpha6_confirmation",
                "high_score_but_not_executed": True,
                "high_score_block_category": "alpha6_sell",
                "trend_score": 1.0,
                "trend_side": "buy",
                "alpha6_score": 0.266,
                "alpha6_side": "sell",
                "f4_volume_expansion": 0.10,
                "f5_rsi_trend_confirm": 0.37,
                "current_level": "PROTECT",
                "regime": "Trending",
            },
            {
                "symbol": "BNB/USDT",
                "target_w": 0.15,
                "final_score": 0.95,
                "selected_rank": 2,
                "router_action": "create",
                "router_reason": "ok",
                "high_score_but_not_executed": False,
                "high_score_block_category": None,
                "trend_score": None,
                "trend_side": None,
                "alpha6_score": 0.56,
                "alpha6_side": "buy",
                "f4_volume_expansion": 0.10,
                "f5_rsi_trend_confirm": 0.46,
                "current_level": "PROTECT",
                "regime": "Trending",
            },
        ],
    })
    write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    write_text(
        root / "reports/skipped_candidate_labels.jsonl",
        "\n".join(
            json.dumps(row, ensure_ascii=False)
            for row in [
                {
                "ts_utc": iso(window_end),
                "run_id": run_id,
                "symbol": "ETH/USDT",
                "intended_side": "buy",
                "skip_reason": "protect_entry_trend_only",
                "high_score_blocked_target": True,
                "high_score_block_category": "trend_only",
                "final_score": 1.0,
                "target_w": 0.15,
                "trend_score": 1.0,
                "trend_side": "buy",
                "entry_px": 100.0,
                "rt_cost_bps": 30.0,
                "current_level": "PROTECT",
                "regime": "Trending",
                "label_4h_net_bps": 70.0,
                "label_8h_net_bps": 170.0,
                "label_12h_net_bps": 270.0,
                "label_24h_net_bps": 370.0,
                "label_status": "complete",
                },
                {
                    "ts_utc": iso(window_end),
                    "run_id": run_id,
                    "symbol": "ETH/USDT",
                    "intended_side": "buy",
                    "skip_reason": "protect_entry_no_alpha6_confirmation",
                    "high_score_blocked_target": True,
                    "high_score_block_category": "alpha6_sell",
                    "final_score": 0.823,
                    "target_w": 0.12,
                    "trend_score": 1.0,
                    "trend_side": "buy",
                    "alpha6_score": 0.266,
                    "alpha6_side": "sell",
                    "f4_volume_expansion": 0.10,
                    "f5_rsi_trend_confirm": 0.37,
                    "entry_px": 100.0,
                    "rt_cost_bps": 30.0,
                    "current_level": "PROTECT",
                    "regime": "Trending",
                    "label_status": "pending",
                },
            ]
        )
        + "\n",
    )
    return run_id


def fixture_high_score_missing_label_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    old_window_end = window_end - 30 * 3600
    current_run_id = now.strftime("%Y%m%d_%H")
    old_run_id = dt.datetime.fromtimestamp(old_window_end, dt.timezone.utc).strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "protect_entry_alpha6_min_score: 0.40\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    current_run = root / "reports/runs/prod" / current_run_id
    write_json(current_run / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "regime": "Trending",
        "target_execution_explain": [],
    })
    write_text(current_run / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(current_run / "equity.jsonl", "{}\n")
    write_json(current_run / "summary.json", {"run_id": current_run_id})

    old_run = root / "reports/runs/prod" / old_run_id
    write_json(old_run / "decision_audit.json", {
        "now_ts": old_window_end + 15,
        "window_end_ts": old_window_end,
        "regime": "Trending",
        "target_execution_explain": [
            {
                "symbol": "ETH/USDT",
                "target_w": 0.15,
                "final_score": 1.0,
                "selected_rank": 1,
                "router_action": "skip",
                "router_reason": "protect_entry_trend_only",
                "high_score_but_not_executed": True,
                "high_score_block_category": "trend_only",
                "trend_score": 1.0,
                "trend_side": "buy",
                "current_level": "PROTECT",
                "regime": "Trending",
            }
        ],
    })
    write_text(old_run / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(old_run / "equity.jsonl", "{}\n")
    write_json(old_run / "summary.json", {"run_id": old_run_id})
    return old_run_id


def fixture_high_score_same_symbol_reentry_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    old_window_end = window_end - 49 * 3600
    current_run_id = now.strftime("%Y%m%d_%H")
    old_run_id = dt.datetime.fromtimestamp(old_window_end, dt.timezone.utc).strftime("%Y%m%d_%H")
    old_ts = old_window_end + 15

    write_text(
        root / "configs/live_prod.yaml",
        "diagnostics:\n  extended_label_horizons_hours: [4, 8, 12, 24, 48, 72, 120]\n",
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    current_run = root / "reports/runs/prod" / current_run_id
    write_json(current_run / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "regime": "Trending",
        "target_execution_explain": [],
    })
    write_text(current_run / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(current_run / "equity.jsonl", "{}\n")
    write_json(current_run / "summary.json", {"run_id": current_run_id})

    old_run = root / "reports/runs/prod" / old_run_id
    write_json(old_run / "decision_audit.json", {
        "now_ts": old_ts,
        "window_end_ts": old_window_end,
        "regime": "Trending",
        "targets_post_risk": {"SOL/USDT": 0.15},
        "target_execution_explain": [
            {
                "symbol": "SOL/USDT",
                "target_w": 0.15,
                "final_score": 0.96,
                "selected_rank": 1,
                "router_action": "skip",
                "router_reason": "same_symbol_reentry_cooldown",
                "high_score_but_not_executed": True,
                "high_score_block_category": "same_symbol_reentry_cooldown",
                "trend_score": 0.91,
                "trend_side": "buy",
                "alpha6_score": 0.72,
                "alpha6_side": "buy",
                "f4_volume_expansion": 0.4,
                "f5_rsi_trend_confirm": 0.5,
                "latest_px": 100.0,
                "current_level": "PROTECT",
                "regime": "Trending",
            }
        ],
        "router_decisions": [
            {
                "symbol": "SOL/USDT",
                "action": "skip",
                "reason": "same_symbol_reentry_cooldown",
                "final_score": 0.96,
                "selected_rank": 1,
                "target_w": 0.15,
                "latest_px": 100.0,
                "last_exit_reason": "protect_profit_lock_trailing",
                "last_exit_px": 100.5,
                "highest_px_before_exit": 101.2,
                "elapsed_hours": 5.99,
                "required_cooldown_hours": 6.0,
                "breakout_exception_met": False,
            }
        ],
    })
    write_text(old_run / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(old_run / "equity.jsonl", "{}\n")
    write_json(old_run / "summary.json", {"run_id": old_run_id})

    cache_path = root / "data/cache/SOL_USDT_1H_fixture.csv"
    write_text(
        cache_path,
        "timestamp,open,high,low,close,volume\n"
        f"{iso(old_window_end)},100,100,100,100,1000\n"
        f"{iso(old_window_end + 4 * 3600)},102,102,102,102,1000\n"
        f"{iso(old_window_end + 8 * 3600)},101,101,101,101,1000\n"
        f"{iso(old_window_end + 12 * 3600)},103,103,103,103,1000\n"
        f"{iso(old_window_end + 24 * 3600)},105,105,105,105,1000\n"
        f"{iso(old_window_end + 48 * 3600)},110,110,110,110,1000\n",
    )
    return old_run_id


def fixture_high_score_non_labelable_management_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    old_window_end = window_end - 30 * 3600
    current_run_id = now.strftime("%Y%m%d_%H")
    old_run_id = dt.datetime.fromtimestamp(old_window_end, dt.timezone.utc).strftime("%Y%m%d_%H")
    old_ts = old_window_end + 15

    write_text(root / "configs/live_prod.yaml", "protect_entry_alpha6_min_score: 0.40\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    current_run = root / "reports/runs/prod" / current_run_id
    write_json(current_run / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "regime": "Trending",
        "target_execution_explain": [],
    })
    write_text(current_run / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(current_run / "equity.jsonl", "{}\n")
    write_json(current_run / "summary.json", {"run_id": current_run_id})

    old_run = root / "reports/runs/prod" / old_run_id
    write_json(old_run / "decision_audit.json", {
        "now_ts": old_ts,
        "window_end_ts": old_window_end,
        "regime": "Trending",
        "target_execution_explain": [
            {
                "symbol": "SOL/USDT",
                "target_w": 0.15,
                "final_score": 0.95,
                "selected_rank": 1,
                "router_action": "skip",
                "router_reason": "rank_exit_target_still_positive",
                "high_score_but_not_executed": True,
                "high_score_block_category": "other",
                "current_level": "PROTECT",
                "regime": "Trending",
            },
            {
                "symbol": "SOL/USDT",
                "target_w": 0.15,
                "final_score": 0.91,
                "selected_rank": 1,
                "router_action": "skip",
                "router_reason": "exit_order_selected",
                "high_score_but_not_executed": True,
                "high_score_block_category": "other",
                "current_level": "PROTECT",
                "regime": "Trending",
            },
            {
                "symbol": "BNB/USDT",
                "target_w": 0.15,
                "final_score": 0.90,
                "selected_rank": 2,
                "router_action": "skip",
                "router_reason": "protect_entry_trend_only",
                "high_score_but_not_executed": True,
                "high_score_block_category": "trend_only",
                "current_level": "PROTECT",
                "regime": "Trending",
            },
        ],
    })
    write_text(old_run / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(old_run / "equity.jsonl", "{}\n")
    write_json(old_run / "summary.json", {"run_id": old_run_id})
    write_text(
        root / "reports/skipped_candidate_labels.jsonl",
        json.dumps(
            {
                "ts_utc": iso(old_ts),
                "run_id": old_run_id,
                "symbol": "BNB/USDT",
                "intended_side": "buy",
                "skip_reason": "protect_entry_trend_only",
                "high_score_blocked_target": True,
                "high_score_block_category": "trend_only",
                "final_score": 0.90,
                "target_w": 0.15,
                "entry_px": 200.0,
                "rt_cost_bps": 30.0,
                "label_24h_net_bps": 70.0,
                "label_status": "complete",
            },
            ensure_ascii=False,
        )
        + "\n",
    )
    return old_run_id


def fixture_alt_impulse_shadow_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    run_id = now.strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "alt_impulse_shadow_enabled: true\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")
    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "regime": "Trending",
        "target_execution_explain": [],
    })
    write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    write_text(
        root / "reports/alt_impulse_shadow_labels.jsonl",
        "\n".join(
            json.dumps(row, ensure_ascii=False)
            for row in [
                {
                    "ts_utc": iso(window_end),
                    "run_id": run_id,
                    "symbol": "ETH/USDT",
                    "entry_px": 100.0,
                    "final_score": 1.0,
                    "trend_score": 1.0,
                    "trend_side": "buy",
                    "skip_reason": "protect_entry_trend_only",
                    "btc_4h_ret_bps": 100.0,
                    "whitelist_positive_4h_count": 3,
                    "regime": "Trending",
                    "current_level": "PROTECT",
                    "rt_cost_bps": 30.0,
                    "label_4h_net_bps": 70.0,
                    "label_8h_net_bps": 170.0,
                    "label_12h_net_bps": 270.0,
                    "label_24h_net_bps": 370.0,
                    "label_status": "complete",
                    "label_not_observable_reason": "missing_entry_px",
                },
                {
                    "ts_utc": iso(window_end),
                    "run_id": run_id,
                    "symbol": "SOL/USDT",
                    "entry_px": 50.0,
                    "final_score": 0.91,
                    "trend_score": 0.95,
                    "trend_side": "buy",
                    "skip_reason": "protect_entry_no_alpha6_confirmation",
                    "btc_4h_ret_bps": 100.0,
                    "whitelist_positive_4h_count": 3,
                    "regime": "Trending",
                    "current_level": "PROTECT",
                    "rt_cost_bps": 30.0,
                    "label_status": "pending",
                },
            ]
        )
        + "\n",
    )
    return run_id


def write_ohlcv_cache(root, symbol, rows):
    prefix = symbol.replace("/", "_").replace("-", "_")
    path = root / "data/cache" / f"{prefix}_1H_fixture.csv"
    lines = ["timestamp,open,high,low,close,volume"]
    for ts, close in rows:
        lines.append(f"{iso(ts)},{close},{close},{close},{close},1")
    write_text(path, "\n".join(lines) + "\n")
    return path


def fixture_multi_position_swing_shadow_root(root):
    run_id = fixture_root(root)
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    entry_ts = window_end - 50 * 3600

    write_text(
        root / "reports/multi_position_swing_shadow_labels.jsonl",
        "\n".join(
            json.dumps(row, ensure_ascii=False)
            for row in [
                {
                    "ts_utc": iso(entry_ts),
                    "run_id": run_id,
                    "k": 1,
                    "symbols": ["ETH/USDT"],
                    "equal_weight": 1.0,
                    "entry_px": {"ETH/USDT": 100.0},
                    "final_score": {"ETH/USDT": 0.9},
                    "selected_rank": {"ETH/USDT": 1},
                    "rt_cost_bps": 30.0,
                    "label_status": "pending",
                },
                {
                    "ts_utc": iso(entry_ts),
                    "run_id": run_id,
                    "k": 2,
                    "symbols": ["ETH/USDT", "BTC/USDT"],
                    "equal_weight": 0.5,
                    "entry_px": {"ETH/USDT": 100.0, "BTC/USDT": 200.0},
                    "final_score": {"ETH/USDT": 0.9, "BTC/USDT": 0.8},
                    "selected_rank": {"ETH/USDT": 1, "BTC/USDT": 2},
                    "rt_cost_bps": 30.0,
                    "label_status": "pending",
                },
                {
                    "ts_utc": iso(entry_ts),
                    "run_id": run_id,
                    "k": 3,
                    "symbols": ["ETH/USDT", "BTC/USDT", "SOL/USDT"],
                    "equal_weight": 0.33333333,
                    "entry_px": {"ETH/USDT": 100.0, "BTC/USDT": 200.0, "SOL/USDT": 50.0},
                    "final_score": {"ETH/USDT": 0.9, "BTC/USDT": 0.8, "SOL/USDT": 0.7},
                    "selected_rank": {"ETH/USDT": 1, "BTC/USDT": 2, "SOL/USDT": 3},
                    "rt_cost_bps": 30.0,
                    "label_status": "pending",
                },
            ]
        )
        + "\n",
    )
    write_ohlcv_cache(root, "ETH/USDT", [(entry_ts, 100.0), (entry_ts + 24 * 3600, 104.0), (entry_ts + 48 * 3600, 106.0)])
    write_ohlcv_cache(root, "BTC/USDT", [(entry_ts, 200.0), (entry_ts + 24 * 3600, 202.0), (entry_ts + 48 * 3600, 204.0)])
    write_ohlcv_cache(root, "SOL/USDT", [(entry_ts, 50.0), (entry_ts + 24 * 3600, 49.0), (entry_ts + 48 * 3600, 51.0)])
    return run_id


def fixture_sol_swing_performance_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    run_id = now.strftime("%Y%m%d_%H")
    entry_ts = window_end - 6 * 3600
    exit_ts = window_end - 3600
    label_ts = window_end - 50 * 3600

    write_text(
        root / "configs/live_prod.yaml",
        "diagnostics:\n"
        "  multi_position_swing_shadow_enabled: true\n"
        "execution:\n"
        "  swing_hold_enabled: true\n",
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "current_level": "PROTECT",
        "regime": "Trending",
        "targets_post_risk": {"SOL/USDT": 0.15},
        "target_execution_explain": [
            {
                "symbol": "SOL/USDT",
                "target_w": 0.15,
                "final_score": 0.92,
                "selected_rank": 1,
                "router_action": "skip",
                "router_reason": "protect_entry_trend_only",
                "high_score_but_not_executed": True,
                "high_score_block_category": "trend_only",
                "trend_score": 0.95,
                "trend_side": "buy",
                "current_level": "PROTECT",
                "regime": "Trending",
            },
        ],
        "router_decisions": [
            {
                "symbol": "SOL/USDT",
                "action": "create",
                "intent": "OPEN_LONG",
                "side": "buy",
                "reason": "ok",
                "alpha6_score": 0.6,
                "f4_volume_expansion": 0.4,
                "f5_rsi_trend_confirm": 0.35,
            },
            {
                "symbol": "SOL/USDT",
                "action": "create",
                "intent": "CLOSE_LONG",
                "side": "sell",
                "reason": "protect_profit_lock_trailing",
                "source_reason": "protect_profit_lock_trailing",
            },
            {
                "symbol": "SOL/USDT",
                "action": "skip",
                "reason": "protect_entry_trend_only",
            },
        ],
    })
    write_text(
        run_dir / "trades.csv",
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,raw_meta\n"
        f"{iso(entry_ts)},{run_id},SOL/USDT,OPEN_LONG,buy,1,100,100,0.01,\"{{\"\"swing_hold_position\"\": true}}\"\n"
        f"{iso(exit_ts)},{run_id},SOL/USDT,CLOSE_LONG,sell,1,101.3,101.3,0.01,\n",
    )
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})

    write_text(
        root / "reports/skipped_candidate_labels.jsonl",
        json.dumps(
            {
                "ts_utc": iso(label_ts),
                "run_id": run_id,
                "symbol": "SOL/USDT",
                "intended_side": "buy",
                "skip_reason": "protect_entry_trend_only",
                "high_score_blocked_target": True,
                "high_score_block_category": "trend_only",
                "final_score": 0.92,
                "target_w": 0.15,
                "entry_px": 100.0,
                "rt_cost_bps": 30.0,
                "label_24h_net_bps": 110.0,
                "label_48h_net_bps": 160.0,
                "label_72h_net_bps": 210.0,
                "label_status": "complete",
            },
            ensure_ascii=False,
        )
        + "\n",
    )
    write_text(
        root / "reports/multi_position_swing_shadow_labels.jsonl",
        json.dumps(
            {
                "ts_utc": iso(label_ts),
                "run_id": run_id,
                "shadow_mode": "protect_recovery_rules",
                "k": 1,
                "symbols": ["SOL/USDT"],
                "equal_weight": 1.0,
                "entry_px": {"SOL/USDT": 100.0},
                "final_score": {"SOL/USDT": 0.92},
                "selected_rank": {"SOL/USDT": 1},
                "rt_cost_bps": 30.0,
                "label_24h_portfolio_avg_net_bps": 130.0,
                "label_24h_symbol_net_bps": {"SOL/USDT": 130.0},
                "label_48h_portfolio_avg_net_bps": 180.0,
                "label_48h_symbol_net_bps": {"SOL/USDT": 180.0},
                "label_72h_portfolio_avg_net_bps": 230.0,
                "label_72h_symbol_net_bps": {"SOL/USDT": 230.0},
                "label_status": "complete",
            },
            ensure_ascii=False,
        )
        + "\n",
    )
    return run_id


def fixture_swing_early_exit_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    window_end = int(now.timestamp())
    run_id = now.strftime("%Y%m%d_%H")
    entry_ts = window_end - 50 * 3600
    exit_ts = entry_ts + 23 * 3600

    write_text(
        root / "configs/live_prod.yaml",
        "execution:\n"
        "  swing_hold_enabled: true\n"
        "  swing_min_hold_hours: 24\n"
        "  fee_bps: 0\n"
        "  slippage_bps: 0\n",
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "current_level": "PROTECT",
        "regime": "Trending",
        "router_decisions": [
            {"symbol": "SOL/USDT", "action": "create", "intent": "OPEN_LONG", "side": "buy", "reason": "normal_entry"},
            {"symbol": "SOL/USDT", "action": "create", "intent": "CLOSE_LONG", "side": "sell", "reason": "atr_trailing", "source_reason": "atr_trailing"},
            {"symbol": "ETH/USDT", "action": "create", "intent": "OPEN_LONG", "side": "buy", "reason": "normal_entry"},
            {"symbol": "ETH/USDT", "action": "create", "intent": "CLOSE_LONG", "side": "sell", "reason": "zero_target_close", "source_reason": "zero_target_close"},
            {"symbol": "BTC/USDT", "action": "create", "intent": "OPEN_LONG", "side": "buy", "reason": "normal_entry"},
            {"symbol": "BTC/USDT", "action": "create", "intent": "CLOSE_LONG", "side": "sell", "reason": "rank_exit_4", "source_reason": "rank_exit_4"},
            {"symbol": "BNB/USDT", "action": "create", "intent": "OPEN_LONG", "side": "buy", "reason": "normal_entry"},
            {"symbol": "BNB/USDT", "action": "create", "intent": "CLOSE_LONG", "side": "sell", "reason": "stop_loss", "source_reason": "stop_loss"},
        ],
    })
    write_text(
        run_dir / "trades.csv",
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,raw_meta\n"
        f"{iso(entry_ts)},{run_id},SOL/USDT,OPEN_LONG,buy,1,100,100,0,\"{{\"\"swing_hold_position\"\": true, \"\"swing_min_hold_hours\"\": 24}}\"\n"
        f"{iso(exit_ts)},{run_id},SOL/USDT,CLOSE_LONG,sell,1,99.98,99.98,0,\n"
        f"{iso(entry_ts)},{run_id},ETH/USDT,OPEN_LONG,buy,1,200,200,0,\"{{\"\"swing_hold_position\"\": true, \"\"swing_min_hold_hours\"\": 24}}\"\n"
        f"{iso(exit_ts)},{run_id},ETH/USDT,CLOSE_LONG,sell,1,199.5,199.5,0,\n"
        f"{iso(entry_ts)},{run_id},BTC/USDT,OPEN_LONG,buy,1,300,300,0,\"{{\"\"swing_hold_position\"\": true, \"\"swing_min_hold_hours\"\": 24}}\"\n"
        f"{iso(exit_ts)},{run_id},BTC/USDT,CLOSE_LONG,sell,1,299,299,0,\n"
        f"{iso(entry_ts)},{run_id},BNB/USDT,OPEN_LONG,buy,1,400,400,0,\"{{\"\"swing_hold_position\"\": true, \"\"swing_min_hold_hours\"\": 24}}\"\n"
        f"{iso(exit_ts)},{run_id},BNB/USDT,CLOSE_LONG,sell,1,390,390,0,\n",
    )
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})

    write_ohlcv_cache(root, "SOL/USDT", [
        (entry_ts, 100),
        (entry_ts + 24 * 3600, 101.0),
        (entry_ts + 48 * 3600, 102.0),
        (exit_ts + 24 * 3600, 101.5),
        (exit_ts + 48 * 3600, 102.5),
    ])
    write_ohlcv_cache(root, "ETH/USDT", [
        (entry_ts, 200),
        (entry_ts + 24 * 3600, 204.0),
        (entry_ts + 48 * 3600, 206.0),
        (exit_ts + 24 * 3600, 205.0),
        (exit_ts + 48 * 3600, 207.0),
    ])
    write_ohlcv_cache(root, "BTC/USDT", [
        (entry_ts, 300),
        (entry_ts + 24 * 3600, 306.0),
        (entry_ts + 48 * 3600, 309.0),
        (exit_ts + 24 * 3600, 307.0),
        (exit_ts + 48 * 3600, 310.0),
    ])
    write_ohlcv_cache(root, "BNB/USDT", [
        (entry_ts, 400),
        (entry_ts + 24 * 3600, 390.0),
        (entry_ts + 48 * 3600, 388.0),
        (exit_ts + 24 * 3600, 389.0),
        (exit_ts + 48 * 3600, 388.0),
    ])
    return run_id


def fixture_bnb_swing_early_exit_router_raw_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    window_end = int(now.timestamp())
    run_id = now.strftime("%Y%m%d_%H")
    entry_ts = window_end - 6 * 3600
    exit_ts = entry_ts + 5 * 3600

    write_text(
        root / "configs/live_prod.yaml",
        "execution:\n"
        "  swing_hold_enabled: true\n"
        "  swing_min_hold_hours: 24\n"
        "  fee_bps: 0\n"
        "  slippage_bps: 0\n",
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "current_level": "PROTECT",
        "regime": "Trending",
        "router_decisions": [
            {
                "symbol": "BNB/USDT",
                "action": "create",
                "intent": "OPEN_LONG",
                "side": "buy",
                "reason": "ok / normal_entry",
                "swing_hold_position": True,
                "swing_min_hold_hours": 24,
            },
            {
                "symbol": "BNB/USDT",
                "action": "create",
                "intent": "CLOSE_LONG",
                "side": "sell",
                "reason": "atr_trailing",
                "source_reason": "atr_trailing",
            },
        ],
    })
    write_text(
        run_dir / "trades.csv",
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,raw_meta\n"
        f"{iso(entry_ts)},{run_id},BNB/USDT,OPEN_LONG,buy,1,400,400,0,\n"
        f"{iso(exit_ts)},{run_id},BNB/USDT,CLOSE_LONG,sell,1,398,398,0,\n",
    )
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    return run_id


def fixture_multi_position_swing_shadow_from_audit_root(root):
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    entry_dt = now - dt.timedelta(hours=50)
    entry_ts = int(entry_dt.timestamp())
    current_ts = int(now.timestamp())
    entry_run_id = entry_dt.strftime("%Y%m%d_%H")
    current_run_id = now.strftime("%Y%m%d_%H")

    write_text(
        root / "configs/live_prod.yaml",
        "diagnostics:\n"
        "  multi_position_swing_shadow_enabled: true\n"
        "  multi_position_swing_shadow_symbols: [\"BTC/USDT\", \"ETH/USDT\", \"SOL/USDT\", \"BNB/USDT\"]\n"
        "  multi_position_swing_shadow_min_final_score: 0.30\n"
        "  multi_position_swing_shadow_horizons_hours: [24, 48, 72]\n"
        "  multi_position_swing_shadow_rt_cost_bps: 30\n"
        "execution:\n"
        "  protect_recovery_allowed_symbols: [\"BTC/USDT\", \"SOL/USDT\", \"ETH/USDT\"]\n"
        "  protect_recovery_require_market_context: true\n"
        "  protect_recovery_min_positive_whitelist_4h_count: 0\n"
        "  protect_recovery_disallow_symbols_with_negative_expectancy: true\n",
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    entry_run_dir = root / "reports/runs/prod" / entry_run_id
    write_json(entry_run_dir / "decision_audit.json", {
        "now_ts": entry_ts + 15,
        "window_end_ts": entry_ts,
        "regime": "Trending",
        "top_scores": [
            {"symbol": "SOL/USDT", "score": 0.61, "display_score": 0.61, "rank": 1},
            {"symbol": "BNB/USDT", "score": 0.50, "display_score": 0.50, "rank": 2},
            {"symbol": "ETH/USDT", "score": -0.20, "display_score": -0.20, "rank": 3},
        ],
        "targets_post_risk": {"SOL/USDT": 0.15},
        "target_execution_explain": [
            {
                "symbol": "SOL/USDT",
                "target_w": 0.15,
                "final_score": 0.61,
                "selected_rank": 1,
                "router_action": "skip",
                "router_reason": "protect_entry_rsi_confirm_too_weak",
                "current_level": "PROTECT",
                "regime": "Trending",
            },
            {
                "symbol": "BNB/USDT",
                "target_w": 0.0,
                "final_score": 0.50,
                "selected_rank": 2,
                "router_action": "skip",
                "router_reason": "deadband",
                "current_level": "PROTECT",
                "regime": "Trending",
            },
        ],
        "router_decisions": [
            {"symbol": "SOL/USDT", "action": "skip", "reason": "protect_entry_rsi_confirm_too_weak"},
            {"symbol": "BNB/USDT", "action": "skip", "reason": "deadband"},
        ],
    })
    write_text(entry_run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(entry_run_dir / "equity.jsonl", "{}\n")
    write_json(entry_run_dir / "summary.json", {"run_id": entry_run_id})

    current_run_dir = root / "reports/runs/prod" / current_run_id
    write_json(current_run_dir / "decision_audit.json", {
        "now_ts": current_ts + 15,
        "window_end_ts": current_ts,
        "regime": "Trending",
        "top_scores": [{"symbol": "SOL/USDT", "score": 0.20, "display_score": 0.20, "rank": 1}],
        "targets_post_risk": {},
        "target_execution_explain": [],
        "router_decisions": [],
    })
    write_text(current_run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(current_run_dir / "equity.jsonl", "{}\n")
    write_json(current_run_dir / "summary.json", {"run_id": current_run_id})

    entry_label_ts = entry_ts + 15
    write_ohlcv_cache(root, "SOL/USDT", [(entry_label_ts, 100.0), (entry_label_ts + 24 * 3600, 104.0), (entry_label_ts + 48 * 3600, 106.0)])
    write_ohlcv_cache(root, "BNB/USDT", [(entry_label_ts, 200.0), (entry_label_ts + 24 * 3600, 198.0), (entry_label_ts + 48 * 3600, 210.0)])
    return entry_run_id


def fixture_alt_impulse_shadow_cache_fill_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    entry_ts = window_end - 5 * 3600
    run_id = dt.datetime.fromtimestamp(entry_ts, dt.timezone.utc).strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "alt_impulse_shadow_enabled: true\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")
    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": entry_ts + 15,
        "window_end_ts": entry_ts,
        "regime": "Trending",
        "target_execution_explain": [
            {
                "symbol": "SOL/USDT",
                "target_w": 0.15,
                "final_score": 1.0,
                "router_action": "skip",
                "router_reason": "protect_entry_trend_only",
            },
            {
                "symbol": "ETH/USDT",
                "target_w": 0.15,
                "final_score": 1.0,
                "router_action": "skip",
                "router_reason": "protect_entry_no_alpha6_confirmation",
            },
        ],
        "router_decisions": [],
    })
    write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    write_text(
        root / "reports/alt_impulse_shadow_labels.jsonl",
        "\n".join(
            json.dumps(row, ensure_ascii=False)
            for row in [
                {
                    "ts_utc": iso(entry_ts),
                    "run_id": run_id,
                    "symbol": "SOL/USDT",
                    "entry_px": "not_observable",
                    "final_score": 1.0,
                    "trend_score": 1.0,
                    "trend_side": "buy",
                    "skip_reason": "protect_entry_trend_only",
                    "current_level": "PROTECT",
                    "rt_cost_bps": 30.0,
                    "label_not_observable_reason": "missing_entry_px",
                },
                {
                    "ts_utc": iso(entry_ts),
                    "run_id": run_id,
                    "symbol": "ETH/USDT",
                    "entry_px": "not_observable",
                    "final_score": 1.0,
                    "trend_score": 1.0,
                    "trend_side": "buy",
                    "skip_reason": "protect_entry_no_alpha6_confirmation",
                    "current_level": "PROTECT",
                    "rt_cost_bps": 30.0,
                },
            ]
        )
        + "\n",
    )
    write_ohlcv_cache(root, "SOL/USDT", [(entry_ts, 100.0), (entry_ts + 4 * 3600, 105.0)])
    write_ohlcv_cache(root, "ETH/USDT", [(entry_ts, 2000.0)])
    return run_id


def fixture_alt_impulse_shadow_skipped_provider_future_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    entry_ts = window_end - 56 * 3600 + 15
    provider_entry_ts = entry_ts - 15
    entry_run_id = dt.datetime.fromtimestamp(entry_ts, dt.timezone.utc).strftime("%Y%m%d_%H")
    current_run_id = now.strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "alt_impulse_shadow_enabled: true\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    for run_id, ts_value in ((entry_run_id, entry_ts), (current_run_id, window_end)):
        run_dir = root / "reports/runs/prod" / run_id
        write_json(run_dir / "decision_audit.json", {
            "now_ts": ts_value + 15,
            "window_end_ts": ts_value,
            "regime": "Trending",
            "target_execution_explain": [],
            "router_decisions": [],
        })
        write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
        write_text(run_dir / "equity.jsonl", "{}\n")
        write_json(run_dir / "summary.json", {"run_id": run_id})

    write_text(
        root / "reports/alt_impulse_shadow_labels.jsonl",
        json.dumps(
            {
                "ts_utc": iso(entry_ts),
                "run_id": entry_run_id,
                "symbol": "SOL/USDT",
                "entry_px": 88.96,
                "final_score": 1.0,
                "trend_score": 1.0,
                "trend_side": "buy",
                "skip_reason": "protect_entry_trend_only",
                "current_level": "PROTECT",
                "rt_cost_bps": 30.0,
                "label_not_observable_reason": "missing_entry_px",
            },
            ensure_ascii=False,
        )
        + "\n",
    )
    write_text(
        root / "reports/skipped_candidate_labels.jsonl",
        json.dumps(
            {
                "ts_utc": iso(provider_entry_ts),
                "entry_ts_ms": provider_entry_ts * 1000,
                "run_id": entry_run_id,
                "symbol": "SOL/USDT",
                "entry_px": 86.88,
                "skip_reason": "protect_entry_confirmation_not_stable",
                "rt_cost_bps": 30.0,
                "label_4h_gross_bps": 70.211786,
                "label_4h_net_bps": 40.211786,
                "label_4h_status": "complete",
                "label_8h_gross_bps": 287.753223,
                "label_8h_net_bps": 257.753223,
                "label_8h_status": "complete",
                "label_12h_gross_bps": 174.953959,
                "label_12h_net_bps": 144.953959,
                "label_12h_status": "complete",
                "label_24h_gross_bps": 145.027624,
                "label_24h_net_bps": 115.027624,
                "label_24h_status": "complete",
                "label_48h_gross_bps": 136.970534,
                "label_48h_net_bps": 106.970534,
                "label_48h_status": "complete",
                "label_72h_status": "pending",
                "label_120h_status": "pending",
                "label_status": "complete",
            },
            ensure_ascii=False,
        )
        + "\n",
    )
    return entry_run_id


def fixture_alt_impulse_shadow_extended_horizon_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    current_run_id = now.strftime("%Y%m%d_%H")
    entry_ts = window_end - 60 * 3600
    entry_run_id = dt.datetime.fromtimestamp(entry_ts, dt.timezone.utc).strftime("%Y%m%d_%H")

    write_text(
        root / "configs/live_prod.yaml",
        "diagnostics:\n  extended_label_horizons_hours: [48, 56, 72]\n",
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    entry_run_dir = root / "reports/runs/prod" / entry_run_id
    write_json(entry_run_dir / "decision_audit.json", {
        "now_ts": entry_ts + 15,
        "window_end_ts": entry_ts,
        "regime": "Trending",
        "target_execution_explain": [],
        "router_decisions": [],
    })
    write_text(entry_run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(entry_run_dir / "equity.jsonl", "{}\n")
    write_json(entry_run_dir / "summary.json", {"run_id": entry_run_id})

    current_run_dir = root / "reports/runs/prod" / current_run_id
    write_json(current_run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "regime": "Trending",
        "target_execution_explain": [],
        "router_decisions": [],
    })
    write_text(current_run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(current_run_dir / "equity.jsonl", "{}\n")
    write_json(current_run_dir / "summary.json", {"run_id": current_run_id})

    write_text(
        root / "reports/alt_impulse_shadow_labels.jsonl",
        json.dumps(
            {
                "ts_utc": iso(entry_ts),
                "run_id": entry_run_id,
                "symbol": "SOL/USDT",
                "entry_px": "not_observable",
                "final_score": 1.0,
                "trend_score": 1.0,
                "trend_side": "buy",
                "skip_reason": "protect_entry_trend_only",
                "current_level": "PROTECT",
                "rt_cost_bps": 30.0,
                "label_not_observable_reason": "missing_entry_px",
            },
            ensure_ascii=False,
        )
        + "\n",
    )
    write_ohlcv_cache(root, "SOL/USDT", [(entry_ts, 100.0), (entry_ts + 48 * 3600, 110.0)])
    return entry_run_id


def fixture_alt_impulse_shadow_missing_entry_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    entry_ts = window_end - 5 * 3600
    run_id = dt.datetime.fromtimestamp(entry_ts, dt.timezone.utc).strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "alt_impulse_shadow_enabled: true\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")
    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {"now_ts": entry_ts + 15, "window_end_ts": entry_ts, "target_execution_explain": []})
    write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    write_text(
        root / "reports/alt_impulse_shadow_labels.jsonl",
        json.dumps(
            {
                "ts_utc": iso(entry_ts),
                "run_id": run_id,
                "symbol": "SOL/USDT",
                "entry_px": "not_observable",
                "final_score": 1.0,
                "trend_score": 1.0,
                "trend_side": "buy",
                "skip_reason": "protect_entry_trend_only",
                "current_level": "PROTECT",
                "rt_cost_bps": 30.0,
            },
            ensure_ascii=False,
        )
        + "\n",
    )
    return run_id


def fixture_market_impulse_selection_shadow_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    run_id = now.strftime("%Y%m%d_%H")

    write_text(root / "configs/live_prod.yaml", "market_impulse_probe_selection_mode: priority\n")
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")
    run_dir = root / "reports/runs/prod" / run_id
    write_json(run_dir / "decision_audit.json", {
        "now_ts": window_end + 15,
        "window_end_ts": window_end,
        "regime": "Trending",
        "market_impulse_selection_mode": "priority",
        "market_impulse_shadow_selection": {
            "active": True,
            "trend_buy_count": 3,
            "btc_trend_score": 0.90,
            "selected_live": "BTC/USDT",
            "selected_by_priority": "BTC/USDT",
            "selected_by_trend_score": "ETH/USDT",
            "selected_by_alpha6_confirmed": "SOL/USDT",
            "selected_by_expected_net_shadow": "ETH/USDT",
            "candidates": [
                {"symbol": "BTC/USDT", "trend_score": 0.90, "priority_rank": 0},
                {"symbol": "ETH/USDT", "trend_score": 1.00, "priority_rank": 1, "expected_net_bps": 25.0},
                {"symbol": "SOL/USDT", "trend_score": 0.78, "priority_rank": 2, "alpha6_confirmed": True},
            ],
            "live_missed_eth_by_trend_score": True,
        },
    })
    write_text(run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(run_dir / "equity.jsonl", "{}\n")
    write_json(run_dir / "summary.json", {"run_id": run_id})
    return run_id


def fixture_factor_contribution_root(root):
    now = dt.datetime.now(dt.timezone.utc)
    current_window_end = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    old_window_end = current_window_end - 25 * 3600
    current_run_id = dt.datetime.fromtimestamp(current_window_end, dt.timezone.utc).strftime("%Y%m%d_%H")
    old_run_id = dt.datetime.fromtimestamp(old_window_end, dt.timezone.utc).strftime("%Y%m%d_%H")

    write_text(
        root / "configs/live_prod.yaml",
        "\n".join(
            [
                "f1_mom_5d: 0.10",
                "f2_mom_20d: 0.30",
                "f3_vol_adj_ret: 0.35",
                "f4_volume_expansion: 0.15",
                "f5_rsi_trend_confirm: 0.10",
                "",
            ]
        ),
    )
    for name in (
        "kill_switch",
        "reconcile_status",
        "ledger_status",
        "ledger_state",
        "auto_risk_eval",
        "negative_expectancy_cooldown",
    ):
        write_json(root / "reports" / f"{name}.json", {"ok": True})
    write_text(root / "logs/v5_runtime.log", "fixture log\n")

    old_run_dir = root / "reports/runs/prod" / old_run_id
    old_audit_ts = old_window_end + 15
    write_json(old_run_dir / "decision_audit.json", {
        "now_ts": old_audit_ts,
        "window_end_ts": old_window_end,
        "effective_alpha6_weights": {
            "f1_mom_5d": 0.10,
            "f2_mom_20d": 0.30,
            "f3_vol_adj_ret": 0.35,
            "f4_volume_expansion": 0.15,
            "f5_rsi_trend_confirm": 0.10,
        },
        "top_scores": [{"symbol": "ETH/USDT", "score": 1.0, "rank": 1}],
        "targets_post_risk": {"ETH/USDT": 0.15},
        "router_decisions": [
            {
                "symbol": "ETH/USDT",
                "action": "skip",
                "reason": "protect_entry_no_alpha6_confirmation",
            }
        ],
        "strategy_signals": [
            {
                "strategy": "Alpha6Factor",
                "signals": [
                    {
                        "symbol": "ETH/USDT",
                        "side": "buy",
                        "score": 0.91,
                        "metadata": {
                            "raw_factors": {
                                "f1_mom_5d": 0.01,
                                "f2_mom_20d": 0.02,
                                "f3_vol_adj_ret": 4.2,
                                "f4_volume_expansion": -0.1,
                                "f5_rsi_trend_confirm": -0.2,
                            },
                            "z_factors": {
                                "f1_mom_5d": 0.10,
                                "f2_mom_20d": 0.20,
                                "f3_vol_adj_ret": 2.00,
                                "f4_volume_expansion": -0.10,
                                "f5_rsi_trend_confirm": -0.10,
                            },
                        },
                    }
                ],
            }
        ],
    })
    write_text(old_run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(old_run_dir / "equity.jsonl", "{}\n")
    write_json(old_run_dir / "summary.json", {"run_id": old_run_id})

    current_run_dir = root / "reports/runs/prod" / current_run_id
    current_audit_ts = current_window_end + 15
    write_json(current_run_dir / "decision_audit.json", {
        "now_ts": current_audit_ts,
        "window_end_ts": current_window_end,
        "effective_alpha6_weights": {
            "f1_mom_5d": 0.10,
            "f2_mom_20d": 0.30,
            "f3_vol_adj_ret": 0.35,
            "f4_volume_expansion": 0.15,
            "f5_rsi_trend_confirm": 0.10,
        },
        "top_scores": [{"symbol": "SOL/USDT", "final_score": 0.87, "rank": 1}],
        "targets_post_risk": {"SOL/USDT": 0.12},
        "router_decisions": [
            {
                "symbol": "SOL/USDT",
                "action": "skip",
                "reason": "protect_entry_trend_only",
            }
        ],
        "strategy_signals": [
            {
                "strategy": "Alpha6Factor",
                "signals": [
                    {
                        "symbol": "SOL/USDT",
                        "side": "buy",
                        "score": 0.55,
                        "metadata": {
                            "raw_factors": {
                                "f1_mom_5d": 0.02,
                                "f2_mom_20d": 3.0,
                                "f3_vol_adj_ret": 0.5,
                                "f4_volume_expansion": 0.1,
                                "f5_rsi_trend_confirm": 0.1,
                            },
                            "z_factors": {
                                "f1_mom_5d": 0.10,
                                "f2_mom_20d": 1.20,
                                "f3_vol_adj_ret": 0.20,
                                "f4_volume_expansion": 0.10,
                                "f5_rsi_trend_confirm": 0.10,
                            },
                        },
                    }
                ],
            }
        ],
    })
    write_text(current_run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(current_run_dir / "equity.jsonl", "{}\n")
    write_json(current_run_dir / "summary.json", {"run_id": current_run_id})

    write_text(
        root / "reports/skipped_candidate_labels.jsonl",
        json.dumps(
            {
                "run_id": old_run_id,
                "ts_utc": iso(old_audit_ts),
                "symbol": "ETH/USDT",
                "skip_reason": "protect_entry_no_alpha6_confirmation",
                "entry_px": 2000.0,
                "label_status": "complete",
                "label_4h_net_bps": -10.0,
                "label_8h_net_bps": -20.0,
                "label_12h_net_bps": -30.0,
                "label_24h_net_bps": -40.0,
            },
            ensure_ascii=False,
        )
        + "\n",
    )
    return old_run_id, current_run_id


def fixture_factor_contribution_f3_risk_root(root):
    fixture_factor_contribution_root(root)
    now = dt.datetime.now(dt.timezone.utc)
    risk_window_end = int((now.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=30)).timestamp())
    risk_run_id = dt.datetime.fromtimestamp(risk_window_end, dt.timezone.utc).strftime("%Y%m%d_%H")
    risk_audit_ts = risk_window_end + 15
    symbols = [f"F3{i:02d}/USDT" for i in range(20)]
    risk_run_dir = root / "reports/runs/prod" / risk_run_id
    write_json(risk_run_dir / "decision_audit.json", {
        "now_ts": risk_audit_ts,
        "window_end_ts": risk_window_end,
        "effective_alpha6_weights": {
            "f1_mom_5d": 0.10,
            "f2_mom_20d": 0.30,
            "f3_vol_adj_ret": 0.35,
            "f4_volume_expansion": 0.15,
            "f5_rsi_trend_confirm": 0.10,
        },
        "top_scores": [
            {"symbol": symbol, "final_score": 0.95, "rank": idx + 1}
            for idx, symbol in enumerate(symbols)
        ],
        "targets_post_risk": {symbol: 0.05 for symbol in symbols},
        "router_decisions": [
            {
                "symbol": symbol,
                "action": "skip",
                "reason": "protect_entry_no_alpha6_confirmation",
            }
            for symbol in symbols
        ],
        "strategy_signals": [
            {
                "strategy": "Alpha6Factor",
                "signals": [
                    {
                        "symbol": symbol,
                        "side": "buy",
                        "score": 0.88,
                        "metadata": {
                            "raw_factors": {
                                "f1_mom_5d": 0.01,
                                "f2_mom_20d": 0.02,
                                "f3_vol_adj_ret": 4.0,
                                "f4_volume_expansion": -0.1,
                                "f5_rsi_trend_confirm": -0.2,
                            },
                            "z_factors": {
                                "f1_mom_5d": 0.10,
                                "f2_mom_20d": 0.20,
                                "f3_vol_adj_ret": 2.00,
                                "f4_volume_expansion": -0.10,
                                "f5_rsi_trend_confirm": -0.10,
                            },
                        },
                    }
                    for symbol in symbols
                ],
            }
        ],
    })
    write_text(risk_run_dir / "trades.csv", "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n")
    write_text(risk_run_dir / "equity.jsonl", "{}\n")
    write_json(risk_run_dir / "summary.json", {"run_id": risk_run_id})

    labels_path = root / "reports/skipped_candidate_labels.jsonl"
    with labels_path.open("a", encoding="utf-8") as fh:
        for symbol in symbols:
            fh.write(
                json.dumps(
                    {
                        "run_id": risk_run_id,
                        "ts_utc": iso(risk_audit_ts),
                        "symbol": symbol,
                        "skip_reason": "protect_entry_no_alpha6_confirmation",
                        "entry_px": 100.0,
                        "label_status": "complete",
                        "label_4h_net_bps": -60.0,
                        "label_8h_net_bps": -70.0,
                        "label_12h_net_bps": -90.0,
                        "label_24h_net_bps": -100.0,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    return risk_run_id


def extract_member(tf, suffix):
    matches = [name for name in tf.getnames() if name.endswith(suffix)]
    assert matches, suffix
    return matches[0]


def run_bundle(root):
    script_path = str(SCRIPT)
    if len(script_path) >= 3 and script_path[1] == ":":
        tail = script_path[3:].replace("\\", "/")
        script_path = f"/mnt/{script_path[0].lower()}/{tail}"
    else:
        script_path = script_path.replace("\\", "/")
    root_path = str(root)
    if len(root_path) >= 3 and root_path[1] == ":":
        tail = root_path[3:].replace("\\", "/")
        root_path = f"/mnt/{root_path[0].lower()}/{tail}"
    else:
        root_path = root_path.replace("\\", "/")
    proc = subprocess.run(
        ["bash", script_path, root_path],
        env=os.environ.copy(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    bundle_path = None
    for line in proc.stdout.splitlines():
        if line.startswith("BUNDLE_PATH="):
            raw_bundle_path = line.split("=", 1)[1]
            if os.name == "nt" and raw_bundle_path.startswith("/"):
                converted = subprocess.check_output(["wsl.exe", "wslpath", "-w", raw_bundle_path], text=True).strip()
                bundle_path = pathlib.Path(converted)
            else:
                bundle_path = pathlib.Path(raw_bundle_path)
            break
    assert bundle_path and bundle_path.is_file(), proc.stdout + proc.stderr
    return bundle_path


def main():
    with tempfile.TemporaryDirectory(prefix="v5-btc-labeler-") as tmp:
        root = pathlib.Path(tmp) / "root"
        run_id = fixture_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                blocked = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/btc_leadership_probe_blocked_outcomes.csv")).read().decode().splitlines()))
                maturity = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/skipped_candidate_maturity_audit.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
                labels_lines = tf.extractfile(extract_member(tf, "raw/reports/skipped_candidate_labels.jsonl")).read().decode().splitlines()

            keys = [(r["run_id"], r["ts_utc"], r["symbol"], r["skip_reason"]) for r in blocked]
            assert len(keys) == len(set(keys)), keys
            assert len(labels_lines) == 2, labels_lines

            not_flat = next(r for r in blocked if r["skip_reason"] == "btc_leadership_probe_not_flat")
            cooldown = next(r for r in blocked if r["skip_reason"] == "btc_leadership_probe_cooldown")
            assert not_flat["label_status"] == "not_observable" and not_flat["not_observable_reason"] == "not_flat"
            assert cooldown["label_status"] == "not_observable" and cooldown["not_observable_reason"] == "cooldown"

            alpha = next(r for r in blocked if r["skip_reason"] == "btc_leadership_probe_alpha6_score_too_low")
            no_buy = next(r for r in blocked if r["skip_reason"] == "btc_leadership_probe_no_alpha6_buy")
            assert alpha["label_status"] == "complete" and alpha["label_24h_net_bps"] == "4"
            assert no_buy["label_status"] == "complete" and no_buy["label_24h_net_bps"] == "8"

            high_unlabeled = [
                item for item in issues["issues"]
                if item.get("severity") == "high" and item.get("code") == "btc_leadership_blocked_cases_not_labeled"
            ]
            assert len(high_unlabeled) == 1, high_unlabeled
            summary = window["btc_leadership_blocked_labeler_summary"]
            assert summary["total_blocked"] == 5, summary
            assert summary["labeled_complete"] == 2, summary
            assert summary["not_observable"] == 2, summary
            assert summary["duplicated_removed"] == 2, summary
            assert summary["unlabeled_high_issue_count"] == len(high_unlabeled), summary
            assert len(maturity) == len(blocked), (maturity, blocked)
            assert window["has_trade_data"] is True, window
            assert window["trade_observation_status"] == "no_trades", window
            assert window["raw_trade_rows"] == 0 and window["trade_rows"] == 0, window
            assert window["latest_24h_trade_count"] == 0, window
            assert window["latest_24h_roundtrip_count"] == 0, window
            assert window["last_72h_trade_count"] == 0, window
            assert window["last_72h_roundtrip_count"] == 0, window
            assert "是否真实成交: no / 0" in readme, readme
            assert "closed roundtrip gross/net bps: not_applicable_no_trades" in readme, readme
            assert "probe lifecycle: not_applicable_no_probe_trade" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-high-score-blocked-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_high_score_blocked_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/high_score_blocked_targets.csv")).read().decode().splitlines()))
                outcome_rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/high_score_blocked_outcomes.csv")).read().decode().splitlines()))
                by_symbol = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/high_score_blocked_outcomes_by_symbol.csv")).read().decode().splitlines()))
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(rows) == 2, rows
            row = next(item for item in rows if item["router_reason"] == "protect_entry_trend_only")
            alpha_sell_row = next(item for item in rows if item["router_reason"] == "protect_entry_no_alpha6_confirmation")
            assert row["symbol"] == "ETH/USDT", row
            assert row["final_score"] == "1.0", row
            assert row["selected_rank"] == "1", row
            assert row["target_w"] == "0.15", row
            assert row["router_action"] == "skip", row
            assert row["router_reason"] == "protect_entry_trend_only", row
            assert row["high_score_block_category"] == "trend_only", row
            assert alpha_sell_row["final_score"] == "0.823", alpha_sell_row
            assert alpha_sell_row["alpha6_side"] == "sell", alpha_sell_row
            assert alpha_sell_row["high_score_block_category"] in {"alpha6_sell", "no_alpha6_confirmation"}, alpha_sell_row
            assert "BNB/USDT" not in {item["symbol"] for item in rows}, rows
            assert window["high_score_blocked_target_count"] == 2, window
            assert window["high_score_blocked_recent_24h_target_count"] == 2, window
            assert window["high_score_block_category_counts"]["trend_only"] == 1, window
            assert window["high_score_block_category_counts"]["alpha6_sell"] == 1, window
            assert window["high_score_blocked_outcome_count"] == 2, window
            assert window["high_score_blocked_pending_count"] == 1, window
            assert len(outcome_rows) == 2, outcome_rows
            complete = next(item for item in outcome_rows if item["skip_reason"] == "protect_entry_trend_only")
            pending = next(item for item in outcome_rows if item["skip_reason"] == "protect_entry_no_alpha6_confirmation")
            assert complete["symbol"] == "ETH/USDT", outcome_rows
            assert complete["label_4h_net_bps"] == "70.0", outcome_rows
            assert complete["label_status"] == "complete", outcome_rows
            assert pending["label_status"] == "pending", outcome_rows
            assert not any(item.get("label_status") == "complete" and item.get("skip_reason") == "protect_entry_no_alpha6_confirmation" for item in outcome_rows), outcome_rows
            by_symbol_map = {(item["symbol"], item["skip_reason"]): item for item in by_symbol}
            trend_summary = by_symbol_map[("ETH/USDT", "protect_entry_trend_only")]
            alpha_summary = by_symbol_map[("ETH/USDT", "protect_entry_no_alpha6_confirmation")]
            assert trend_summary["avg_4h_net_bps"] == "70.0", by_symbol
            assert alpha_summary["avg_4h_net_bps"] == "not_observable", by_symbol
            assert "## 高分但未成交目标" in readme, readme
            assert "最近 24h 哪些 symbol 高分但没买: ETH/USDT" in readme, readme
            assert "ETH 是否出现高分但未成交: yes" in readme, readme
            assert "这些被挡样本历史 forward net bps: ETH/USDT/protect_entry_no_alpha6_confirmation: count=1, 4h=not_observable" in readme, readme
            assert "是否建议进入 skipped label: yes" in readme, readme
            assert "是否支持放松 gate: diagnostic_only_review_required" in readme, readme
            assert "## ETH/ALT 高分被挡事后表现" in readme, readme
            assert "ETH 高分被挡样本数: 2" in readme, readme
            assert "是否支持放松 gate: diagnostic_only_review_required" in readme, readme
            assert not any(item.get("code", "").startswith("high_score_blocked_matured") for item in issues["issues"]), issues
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-high-score-missing-label-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_high_score_missing_label_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/high_score_blocked_targets.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert len(rows) == 1, rows
            high_score_issues = [
                item for item in issues["issues"]
                if item.get("severity") == "high" and item.get("code") == "high_score_blocked_matured_without_label"
            ]
            assert len(high_score_issues) == 1, issues
            assert window["high_score_blocked_matured_unlabeled_count"] == 1, window
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-high-score-same-symbol-reentry-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_high_score_same_symbol_reentry_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                targets = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/high_score_blocked_targets.csv")).read().decode().splitlines()))
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/high_score_blocked_outcomes.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert len(targets) == 1, targets
            assert targets[0]["router_reason"] == "same_symbol_reentry_cooldown", targets
            assert targets[0]["last_exit_reason"] == "protect_profit_lock_trailing", targets
            assert len(outcomes) == 1, outcomes
            outcome = outcomes[0]
            assert outcome["symbol"] == "SOL/USDT", outcomes
            assert outcome["skip_reason"] == "same_symbol_reentry_cooldown", outcomes
            assert outcome["last_exit_reason"] == "protect_profit_lock_trailing", outcomes
            assert outcome["last_exit_px"] == "100.5", outcomes
            assert outcome["highest_px_before_exit"] == "101.2", outcomes
            assert outcome["elapsed_hours"] == "5.99", outcomes
            assert outcome["required_cooldown_hours"] == "6.0", outcomes
            assert outcome["breakout_exception_met"] == "False", outcomes
            assert outcome["label_48h_status"] == "complete", outcomes
            assert outcome["label_48h_net_bps"] == "970", outcomes
            assert outcome["label_72h_status"] == "pending", outcomes
            assert window["high_score_blocked_matured_unlabeled_count"] == 0, window
            assert not [
                item for item in issues["issues"]
                if item.get("code") == "high_score_blocked_matured_without_label"
                and item.get("context", {}).get("skip_reason") == "same_symbol_reentry_cooldown"
            ], issues
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-high-score-non-labelable-management-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_high_score_non_labelable_management_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                targets = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/high_score_blocked_targets.csv")).read().decode().splitlines()))
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/high_score_blocked_outcomes.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert {row["router_reason"] for row in targets} == {
                "rank_exit_target_still_positive",
                "exit_order_selected",
                "protect_entry_trend_only",
            }, targets
            assert [(row["symbol"], row["skip_reason"]) for row in outcomes] == [
                ("BNB/USDT", "protect_entry_trend_only")
            ], outcomes
            assert window["high_score_blocked_target_count"] == 3, window
            assert window["high_score_blocked_labelable_target_count"] == 1, window
            assert window["high_score_blocked_non_entry_management_count"] == 2, window
            assert window["high_score_blocked_matured_unlabeled_count"] == 0, window
            assert not [
                item for item in issues["issues"]
                if item.get("code") == "high_score_blocked_matured_without_label"
            ], issues
            assert "high-score blocked targets total: 3" in readme, readme
            assert "labelable high-score blocked targets: 1" in readme, readme
            assert "non-entry management blocks: 2" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-alt-impulse-shadow-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_alt_impulse_shadow_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes.csv")).read().decode().splitlines()))
                by_symbol = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes_by_symbol.csv")).read().decode().splitlines()))
                by_reason = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes_by_reason.csv")).read().decode().splitlines()))
                by_horizon = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes_by_horizon.csv")).read().decode().splitlines()))
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(outcomes) == 2, outcomes
            eth = next(row for row in outcomes if row["symbol"] == "ETH/USDT")
            assert eth["label_4h_net_bps"] == "70.0", outcomes
            assert eth["label_status"] == "complete", outcomes
            assert eth["label_not_observable_reason"] == "", outcomes
            sol = next(row for row in outcomes if row["symbol"] == "SOL/USDT")
            assert sol["label_status"] == "pending", outcomes
            by_symbol_map = {(row["symbol"], row["skip_reason"]): row for row in by_symbol}
            assert by_symbol_map[("ETH/USDT", "protect_entry_trend_only")]["avg_4h_net_bps"] == "70.0", by_symbol
            assert by_symbol_map[("ETH/USDT", "protect_entry_trend_only")]["win_rate_4h"] == "1.0", by_symbol
            assert any(row["skip_reason"] == "protect_entry_no_alpha6_confirmation" for row in by_reason), by_reason
            assert any(row["horizon_hours"] == "48" for row in by_horizon), by_horizon
            assert window["alt_impulse_shadow_label_count"] == 2, window
            assert "## ALT impulse shadow" in readme, readme
            assert "ETH/USDT: count=1, 4h_avg=70.0" in readme, readme
            assert "SOL/USDT: count=1, 4h_avg=not_observable" in readme, readme
            assert "BNB/USDT: count=0" in readme, readme
            assert "是否支持未来 live probe: diagnostic_only_review_required" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-multi-position-swing-shadow-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_multi_position_swing_shadow_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/multi_position_swing_shadow_outcomes.csv")).read().decode().splitlines()))
                by_k = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/multi_position_swing_shadow_by_k.csv")).read().decode().splitlines()))
                by_symbol = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/multi_position_swing_shadow_by_symbol.csv")).read().decode().splitlines()))
                labels_lines = tf.extractfile(extract_member(tf, "raw/reports/multi_position_swing_shadow_labels.jsonl")).read().decode().splitlines()
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(outcomes) == 3, outcomes
            assert len(labels_lines) == 3, labels_lines
            top2 = next(row for row in outcomes if row["k"] == "2")
            assert top2["label_24h_status"] == "complete", top2
            assert top2["label_24h_portfolio_avg_net_bps"] == "220", top2
            assert top2["label_24h_worst_symbol_net_bps"] == "70", top2
            assert top2["label_24h_win_count"] == "2", top2
            assert top2["label_48h_status"] == "complete", top2
            assert top2["label_72h_status"] == "pending", top2
            by_k_map = {(row["shadow_mode"], row["k"]): row for row in by_k}
            assert by_k_map[("all_candidates", "2")]["avg_24h_net_bps"] == "220.0", by_k
            assert by_k_map[("all_candidates", "3")]["worst_avg"] == "-230.0", by_k
            by_symbol_map = {(row["shadow_mode"], row["symbol"]): row for row in by_symbol}
            assert by_symbol_map[("all_candidates", "ETH/USDT")]["avg_24h_net_bps"] == "370.0", by_symbol
            assert window["multi_position_swing_shadow_label_count"] == 3, window
            assert window["multi_position_swing_shadow_complete_count"] == 3, window
            assert "## 多币 swing shadow" in readme, readme
            assert "all_candidates top2 是否优于 top1: no / 24h top1=370" in readme, readme
            assert "top3 是否增加风险: yes" in readme, readme
            assert "哪些组合表现最好: mode=all_candidates k=1 symbols=[\"ETH/USDT\"] 24h_avg=370" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-sol-swing-performance-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_sol_swing_performance_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/sol_swing_performance.csv")).read().decode().splitlines()))
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(rows) == 1, rows
            row = rows[0]
            assert row["window"] == "last_72h", row
            assert row["real_roundtrip_count"] == "1", row
            assert row["real_net_bps_avg"] == "128", row
            assert row["real_net_pnl_usdt"] == "1.28", row
            assert row["high_score_blocked_count"] == "1", row
            assert row["high_score_blocked_24h_avg"] == "110", row
            assert row["high_score_blocked_48h_avg"] == "160", row
            assert row["high_score_blocked_72h_avg"] == "210", row
            assert row["multi_position_shadow_24h_avg"] == "130.0", row
            assert row["multi_position_shadow_48h_avg"] == "180.0", row
            assert row["multi_position_shadow_72h_avg"] == "230.0", row
            assert row["latest_selected_count"] == "1", row
            assert "protect_entry_trend_only" in row["latest_block_reasons"], row
            assert "## SOL swing 观察" in readme, readme
            assert "真实 SOL swing 是否赚钱: yes" in readme, readme
            assert "是否建议启用多币: no / diagnostic_only_default_disabled" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-swing-early-exit-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_swing_early_exit_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/swing_early_exit_audit.csv")).read().decode().splitlines()))
                by_reason = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/swing_early_exit_outcomes_by_reason.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(rows) == 4, rows
            sol = next(row for row in rows if row["symbol"] == "SOL/USDT")
            assert sol["exit_reason"] == "atr_trailing", sol
            assert sol["exited_before_min_hold"] == "true", sol
            assert float(sol["hold_hours"]) < 24.0, sol
            assert sol["future_24h_net_bps_from_entry"] == "100", sol
            assert sol["future_48h_net_bps_from_entry"] == "200", sol
            assert sol["future_72h_net_bps_from_entry"] == "pending", sol
            assert sol["would_have_been_better_to_hold_24h"] == "true", sol
            bnb = next(row for row in rows if row["symbol"] == "BNB/USDT")
            assert bnb["exit_reason"] == "stop_loss", bnb
            assert bnb["exited_before_min_hold"] == "false", bnb
            by_reason_map = {row["exit_reason"]: row for row in by_reason}
            assert by_reason_map["atr_trailing"]["early_exit_count"] == "1", by_reason
            assert by_reason_map["atr_trailing"]["better_to_hold_24h_rate"] == "1", by_reason
            assert by_reason_map["stop_loss"]["early_exit_count"] == "0", by_reason
            medium_issues = [
                item for item in issues["issues"]
                if item.get("severity") == "medium" and item.get("code") == "swing_early_exit_premature"
            ]
            assert len(medium_issues) == 1, issues
            assert window["swing_early_exit_audit_rows"] == 4, window
            assert window["swing_early_exit_count"] == 3, window
            assert window["swing_early_exit_atr_trailing_count"] == 1, window
            assert window["swing_early_exit_medium_issue"] is True, window
            assert "## Swing early exit audit" in readme, readme
            assert "early exit count: 3" in readme, readme
            assert "ATR trailing before min_hold: yes / 1" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-bnb-swing-early-exit-router-raw-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_bnb_swing_early_exit_router_raw_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/swing_early_exit_audit.csv")).read().decode().splitlines()))
                roundtrips = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/trades_roundtrips.csv")).read().decode().splitlines()))
            assert len(rows) == 1, rows
            bnb = rows[0]
            assert bnb["symbol"] == "BNB/USDT", bnb
            assert bnb["exit_reason"] == "atr_trailing", bnb
            assert bnb["exited_before_min_hold"] == "true", bnb
            assert float(bnb["hold_hours"]) == 5.0, bnb
            assert bnb["required_hold_hours"] == "24", bnb
            raw_payload = json.loads(roundtrips[0]["raw_json"])
            assert raw_payload["entry_router_decision"]["reason"] == "ok / normal_entry", raw_payload
            assert raw_payload["entry_router_decision"]["swing_hold_position"] is True, raw_payload
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-multi-position-swing-shadow-from-audit-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_multi_position_swing_shadow_from_audit_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/multi_position_swing_shadow_outcomes.csv")).read().decode().splitlines()))
                by_k = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/multi_position_swing_shadow_by_k.csv")).read().decode().splitlines()))
                by_symbol = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/multi_position_swing_shadow_by_symbol.csv")).read().decode().splitlines()))
                labels_lines = tf.extractfile(extract_member(tf, "raw/reports/multi_position_swing_shadow_labels.jsonl")).read().decode().splitlines()
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                manifest = json.loads(tf.extractfile(extract_member(tf, "manifest.json")).read().decode())
            assert len(labels_lines) == 3, labels_lines
            assert len(outcomes) == 3, outcomes
            top1 = next(row for row in outcomes if row["shadow_mode"] == "all_candidates" and row["k"] == "1")
            top2 = next(row for row in outcomes if row["shadow_mode"] == "all_candidates" and row["k"] == "2")
            protect_top1 = next(row for row in outcomes if row["shadow_mode"] == "protect_recovery_rules" and row["k"] == "1")
            assert top1["symbols"] == "[\"SOL/USDT\"]", top1
            assert top1["entry_px_by_symbol"] == "{\"SOL/USDT\": 100.0}", top1
            assert top1["final_score_by_symbol"] == "{\"SOL/USDT\": 0.61}", top1
            assert top1["label_24h_status"] == "complete", top1
            assert top1["label_24h_net_bps"] == "370", top1
            assert top1["label_48h_status"] == "complete", top1
            assert top1["label_72h_status"] == "pending", top1
            assert top2["symbols"] == "[\"SOL/USDT\", \"BNB/USDT\"]", top2
            assert top2["label_24h_portfolio_avg_net_bps"] == "120", top2
            assert top2["label_24h_worst_symbol_net_bps"] == "-130", top2
            assert top2["label_24h_win_count"] == "1", top2
            assert protect_top1["symbols"] == "[\"SOL/USDT\"]", protect_top1
            by_k_map = {(row["shadow_mode"], row["k"]): row for row in by_k}
            assert by_k_map[("all_candidates", "1")]["count"] == "1", by_k
            assert by_k_map[("all_candidates", "2")]["count"] == "1", by_k
            assert by_k_map[("protect_recovery_rules", "1")]["count"] == "1", by_k
            assert ("protect_recovery_rules", "2") not in by_k_map, by_k
            by_symbol_map = {(row["shadow_mode"], row["symbol"]): row for row in by_symbol}
            assert by_symbol_map[("all_candidates", "SOL/USDT")]["count"] == "2", by_symbol
            assert by_symbol_map[("all_candidates", "BNB/USDT")]["count"] == "1", by_symbol
            assert by_symbol_map[("protect_recovery_rules", "SOL/USDT")]["count"] == "1", by_symbol
            assert ("protect_recovery_rules", "BNB/USDT") not in by_symbol_map, by_symbol
            assert window["multi_position_swing_shadow_label_count"] == 3, window
            assert window["multi_position_swing_shadow_complete_count"] == 3, window
            assert "reports/multi_position_swing_shadow_labels.jsonl" not in manifest["missing_paths"], manifest
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-alt-impulse-shadow-cache-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_alt_impulse_shadow_cache_fill_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes.csv")).read().decode().splitlines()))
            sol = next(row for row in outcomes if row["symbol"] == "SOL/USDT")
            assert sol["entry_px"] == "100", outcomes
            assert sol["label_4h_net_bps"] == "470", sol
            assert sol["label_8h_net_bps"] == "pending", sol
            assert sol["label_status"] == "complete", sol
            assert sol["label_not_observable_reason"] == "", sol
            eth = next(row for row in outcomes if row["symbol"] == "ETH/USDT")
            assert eth["entry_px"] == "2000", outcomes
            assert eth["label_4h_net_bps"] == "not_observable", eth
            assert eth["label_4h_reason"] == "missing_future_px", eth
            assert eth["label_8h_status"] == "pending", eth
            assert eth["label_status"] == "pending", eth
            assert eth["label_not_observable_reason"] == "", eth
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-alt-impulse-shadow-skipped-provider-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_alt_impulse_shadow_skipped_provider_future_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes.csv")).read().decode().splitlines()))
                by_horizon = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes_by_horizon.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert len(outcomes) == 1, outcomes
            sol = outcomes[0]
            assert sol["symbol"] == "SOL/USDT", sol
            assert sol["entry_px"] == "88.96", sol
            assert sol["label_4h_status"] == "complete", sol
            assert sol["label_8h_status"] == "complete", sol
            assert sol["label_12h_status"] == "complete", sol
            assert sol["label_24h_status"] == "complete", sol
            assert sol["label_48h_status"] == "complete", sol
            assert sol["label_72h_status"] == "pending", sol
            assert sol["label_not_observable_reason"] == "", sol
            assert sol["future_price_source_4h"] == "skipped_candidate_label_provider", sol
            assert sol["future_px_4h"] != "not_observable", sol
            expected_future_4h = 86.88 * (1.0 + 70.211786 / 10000.0)
            expected_net_4h = ((expected_future_4h / 88.96) - 1.0) * 10000.0 - 30.0
            assert abs(float(sol["label_4h_net_bps"]) - expected_net_4h) < 0.00001, sol
            horizon_map = {row["horizon_hours"]: row for row in by_horizon}
            for horizon in ("4", "8", "12", "24", "48"):
                assert horizon_map[horizon]["complete_count"] == "1", by_horizon
                assert horizon_map[horizon]["not_observable_count"] == "0", by_horizon
            assert horizon_map["72"]["pending_count"] == "1", by_horizon
            assert window["alt_impulse_shadow_matured_horizon_count"] >= 5, window
            assert window["alt_impulse_shadow_missing_future_px_count"] == 0, window
            assert not any(
                item.get("severity") == "medium" and item.get("code") == "alt_impulse_shadow_future_px_not_observable"
                for item in issues["issues"]
            ), issues
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-alt-impulse-shadow-extended-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_alt_impulse_shadow_extended_horizon_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes.csv")).read().decode().splitlines()))
                by_horizon = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes_by_horizon.csv")).read().decode().splitlines()))
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            sol = next(row for row in outcomes if row["symbol"] == "SOL/USDT")
            assert sol["entry_px"] == "100", sol
            assert sol["label_48h_net_bps"] == "970", sol
            assert sol["label_48h_status"] == "complete", sol
            assert sol["label_56h_status"] == "not_observable", sol
            assert sol["label_72h_status"] == "pending", sol
            assert sol["label_not_observable_reason"] == "", sol
            horizon_map = {row["horizon_hours"]: row for row in by_horizon}
            assert horizon_map["48"]["avg_net_bps"] == "970.0", by_horizon
            assert horizon_map["48"]["complete_count"] == "1", by_horizon
            assert horizon_map["56"]["not_observable_count"] == "1", by_horizon
            assert horizon_map["72"]["pending_count"] == "1", by_horizon
            assert "48h: count=1, avg=970.0" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-alt-impulse-shadow-missing-entry-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_alt_impulse_shadow_missing_entry_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert len(outcomes) == 1, outcomes
            row = outcomes[0]
            assert row["entry_px"] == "not_observable", row
            assert row["label_4h_net_bps"] == "not_observable", row
            assert row["label_status"] == "not_observable", row
            assert row["label_not_observable_reason"] == "missing_entry_px", row
            assert window["alt_impulse_shadow_entry_px_not_observable_count"] == 1, window
            assert any(
                item.get("severity") == "medium" and item.get("code") == "alt_impulse_shadow_entry_px_not_observable"
                for item in issues["issues"]
            ), issues
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-market-impulse-selection-shadow-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_market_impulse_selection_shadow_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/market_impulse_selection_shadow.csv")).read().decode().splitlines()))
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert len(rows) == 1, rows
            row = rows[0]
            assert row["active"] == "true", row
            assert row["selected_live"] == "BTC/USDT", row
            assert row["selected_by_priority"] == "BTC/USDT", row
            assert row["selected_by_trend_score"] == "ETH/USDT", row
            assert row["selected_by_alpha6_confirmed"] == "SOL/USDT", row
            assert row["selected_by_expected_net_shadow"] == "ETH/USDT", row
            assert "ETH/USDT" in row["candidates_json"], row
            assert window["market_impulse_selection_shadow_rows"] == 1, window
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-factor-contribution-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_factor_contribution_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/factor_contribution_audit.csv")).read().decode().splitlines()))
                by_factor = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/factor_contribution_outcomes_by_factor.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(rows) == 2, rows
            eth = next(row for row in rows if row["symbol"] == "ETH/USDT")
            sol = next(row for row in rows if row["symbol"] == "SOL/USDT")
            assert eth["dominant_factor"] == "f3_vol_adj_ret", eth
            assert float(eth["contribution_f3_vol_adj_ret"]) == 0.7, eth
            assert eth["router_reason"] == "protect_entry_no_alpha6_confirmation", eth
            assert eth["forward_4h_net_bps"] == "-10.0", eth
            assert eth["forward_24h_net_bps"] == "-40.0", eth
            assert sol["forward_4h_net_bps"] == "pending", sol
            f3 = next(row for row in by_factor if row["dominant_factor"] == "f3_vol_adj_ret")
            assert float(f3["avg_24h_net_bps"]) == -40.0, by_factor
            assert f3["win_rate_24h"] == "0.0", by_factor
            assert window["factor_contribution_audit_rows"] == 2, window
            assert window["f3_dominant_count"] == 1, window
            assert window["f3_dominant_negative_evidence"] is False, window
            assert not any(item.get("code") == "f3_dominant_negative_evidence" for item in issues["issues"]), issues
            assert "## Alpha6 factor contribution audit" in readme, readme
            assert "## F3-dominant 风险检查" in readme, readme
            assert "f3_dominant_count: 1" in readme, readme
            assert "f3_dominant_negative_evidence: false" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-factor-contribution-f3-risk-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_factor_contribution_f3_risk_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                by_factor = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/factor_contribution_outcomes_by_factor.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            f3 = next(row for row in by_factor if row["dominant_factor"] == "f3_vol_adj_ret")
            assert int(f3["count"]) == 21, by_factor
            assert float(f3["avg_24h_net_bps"]) < -50.0, f3
            assert float(f3["win_rate_24h"]) < 0.3, f3
            assert window["f3_dominant_count"] == 21, window
            assert window["f3_dominant_negative_evidence"] is True, window
            f3_issues = [
                item for item in issues["issues"]
                if item.get("severity") == "medium" and item.get("code") == "f3_dominant_negative_evidence"
            ]
            assert len(f3_issues) == 1, issues
            assert f3_issues[0]["evidence"]["f3_dominant_count"] == 21, f3_issues
            assert "## F3-dominant 风险检查" in readme, readme
            assert "f3_dominant_count: 21" in readme, readme
            assert "f3_dominant_negative_evidence: true" in readme, readme
            assert "action: diagnostic_only_monitor_no_trade_block" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-missing-trades-") as tmp:
        root = pathlib.Path(tmp) / "root"
        run_id = fixture_root(root)
        (root / "reports/runs/prod" / run_id / "trades.csv").unlink()
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert window["has_trade_data"] is False, window
            assert window["trade_observation_status"] == "not_observable", window
            assert "是否真实成交: not_observable" in readme, readme
            assert "closed roundtrip gross/net bps: not_observable" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-72h-no-24h-trades-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_last_72h_trade_no_24h_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                negexp = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/negative_expectancy_consistency.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert window["latest_24h_trade_count"] == 0, window
            assert window["latest_24h_roundtrip_count"] == 0, window
            assert window["last_72h_trade_count"] == 2, window
            assert window["last_72h_roundtrip_count"] == 1, window
            assert window["negative_expectancy_consistency_rows"] == 1, window
            assert window["negative_expectancy_mismatch_count"] == 1, window
            assert len(negexp) == 1, negexp
            assert negexp[0]["symbol"] == "BTC/USDT", negexp
            assert negexp[0]["roundtrip_closed_count"] == "1", negexp
            assert negexp[0]["roundtrip_net_pnl_sum_usdt"] == "9.79", negexp
            assert negexp[0]["roundtrip_weighted_net_bps"] == "979", negexp
            assert negexp[0]["negexp_net_pnl_sum_usdt"] == "-0.1", negexp
            assert negexp[0]["negexp_fast_fail_net_expectancy_bps"] == "-12", negexp
            assert negexp[0]["bps_mismatch"] == "989", negexp
            assert negexp[0]["mismatch_suspected"] == "true", negexp
            high_mismatch = [
                item for item in issues["issues"]
                if item.get("severity") == "high" and item.get("code") == "negative_expectancy_roundtrip_mismatch"
            ]
            assert len(high_mismatch) == 1, issues
            assert "latest_24h_trade_count: 0" in readme, readme
            assert "latest_24h_roundtrip_count: 0" in readme, readme
            assert "last_72h_trade_count: 2" in readme, readme
            assert "last_72h_roundtrip_count: 1" in readme, readme
            assert "latest_24h 是否真实成交: no / 0" in readme, readme
            assert "last_72h 是否真实成交: yes / 2" in readme, readme
            assert "closed roundtrip gross/net bps: gross=" in readme, readme
            assert "## Negative expectancy 口径一致性" in readme, readme
            assert "mismatch_suspected_count: 1" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-negexp-consistent-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_negative_expectancy_consistent_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                negexp = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/negative_expectancy_consistency.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert len(negexp) == 1, negexp
            row = negexp[0]
            assert row["roundtrip_net_pnl_sum_usdt"] == "9.79", row
            assert row["roundtrip_weighted_net_bps"] == "979", row
            assert row["negexp_net_pnl_sum_usdt"] == "9.78", row
            assert row["negexp_net_expectancy_bps"] == "978", row
            assert row["negexp_fast_fail_net_expectancy_bps"] == "977", row
            assert row["pnl_mismatch_usdt"] == "0.01", row
            assert row["bps_mismatch"] == "1", row
            assert row["mismatch_suspected"] == "false", row
            assert row["diagnosis"] == "ok", row
            assert window["negative_expectancy_mismatch_count"] == 0, window
            assert not any(item.get("code") == "negative_expectancy_roundtrip_mismatch" for item in issues["issues"]), issues
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-negexp-missing-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_negative_expectancy_missing_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                negexp = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/negative_expectancy_consistency.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert len(negexp) == 1, negexp
            row = negexp[0]
            assert row["symbol"] == "BTC/USDT", row
            assert row["negexp_net_pnl_sum_usdt"] == "not_observable", row
            assert row["negexp_fast_fail_net_expectancy_bps"] == "not_observable", row
            assert row["mismatch_suspected"] == "false", row
            assert row["diagnosis"] == "not_observable_negative_expectancy_symbol_missing", row
            assert window["negative_expectancy_mismatch_count"] == 0, window
            assert not any(item.get("code") == "negative_expectancy_roundtrip_mismatch" for item in issues["issues"]), issues
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-config-runtime-consumption-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_config_runtime_consumption_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/config_runtime_consumption_audit.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            by_key = {row["config_key"]: row for row in rows}
            assert by_key["split_orders"]["defined_in_schema"] == "true", by_key["split_orders"]
            assert by_key["split_orders"]["present_in_live_prod"] == "true", by_key["split_orders"]
            assert by_key["split_orders"]["present_in_effective_config"] == "true", by_key["split_orders"]
            assert by_key["split_orders"]["consumed_in_runtime_code"] == "false", by_key["split_orders"]
            assert by_key["split_orders"]["consumer_category"] == "not_consumed", by_key["split_orders"]
            assert by_key["split_orders"]["diagnosis"] == "configured_not_consumed", by_key["split_orders"]
            assert by_key["split_interval_sec"]["diagnosis"] == "configured_not_consumed", by_key["split_interval_sec"]
            assert by_key["same_symbol_reentry_enabled"]["consumed_in_runtime_code"] == "true", by_key["same_symbol_reentry_enabled"]
            assert by_key["same_symbol_reentry_enabled"]["consumer_category"] == "live_runtime", by_key["same_symbol_reentry_enabled"]
            assert by_key["same_symbol_reentry_enabled"]["consumer_files"] == "src/core/pipeline.py", by_key["same_symbol_reentry_enabled"]
            assert by_key["btc_leadership_probe_enabled"]["diagnosis"] == "live_runtime_consumed", by_key["btc_leadership_probe_enabled"]
            assert by_key["protect_profit_lock_enabled"]["diagnosis"] == "live_runtime_consumed", by_key["protect_profit_lock_enabled"]
            assert by_key["swing_hold_enabled"]["consumer_category"] == "live_runtime", by_key["swing_hold_enabled"]
            assert by_key["swing_hold_enabled"]["diagnosis"] == "live_runtime_consumed", by_key["swing_hold_enabled"]
            assert by_key["swing_min_hold_hours"]["consumer_files"] == "main.py", by_key["swing_min_hold_hours"]
            assert by_key["protect_recovery_multi_position_enabled"]["consumer_category"] == "live_runtime", by_key["protect_recovery_multi_position_enabled"]
            assert by_key["protect_negative_expectancy_short_cycle_guard_enabled"]["consumer_category"] == "live_runtime", by_key["protect_negative_expectancy_short_cycle_guard_enabled"]
            assert by_key["open_long_entry_guard_fail_open_buy"]["consumer_files"] == "src/execution/live_execution_engine.py", by_key["open_long_entry_guard_fail_open_buy"]
            assert by_key["open_long_entry_guard_fail_open_sell"]["consumer_category"] == "live_runtime", by_key["open_long_entry_guard_fail_open_sell"]
            assert by_key["multi_position_swing_shadow_enabled"]["present_in_live_prod"] == "true", by_key["multi_position_swing_shadow_enabled"]
            assert by_key["multi_position_swing_shadow_enabled"]["consumer_category"] == "diagnostics", by_key["multi_position_swing_shadow_enabled"]
            assert by_key["multi_position_swing_shadow_enabled"]["diagnosis"] == "diagnostics_consumed", by_key["multi_position_swing_shadow_enabled"]
            assert by_key["alt_impulse_shadow_enabled"]["consumer_category"] == "diagnostics", by_key["alt_impulse_shadow_enabled"]
            assert by_key["alt_impulse_shadow_enabled"]["diagnosis"] == "diagnostics_consumed", by_key["alt_impulse_shadow_enabled"]
            assert by_key["quant_lab_fail_policy"]["consumer_category"] == "legacy_inactive", by_key["quant_lab_fail_policy"]
            assert by_key["quant_lab_fail_policy"]["diagnosis"] == "legacy_execution_quant_lab_inactive_top_level_authoritative", by_key["quant_lab_fail_policy"]
            assert by_key["probe_exit_enabled"]["present_in_live_prod"] == "false", by_key["probe_exit_enabled"]
            low_issues = [
                item for item in issues["issues"]
                if item.get("severity") == "low" and item.get("code") == "config_key_not_consumed"
            ]
            assert {item["evidence"]["config_key"] for item in low_issues} == {"split_orders", "split_interval_sec"}, issues
            assert window["config_runtime_not_consumed_count"] == 2, window
            assert "## 配置消费审计" in readme, readme
            assert "live config keys not consumed in runtime: 2" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-rank-exit-consistency-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_rank_exit_consistency_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/rank_exit_consistency.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(rows) == 1, rows
            row = rows[0]
            assert row["symbol"] == "BNB/USDT", row
            assert row["exit_reason"] == "rank_exit_4", row
            assert row["source"].startswith("trades:"), row
            assert row["target_w"] == "0.15", row
            assert row["rank"] == "4", row
            assert row["close_only_weight_eps"] == "0.001", row
            assert row["has_exit_signal"] == "false", row
            assert row["has_router_close_create"] == "false", row
            assert row["has_target_still_positive_note"] == "true", row
            assert row["target_positive"] == "true", row
            assert row["conflict_suspected"] == "true", row
            assert row["diagnosis"].startswith("high_issue_rank_exit_target_positive_execution_conflict"), row
            rank_issues = [
                item for item in issues["issues"]
                if item.get("severity") == "high" and item.get("code") == "rank_exit_target_positive_execution_conflict"
            ]
            assert len(rank_issues) == 1, issues
            assert issues["high_issue_count"] >= 1, issues
            assert window["rank_exit_sell_count"] == 1, window
            assert window["rank_exit_conflict_count"] == 1, window
            assert window["rank_exit_target_positive_sell_count"] == 1, window
            assert "## Rank exit 一致性检查" in readme, readme
            assert "rank_exit sell 数量: 1" in readme, readme
            assert "conflict 数量: 1" in readme, readme
            assert "是否存在 target 仍为正但实盘卖出: yes" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-rank-exit-log-only-consistency-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_rank_exit_log_only_consistency_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/rank_exit_consistency.csv")).read().decode().splitlines()))
                roundtrips = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/trades_roundtrips.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert len(rows) == 1, rows
            assert roundtrips and roundtrips[0]["exit_reason"] == "not_observable", roundtrips
            row = rows[0]
            assert row["symbol"] == "BNB/USDT", row
            assert row["exit_reason"] == "rank_exit_4", row
            assert row["source"].startswith("log:raw/logs/"), row
            assert row["target_w"] == "0.15", row
            assert row["rank"] == "2", row
            assert row["has_exit_signal"] == "false", row
            assert row["has_router_close_create"] == "false", row
            assert row["has_target_still_positive_note"] == "true", row
            assert row["target_positive"] == "true", row
            assert row["conflict_suspected"] == "true", row
            assert row["diagnosis"].startswith("high_issue_rank_exit_target_positive_execution_conflict"), row
            rank_issues = [
                item for item in issues["issues"]
                if item.get("severity") == "high" and item.get("code") == "rank_exit_target_positive_execution_conflict"
            ]
            assert len(rank_issues) == 1, issues
            assert issues["high_issue_count"] >= 1, issues
            assert window["rank_exit_sell_count"] == 1, window
            assert window["rank_exit_conflict_count"] == 1, window
            assert window["rank_exit_target_positive_sell_count"] == 1, window
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-legacy-rank-exit-log-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_legacy_rank_exit_log_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/rank_exit_consistency.csv")).read().decode().splitlines()))
                legacy_rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/legacy_rank_exit_events.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
            assert rows == [], rows
            assert len(legacy_rows) == 1, legacy_rows
            legacy = legacy_rows[0]
            assert legacy["ts_utc"] == "2026-03-17T08:00:49Z", legacy
            assert legacy["run_id"] == "not_observable", legacy
            assert legacy["symbol"] == "ETH/USDT", legacy
            assert legacy["exit_reason"] == "rank_exit_4", legacy
            assert legacy["diagnosis"] == "legacy_rank_exit_event_outside_current_window", legacy
            rank_issues = [
                item for item in issues["issues"]
                if item.get("severity") == "high" and item.get("code") == "rank_exit_target_positive_execution_conflict"
            ]
            assert rank_issues == [], issues
            assert window["rank_exit_sell_count"] == 0, window
            assert window["rank_exit_conflict_count"] == 0, window
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-protect-sideways-normal-entry-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_protect_sideways_normal_entry_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                rows = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/protect_sideways_normal_entry_outcomes.csv")).read().decode().splitlines()))
                by_symbol = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/protect_sideways_normal_entry_outcomes_by_symbol.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(rows) == 5, rows
            row = rows[0]
            assert row["symbol"] == "BNB/USDT", row
            assert row["entry_px"] == "628.4", row
            assert row["exit_reason"] == "stop_loss", row
            assert row["alpha6_score_at_entry"] == "0.572", row
            assert row["f4_at_entry"] == "1.925", row
            assert row["f5_at_entry"] == "0.302", row
            assert row["trend_score_at_entry"] == "0.81", row
            assert row["result_bucket"] == "loss_le_-100bps", row
            assert len(by_symbol) == 1, by_symbol
            assert by_symbol[0]["symbol"] == "BNB/USDT", by_symbol
            assert by_symbol[0]["count"] == "5", by_symbol
            assert float(by_symbol[0]["avg_net_bps"]) < -100.0, by_symbol
            assert by_symbol[0]["win_rate"] == "0.0", by_symbol
            assert window["protect_sideways_normal_entry_count"] == 5, window
            assert window["protect_sideways_normal_entry_medium_issue"] is True, window
            medium_issues = [
                item for item in issues["issues"]
                if item.get("severity") == "medium" and item.get("code") == "protect_sideways_normal_entry_negative"
            ]
            assert len(medium_issues) == 1, issues
            assert "## PROTECT Sideways 普通开仓表现" in readme, readme
            assert "sample_count: 5" in readme, readme
            assert "BNB/USDT: count=5" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-open-position-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_open_position_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                open_positions = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/open_positions.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(open_positions) == 1, open_positions
            row = open_positions[0]
            assert row["symbol"] == "BTC/USDT", row
            assert row["entry_px"] == "100", row
            assert row["current_px"] == "112", row
            assert row["unrealized_net_bps"] == "1178.8", row
            assert row["profit_lock_active"] == "false", row
            assert row["trailing_active"] == "false", row
            assert window["open_position_count"] == 1, window
            medium_open = [
                item for item in issues["issues"]
                if item.get("severity") == "medium" and item.get("code") == "open_profit_without_profit_lock"
            ]
            assert len(medium_open) == 1, medium_open
            assert "## Open position 检查" in readme, readme
            assert "当前是否有持仓: yes / 1" in readme, readme
            assert "unrealized net bps: BTC/USDT=1178.8" in readme, readme
            assert "当前 stop 是否足够保护浮盈: no" in readme, readme
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    with tempfile.TemporaryDirectory(prefix="v5-dust-residual-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_dust_residual_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                roundtrips = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/trades_roundtrips.csv")).read().decode().splitlines()))
                dust_roundtrips = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/dust_residual_roundtrips.csv")).read().decode().splitlines()))
                open_positions = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/open_positions.csv")).read().decode().splitlines()))
                lifecycle = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/probe_lifecycle_audit.csv")).read().decode().splitlines()))
                issues = json.loads(tf.extractfile(extract_member(tf, "summaries/issues_to_fix.json")).read().decode())
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()

            assert open_positions == [], open_positions
            assert window["effective_open_position_count"] == 0, window
            assert window["open_position_count"] == 0, window
            assert window["dust_residual_position_count"] >= 1, window
            assert window["dust_residual_roundtrip_count"] >= 2, window
            assert "account status: flat / dust-only" in readme, readme

            assert all(row["qty"] != "0.00000021" for row in roundtrips), roundtrips
            assert any(row["roundtrip_status"] == "open_dust_residual_ignored" for row in dust_roundtrips), dust_roundtrips
            assert any(row["roundtrip_status"] == "dust_close_ignored" for row in dust_roundtrips), dust_roundtrips
            assert lifecycle == [], lifecycle
            assert window["probe_trade_net_bps"]["avg"] == "not_observable", window
            high_issues = [item for item in issues["issues"] if item.get("severity") == "high"]
            assert high_issues == [], high_issues
        finally:
            bundle.unlink(missing_ok=True)
            pathlib.Path(f"{bundle}.sha256").unlink(missing_ok=True)
            shutil.rmtree(pathlib.Path("/tmp") / bundle.name.removesuffix(".tar.gz"), ignore_errors=True)

    print("btc leadership labeler tests passed")


if __name__ == "__main__":
    main()
