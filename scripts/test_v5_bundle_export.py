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
    proc = subprocess.run(
        ["bash", script_path],
        env={**os.environ, "V5_REMOTE_ROOT": str(root)},
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    bundle_path = None
    for line in proc.stdout.splitlines():
        if line.startswith("BUNDLE_PATH="):
            bundle_path = pathlib.Path(line.split("=", 1)[1])
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
            assert "这些被挡样本历史 forward net bps: ETH/USDT/protect_entry_no_alpha6_confirmation: 4h=not_observable" in readme, readme
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

    with tempfile.TemporaryDirectory(prefix="v5-alt-impulse-shadow-") as tmp:
        root = pathlib.Path(tmp) / "root"
        fixture_alt_impulse_shadow_root(root)
        bundle = run_bundle(root)
        try:
            with tarfile.open(bundle, "r:gz") as tf:
                outcomes = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes.csv")).read().decode().splitlines()))
                by_symbol = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes_by_symbol.csv")).read().decode().splitlines()))
                by_reason = list(csv.DictReader(tf.extractfile(extract_member(tf, "summaries/alt_impulse_shadow_outcomes_by_reason.csv")).read().decode().splitlines()))
                window = json.loads(tf.extractfile(extract_member(tf, "summaries/window_summary.json")).read().decode())
                readme = tf.extractfile(extract_member(tf, "README.md")).read().decode()
            assert len(outcomes) == 2, outcomes
            eth = next(row for row in outcomes if row["symbol"] == "ETH/USDT")
            assert eth["label_4h_net_bps"] == "70.0", outcomes
            assert eth["label_status"] == "complete", outcomes
            sol = next(row for row in outcomes if row["symbol"] == "SOL/USDT")
            assert sol["label_status"] == "pending", outcomes
            by_symbol_map = {(row["symbol"], row["skip_reason"]): row for row in by_symbol}
            assert by_symbol_map[("ETH/USDT", "protect_entry_trend_only")]["avg_4h_net_bps"] == "70.0", by_symbol
            assert by_symbol_map[("ETH/USDT", "protect_entry_trend_only")]["win_rate_4h"] == "1.0", by_symbol
            assert any(row["skip_reason"] == "protect_entry_no_alpha6_confirmation" for row in by_reason), by_reason
            assert window["alt_impulse_shadow_label_count"] == 2, window
            assert "## ALT impulse shadow" in readme, readme
            assert "ETH/USDT: count=1, avg_net_bps 4h=70.0" in readme, readme
            assert "SOL/USDT: count=1, avg_net_bps 4h=not_observable" in readme, readme
            assert "BNB/USDT: count=0" in readme, readme
            assert "是否支持未来 live probe: diagnostic_only_review_required" in readme, readme
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
            assert window["f3_dominant_negative_evidence"] is True, window
            assert "## Alpha6 factor contribution audit" in readme, readme
            assert "f3_dominant_negative_evidence: true" in readme, readme
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
