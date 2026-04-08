from __future__ import annotations

import json
import tempfile
import time

from src.execution.kill_switch_guard import GuardConfig, KillSwitchGuard


def _write(p: str, obj: dict) -> None:
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def test_hard_mismatch_three_times_triggers_kill() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(reconcile_status_path=status, failure_state_path=state, kill_switch_path=kill, hard_fail_threshold=3, stale_threshold_sec=10**9)
        g = KillSwitchGuard(cfg)

        base = int(time.time() * 1000)
        for i in range(3):
            _write(
                status,
                {
                    "schema_version": 1,
                    "generated_ts_ms": base + i,
                    "ok": False,
                    "reason": "usdt_mismatch",
                    "stats": {"max_abs_usdt_delta": 10.0},
                },
            )
            out = g.apply()

        assert (out.get("kill_switch") or {}).get("enabled") is True


def test_auth_error_one_time_triggers_kill() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(reconcile_status_path=status, failure_state_path=state, kill_switch_path=kill, auth_fail_threshold=1, stale_threshold_sec=10**9)
        g = KillSwitchGuard(cfg)

        _write(
            status,
            {
                "schema_version": 1,
                "generated_ts_ms": int(time.time() * 1000),
                "ok": False,
                "reason": "network_error",
                "error": {"okx_code": "50103", "okx_msg": "Invalid signature"},
            },
        )
        out = g.apply()
        assert (out.get("kill_switch") or {}).get("enabled") is True


def test_ok_resets_counters() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(reconcile_status_path=status, failure_state_path=state, kill_switch_path=kill, stale_threshold_sec=10**9)
        g = KillSwitchGuard(cfg)

        base = int(time.time() * 1000)
        _write(status, {"generated_ts_ms": base + 1, "ok": False, "reason": "usdt_mismatch"})
        g.apply()
        _write(status, {"generated_ts_ms": base + 2, "ok": True, "reason": None})
        out = g.apply()
        st = out.get("failure_state") or {}
        assert int(st.get("consecutive_hard")) == 0
        assert int(st.get("consecutive_soft")) == 0


def test_kill_not_auto_disabled() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(reconcile_status_path=status, failure_state_path=state, kill_switch_path=kill, stale_threshold_sec=10**9)
        g = KillSwitchGuard(cfg)

        # pre-existing kill
        _write(kill, {"enabled": True, "ts_ms": 1, "trigger": "manual"})
        _write(status, {"generated_ts_ms": int(time.time() * 1000), "ok": True})
        out = g.apply()
        assert (out.get("kill_switch") or {}).get("enabled") is True


def test_nested_manual_kill_switch_remains_enabled() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(reconcile_status_path=status, failure_state_path=state, kill_switch_path=kill, stale_threshold_sec=10**9)
        g = KillSwitchGuard(cfg)

        _write(kill, {"kill_switch": {"enabled": True, "trigger": "manual", "reason": "ops_lock"}})
        _write(status, {"generated_ts_ms": int(time.time() * 1000), "ok": True})
        out = g.apply()
        ks = out.get("kill_switch") or {}
        assert ks.get("enabled") is True
        assert ks.get("trigger") == "manual"


def test_manual_flag_kill_switch_not_auto_cleared() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(
            reconcile_status_path=status,
            failure_state_path=state,
            kill_switch_path=kill,
            stale_threshold_sec=10**9,
            auto_clear_enabled=True,
            auto_clear_after_ok_count=1,
        )
        g = KillSwitchGuard(cfg)

        _write(kill, {"enabled": True, "manual": True, "trigger": "ops_override"})
        _write(status, {"generated_ts_ms": int(time.time() * 1000), "ok": True})
        out = g.apply()
        ks = out.get("kill_switch") or {}
        assert ks.get("enabled") is True
        assert ks.get("manual") is True
        persisted = json.loads(open(kill, "r", encoding="utf-8").read())
        assert persisted["enabled"] is True
        assert persisted["manual"] is True
