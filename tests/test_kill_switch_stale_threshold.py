from __future__ import annotations

import json
import tempfile
import time

from src.execution.kill_switch_guard import GuardConfig, KillSwitchGuard


def _write(p: str, obj: dict) -> None:
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def test_stale_three_times_triggers_kill() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(
            reconcile_status_path=status,
            failure_state_path=state,
            kill_switch_path=kill,
            stale_threshold_sec=0,
            stale_soft_threshold=3,
        )
        g = KillSwitchGuard(cfg)

        base = int(time.time() * 1000) - 10_000
        for i in range(3):
            _write(status, {"generated_ts_ms": base + i, "ok": True, "reason": None})
            out = g.apply()

        assert (out.get("kill_switch") or {}).get("enabled") is True
        assert (out.get("kill_switch") or {}).get("trigger") == "reconcile_stale_fail"


def test_stale_resets_on_timeout() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(
            reconcile_status_path=status,
            failure_state_path=state,
            kill_switch_path=kill,
            stale_threshold_sec=0,
            stale_soft_threshold=3,
        )
        g = KillSwitchGuard(cfg)

        base = int(time.time() * 1000) - 10_000
        # 2 stale
        _write(status, {"generated_ts_ms": base + 1, "ok": True})
        g.apply()
        _write(status, {"generated_ts_ms": base + 2, "ok": True})
        g.apply()

        # then a timeout (non-stale reason) => consecutive_stale resets
        _write(status, {"generated_ts_ms": int(time.time() * 1000), "ok": False, "reason": "timeout"})
        out = g.apply()
        st = out.get("failure_state") or {}
        assert int(st.get("consecutive_stale") or 0) == 0
        assert (out.get("kill_switch") or {}).get("enabled") is not True


def test_timeout_many_times_does_not_kill() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(
            reconcile_status_path=status,
            failure_state_path=state,
            kill_switch_path=kill,
            stale_threshold_sec=10**9,
            stale_soft_threshold=3,
        )
        g = KillSwitchGuard(cfg)

        base = int(time.time() * 1000)
        for i in range(10):
            _write(status, {"generated_ts_ms": base + i, "ok": False, "reason": "timeout"})
            out = g.apply()

        assert (out.get("kill_switch") or {}).get("enabled") is not True


def test_ok_resets_stale() -> None:
    with tempfile.TemporaryDirectory() as td:
        status = f"{td}/reconcile_status.json"
        state = f"{td}/reconcile_failure_state.json"
        kill = f"{td}/kill_switch.json"
        cfg = GuardConfig(
            reconcile_status_path=status,
            failure_state_path=state,
            kill_switch_path=kill,
            stale_threshold_sec=0,
            stale_soft_threshold=3,
        )
        g = KillSwitchGuard(cfg)

        base = int(time.time() * 1000) - 10_000
        _write(status, {"generated_ts_ms": base, "ok": True})
        g.apply()

        _write(status, {"generated_ts_ms": int(time.time() * 1000), "ok": True})
        out = g.apply()
        st = out.get("failure_state") or {}
        assert int(st.get("consecutive_stale") or 0) == 0
