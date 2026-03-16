from __future__ import annotations

from datetime import datetime

from event_driven_check import evaluate_live_trigger_throttle, get_current_live_window_run_id


def test_current_live_window_run_id_matches_hour_format() -> None:
    rid = get_current_live_window_run_id(datetime(2026, 3, 8, 13, 5, 0))
    assert rid == "20260308_13"


def test_same_window_is_throttled_even_if_last_run_is_old() -> None:
    res = evaluate_live_trigger_throttle(
        last_run_age_sec=7200,
        last_run_id="20260308_13",
        current_run_id="20260308_13",
        min_interval_minutes=15,
    )

    assert res["throttled"] is True
    assert res["reason"] == "same_window_already_ran"


def test_recent_run_is_throttled_by_min_interval() -> None:
    res = evaluate_live_trigger_throttle(
        last_run_age_sec=300,
        last_run_id="20260308_12",
        current_run_id="20260308_13",
        min_interval_minutes=10,
    )

    assert res["throttled"] is True
    assert res["reason"] == "min_interval"


def test_different_window_after_interval_is_allowed() -> None:
    res = evaluate_live_trigger_throttle(
        last_run_age_sec=900,
        last_run_id="20260308_12",
        current_run_id="20260308_13",
        min_interval_minutes=10,
    )

    assert res["throttled"] is False
    assert res["reason"] is None
