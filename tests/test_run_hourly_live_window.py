from pathlib import Path


def test_run_hourly_live_window_uses_utc_run_ids_for_rollup_contract() -> None:
    text = Path("scripts/run_hourly_live_window.sh").read_text(encoding="utf-8")
    assert 'WIN_ID="$(date -u +%Y%m%d_%H)"' in text
    assert 'window=[${START_EPOCH}, ${END_EPOCH}) UTC' in text


def test_run_hourly_live_window_only_uses_fresh_trend_cache() -> None:
    text = Path("scripts/run_hourly_live_window.sh").read_text(encoding="utf-8")
    assert 'resolve_trend_cache_path()' in text
    assert 'resolve_trend_cache_timestamp()' in text
    assert 'TREND_CACHE_PATH="${V5_TREND_CACHE_PATH:-$(resolve_trend_cache_path)}"' in text
    assert 'TREND_CACHE_MAX_AGE_SEC="${V5_TREND_CACHE_MAX_AGE_SEC:-300}"' in text
    assert 'cache_ts="$(resolve_trend_cache_timestamp "$TREND_CACHE_PATH")"' in text
    assert 'cache_age=$(( NOW_EPOCH - cache_ts ))' in text
    assert 'export V5_USE_CACHED_TREND="1"' in text
    assert "unset V5_USE_CACHED_TREND" in text


def test_run_shadow_tuned_xgboost_hourly_only_uses_fresh_runtime_trend_cache() -> None:
    text = Path("scripts/run_shadow_tuned_xgboost_hourly.sh").read_text(encoding="utf-8")
    assert 'resolve_trend_cache_path()' in text
    assert 'resolve_trend_cache_timestamp()' in text
    assert 'TREND_CACHE_PATH="${V5_TREND_CACHE_PATH:-$(resolve_trend_cache_path)}"' in text
    assert 'TREND_CACHE_MAX_AGE_SEC="${V5_TREND_CACHE_MAX_AGE_SEC:-300}"' in text
    assert 'cache_ts="$(resolve_trend_cache_timestamp "$TREND_CACHE_PATH")"' in text
    assert 'cache_age=$(( NOW_EPOCH - cache_ts ))' in text
    assert 'export V5_USE_CACHED_TREND="1"' in text
    assert "unset V5_USE_CACHED_TREND" in text
