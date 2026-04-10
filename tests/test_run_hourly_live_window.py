from pathlib import Path


def test_run_hourly_live_window_uses_utc_run_ids_for_rollup_contract() -> None:
    text = Path("scripts/run_hourly_live_window.sh").read_text(encoding="utf-8")
    assert 'WIN_ID="$(date -u +%Y%m%d_%H)"' in text
    assert 'window=[${START_EPOCH}, ${END_EPOCH}) UTC' in text


def test_run_hourly_live_window_only_uses_fresh_trend_cache() -> None:
    text = Path("scripts/run_hourly_live_window.sh").read_text(encoding="utf-8")
    assert 'TREND_CACHE_PATH="${V5_TREND_CACHE_PATH:-$ROOT/reports/trend_cache.json}"' in text
    assert 'TREND_CACHE_MAX_AGE_SEC="${V5_TREND_CACHE_MAX_AGE_SEC:-300}"' in text
    assert 'cache_age=$(( NOW_EPOCH - cache_mtime ))' in text
    assert 'export V5_USE_CACHED_TREND="1"' in text
    assert "unset V5_USE_CACHED_TREND" in text
