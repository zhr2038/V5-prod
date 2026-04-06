from pathlib import Path


def test_run_hourly_window_uses_utc_run_ids_and_repo_root_v4_dir() -> None:
    text = Path("scripts/run_hourly_window.sh").read_text(encoding="utf-8")
    assert 'WIN_ID="$(date -u +%Y%m%d_%H)"' in text
    assert 'NOW_EPOCH="$(date -u +%s)"' in text
    assert 'window=[${START_EPOCH}, ${END_EPOCH}) UTC' in text
    assert 'V4_REPORTS_DIR="${V4_REPORTS_DIR:-$ROOT/v4_export}"' in text
    assert '--v4_reports_dir "$V4_REPORTS_DIR"' in text
