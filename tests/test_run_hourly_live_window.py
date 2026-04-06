from pathlib import Path


def test_run_hourly_live_window_uses_utc_run_ids_for_rollup_contract() -> None:
    text = Path("scripts/run_hourly_live_window.sh").read_text(encoding="utf-8")
    assert 'WIN_ID="$(date -u +%Y%m%d_%H)"' in text
    assert 'window=[${START_EPOCH}, ${END_EPOCH}) UTC' in text
