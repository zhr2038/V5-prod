from __future__ import annotations

import numpy as np

from src.regime.hmm_regime_detector import HMMRegimeDetector


def test_hmm_regime_detector_cache_prefers_logically_newer_file_for_duplicate_timestamp(tmp_path, monkeypatch) -> None:
    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    daily_file = cache_dir / "BTC_USDT_1H_20260101.csv"
    range_file = cache_dir / "BTC_USDT_1H_2026-01-01_2026-01-03.csv"

    daily_rows = ["timestamp,close"]
    for hour in range(51):
        day = 1 + (hour // 24)
        hh = hour % 24
        daily_rows.append(f"2026-01-{day:02d} {hh:02d}:00:00,{100 + hour}")
    daily_file.write_text("\n".join(daily_rows), encoding="utf-8")

    range_rows = [
        "timestamp,close",
        "2026-01-02 01:00:00,999",
        "2026-01-03 03:00:00,151",
    ]
    range_file.write_text("\n".join(range_rows), encoding="utf-8")

    monkeypatch.setattr(HMMRegimeDetector, "build_features_from_closes", staticmethod(lambda closes, min_periods=14: np.asarray(closes, dtype=float)))

    detector = HMMRegimeDetector()
    closes = detector._load_training_data_from_cache(cache_dir, "BTC/USDT", lookback_days=7)

    assert closes is not None
    assert closes.tolist()[25] == 999.0
    assert closes.tolist()[-1] == 151.0
