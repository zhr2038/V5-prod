from __future__ import annotations

from pathlib import Path

import scripts.backfill_ml_multihorizon_labels as multihorizon


def test_load_cache_candles_prefers_logically_newer_file_for_duplicate_timestamp(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    (cache_dir / "BTC_USDT_1H_20260101.csv").write_text(
        "\n".join(
            [
                "timestamp,close",
                "2026-01-01 00:00:00,100",
                "2026-01-01 01:00:00,101",
            ]
        ),
        encoding="utf-8",
    )
    (cache_dir / "BTC_USDT_1H_2026-01-01_2026-01-02.csv").write_text(
        "\n".join(
            [
                "timestamp,close",
                "2026-01-01 01:00:00,999",
                "2026-01-01 02:00:00,103",
            ]
        ),
        encoding="utf-8",
    )

    candles = multihorizon.load_cache_candles(cache_dir, "BTC/USDT")

    assert list(candles["close"]) == [100.0, 999.0, 103.0]
