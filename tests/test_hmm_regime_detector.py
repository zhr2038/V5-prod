from __future__ import annotations

import json
from datetime import datetime

import numpy as np
import pytest

from src.regime.hmm_regime_detector import HMMRegimeDetector
from src.regime.hmm_model import SimpleGaussianHMM


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


def test_hmm_cache_file_epoch_uses_utc_for_filename_tokens(tmp_path) -> None:
    hourly = tmp_path / "BTC_USDT_1H_20260101_03.csv"
    daily = tmp_path / "BTC_USDT_1H_2026-01-02.csv"

    assert HMMRegimeDetector._cache_file_epoch(hourly, prefix="BTC_USDT_1H_") == datetime.fromisoformat(
        "2026-01-01T03:00:00+00:00"
    ).timestamp()
    assert HMMRegimeDetector._cache_file_epoch(daily, prefix="BTC_USDT_1H_") == datetime.fromisoformat(
        "2026-01-02T00:00:00+00:00"
    ).timestamp()


def _trained_hmm_payload() -> SimpleGaussianHMM:
    model = SimpleGaussianHMM(n_components=2)
    model.startprob_ = np.asarray([0.6, 0.4])
    model.transmat_ = np.asarray([[0.8, 0.2], [0.3, 0.7]])
    model.means_ = np.asarray([[0.1, 0.2], [-0.1, 0.3]])
    model.covs_ = np.asarray([[0.01, 0.02], [0.03, 0.04]])
    model.n_features = 2
    model.converged = True
    return model


def test_hmm_model_info_trained_at_is_timezone_aware(tmp_path) -> None:
    detector = HMMRegimeDetector(n_components=2, model_path=tmp_path / "hmm_regime.pkl")
    detector.model = _trained_hmm_payload()

    detector._write_model_info(100, model_sha256="abc")

    info = json.loads((tmp_path / "hmm_regime_info.json").read_text(encoding="utf-8"))
    assert datetime.fromisoformat(info["trained_at"]).tzinfo is not None


def test_hmm_detect_regime_timestamp_is_timezone_aware(tmp_path, monkeypatch) -> None:
    detector = HMMRegimeDetector(n_components=2, model_path=tmp_path / "hmm_regime.pkl")
    monkeypatch.setattr(detector, "predict", lambda features: {"state": "Sideways"})

    result = detector.detect_regime([[0.1, 0.2, 0.3, 55.0]])

    assert datetime.fromisoformat(result["timestamp"]).tzinfo is not None


def test_hmm_pickle_load_verifies_sha256_from_info(tmp_path) -> None:
    model_path = tmp_path / "hmm_regime.pkl"
    sha256 = _trained_hmm_payload().save(model_path)
    (tmp_path / "hmm_regime_info.json").write_text(
        json.dumps({"model_sha256": sha256}),
        encoding="utf-8",
    )

    loaded = SimpleGaussianHMM(n_components=2).load(model_path)
    assert loaded.converged is True
    assert loaded.n_features == 2

    with model_path.open("ab") as handle:
        handle.write(b"tamper")

    with pytest.raises(RuntimeError, match="sha256 mismatch"):
        SimpleGaussianHMM(n_components=2).load(model_path)


def test_hmm_pickle_load_rejects_info_without_sha256(tmp_path, monkeypatch) -> None:
    model_path = tmp_path / "hmm_regime.pkl"
    _trained_hmm_payload().save(model_path)
    (tmp_path / "hmm_regime_info.json").write_text("{}", encoding="utf-8")
    monkeypatch.delenv("V5_ALLOW_LEGACY_HMM_PICKLE_LOAD", raising=False)

    with pytest.raises(RuntimeError, match="sha256 missing"):
        SimpleGaussianHMM(n_components=2).load(model_path)
