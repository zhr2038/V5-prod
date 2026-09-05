from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from src.alpha.alpha_engine import AlphaEngine
from src.alpha.weight_evidence import UNIVERSE, bind_evidence, factor_version, validate_weight_report
from src.strategy.multi_strategy_system import Alpha6FactorStrategy

NOW = 1788652800000


def report():
    rows = [{"from_ts_ms": NOW - (96 - i) * 3600000, "to_ts_ms": NOW - (95 - i) * 3600000,
             "universe": list(UNIVERSE), "factor_version": factor_version(), "score_rank_ic": (-1) ** i * 0.2,
             "valid_for_weighting": True} for i in range(96)]
    payload = {"factor_ic": {key: {"rank_ic_short": {"count": 24, "mean": value}, "rank_ic_long": {"count": 96, "mean": value}} for key, value in (("f1", 0.1), ("f2", -0.1))}}
    return bind_evidence(payload, rows)


@pytest.mark.parametrize("field,value,reason", [
    ("cutoff_ts_ms", NOW - 7 * 3600000, "report_future_or_expired"),
    ("cutoff_ts_ms", NOW + 6000, "report_future_or_expired"),
    ("sample_count", 1, "insufficient_samples"),
    ("independent_samples", 1, "insufficient_independent_samples"),
    ("universe", ["BTC/USDT"], "universe_mismatch"),
    ("factor_version", "old", "factor_version_mismatch"),
    ("source_sha256", "0" * 64, "source_or_payload_hash_mismatch"),
    ("horizon_hours", 24, "horizon_mismatch"),
])
def test_weight_report_rejects_invalid_evidence(field, value, reason):
    obj = report()
    assert validate_weight_report(obj, {}, factors=["f1", "f2"], now_ms=NOW) is None
    obj["evidence"][field] = value
    assert validate_weight_report(obj, {}, factors=["f1", "f2"], now_ms=NOW) == reason


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf"), True])
def test_nonfinite_or_boolean_ic_rejected(bad):
    obj = report()
    obj["factor_ic"]["f1"]["rank_ic_long"]["mean"] = bad
    assert validate_weight_report(obj, {}, factors=["f1"], now_ms=NOW)


def test_all_weight_consumers_preserve_nonzero_static_on_invalid_report(tmp_path, monkeypatch):
    path = tmp_path / "weights.json"
    obj = report()
    obj["evidence"]["factor_version"] = "old"
    path.write_text(json.dumps(obj), encoding="utf-8")
    cfg = {"enabled": True, "ic_monitor_path": str(path)}
    static = {"f1": 0.4, "f2": 0.6}
    engine = AlphaEngine.__new__(AlphaEngine)
    engine.cfg = SimpleNamespace(dynamic_ic_weighting=SimpleNamespace(**cfg), dynamic_weights_by_regime_enabled=True, dynamic_weights_by_regime_path=str(path))
    engine.current_regime_key = "Trend"
    engine._resolve_repo_path = lambda *_: path
    strategy = Alpha6FactorStrategy.__new__(Alpha6FactorStrategy)
    strategy.config = {"dynamic_ic_weighting": cfg}
    monkeypatch.setattr("src.alpha.weight_evidence.time.time", lambda: NOW / 1000)
    assert engine._load_dynamic_ic_weights(static) == static
    assert strategy._resolve_dynamic_weights(static) == static
    assert engine._load_regime_weight_override() == {}
    assert engine.dynamic_weight_status == strategy.dynamic_weight_status == "factor_version_mismatch"
    path.write_text(json.dumps(report()), encoding="utf-8")
    adjusted = engine._load_dynamic_ic_weights(static)
    assert adjusted != static and sum(adjusted.values()) == pytest.approx(sum(static.values()))
    assert strategy._resolve_dynamic_weights(static) == adjusted


def test_missing_evidence_and_low_count_means_are_not_zero_weights():
    assert validate_weight_report({}, {}, now_ms=NOW) == "missing_or_invalid_weight_evidence"
    rec = {"rank_ic_short": {"count": 1, "mean": 0.9}}
    assert AlphaEngine._extract_factor_ic_means(deepcopy(rec)) == (None, None)
