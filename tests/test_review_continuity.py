"""N1/N2 regressions against real collection and comparison; synthetic public inputs."""
import copy
import json
from types import SimpleNamespace

import pytest

from src.core.models import MarketSeries
from src.research import review_forward as forward
from src.research.review_comparison import summarize
from src.research.review_acceptance import evaluate_acceptance
from tests.test_review_boundary_regressions import comparison, market
from tests.test_review_comparison import POLICY, EXPERIMENT, frame
from tests.test_review_acceptance import sufficient_report

HOUR = 1788652800


@pytest.fixture
def source(monkeypatch, tmp_path):
    clock = SimpleNamespace(now=HOUR + 50, calls=0, broken=None)
    def fetch(*args, **kwargs):
        clock.calls += 1
        data = frame(clock.now)["market_data"]
        if clock.broken:
            for value in data.values():
                index = -1 if clock.broken == "tail" else 200
                for key in ("ts", "open", "high", "low", "close", "volume"):
                    value[key].pop(index)
        return {s: MarketSeries(**v) for s, v in data.items()}
    def public(path, params):
        value = frame(clock.now)
        if path.endswith("instruments"):
            return value["instrument_raw"]["data"]
        return [{"instId": s.replace("/", "-"), "bidPx": str(r["quote"]["bid"]),
                 "askPx": str(r["quote"]["ask"]), "ts": str(clock.now * 1000)}
                for s, r in value["symbols"].items()]
    monkeypatch.setattr(forward.time, "time", lambda: clock.now)
    monkeypatch.setattr(forward, "OKXCCXTProvider", lambda: SimpleNamespace(fetch_ohlcv=fetch))
    monkeypatch.setattr(forward, "public_get", public)
    monkeypatch.setattr(forward, "capture_artifacts", lambda *a: {})
    clock.collect = lambda: forward.collect(symbols=POLICY["symbols"], root=tmp_path,
                                             reference_path=tmp_path / "missing.sqlite")
    return clock


@pytest.mark.parametrize("defect", ["tail", "gap"])
def test_bad_hour_is_evidence_not_valid_cache_and_recovers_same_hour(source, tmp_path, defect):
    source.broken = defect
    first = source.collect()
    assert first["signal_data"]["status"] == "unavailable"
    assert first["market_data"] == {} and first["symbols"]
    assert not (tmp_path / "hour-input.json").exists()
    originals = {p.name: p.read_bytes() for p in (tmp_path / "raw-hour-inputs").iterdir()}
    assert originals
    source.now += 5
    source.collect()
    assert source.calls == 1  # bounded refresh, no busy retry
    source.broken = None
    source.now = HOUR + 410
    second = source.collect()
    assert second["signal_data"]["status"] == "valid" and source.calls == 2
    assert json.loads((tmp_path / "hour-input.json").read_text())["bar_ts"] == HOUR
    source.now += 60
    source.collect()
    assert source.calls == 2
    assert all((tmp_path / "raw-hour-inputs" / name).read_bytes() == raw for name, raw in originals.items())
    source.now = HOUR + 3650
    source.collect()
    assert source.calls == 3


@pytest.mark.parametrize("defect", ["json", "tail", "gap"])
def test_existing_bad_cache_is_revalidated_and_archived(source, tmp_path, defect):
    if defect == "json":
        raw = b"{broken-json"
    else:
        values = frame()["market_data"]
        for value in values.values():
            for key in ("ts", "open", "high", "low", "close", "volume"):
                value[key].pop(-1 if defect == "tail" else 100)
        raw = json.dumps({"bar_ts": HOUR, "market_data": values,
                          "instrument_raw": frame()["instrument_raw"]}).encode()
    (tmp_path / "hour-input.json").write_bytes(raw)
    result = source.collect()
    assert result["signal_data"]["status"] == "valid" and source.calls == 1
    assert raw in [p.read_bytes() for p in (tmp_path / "raw-hour-inputs").iterdir()]


def unavailable(now):
    value = market(now)
    value.update(market_data={}, signal_data={"status": "unavailable", "reason": "non_contiguous_closed_candles"})
    return value


@pytest.mark.parametrize("recover,decided", [(410, True), (600, False)])
def test_recovery_obeys_common_deadline(monkeypatch, tmp_path, recover, decided):
    runner = comparison(monkeypatch, tmp_path)
    assert not runner.observe(unavailable(HOUR + 370))["hourly_decision"]
    result = runner.observe(market(HOUR + recover))
    assert result["hourly_decision"] is decided
    for cost, scenario in result["scenarios"].items():
        c, d = (scenario["cohorts"][n] for n in ("C_hold24_only", "D_reference_only"))
        assert c["snapshot_hash"] == d["snapshot_hash"]


def test_quote_only_observation_marks_existing_holdings_and_runs_existing_hard_exit(monkeypatch, tmp_path):
    runner = comparison(monkeypatch, tmp_path)
    runner.observe(market(HOUR + 410))
    runner.observe(market(HOUR + 470))
    held = runner.scenarios["30"]["C_hold24_only"].book.positions
    assert held
    bad = unavailable(HOUR + 530)
    for r in bad["symbols"].values():
        r["quote"].update(bid=98, ask=98.01)
    result = runner.observe(bad)
    c = result["scenarios"]["30"]["cohorts"]["C_hold24_only"]
    assert c["portfolio"]["equity_usdt"] != "100"
    assert c["decision"]["reason"] == "hard_stop"
    next_quote = copy.deepcopy(bad)
    next_quote["observed_at"] += 60
    for r in next_quote["symbols"].values():
        r["quote"]["ts"] += 60
    runner.observe(next_quote)
    book = runner.scenarios["30"]["C_hold24_only"].book
    assert [f["side"] for f in book.fills] == ["buy", "sell"]
    assert runner.last_decision_bar == HOUR


def test_integrity_counts_missing_slots_held_gaps_and_uncovered_decisions(monkeypatch, tmp_path):
    runner = comparison(monkeypatch, tmp_path)
    first = runner.observe(market(HOUR + 410))
    runner.observe(market(HOUR + 470))
    last = runner.observe(market(HOUR + 2 * 3600 + 530))
    report = summarize([first, last], runner.checkpoint(), runner.experiment)
    quality = report["observation_integrity"]
    assert quality["planned_observations"] > quality["valid_quote_observations"] == 3
    assert quality["maximum_interval_seconds"] == 7260
    assert quality["missing_duration_seconds"] > 7000
    assert quality["holding_missing_seconds"]["30:C_hold24_only"] > 7000
    assert quality["uncovered_common_decision_count"] == 2
    assert quality["judgment"] == "INSUFFICIENT"
    assert report["scenarios"]["30"]["C_hold24_only"]["drawdown_scope"] == "observed_quotes_only_unknown_between_observations"


def test_report_with_missing_observation_evidence_cannot_be_ready():
    report = sufficient_report()
    report.pop("observation_integrity", None)
    report.update(observation_gaps=1000, observations=2000)
    result = evaluate_acceptance(report, EXPERIMENT)
    assert result["status"] != "READY_FOR_MANUAL_RESEARCH_REVIEW"
    assert all(any(r["criterion"] == "observation_integrity" and r["result"] == "INSUFFICIENT"
                   for r in group["requirements"]) for group in result["comparisons"].values())


def test_cross_hour_during_fetch_does_not_stamp_old_signals_as_current(source, monkeypatch):
    source.now = HOUR + 3599
    original = forward.public_get
    def public(path, params):
        if path.endswith("tickers"):
            source.now = HOUR + 3602
        return original(path, params)
    monkeypatch.setattr(forward, "public_get", public)
    result = source.collect()
    assert result["bar_ts"] == HOUR + 3600
    assert result["market_data"] == {}
    assert result["signal_data"]["reason"] == "hour_changed_during_acquisition"


def test_restored_metrics_do_not_hide_gap_or_duplicate_same_minute(monkeypatch, tmp_path):
    runner = comparison(monkeypatch, tmp_path)
    first = runner.observe(market(HOUR + 410))
    runner.observe(market(HOUR + 420))
    saved = runner.checkpoint()
    restored = type(runner)(runner.cfg, runner.policy, runner.experiment, root=tmp_path, checkpoint=saved)
    last = restored.observe(market(HOUR + 800))
    quality = summarize([first, last], restored.checkpoint(), restored.experiment)["observation_integrity"]
    assert quality["valid_quote_observations"] == 3 and quality["observed_quote_slots"] == 2
    assert quality["maximum_interval_seconds"] == 380 and quality["missing_duration_seconds"] == 320


def test_quote_only_time_exit_uses_unchanged_holding_limit(monkeypatch, tmp_path):
    runner = comparison(monkeypatch, tmp_path)
    runner.observe(market(HOUR + 410))
    runner.observe(market(HOUR + 470))
    # No intervening quotes are fabricated. The observed late quote can create
    # the declared time exit; its execution still needs a subsequent quote.
    at = HOUR + 470 + 24 * 3600
    event = runner.observe(unavailable(at))
    assert event["scenarios"]["30"]["cohorts"]["C_hold24_only"]["decision"]["reason"] == "time_stop"
    assert event["scenarios"]["30"]["cohorts"]["B_participation_v1"]["decision"]["reason"] == "position_open"
    assert not event["hourly_decision"]


def test_retained_event_quality_is_retrospective_and_never_replays_accounts(monkeypatch, tmp_path):
    from src.research.review_integrity import retained_observation_report
    runner = comparison(monkeypatch, tmp_path)
    events = [runner.observe(market(HOUR + 410)), runner.observe(market(HOUR + 470)),
              runner.observe(market(HOUR + 650))]
    checkpoint = copy.deepcopy(runner.checkpoint())
    originals = copy.deepcopy(events)
    quality = retained_observation_report(iter(events), runner.experiment)
    assert quality['valid_quote_observations'] == 3
    assert quality['holding_missing_seconds']['30:C_hold24_only'] == 120
    assert quality['judgment'] == 'INSUFFICIENT' and quality['prospective_days'] == 0
    assert runner.checkpoint() == checkpoint and events == originals
