import copy
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from configs.schema import ParticipationRuntimeConfig
from src.reporting.participation_quotes import PublicQuoteFeed
from src.reporting.participation_runtime import (
    process_observation, process_quote_observation, publish_latest,
)
from src.reporting.participation_store import ParticipationStore

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def trial(tmp_path):
    config = json.loads((ROOT / "configs/research/participation_policy_v1.json").read_text())
    now = 1788591649.0  # Synthetic regression quotes around the recorded 15h BNB signal.
    for name, obj in [("kill_switch.json", {"enabled": False}),
                      ("reconcile_status.json", {"ok": True, "ts_ms": now * 1000}),
                      ("ledger_status.json", {"ok": True, "ts_ms": now * 1000})]:
        (tmp_path / name).write_text(json.dumps(obj))
    snapshot = {"now_ts": now, "bar_ts": 1788591600, "regime": "Trending", "operational_block": None,
                "data_errors": {}, "symbols": {}}
    for symbol in config["symbols"]:
        snapshot["symbols"][symbol] = {"bar_ts": snapshot["bar_ts"], "close": 727.3, "ema20": 720.596,
                                       "ema20_4h_ago": 718.952, "ret4_bps": 88.778, "rank_score": 2.,
                                       "cost_bps": 30., "volume": 489.,
                                       "quote": {"bid": 726.6, "ask": 726.7, "ts": now - 1}}
    store = ParticipationStore(tmp_path / "forward.sqlite")
    first = process_observation(snapshot=snapshot, config=config, store=store, identity="cohort", source_run_id="20260905_15")
    return config, store, snapshot, first, tmp_path


def tick(trial, *, seconds=2, price=727., quotes=None):
    config, store, snapshot, _, reports = trial
    now = snapshot["now_ts"] + seconds
    quotes = quotes if quotes is not None else {symbol: {"bid": price, "ask": price + .1, "ts": now - .1} for symbol in config["symbols"]}
    return process_quote_observation(quotes=quotes, config=config, store=store, identity="cohort", reports_dir=reports, now=now)


def test_same_hour_subsequent_quote_fills_without_waiting_one_hour(trial):
    assert trial[3]["portfolio"]["position"] is None
    event = tick(trial)
    assert event["execution"]["action"] == "fill"
    assert event["execution"]["side"] == "buy"
    assert event["execution"]["latency_seconds"] == 2
    assert event["bar_ts"] == trial[3]["bar_ts"]
    assert event["portfolio"]["entry_count"] == 1
    assert event["live_order_effect"] == "none"


def test_repeated_ticks_restart_and_duplicate_hour_never_refill_or_reset_portfolio(trial):
    first = tick(trial)
    assert tick(trial)["status"] == "duplicate_or_older_quote"
    assert tick(trial, seconds=4)["status"] == "checked"
    config, store, snapshot, _, _ = trial
    duplicate = process_observation(snapshot=snapshot, config=config, store=store, identity="cohort", source_run_id="duplicate")
    assert duplicate["status"] == "duplicate_or_older_bar"
    restarted = ParticipationStore(store.path)
    with restarted.transaction() as con:
        assert restarted.load(con, "cohort")["position"] == first["portfolio"]["position"]
        assert restarted.load(con, "cohort")["entry_count"] == 1
        assert con.execute("SELECT count(*) FROM decisions WHERE event_kind='hourly'").fetchone()[0] == 1


def test_concurrent_workers_commit_only_one_fill(trial):
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: tick(trial), range(2)))
    assert sum((r.get("execution") or {}).get("action") == "fill" for r in results) == 1
    with trial[1].transaction() as con:
        assert trial[1].load(con, "cohort")["entry_count"] == 1


@pytest.mark.parametrize("kind", ["missing", "future", "same_as_signal", "stale"])
def test_invalid_or_non_subsequent_quotes_never_fill(trial, kind):
    now = trial[2]["now_ts"]
    ts = {"future": now + 100, "same_as_signal": now, "stale": now - 100}.get(kind, now)
    quotes = {} if kind == "missing" else {s: {"bid": 727., "ask": 727.1, "ts": ts} for s in trial[0]["symbols"]}
    event = tick(trial, quotes=quotes)
    assert event["execution"]["action"] == "wait"
    with trial[1].transaction() as con:
        assert trial[1].load(con, "cohort")["entry_count"] == 0


def test_premium_gate_preserved_and_cancel_does_not_create_another_same_hour_entry(trial):
    event = tick(trial, price=738.3)
    assert event["execution"]["reason"] == "entry_price_premium"
    assert event["portfolio"]["pending"] is None
    assert tick(trial, seconds=4)["status"] == "idle_no_intent"


def test_current_operational_state_is_rechecked_before_fill(trial):
    (trial[4] / "kill_switch.json").write_text('{"enabled":true}')
    event = tick(trial)
    assert event["execution"]["reason"] == "execution_operational_block"
    assert event["portfolio"]["entry_count"] == 0


def test_new_negative_expectancy_cooldown_blocks_pending_fill(trial):
    (trial[4] / "negative_expectancy_cooldown.json").write_text(json.dumps({"symbols": {
        "BNB/USDT": {"cooldown_until_ms": (trial[2]["now_ts"] + 100) * 1000}}}))
    assert tick(trial)["execution"]["reason"] == "execution_operational_block"


def test_intrahour_hard_stop_requires_a_later_quote_then_closes_once(trial):
    opened = tick(trial)
    assert opened["portfolio"]["position"]["symbol"] == "BNB/USDT"
    stop = tick(trial, seconds=4, price=710.)
    assert stop["decision"]["reason"] == "hard_stop"
    assert stop["portfolio"]["position"] is not None
    closed = tick(trial, seconds=6, price=709.)
    assert closed["execution"]["side"] == "sell"
    assert closed["portfolio"]["closed_trade_count"] == 1
    assert closed["portfolio"]["position"] is None
    assert closed["closed_trade"]["net_pnl_usdt"] < 0
    assert tick(trial, seconds=8)["status"] == "idle_no_intent"


def test_disconnection_marks_valuation_unknown_without_inventing_exit(trial):
    tick(trial)
    event = tick(trial, seconds=40, quotes={})
    assert event["portfolio"]["valuation_valid"] is False
    assert event["portfolio"]["position"] is not None
    assert event["execution"] is None


def test_intraminute_equity_peak_survives_retracement_and_restart(trial):
    tick(trial)
    peak = tick(trial, seconds=4, price=750.)
    assert peak["status"] == "observed"
    tick(trial, seconds=6, price=745.)
    restarted = ParticipationStore(trial[1].path)
    with restarted.transaction() as con:
        assert restarted.load(con, "cohort")["peak_equity_usdt"] == peak["portfolio"]["equity_usdt"]


def test_slow_hourly_publication_reads_latest_committed_quote_fill(trial):
    event = tick(trial)
    path = trial[4] / "latest.json"
    publish_latest(store=trial[1], identity="cohort", path=path, source_hashes={})
    assert json.loads(path.read_text())["portfolio"] == event["portfolio"]


def test_hourly_snapshot_timestamp_is_captured_after_quote_writer_lock(trial):
    tick(trial)
    config, store, snap, _, _ = trial
    later = copy.deepcopy(snap)
    later["bar_ts"] += 3600
    later["now_ts"] += 3600
    for row in later["symbols"].values():
        row["bar_ts"] += 3600
        row["quote"]["ts"] += 3600
    result = process_observation(snapshot=later, config=config, store=store, identity="cohort", source_run_id="next",
                                 observation_clock=lambda: later["now_ts"] + 1)
    assert result["observed_ts"] == later["now_ts"] + 1


def test_legacy_unique_hour_ledger_requires_new_cohort_without_migration(tmp_path):
    path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(path) as con:
        con.execute("CREATE TABLE decisions(sequence INTEGER PRIMARY KEY, observed_ts REAL, bar_ts INTEGER UNIQUE, event TEXT)")
        con.execute("INSERT INTO decisions VALUES(1,3601,3600,'{}')")
    with pytest.raises(ValueError, match="new explicit cohort"):
        with ParticipationStore(path).transaction():
            pass
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT count(*) FROM decisions").fetchone()[0] == 1


def test_public_feed_rejects_out_of_order_future_and_nonfinite_quotes():
    feed = PublicQuoteFeed()
    row = {"instId": "BNB-USDT", "bidPx": "727", "askPx": "727.1", "ts": "100000"}
    assert feed.accept(row, received_at=101, source="test")
    assert not feed.accept(row, received_at=102, source="test")
    for change in ({"ts": "99000"}, {"ts": "105000"}, {"bidPx": "nan"}, {"askPx": "0"}, {"instId": "DOGE-USDT"}):
        assert not feed.accept({**row, **change}, received_at=102, source="test")
    assert feed.received_count == 1


def test_execution_settings_fail_fast():
    with pytest.raises(ValueError):
        ParticipationRuntimeConfig(quote_execution_interval_seconds=0)
    with pytest.raises(ValueError):
        ParticipationRuntimeConfig(quote_execution_intervl_seconds=2)
