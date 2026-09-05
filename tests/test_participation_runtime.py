import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.core.models import MarketSeries
from src.reporting.participation_runtime import (
    build_snapshot, operational_block, process_observation, update_participation_runtime,
)
from src.reporting.participation_store import ParticipationStore

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def config():
    return json.loads((ROOT / "configs/research/participation_policy_v1.json").read_text())


def observed(config, count=60):
    start = 1788307200
    end = start + count * 3600
    now = end + 5
    prices = [100 * 1.003 ** i for i in range(count)]
    market, quotes, factors = {}, {}, {}
    weights = {key: 0.0 for key in ("f1_mom_5d", "f2_mom_20d", "f3_vol_adj_ret", "f4_volume_expansion", "f5_rsi_trend_confirm", "f6_sentiment")}
    weights["f3_vol_adj_ret"] = 1.0
    for symbol in config["symbols"]:
        market[symbol] = MarketSeries(symbol=symbol, timeframe="1h", ts=[(start + i * 3600) * 1000 for i in range(count)],
                                     open=list(prices), high=[v * 1.001 for v in prices], low=[v * .999 for v in prices],
                                     close=list(prices), volume=[1000] * count)
        quotes[symbol] = {"bid": prices[-1], "ask": prices[-1] * 1.00001, "timestamp": (now - 1) * 1000}
        factors[symbol] = {"z_factors": dict(weights), "raw_factors": {"alpha6_final_score": 1.0}}
    audit = SimpleNamespace(window_end_ts=end, run_id=f"run-{count}", regime="Trending",
                            alpha_factor_snapshot=factors, effective_alpha6_weights=weights, router_decisions=[])
    return market, quotes, audit, now


def snapshot(config, count=60):
    market, quotes, audit, now = observed(config, count)
    return build_snapshot(market_data=market, top_of_book=quotes, audit=audit, config=config, now=now)


def test_forward_persists_pending_then_only_fills_on_subsequent_quote(tmp_path, config):
    store = ParticipationStore(tmp_path / "forward.sqlite")
    first = process_observation(snapshot=snapshot(config), config=config, store=store, identity="one", source_run_id="first")
    assert first["decision"]["action"] == "entry_intent"
    assert first["portfolio"]["position"] is None
    next_cycle = process_observation(snapshot=snapshot(config, 61), config=config, store=store, identity="one", source_run_id="next")
    assert next_cycle["execution"]["side"] == "buy"
    assert next_cycle["portfolio"]["entry_count"] == 1
    assert next_cycle["portfolio"]["cash_usdt"] < config["initial_cash_usdt"]
    assert next_cycle["live_order_effect"] == "none"
    duplicate = process_observation(snapshot=snapshot(config, 61), config=config, store=store, identity="one", source_run_id="duplicate")
    assert duplicate["status"] == "duplicate_or_older_bar"
    assert duplicate["portfolio"] == next_cycle["portfolio"]
    assert duplicate["observed_ts"] == next_cycle["observed_ts"]
    older = process_observation(snapshot=snapshot(config), config=config, store=store, identity="one", source_run_id="older")
    assert older["bar_ts"] == next_cycle["bar_ts"]
    assert older["portfolio"] == next_cycle["portfolio"]
    with store.transaction() as connection:
        assert store.load(connection, "one")["entry_count"] == 1
        assert connection.execute("SELECT count(*) FROM decisions").fetchone()[0] == 2


def test_missing_forward_hour_cancels_pending_entry_instead_of_backfill(tmp_path, config):
    store = ParticipationStore(tmp_path / "forward.sqlite")
    process_observation(snapshot=snapshot(config), config=config, store=store, identity="one", source_run_id="first")
    later = process_observation(snapshot=snapshot(config, 62), config=config, store=store, identity="one", source_run_id="later")
    assert later["observation_gap"]
    assert later["execution"] == {"action": "cancel", "reason": "missing_forward_observation"}
    assert later["portfolio"]["position"] is None


def test_future_quotes_or_factor_mismatch_never_create_eligible_candidate(config):
    market, quotes, audit, now = observed(config)
    for quote in quotes.values():
        quote["timestamp"] = (now + 1) * 1000
    snap = build_snapshot(market_data=market, top_of_book=quotes, audit=audit, config=config, now=now)
    from src.strategy.participation_policy import decide, new_state
    assert set(decide(snap, new_state(config), config)["candidate_reasons"].values()) == {"future_quote"}
    for symbol in config["symbols"]:
        audit.alpha_factor_snapshot[symbol]["raw_factors"]["alpha6_final_score"] = 2.0
    snap = build_snapshot(market_data=market, top_of_book=quotes, audit=audit, config=config, now=now)
    assert len(snap["data_errors"]) == 4
    assert decide(snap, new_state(config), config)["action"] == "hold"


def test_unclosed_candle_is_not_used(config):
    market, quotes, audit, now = observed(config)
    baseline = build_snapshot(market_data=market, top_of_book=quotes, audit=audit, config=config, now=now)
    changed = copy.deepcopy(market)
    for series in changed.values():
        series.ts.append(audit.window_end_ts * 1000)
        for key in ("open", "high", "low", "close"):
            getattr(series, key).append(9999999)
        series.volume.append(99999999)
    actual = build_snapshot(market_data=changed, top_of_book=quotes, audit=audit, config=config, now=now)
    assert actual == baseline


def test_stale_run_cannot_seed_a_prospective_cohort(tmp_path, config):
    market, quotes, audit, now = observed(config)
    cfg = SimpleNamespace(participation=SimpleNamespace(enabled=True, mode="forward_paper", max_signal_age_seconds=900))
    with pytest.raises(ValueError, match="prospective"):
        update_participation_runtime(cfg=cfg, market_data_1h=market, top_of_book=quotes, audit=audit,
                                     run_dir=tmp_path, reports_dir=tmp_path, now=now + 3600)
    assert list(tmp_path.iterdir()) == []


def test_failed_or_unknown_operational_state_blocks_entry(tmp_path):
    assert operational_block(tmp_path, 10000) == "kill_switch.json:unobservable"
    (tmp_path / "kill_switch.json").write_text('{"enabled":false}')
    for name in ("reconcile_status.json", "ledger_status.json"):
        (tmp_path / name).write_text('{"ok":true,"ts_ms":10000}')
    assert operational_block(tmp_path, 10000) is None
    assert "stale" in operational_block(tmp_path, 12000)


def test_state_path_cannot_write_live_database(tmp_path, config):
    market, quotes, audit, now = observed(config)
    live_db = tmp_path / "orders.sqlite"
    cfg = SimpleNamespace(participation=SimpleNamespace(
        enabled=True, mode="forward_paper", max_signal_age_seconds=900,
        policy_path=str(ROOT / "configs/research/participation_policy_v1.json"), state_path=str(live_db)))
    with pytest.raises(ValueError, match="isolated"):
        update_participation_runtime(cfg=cfg, market_data_1h=market, top_of_book=quotes, audit=audit,
                                     run_dir=tmp_path, reports_dir=tmp_path, now=now)
    assert not live_db.exists()


def test_cost_reserve_and_negative_cooldown_are_preserved(config):
    market, quotes, audit, now = observed(config)
    snap = build_snapshot(market_data=market, top_of_book=quotes, audit=audit, config=config, now=now,
                          costs={"BTC/USDT": {"selected_entry_gate_cost_bps": 100}},
                          negative_state={"symbols": {"ETH/USDT": {"cooldown_until_ms": (now + 100) * 1000}}})
    assert snap["symbols"]["BTC/USDT"]["cost_bps"] == 100
    assert snap["symbols"]["BTC/USDT"]["cost_trusted_for_live"] is False
    assert snap["symbols"]["ETH/USDT"]["operational_block"] == "negative_expectancy_cooldown"
