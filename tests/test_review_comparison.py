import copy
import json
from pathlib import Path

import pytest

from configs.loader import load_config
from src.research.review_comparison import Comparison, summarize, validate_frame
from src.research.review_forward import process


PROJECT = Path(__file__).resolve().parents[1]
POLICY = json.loads((PROJECT / "configs/research/participation_policy_v1.json").read_text())
EXPERIMENT = json.loads((PROJECT / "configs/research/review_experiment_v1.json").read_text())


def frame(now=1788652810):
    bar = int(now // 3600) * 3600
    rows, market, instruments = {}, {}, []
    for index, symbol in enumerate(POLICY["symbols"]):
        prices = [100 * (1 + (index + 1) * .0002) ** i for i in range(600)]
        market[symbol] = {"symbol": symbol, "timeframe": "1h", "ts": [(bar - (600-i)*3600)*1000 for i in range(600)],
                          "open": prices, "high": [p*1.002 for p in prices], "low": [p*.998 for p in prices],
                          "close": prices, "volume": [100000.] * 600}
        rows[symbol] = {"quote": {"bid": prices[-1], "ask": prices[-1]*1.0001, "ts": now},
                        "instrument": {"symbol": symbol, "lot_size": "0.00000001", "minimum_qty": "0.000001",
                                       "minimum_notional_usdt": 10, "buy_fee_currency": symbol.split("/")[0],
                                       "sell_fee_currency": "USDT", "observed_ts": now, "source_hash": "a"*64}}
        instruments.append({"instId": symbol.replace("/", "-"), "baseCcy": symbol.split("/")[0],
                            "quoteCcy": "USDT", "lotSz": "0.00000001", "minSz": "0.000001"})
    return {"observed_at": now, "bar_ts": bar, "market_data": market, "symbols": rows,
            "historical_backfill": False, "live_order_effect": "none", "instrument_raw": {"ts": now, "data": instruments}}


def config():
    return load_config(str(PROJECT / "configs/live_prod.yaml"), env_path=None)


def test_all_cost_scenarios_share_inputs_and_restore_independent_accounts(tmp_path):
    comparison = Comparison(config(), POLICY, EXPERIMENT, root=tmp_path / "accounts")
    for scenario in comparison.scenarios.values():
        path = scenario["adapter"].root / "reports/okx_spot_instruments.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(frame()["instrument_raw"]))
    first = comparison.observe(frame())
    saved = comparison.checkpoint()
    restored = Comparison(config(), POLICY, EXPERIMENT, root=tmp_path / "accounts", checkpoint=saved)
    second = restored.observe(frame(1788652870))
    expected = comparison.observe(frame(1788652870))
    assert second == expected
    assert second["scenarios"]["30"]["cohorts"]["C_hold24_only"] == second["scenarios"]["30"]["cohorts"]["D_reference_only"]
    report = summarize([first, second], restored.checkpoint(), EXPERIMENT)
    assert report["status"] == "INSUFFICIENT_FORWARD_EVIDENCE"
    assert report["live_execution_eligible"] is False
    assert set(report["scenarios"]) == {"30", "60", "120"}
    assert all(p["real_live_trades"] == 0 for scenario in report["scenarios"].values() for p in scenario.values())
    assert all(p["paired_daily_block_bootstrap_95pct_delta_usdt"] is None for scenario in report["comparisons"].values() for p in scenario.values())


@pytest.mark.parametrize("case", ["future_quote", "unclosed_bar", "missing_lot", "wrong_instrument", "backfill"])
def test_unobservable_inputs_cannot_enter_comparison(case):
    value = frame()
    if case == "future_quote":
        value["symbols"]["BNB/USDT"]["quote"]["ts"] += 100
    elif case == "unclosed_bar":
        value["market_data"]["BNB/USDT"]["ts"][-1] += 3600000
    elif case == "missing_lot":
        value["symbols"]["BNB/USDT"]["instrument"]["lot_size"] = "0"
    elif case == "wrong_instrument":
        value["symbols"]["BNB/USDT"]["instrument"]["symbol"] = "SOL/USDT"
    else:
        value["historical_backfill"] = True
    with pytest.raises(ValueError):
        validate_frame(value)


def test_ledger_rejects_version_reuse_and_interrupted_processing(tmp_path):
    import sqlite3

    root = tmp_path / "experiment"
    root.mkdir()
    with sqlite3.connect(root / "comparison.sqlite") as con:
        con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        con.execute("INSERT INTO meta VALUES('identity','original')")
    kwargs = dict(cfg=config(), policy=POLICY, experiment=EXPERIMENT, project=PROJECT, root=root, frame=frame())
    with pytest.raises(ValueError, match="version_changed"):
        process(**kwargs, identity={"identity": "changed"})
    with sqlite3.connect(root / "comparison.sqlite") as con:
        con.execute("INSERT INTO meta VALUES('processing','unfinished')")
    with pytest.raises(ValueError, match="interrupted_comparison"):
        process(**kwargs, identity={"identity": "original"})


def test_quote_observation_does_not_consume_new_hour_signal():
    from src.research.participation_comparison import ParticipationComparison
    from tests.test_participation_comparison import observed
    from tests.test_participation_policy import CONFIG

    candidate = ParticipationComparison(copy.deepcopy(CONFIG))
    candidate.observe(observed(), allow_new_signal=False)
    assert candidate.observe(observed(360012))["decision"]["action"] == "entry_intent"
