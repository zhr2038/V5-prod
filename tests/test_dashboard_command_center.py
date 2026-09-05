"""Evidence boundaries for the read-only command center, including real fill units."""
import csv
import hashlib
import importlib.util
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.reporting.dashboard_command_center import build_command_center

NOW = datetime(2026, 9, 5, 2, 5, tzinfo=timezone.utc).timestamp()
REPO = Path(__file__).resolve().parents[1]


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture
def runtime(tmp_path):
    reports = tmp_path / "reports"
    paths = SimpleNamespace(
        reports_dir=reports, runs_dir=reports / "runs", orders_db=reports / "orders.sqlite",
        fills_db=reports / "fills.sqlite", kill_switch_path=reports / "kill_switch.json",
        reconcile_status_path=reports / "reconcile_status.json",
        auto_risk_guard_path=reports / "auto_risk_guard.json", auto_risk_eval_path=reports / "auto_risk_eval.json",
    )
    config = {"participation": {"enabled": False}}
    return SimpleNamespace(workspace=tmp_path, paths=paths, config=config)


def build(runtime, now=NOW):
    return build_command_center(config=runtime.config, paths=runtime.paths, workspace=runtime.workspace, now=now)


def run(runtime, stamp=NOW - 60, **changes):
    folder = datetime.fromtimestamp(stamp, timezone.utc).strftime("%Y%m%d_%H")
    audit = {
        "run_id": folder, "now_ts": stamp, "window_start_ts": stamp - 3600,
        "window_end_ts": stamp - 30, "regime": "Sideways",
        "counts": {"selected": 1, "orders_rebalance": 0},
        "alpha_factor_snapshot": {"BTC/USDT": {"alpha6_score": 0.3, "f5_rsi_trend_confirm": -0.2}},
        "router_decisions": [{"symbol": "BTC/USDT", "action": "skip", "reason": "rsi_unconfirmed"}],
        "quant_lab": {"mode": "shadow", "raw_permission_decision": "ABORT", "final_permission": "ALLOW",
                      "permission_gate_enforced": False},
    }
    audit.update(changes)
    path = runtime.paths.runs_dir / folder / "decision_audit.json"
    write_json(path, audit)
    return folder, path


def exchange(runtime, fills=(), orders=()):
    runtime.paths.reports_dir.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(runtime.paths.fills_db) as con:
        con.execute("CREATE TABLE fills(inst_id,trade_id,ord_id,cl_ord_id,side,fill_sz,ts_ms)")
        con.executemany("INSERT INTO fills VALUES(?,?,?,?,?,?,?)", fills)
    with sqlite3.connect(runtime.paths.orders_db) as con:
        con.execute("CREATE TABLE orders(cl_ord_id,ord_id,inst_id,run_id,side)")
        con.executemany("INSERT INTO orders VALUES(?,?,?,?,?)", orders)


def enable_participation(runtime):
    settings = {"enabled": True, "mode": "forward_paper", "policy_path": "configs/policy.json",
                "state_path": "reports/participation/forward.sqlite"}
    runtime.config["participation"] = settings
    policy = {"mode": "research_shadow", "live_promotion_allowed": False, "fee_bps": 10}
    write_json(runtime.workspace / settings["policy_path"], policy)
    source_hashes = {}
    for name, path in (("policy", "src/strategy/participation_policy.py"),
                       ("runtime", "src/reporting/participation_runtime.py"),
                       ("store", "src/reporting/participation_store.py")):
        target = runtime.workspace / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"# {name} tested executable identity\n", encoding="utf-8")
        source_hashes[name] = hashlib.sha256(target.read_bytes()).hexdigest()
    policy_hash = hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    identity = hashlib.sha256(json.dumps({"policy": policy_hash, "code": source_hashes}, sort_keys=True).encode()).hexdigest()
    return identity, policy_hash


def forward(runtime, stamp=NOW - 60, count=1, **changes):
    identity, policy_hash = enable_participation(runtime)
    state = {"entry_count": 0, "closed_trade_count": 0, "net_realized_pnl_usdt": 0,
             "equity_usdt": 100, "valuation_valid": True, "valuation_status": "flat_cash", "position": None}
    events = []
    for index in range(count):
        observed = stamp - (count - 1 - index) * 3600
        event = {"schema_version": "v5.participation_forward_observation.v1", "status": "observed",
                 "observed_ts": observed, "bar_ts": int(observed // 3600 * 3600), "cohort_started_at": stamp - count * 3600,
                 "source_run_id": "20260905_02", "identity": identity, "policy_hash": policy_hash,
                 "mode": "forward_paper", "live_order_effect": "none", "live_promotion_allowed": False,
                 "historical_backfill": False, "portfolio": state,
                 "decision": {"action": "hold", "reason": "no_qualified_candidate"}, "execution": None}
        event.update(changes)
        events.append(event)
    write_json(runtime.paths.reports_dir / "participation/latest.json", events[-1])
    with sqlite3.connect(runtime.workspace / runtime.config["participation"]["state_path"]) as con:
        con.execute("CREATE TABLE portfolio(id INTEGER PRIMARY KEY, identity TEXT, state TEXT)")
        con.execute("CREATE TABLE decisions(sequence INTEGER PRIMARY KEY, observed_ts REAL, bar_ts INTEGER, event TEXT)")
        con.execute("INSERT INTO portfolio VALUES(1,?,?)", (identity, json.dumps(state)))
        con.executemany("INSERT INTO decisions(observed_ts,bar_ts,event) VALUES(?,?,?)",
                        [(event["observed_ts"], event["bar_ts"], json.dumps(event)) for event in events])
    return events[-1]


def test_missing_sources_are_unknown_and_read_does_not_create_runtime_files(runtime):
    result = build(runtime)
    assert result["status"] == "unavailable"
    assert result["latest_decision"]["decision_ts"] is None
    assert result["window_72h"]["selected_candidates"]["value"] is None
    assert result["window_72h"]["actual_fill_events"]["value"] is None
    assert result["health"]["reconcile"]["ok"] is None
    assert result["participation"]["status"] == "disabled"
    assert not runtime.paths.reports_dir.exists()


def test_window_is_event_time_bounded_and_units_are_distinct(runtime):
    folder, _ = run(runtime, counts={"selected": 2, "orders_rebalance": 1})
    run(runtime, NOW - 72 * 3600 - 1, counts={"selected": 900, "orders_rebalance": 900})
    run(runtime, NOW + 3600, counts={"selected": 999, "orders_rebalance": 999})
    fills = [("BTC-USDT", trade, "oid", "clid", "buy", ".001", int(stamp * 1000))
             for trade, stamp in (("t1", NOW - 50), ("t2", NOW - 49), ("old", NOW - 72 * 3600 - 1), ("future", NOW + 1))]
    exchange(runtime, fills, [("clid", "oid", "BTC-USDT", folder, "buy")])
    result = build(runtime)
    window = result["window_72h"]
    assert window["selected_candidates"]["value"] == 2
    assert window["generated_orders"]["value"] == 1
    assert window["actual_fill_events"]["value"] == 2
    assert window["actual_filled_orders"]["value"] == 1
    assert window["attributed_filled_candidates"]["value"] == 1
    assert len({window[key]["unit"] for key in ("selected_candidates", "generated_orders", "actual_fill_events", "actual_filled_orders")}) == 4
    assert result["latest_decision"]["status"] == "future"
    assert result["blockers"][0]["count"] == 1


def test_missing_attribution_is_null_without_hiding_real_fills(runtime):
    run(runtime)
    exchange(runtime, [("BTC-USDT", "t1", "unknown", None, "buy", ".001", int(NOW * 1000))])
    result = build(runtime)["window_72h"]
    assert result["actual_fill_events"]["value"] == 1
    assert result["attributed_filled_candidates"]["value"] is None
    assert result["attributed_filled_candidates"]["unmatched_buy_fill_events"] == 1


def test_non_candidate_fill_cannot_be_called_candidate_conversion(runtime):
    folder, _ = run(runtime)
    exchange(runtime, [("SOL-USDT", "t1", "oid", "clid", "buy", "1", int(NOW * 1000))],
             [("clid", "oid", "SOL-USDT", folder, "buy")])
    assert build(runtime)["window_72h"]["attributed_filled_candidates"]["value"] is None


def test_empty_readable_fills_database_is_observed_zero(runtime):
    run(runtime)
    exchange(runtime)
    result = build(runtime)["window_72h"]
    assert result["actual_fill_events"] == {"value": 0, "unit": "exchange_fill_events", "status": "observed", "scope": "locally_recorded_exchange_events"}
    assert result["attributed_filled_candidates"]["value"] == 0


def test_missing_count_does_not_become_zero_and_nan_is_valid_json_null(runtime):
    run(runtime, counts={"selected": float("nan")}, targets_post_risk={"BTC/USDT": float("inf")})
    result = build(runtime)
    assert result["window_72h"]["selected_candidates"]["value"] is None
    assert result["window_72h"]["generated_orders"]["value"] is None
    assert result["candidates"][0]["target_weight"] is None
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("stamp,status", [(NOW - 7200, "stale"), (NOW + 1, "future")])
def test_freshness_uses_real_decision_time_instead_of_copied_mtime(runtime, stamp, status):
    _, path = run(runtime, stamp)
    os.utime(path, (NOW, NOW))
    assert build(runtime)["latest_decision"]["status"] == status


def test_run_identity_mismatch_cannot_be_displayed_as_latest_valid_decision(runtime):
    run(runtime, run_id="another_account_run")
    result = build(runtime)
    assert result["latest_decision"]["status"] == "identity_mismatch"
    assert result["candidates"] == []
    assert result["window_72h"]["observed_runs"] == 0


def test_missing_timestamp_never_falls_back_to_folder_or_mtime(runtime):
    run(runtime, now_ts=None)
    result = build(runtime)
    assert result["latest_decision"]["status"] == "unavailable"
    assert result["latest_decision"]["decision_ts"] is None


def test_candidates_use_actual_quote_direction_and_router_reason(runtime):
    folder, audit_path = run(runtime, target_execution_explain={"BTC/USDT": {"alpha6_side": "buy"}})
    with audit_path.with_name("candidate_snapshot.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["run_id", "symbol", "arrival_mid", "quote_ts", "alpha6_side"])
        writer.writeheader()
        writer.writerow({"run_id": folder, "symbol": "BTC/USDT", "arrival_mid": 70000, "quote_ts": NOW - 65, "alpha6_side": "null"})
    result = build(runtime)
    candidate = result["candidates"][0]
    assert candidate["reference_price"] == 70000
    assert candidate["direction"] == "buy"
    assert candidate["target_weight"] is None
    assert result["latest_decision"]["router_reasons"] == ["rsi_unconfirmed"]
    assert result["quant_lab"] == {"mode": "advisory", "permission": "ABORT", "effective_permission": "ALLOW",
                                  "source_mode": "shadow", "permission_gate_enforced": False}
    assert all(row["reason"] != "ABORT" for row in result["blockers"])


def test_health_distinguishes_persistent_switch_from_fresh_check(runtime):
    write_json(runtime.paths.kill_switch_path, {"enabled": False, "ts_ms": int((NOW - 90 * 86400) * 1000)})
    write_json(runtime.paths.reconcile_status_path, {"ok": True, "ts_ms": int((NOW - 901) * 1000)})
    write_json(runtime.paths.reports_dir / "ledger_status.json", {"ok": True, "ts_ms": int((NOW + 1) * 1000)})
    write_json(runtime.paths.auto_risk_eval_path, {"ts": NOW - 1, "current_level": "PROTECT", "metrics": {"dd_pct": .191},
                                                "config": {"max_positions": 1}, "reason": "drawdown_not_recovered"})
    health = build(runtime)["health"]
    assert health["kill_switch"]["status"] == "observed"
    assert health["kill_switch"]["timestamp_semantics"] == "last_state_change"
    assert health["reconcile"]["status"] == "stale"
    assert health["ledger"]["status"] == "future"
    assert health["risk"]["dd_pct"] == .191
    assert health["risk"]["config"]["max_positions"] == 1


def test_custom_ledger_path_is_resolved_without_flattening_subdirectories(runtime):
    runtime.config["execution"] = {"ledger_status_path": "reports/sub/ledger_status.json"}
    write_json(runtime.paths.reports_dir / "sub/ledger_status.json", {"ok": False, "ts_ms": int(NOW * 1000)})
    assert build(runtime)["health"]["ledger"]["status"] == "failed"


def test_complete_decision_window_does_not_hide_missing_health_sources(runtime):
    for hour in range(72):
        run(runtime, NOW - 60 - hour * 3600)
    exchange(runtime)
    result = build(runtime)
    assert result["window_72h"]["coverage_status"] == "complete"
    assert result["window_72h"]["selected_candidates"]["value"] == 72
    assert result["status"] == "partial"


def test_future_candidate_quote_is_not_a_current_reference_price(runtime):
    folder, audit_path = run(runtime)
    with audit_path.with_name("candidate_snapshot.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["run_id", "symbol", "arrival_mid", "quote_ts"])
        writer.writeheader()
        writer.writerow({"run_id": folder, "symbol": "BTC/USDT", "arrival_mid": 70000, "quote_ts": NOW + 1})
    candidate = build(runtime)["candidates"][0]
    assert candidate["reference_price"] is None
    assert candidate["price_as_of"] is None


def test_forward_enabled_missing_is_distinct_from_disabled_and_research(runtime):
    enable_participation(runtime)
    write_json(runtime.paths.reports_dir / "research/latest.json", {"status": "observed", "net_pnl": 99})
    result = build(runtime)["participation"]
    assert result["enabled"] is True
    assert result["status"] == "missing"
    assert result["entry_count"] is None
    assert result["net_realized_pnl_usdt"] is None
    assert result["curve"] == []


def test_valid_forward_curve_is_bounded_and_is_read_only(runtime):
    forward(runtime, count=175)
    before = {str(path): (path.stat().st_mtime_ns, path.read_bytes()) for path in runtime.workspace.rglob("*") if path.is_file()}
    result = build(runtime)["participation"]
    after = {str(path): (path.stat().st_mtime_ns, path.read_bytes()) for path in runtime.workspace.rglob("*") if path.is_file()}
    assert before == after
    assert result["status"] == "observed"
    assert result["equity_usdt"] == 100
    assert result["entry_count"] == 0
    assert len(result["curve"]) == 168
    assert len(result["events"]) == 12
    assert result["live_promotion_allowed"] is False
    assert result["live_order_effect"] == "none"


@pytest.mark.parametrize("age,status", [(6000, "stale"), (-1, "future")])
def test_forward_freshness_cannot_be_revived_by_reading(runtime, age, status):
    forward(runtime, NOW - age)
    result = build(runtime)["participation"]
    assert result["status"] == status
    if status == "future":
        assert result["equity_usdt"] is None
        assert result["latest_decision"] is None


@pytest.mark.parametrize("change", ["policy", "code", "database"])
def test_forward_identity_change_never_combines_cohorts(runtime, change):
    forward(runtime)
    if change == "policy":
        write_json(runtime.workspace / "configs/policy.json", {"mode": "research_shadow", "live_promotion_allowed": False, "fee_bps": 5})
    elif change == "code":
        (runtime.workspace / "src/strategy/participation_policy.py").write_text("# new strategy\n", encoding="utf-8")
    else:
        with sqlite3.connect(runtime.paths.reports_dir / "participation/forward.sqlite") as con:
            con.execute("UPDATE portfolio SET identity='another-cohort'")
    result = build(runtime)["participation"]
    assert result["status"] == "identity_mismatch"
    assert result["equity_usdt"] is None
    assert result["entry_count"] is None
    assert result["curve"] == []


def test_disabled_forward_does_not_show_previous_cohort_profit(runtime):
    forward(runtime)
    runtime.config["participation"]["enabled"] = False
    result = build(runtime)["participation"]
    assert result["status"] == "disabled"
    assert result["equity_usdt"] is None
    assert result["curve"] == []


def test_forward_research_or_backfill_provenance_is_rejected(runtime):
    forward(runtime, historical_backfill=True)
    result = build(runtime)["participation"]
    assert result["status"] == "unavailable"
    assert result["entry_count"] is None


def test_missing_valuation_quote_yields_null_equity_not_previous_price(runtime):
    forward(runtime, portfolio={"entry_count": 1, "equity_usdt": 101, "valuation_valid": False,
                                "valuation_status": "stale_quote", "position": {"symbol": "BTC-USDT"}})
    result = build(runtime)["participation"]
    assert result["status"] == "observed"
    assert result["equity_usdt"] is None
    assert result["curve"][0]["equity_usdt"] is None
    assert result["entry_count"] == 1


def test_forward_state_path_outside_runtime_root_is_rejected(runtime):
    forward(runtime)
    runtime.config["participation"]["state_path"] = "reports/../unrelated.sqlite"
    result = build(runtime)["participation"]
    assert result["status"] == "unavailable"
    assert result["equity_usdt"] is None
    assert not (runtime.workspace / "unrelated.sqlite").exists()


def test_future_curve_record_is_excluded_from_current_observation(runtime):
    event = forward(runtime)
    future = {**event, "observed_ts": NOW + 3600, "bar_ts": int(NOW + 3500), "portfolio": {"equity_usdt": 999}}
    with sqlite3.connect(runtime.paths.reports_dir / "participation/forward.sqlite") as con:
        con.execute("INSERT INTO decisions(observed_ts,bar_ts,event) VALUES(?,?,?)", (future["observed_ts"], future["bar_ts"], json.dumps(future)))
    result = build(runtime)["participation"]
    assert result["status"] == "observed"
    assert len(result["curve"]) == 1
    assert result["equity_usdt"] == 100


def test_latest_report_must_match_its_committed_forward_observation(runtime):
    event = forward(runtime)
    event["portfolio"]["equity_usdt"] = 999
    write_json(runtime.paths.reports_dir / "participation/latest.json", event)
    result = build(runtime)["participation"]
    assert result["status"] == "unavailable"
    assert result["equity_usdt"] is None
    assert result["curve"] == []


def test_http_endpoint_is_local_read_only_and_does_not_call_legacy_routes(runtime, monkeypatch):
    spec = importlib.util.spec_from_file_location("command_center_test_" + uuid.uuid4().hex, REPO / "scripts/web_dashboard.py")
    dashboard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dashboard)
    monkeypatch.setattr(dashboard, "load_config", lambda: runtime.config)
    monkeypatch.setattr(dashboard, "_resolve_dashboard_runtime_paths", lambda config: runtime.paths)
    monkeypatch.setattr(dashboard, "WORKSPACE", runtime.workspace)
    monkeypatch.setattr(dashboard, "_utc_now", lambda: datetime.fromtimestamp(NOW, timezone.utc))

    def forbidden(*args, **kwargs):
        raise AssertionError("command center must not call network/account/trading routes")

    for name in ("api_account", "api_auto_risk_guard", "api_decision_audit", "api_quant_lab_live_permission"):
        monkeypatch.setattr(dashboard, name, forbidden)
    response = dashboard.app.test_client().get("/api/command_center")
    assert response.status_code == 200
    assert response.json["read_only"] is True
    assert response.json["schema_version"] == "v5.command_center.v1"
    assert response.json["window_72h"]["actual_fill_events"]["value"] is None
    assert dashboard.app.test_client().post("/api/command_center").status_code == 405
