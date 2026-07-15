import csv
import hashlib
import json
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.paper_runtime.contracts import (
    PAPER_STRATEGY_CONTRACT_VERSION,
    PaperRule,
    PaperStrategyProposal,
    paper_proposal_hash,
)
from src.paper_runtime.dsl import PaperRuleInterpreter
from src.paper_runtime.runtime import (
    run_generic_paper_runtime,
    supplement_paper_runtime_market_data,
)
from src.quant_lab_client.exceptions import QuantLabHTTPError
from src.reporting.v5_bundle_exporter import export_v5_bundle


NOW = datetime(2026, 7, 11, 0, 0, tzinfo=UTC)


def _proposal(
    strategy_id: str,
    symbol: str,
    *,
    entry_rule: dict | None = None,
    exit_rule: dict | None = None,
    max_holding_bars: int = 8,
    cooldown_bars: int = 0,
    required_cost_trust_level: str = "PAPER_ONLY",
) -> PaperStrategyProposal:
    payload = {
        "contract_version": PAPER_STRATEGY_CONTRACT_VERSION,
        "proposal_id": f"{strategy_id}:1.0.0",
        "strategy_id": strategy_id,
        "strategy_version": "1.0.0",
        "strategy_family": "generic_test",
        "symbol": symbol,
        "timeframe": "1h",
        "direction": "long",
        "entry_rule": entry_rule
        or {"operator": "momentum_gt", "field": "momentum_8", "value": 0},
        "exit_rule": exit_rule
        or {"operator": "max_holding_bars", "value": max_holding_bars},
        "max_holding_bars": max_holding_bars,
        "min_holding_bars": 1,
        "cooldown_bars": cooldown_bars,
        "signal_confirmation_bars": 1,
        "cost_quantile": "p75",
        "minimum_expected_edge_bps": 0.0,
        "paper_notional_usdt": 20.0,
        "paper_only": True,
        "live_order_effect": "none",
        "max_live_notional_usdt": 0.0,
        "created_at": "2026-07-10T00:00:00+00:00",
        "expires_at": "2026-08-10T00:00:00+00:00",
        "source_pack_sha256": "",
        "source_dataset_versions": {"alpha_discovery_board": "v1"},
        "required_market_fields": ["bid", "ask", "mid", "momentum_8"],
        "required_cost_trust_level": required_cost_trust_level,
        "lifecycle_state": "PAPER_PROPOSAL_READY",
        "lifecycle_reason": "test",
        "blocked_reasons": ["v5_ack_required"],
        "next_required_actions": ["sync_to_v5"],
    }
    payload["proposal_hash"] = paper_proposal_hash(payload)
    return PaperStrategyProposal.model_validate(payload)


def _write_proposals(path: Path, proposals: list[PaperStrategyProposal]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for proposal in proposals:
        row = proposal.model_dump(mode="json")
        for field in (
            "entry_rule",
            "exit_rule",
            "source_dataset_versions",
            "required_market_fields",
            "blocked_reasons",
            "next_required_actions",
        ):
            row[field] = json.dumps(row[field], sort_keys=True)
        row["recommended_mode"] = "paper"
        rows.append(row)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_raw_proposal_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _series(symbol: str, *, bars: int = 32, last_ts: int | None = None) -> MarketSeries:
    start = int((NOW - timedelta(hours=bars)).timestamp() * 1000)
    timestamps = [start + index * 3_600_000 for index in range(bars)]
    if last_ts is not None:
        timestamps[-1] = last_ts
    closes = [100.0 + index for index in range(bars)]
    volumes = [100.0 + index for index in range(bars - 1)] + [1000.0]
    return MarketSeries(
        symbol=symbol,
        timeframe="1h",
        ts=timestamps,
        open=closes,
        high=[value + 1 for value in closes],
        low=[value - 1 for value in closes],
        close=closes,
        volume=volumes,
    )


def _cfg(tmp_path: Path, proposals_path: Path) -> AppConfig:
    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.paper_runtime.enabled = True
    cfg.quant_lab.paper_runtime.state_path = str(tmp_path / "paper_runtime_state.json")
    cfg.quant_lab.canary.enabled = False
    cfg.diagnostics.quant_lab_paper_strategy_proposals_paths = [str(proposals_path)]
    cfg.diagnostics.quant_lab_paper_strategy_proposals_max_age_minutes = 100_000
    return cfg


def _snapshot_payload(
    proposals: list[PaperStrategyProposal],
    *,
    source_commit: str = "a" * 40,
) -> dict:
    rows = [proposal.model_dump(mode="json") for proposal in proposals]
    members = sorted(
        (row["proposal_id"], row["proposal_hash"].lower()) for row in rows
    )
    content_material = {
        "contract_version": PAPER_STRATEGY_CONTRACT_VERSION,
        "proposal_ids": sorted(
            proposal_id for proposal_id, _proposal_hash in members
        ),
        "proposal_hashes": sorted(
            proposal_hash for _proposal_id, proposal_hash in members
        ),
        "proposal_count": len(members),
    }
    content_snapshot_sha = hashlib.sha256(
        json.dumps(
            content_material, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    content_snapshot_id = f"proposal-content-snapshot:{content_snapshot_sha[:24]}"
    material = {
        **content_material,
        "proposal_ids": [proposal_id for proposal_id, _proposal_hash in members],
        "proposal_hashes": [proposal_hash for _proposal_id, proposal_hash in members],
        "source_quant_lab_commit": source_commit,
    }
    snapshot_sha = hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    snapshot_id = f"proposal-snapshot:{snapshot_sha[:24]}"
    generated_at = "2026-07-14T23:30:00+00:00"
    for row in rows:
        row.update(
            {
                "proposal_snapshot_id": snapshot_id,
                "proposal_snapshot_sha256": snapshot_sha,
                "proposal_content_snapshot_id": content_snapshot_id,
                "proposal_content_snapshot_sha256": content_snapshot_sha,
                "snapshot_generated_at": generated_at,
            }
        )
    return {
        "proposal_snapshot_id": snapshot_id,
        "proposal_snapshot_sha256": snapshot_sha,
        "proposal_content_snapshot_id": content_snapshot_id,
        "proposal_content_snapshot_sha256": content_snapshot_sha,
        "snapshot_generated_at": generated_at,
        **material,
        "proposal_contract_version": PAPER_STRATEGY_CONTRACT_VERSION,
        "proposal_compiler_version": "test.compiler.v1",
        "proposals": rows,
    }


def _quote(price: float, now: datetime = NOW) -> dict:
    return {
        "bid": price - 0.1,
        "ask": price + 0.1,
        "mid": price,
        "timestamp": now.isoformat(),
    }


def _provider_quote(price: float, now: datetime = NOW) -> dict:
    return {
        "bid": price - 0.1,
        "ask": price + 0.1,
        "mid": price,
        "quote_ts": now.isoformat().replace("+00:00", "Z"),
        "source": "ccxt_ticker",
    }


def test_content_snapshot_identity_ignores_producer_commit() -> None:
    proposals = [_proposal("SNAPSHOT_STABLE", "TRX/USDT")]
    first = _snapshot_payload(proposals, source_commit="a" * 40)
    second = _snapshot_payload(proposals, source_commit="b" * 40)

    assert first["proposal_content_snapshot_id"] == second[
        "proposal_content_snapshot_id"
    ]
    assert first["proposal_content_snapshot_sha256"] == second[
        "proposal_content_snapshot_sha256"
    ]
    assert first["proposal_snapshot_sha256"] != second["proposal_snapshot_sha256"]


def test_first_three_generic_proposals_ack_and_open_without_live_side_effects(tmp_path):
    reports = tmp_path / "reports"
    run_dir = reports / "runs" / "run-1"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposals = [
        _proposal(
            "TRX_ALT_IMPULSE_48H_PAPER",
            "TRX/USDT",
            entry_rule={
                "operator": "all",
                "children": [
                    {
                        "operator": "regime_in",
                        "field": "market_regime",
                        "values": ["ALT_IMPULSE"],
                    },
                    {"operator": "momentum_gt", "field": "momentum_24", "value": 0},
                ],
            },
            max_holding_bars=48,
        ),
        _proposal("BCH_F3_F4_DEDUP_72H_PAPER", "BCH/USDT", max_holding_bars=72),
        _proposal("TAO_F3_F4_DEDUP_8H_PAPER", "TAO/USDT", max_holding_bars=8),
    ]
    _write_proposals(proposals_path, proposals)
    cfg = _cfg(tmp_path, proposals_path)
    market = {
        symbol: _series(symbol) for symbol in ("TRX/USDT", "BCH/USDT", "TAO/USDT")
    }
    books = {symbol: _quote(100.0 + index) for index, symbol in enumerate(market)}
    audit = SimpleNamespace(
        regime="ALT_IMPULSE", quant_lab={"permission_status": "ACTIVE_ABORT"}
    )

    result = run_generic_paper_runtime(
        run_dir=run_dir,
        market_data_1h=market,
        top_of_book=books,
        cfg=cfg,
        audit=audit,
        now=NOW,
    )

    assert result["accepted"] == 3
    assert result["rejected"] == 0
    assert result["trackers"] == 3
    assert result["live_order_effect"] == "none"
    ack_rows = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack.csv").open())
    )
    assert {row["accepted"] for row in ack_rows} == {"True"}
    assert all(len(row["source_v5_commit"]) == 40 for row in ack_rows)
    assert not any(
        row["reject_reason"] == "no_supported_paper_tracker" for row in ack_rows
    )
    state_rows = list(
        csv.DictReader((reports / "summaries/paper_strategy_state.csv").open())
    )
    assert {row["state"] for row in state_rows} == {"PAPER_OPEN"}
    contract = json.loads(
        (reports / "summaries/quant_lab_contract_status.json").read_text()
    )
    assert contract["real_order_calls"] == 0
    assert contract["real_position_mutations"] == 0
    assert cfg.quant_lab.mode == "shadow"
    assert cfg.quant_lab.canary.enabled is False


def test_canonical_snapshot_api_identity_reaches_ack_tracker_status_and_bundle(
    tmp_path,
    monkeypatch,
):
    from src.quant_lab_client.client import QuantLabClient

    reports = tmp_path / "reports"
    proposals = [
        _proposal("SNAPSHOT_TRX_PAPER", "TRX/USDT"),
        _proposal("SNAPSHOT_BCH_PAPER", "BCH/USDT"),
    ]
    payload = _snapshot_payload(proposals)
    cfg = _cfg(tmp_path, tmp_path / "unused.csv")
    cfg.quant_lab.enabled = True
    cfg.diagnostics.quant_lab_paper_strategy_proposals_api_enabled = True

    class SnapshotClient:
        api_token = "present"

        def __init__(self):
            self.calls = []

        def get_json(self, endpoint):
            self.calls.append(endpoint)
            return SimpleNamespace(ok=True, data=payload)

    client = SnapshotClient()
    monkeypatch.setattr(
        QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: client),
    )

    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "snapshot-api",
        market_data_1h={
            "TRX/USDT": _series("TRX/USDT"),
            "BCH/USDT": _series("BCH/USDT"),
        },
        top_of_book={
            "TRX/USDT": _quote(100.0),
            "BCH/USDT": _quote(200.0),
        },
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    summaries = reports / "summaries"
    ack = list(
        csv.DictReader(
            (summaries / "paper_strategy_proposal_processing.csv").open()
        )
    )
    trackers = list(
        csv.DictReader((summaries / "paper_strategy_trackers_current.csv").open())
    )
    status = json.loads((summaries / "quant_lab_contract_status.json").read_text())
    assert client.calls == ["/v1/paper-strategy/proposals"]
    assert result["proposal_rows"] == 2
    assert {row["processing_status"] for row in ack} == {
        "ACCEPTED_TRACKER_ACTIVE"
    }
    assert {row["source_proposal_snapshot_id"] for row in ack} == {
        payload["proposal_snapshot_id"]
    }
    assert {row["source_proposal_snapshot_sha256"] for row in trackers} == {
        payload["proposal_snapshot_sha256"]
    }
    assert {row["source_proposal_content_snapshot_sha256"] for row in trackers} == {
        payload["proposal_content_snapshot_sha256"]
    }
    assert status["proposal_snapshot_id"] == payload["proposal_snapshot_id"]
    assert status["proposal_snapshot_sha256"] == payload["proposal_snapshot_sha256"]
    assert status["proposal_content_snapshot_sha256"] == payload[
        "proposal_content_snapshot_sha256"
    ]
    assert status["proposal_count"] == 2
    assert status["proposal_processing_complete"] is True
    assert status["unprocessed_proposal_count"] == 0
    assert status["real_order_calls"] == 0
    assert status["real_position_mutations"] == 0

    bundle = export_v5_bundle(
        reports_dir=reports,
        out_dir=tmp_path / "bundles",
        include_logs=False,
        include_config=False,
        refresh_cost_probe_preflight=False,
    )
    with tarfile.open(bundle, "r:gz") as archive:
        manifest = json.loads(archive.extractfile("manifest.json").read())
        names = set(archive.getnames())
    assert manifest["proposal_snapshot_id"] == payload["proposal_snapshot_id"]
    assert manifest["proposal_snapshot_sha256"] == payload[
        "proposal_snapshot_sha256"
    ]
    assert manifest["proposal_content_snapshot_sha256"] == payload[
        "proposal_content_snapshot_sha256"
    ]
    assert manifest["proposal_count"] == 2
    assert manifest["proposal_processing_complete"] is True
    assert "summaries/paper_strategy_proposal_processing.csv" in names


def test_snapshot_capacity_rejection_covers_every_member(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposals = [
        _proposal(f"CAPACITY_MEMBER_{index}", "TRX/USDT") for index in range(5)
    ]
    _write_proposals(proposals_path, proposals)
    cfg = _cfg(tmp_path, proposals_path)
    cfg.quant_lab.paper_runtime.max_trackers = 1

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "capacity",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    rows = list(
        csv.DictReader(
            (
                reports
                / "summaries"
                / "paper_strategy_proposal_processing.csv"
            ).open()
        )
    )
    assert len(rows) == 5
    assert sum(
        row["processing_status"] == "ACCEPTED_TRACKER_ACTIVE" for row in rows
    ) == 1
    assert sum(row["processing_status"] == "REJECTED_CAPACITY" for row in rows) == 4


def test_snapshot_proposal_hash_mismatch_has_closed_processing_status(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    raw = _proposal("HASH_MISMATCH_MEMBER", "TRX/USDT").model_dump(mode="json")
    raw["proposal_hash"] = "0" * 64
    for field in (
        "entry_rule",
        "exit_rule",
        "source_dataset_versions",
        "required_market_fields",
        "blocked_reasons",
        "next_required_actions",
    ):
        raw[field] = json.dumps(raw[field], sort_keys=True)
    _write_raw_proposal_rows(proposals_path, [raw])
    cfg = _cfg(tmp_path, proposals_path)

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "hash-mismatch",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    row = list(
        csv.DictReader(
            (
                reports
                / "summaries"
                / "paper_strategy_proposal_processing.csv"
            ).open()
        )
    )[0]
    assert row["processing_status"] == "REJECTED_HASH_MISMATCH"
    assert row["processing_reason"] == "proposal_hash_mismatch"


@pytest.mark.parametrize("missing_token", [True, False])
def test_snapshot_auth_failure_is_single_endpoint_fail_fast(
    tmp_path,
    monkeypatch,
    missing_token,
):
    from src.quant_lab_client.client import QuantLabClient

    reports = tmp_path / "reports"
    cfg = _cfg(tmp_path, tmp_path / "local-fallback-must-not-be-read.csv")
    cfg.quant_lab.enabled = True
    cfg.diagnostics.quant_lab_paper_strategy_proposals_api_enabled = True

    class AuthFailureClient:
        api_token = None if missing_token else "present"

        def __init__(self):
            self.calls = 0

        def get_json(self, endpoint):
            self.calls += 1
            raise QuantLabHTTPError("quant-lab HTTP 401")

    client = AuthFailureClient()
    monkeypatch.setattr(
        QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: client),
    )
    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "auth-failure",
        market_data_1h={},
        top_of_book={},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    assert client.calls == (0 if missing_token else 1)
    assert result["errors"] == 1
    status = json.loads(
        (reports / "summaries" / "quant_lab_contract_status.json").read_text()
    )
    assert status["proposal_snapshot_id"] == ""
    assert status["real_order_calls"] == 0
    assert status["real_position_mutations"] == 0


def test_runtime_exit_restart_recovery_and_same_bar_idempotency(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal(
        "GENERIC_ONE_BAR_PAPER",
        "TRX/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 0},
        exit_rule={"operator": "max_holding_bars", "value": 1},
        max_holding_bars=1,
    )
    _write_proposals(proposals_path, [proposal])
    cfg = _cfg(tmp_path, proposals_path)
    audit = SimpleNamespace(regime="NORMAL", quant_lab={})
    first_series = _series("TRX/USDT")

    first = run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": first_series},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=audit,
        now=NOW,
    )
    duplicate = run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": first_series},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=audit,
        now=NOW,
    )
    next_ts = first_series.ts[-1] + 3_600_000
    closed = run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-2",
        market_data_1h={"TRX/USDT": _series("TRX/USDT", last_ts=next_ts)},
        top_of_book={"TRX/USDT": _quote(102.0, NOW + timedelta(hours=1))},
        cfg=cfg,
        audit=audit,
        now=NOW + timedelta(hours=1),
    )

    assert first["closed_trades"] == 0
    assert duplicate["signals"] == 0
    assert closed["closed_trades"] == 1
    runs = list(csv.DictReader((reports / "summaries/paper_strategy_runs.csv").open()))
    assert len([row for row in runs if row.get("paper_trade_id")]) == 1
    assert runs[0]["valid_for_promotion"] == "True"
    assert runs[0]["would_enter"] == "True"
    assert runs[0]["would_exit"] == "True"
    assert runs[0]["paper_pnl_bps"] == runs[0]["net_pnl_bps"]
    assert runs[0]["paper_tracker_id"] == f"paper:{proposal.proposal_id}"
    assert runs[0]["exit_reason"] == "max_holding_bars"
    assert runs[0]["exit_timing_state"] == "time_horizon"
    assert float(runs[0]["mfe_bps"]) >= float(runs[0]["net_pnl_bps"])
    assert float(runs[0]["mae_bps"]) <= float(runs[0]["net_pnl_bps"])
    assert float(runs[0]["profit_giveback_bps"]) >= 0.0
    assert runs[0]["exit_timing_bars"] == "1"
    assert float(runs[0]["holding_period_seconds"]) == 3600.0
    assert float(runs[0]["virtual_exit_price"]) < 101.9
    exit_quality = list(
        csv.DictReader(
            (reports / "summaries/paper_strategy_exit_quality.csv").open()
        )
    )[0]
    assert exit_quality["proposal_id"] == proposal.proposal_id
    assert exit_quality["closed_trade_count"] == "1"
    assert exit_quality["diagnosis"] == "observe_more_closed_paper_trades"
    assert json.loads(exit_quality["exit_reason_mix"]) == {"max_holding_bars": 1}
    recovery = list(
        csv.DictReader(
            (reports / "summaries/paper_strategy_restart_recovery.csv").open()
        )
    )
    assert recovery and recovery[0]["open_trade_preserved"] == "True"
    ack_rows = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack.csv").open())
    )
    assert len(ack_rows) == 1
    assert (
        datetime.fromisoformat(ack_rows[0]["accepted_at"].replace("Z", "+00:00")) == NOW
    )


def test_same_proposal_sent_ten_times_creates_one_tracker_and_one_ack(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal("TEN_RETRY_PAPER", "TRX/USDT")
    _write_proposals(proposals_path, [proposal] * 10)
    cfg = _cfg(tmp_path, proposals_path)

    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    ack_rows = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack.csv").open())
    )
    state = json.loads(Path(cfg.quant_lab.paper_runtime.state_path).read_text())
    assert result["accepted"] == 1
    assert result["trackers"] == 1
    assert len(ack_rows) == 1
    assert len(state["trackers"]) == 1


def test_expired_source_does_not_replace_locked_ack_and_open_trade_can_exit(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal(
        "EXPIRING_OPEN_PAPER",
        "TRX/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 0},
        exit_rule={"operator": "max_holding_bars", "value": 1},
        max_holding_bars=1,
    )
    _write_proposals(proposals_path, [proposal])
    cfg = _cfg(tmp_path, proposals_path)
    audit = SimpleNamespace(regime="NORMAL", quant_lab={})
    series = _series("TRX/USDT")

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "open",
        market_data_1h={"TRX/USDT": series},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=audit,
        now=NOW,
    )

    raw = proposal.model_dump(mode="json")
    raw["created_at"] = (NOW - timedelta(days=2)).isoformat()
    raw["expires_at"] = (NOW - timedelta(days=1)).isoformat()
    for field in (
        "entry_rule",
        "exit_rule",
        "source_dataset_versions",
        "required_market_fields",
        "blocked_reasons",
        "next_required_actions",
    ):
        raw[field] = json.dumps(raw[field], sort_keys=True)
    raw["recommended_mode"] = "paper"
    _write_raw_proposal_rows(proposals_path, [raw])

    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "expired-but-open",
        market_data_1h={
            "TRX/USDT": _series("TRX/USDT", last_ts=series.ts[-1] + 3_600_000)
        },
        top_of_book={"TRX/USDT": _quote(102.0, NOW + timedelta(hours=1))},
        cfg=cfg,
        audit=audit,
        now=NOW + timedelta(hours=1),
    )

    ack_rows = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack.csv").open())
    )
    runs = list(csv.DictReader((reports / "summaries/paper_strategy_runs.csv").open()))
    assert result["accepted"] == 1
    assert result["closed_trades"] == 1
    assert len(ack_rows) == 1
    assert ack_rows[0]["accepted"] == "True"
    assert ack_rows[0]["reject_reason"] == ""
    assert len(runs) == 1


def test_superseded_tracker_cannot_open_new_position(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal("SUPERSEDED_NO_NEW_ENTRY", "TRX/USDT")
    _write_proposals(proposals_path, [proposal])
    cfg = _cfg(tmp_path, proposals_path)
    series = _series("TRX/USDT")

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "current-no-quote",
        market_data_1h={"TRX/USDT": series},
        top_of_book={},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )
    proposals_path.unlink()
    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "superseded-with-quote",
        market_data_1h={
            "TRX/USDT": _series("TRX/USDT", last_ts=series.ts[-1] + 3_600_000)
        },
        top_of_book={"TRX/USDT": _quote(102.0, NOW + timedelta(hours=1))},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW + timedelta(hours=1),
    )

    state = json.loads(Path(cfg.quant_lab.paper_runtime.state_path).read_text())
    tracker = state["trackers"][proposal.proposal_id]
    assert result["closed_trades"] == 0
    assert tracker["open_trade"] is None
    assert tracker["supersession_status"] == "SUPERSEDED_CLOSED"
    assert tracker["new_entry_allowed"] is False
    assert tracker["exit_allowed"] is False


def test_superseded_open_position_remains_exit_only(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal(
        "SUPERSEDED_EXIT_ONLY",
        "TRX/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 0},
        exit_rule={"operator": "max_holding_bars", "value": 1},
        max_holding_bars=1,
    )
    _write_proposals(proposals_path, [proposal])
    cfg = _cfg(tmp_path, proposals_path)
    series = _series("TRX/USDT")
    run_generic_paper_runtime(
        run_dir=reports / "runs" / "open",
        market_data_1h={"TRX/USDT": series},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )
    proposals_path.unlink()
    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "exit-only",
        market_data_1h={
            "TRX/USDT": _series("TRX/USDT", last_ts=series.ts[-1] + 3_600_000)
        },
        top_of_book={"TRX/USDT": _quote(102.0, NOW + timedelta(hours=1))},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW + timedelta(hours=1),
    )

    runs = list(csv.DictReader((reports / "summaries/paper_strategy_runs.csv").open()))
    history = list(
        csv.DictReader((reports / "summaries/paper_strategy_registry_history.csv").open())
    )
    current = list(
        csv.DictReader((reports / "summaries/paper_strategy_registry_current.csv").open())
    )
    assert result["closed_trades"] == 1
    assert len(runs) == 1
    assert current == []
    assert history[0]["supersession_status"] == "SUPERSEDED_CLOSED"
    assert history[0]["new_entry_allowed"] == "False"
    assert history[0]["exit_allowed"] == "False"


def test_ack_and_tracker_current_snapshots_exclude_history(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    first = _proposal("CURRENT_SNAPSHOT_FIRST", "TRX/USDT")
    second = _proposal("CURRENT_SNAPSHOT_SECOND", "BCH/USDT")
    _write_proposals(proposals_path, [first, second])
    cfg = _cfg(tmp_path, proposals_path)
    market = {"TRX/USDT": _series("TRX/USDT"), "BCH/USDT": _series("BCH/USDT")}
    books = {"TRX/USDT": _quote(100.0), "BCH/USDT": _quote(200.0)}
    run_generic_paper_runtime(
        run_dir=reports / "runs" / "both",
        market_data_1h=market,
        top_of_book=books,
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )
    _write_proposals(proposals_path, [second])
    run_generic_paper_runtime(
        run_dir=reports / "runs" / "second-only",
        market_data_1h=market,
        top_of_book=books,
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW + timedelta(hours=1),
    )

    ack_current = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack_current.csv").open())
    )
    ack_history = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack_history.csv").open())
    )
    tracker_current = list(
        csv.DictReader((reports / "summaries/paper_strategy_trackers_current.csv").open())
    )
    assert {row["proposal_id"] for row in ack_current} == {second.proposal_id}
    assert {row["proposal_id"] for row in ack_history} == {
        first.proposal_id,
        second.proposal_id,
    }
    assert {row["proposal_id"] for row in tracker_current} == {second.proposal_id}
    contract = json.loads(
        (reports / "summaries/quant_lab_contract_status.json").read_text()
    )
    assert contract["loaded_tracker_count"] == 2
    assert contract["current_active_tracker_count"] == 1
    assert contract["current_pending_tracker_count"] == 0
    assert contract["superseded_closed_count"] == 0
    assert contract["superseded_exit_only_count"] == 1
    assert contract["active_tracker_count"] == 2
    assert contract["active_tracker_count_deprecated"] is True


def test_missing_quote_records_not_observable_without_virtual_position(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    _write_proposals(proposals_path, [_proposal("NO_QUOTE_PAPER", "TRX/USDT")])
    cfg = _cfg(tmp_path, proposals_path)

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    signals = list(
        csv.DictReader((reports / "summaries/paper_strategy_signals.csv").open())
    )
    states = list(
        csv.DictReader((reports / "summaries/paper_strategy_state.csv").open())
    )
    assert signals[0]["observability"] == "NOT_OBSERVABLE"
    assert signals[0]["valid_for_promotion"] == "False"
    assert states[0]["open_paper_position"] == "False"


def test_missing_market_signal_is_idempotent_within_hour(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    _write_proposals(proposals_path, [_proposal("NO_MARKET_PAPER", "TRX/USDT")])
    cfg = _cfg(tmp_path, proposals_path)

    for run_name, observed_at in (
        ("first", NOW),
        ("same-hour", NOW + timedelta(minutes=30)),
        ("next-hour", NOW + timedelta(hours=1)),
    ):
        run_generic_paper_runtime(
            run_dir=reports / "runs" / run_name,
            market_data_1h={},
            top_of_book={},
            cfg=cfg,
            audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
            now=observed_at,
        )

    signals = list(
        csv.DictReader((reports / "summaries/paper_strategy_signals.csv").open())
    )
    daily = list(
        csv.DictReader((reports / "summaries/paper_strategy_daily.csv").open())
    )
    assert len(signals) == 2
    assert len({row["signal_id"] for row in signals}) == 2
    assert daily[-1]["arrival_mid_coverage"] == "0.0"
    persisted = json.loads(
        Path(cfg.quant_lab.paper_runtime.state_path).read_text(encoding="utf-8")
    )
    assert next(iter(persisted["daily_buckets"].values()))["signal_count"] == 2


def test_paper_market_data_supplement_does_not_replace_live_series():
    live_series = object()
    paper_series = object()

    class Provider:
        calls = []

        def fetch_ohlcv(self, symbols, **kwargs):
            self.calls.append((list(symbols), kwargs))
            return {"BTC/USDT": object(), "TAO/USDT": paper_series}

    provider = Provider()
    merged = supplement_paper_runtime_market_data(
        provider=provider,
        market_data_1h={"BTC/USDT": live_series},
        observation_symbols=["BTC/USDT", "TAO-USDT"],
        timeframe="1h",
        limit=1440,
        end_ts_ms=123,
    )

    assert merged["BTC/USDT"] is live_series
    assert merged["TAO/USDT"] is paper_series
    assert provider.calls == [
        (
            ["TAO/USDT"],
            {"timeframe": "1h", "limit": 1440, "end_ts_ms": 123},
        )
    ]


def test_runtime_accepts_production_provider_quote_timestamp_shape(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    _write_proposals(proposals_path, [_proposal("PROVIDER_QUOTE_PAPER", "TRX/USDT")])
    cfg = _cfg(tmp_path, proposals_path)

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _provider_quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    signal = list(
        csv.DictReader((reports / "summaries/paper_strategy_signals.csv").open())
    )[0]
    assert signal["observability"] == "OBSERVABLE"
    assert signal["quote_timestamp"].startswith("2026-07-11T00:00:00")


@pytest.mark.parametrize(
    "quote",
    [
        {"bid": 99.9, "ask": 100.1, "mid": 100.0},
        {
            "bid": 100.1,
            "ask": 99.9,
            "mid": 100.0,
            "timestamp": NOW.isoformat(),
        },
    ],
)
def test_quote_requires_timestamp_and_sane_top_of_book(tmp_path, quote):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    _write_proposals(proposals_path, [_proposal("INVALID_QUOTE_PAPER", "TRX/USDT")])
    cfg = _cfg(tmp_path, proposals_path)

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": quote},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    signal = list(
        csv.DictReader((reports / "summaries/paper_strategy_signals.csv").open())
    )[0]
    state = list(
        csv.DictReader((reports / "summaries/paper_strategy_state.csv").open())
    )[0]
    assert signal["observability"] == "NOT_OBSERVABLE"
    assert state["open_paper_position"] == "False"


def test_stale_quote_is_not_observable_and_cannot_open(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    _write_proposals(proposals_path, [_proposal("STALE_QUOTE_PAPER", "TRX/USDT")])
    cfg = _cfg(tmp_path, proposals_path)
    stale_at = NOW - timedelta(
        seconds=cfg.quant_lab.paper_runtime.max_quote_age_seconds + 1
    )

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0, stale_at)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    signal = list(
        csv.DictReader((reports / "summaries/paper_strategy_signals.csv").open())
    )[0]
    state = list(
        csv.DictReader((reports / "summaries/paper_strategy_state.csv").open())
    )[0]
    assert signal["observability"] == "STALE"
    assert signal["valid_for_promotion"] == "False"
    assert state["open_paper_position"] == "False"


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        (
            {"contract_version": "quant_lab.paper_strategy.v999"},
            "unsupported_contract_version",
        ),
        (
            {"entry_rule": {"operator": "python_eval", "field": "close", "value": 1}},
            "unsupported_operator",
        ),
        (
            {
                "created_at": "2026-07-01T00:00:00+00:00",
                "expires_at": "2026-07-10T00:00:00+00:00",
            },
            "proposal_expired",
        ),
    ],
)
def test_invalid_contract_rows_receive_standard_rejections(
    tmp_path, mutation, expected_reason
):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal("REJECTED_CONTRACT_PAPER", "TRX/USDT")
    raw = proposal.model_dump(mode="json")
    raw.update(mutation)
    for field in (
        "entry_rule",
        "exit_rule",
        "source_dataset_versions",
        "required_market_fields",
        "blocked_reasons",
        "next_required_actions",
    ):
        raw[field] = json.dumps(raw[field], sort_keys=True)
    raw["recommended_mode"] = "paper"
    _write_raw_proposal_rows(proposals_path, [raw])
    cfg = _cfg(tmp_path, proposals_path)

    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    ack = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack.csv").open())
    )[0]
    assert result["trackers"] == 0
    assert ack["accepted"] == "False"
    assert ack["reject_reason"] == expected_reason


def test_proposal_source_failure_is_contained_without_live_effect(
    tmp_path, monkeypatch
):
    from src.paper_runtime import runtime

    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    cfg = _cfg(tmp_path, proposals_path)

    def fail_source(*args, **kwargs):
        raise TimeoutError("quant-lab proposal sync timed out")

    monkeypatch.setattr(runtime, "_proposal_snapshot", fail_source)
    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    errors = list(
        csv.DictReader((reports / "summaries/paper_strategy_errors.csv").open())
    )
    assert result["errors"] == 1
    assert result["live_order_effect"] == "none"
    assert errors[0]["error_code"] == "proposal_source_read_failed"


def test_proposal_source_failure_freezes_entries_but_preserves_open_exit(
    tmp_path, monkeypatch
):
    from src.paper_runtime import runtime

    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    held = _proposal(
        "SOURCE_FAILURE_HOLD",
        "TRX/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 10_000},
    )
    exiting = _proposal(
        "SOURCE_FAILURE_EXIT",
        "BCH/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 0},
        exit_rule={"operator": "max_holding_bars", "value": 1},
        max_holding_bars=1,
    )
    _write_proposals(proposals_path, [held, exiting])
    cfg = _cfg(tmp_path, proposals_path)
    audit = SimpleNamespace(regime="NORMAL", quant_lab={})
    initial_series = {
        "TRX/USDT": _series("TRX/USDT"),
        "BCH/USDT": _series("BCH/USDT"),
    }
    initial_quotes = {
        "TRX/USDT": _quote(100.0),
        "BCH/USDT": _quote(100.0),
    }

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "open",
        market_data_1h=initial_series,
        top_of_book=initial_quotes,
        cfg=cfg,
        audit=audit,
        now=NOW,
    )
    opened_state = json.loads(Path(cfg.quant_lab.paper_runtime.state_path).read_text())
    assert opened_state["trackers"][held.proposal_id]["open_trade"] is None
    assert opened_state["trackers"][exiting.proposal_id]["open_trade"] is not None

    def fail_source(*args, **kwargs):
        raise TimeoutError("quant-lab proposal sync timed out")

    monkeypatch.setattr(runtime, "_proposal_snapshot", fail_source)
    later_ts = int((NOW + timedelta(hours=1)).timestamp() * 1000)
    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "source-failed",
        market_data_1h={
            "TRX/USDT": _series("TRX/USDT", last_ts=later_ts),
            "BCH/USDT": _series("BCH/USDT", last_ts=later_ts),
        },
        top_of_book={
            "TRX/USDT": _quote(100.0, NOW + timedelta(hours=1)),
            "BCH/USDT": _quote(100.0, NOW + timedelta(hours=1)),
        },
        cfg=cfg,
        audit=audit,
        now=NOW + timedelta(hours=1),
    )

    state = json.loads(Path(cfg.quant_lab.paper_runtime.state_path).read_text())
    held_tracker = state["trackers"][held.proposal_id]
    exiting_tracker = state["trackers"][exiting.proposal_id]
    assert result["closed_trades"] == 1
    assert result["live_order_effect"] == "none"
    assert held_tracker["supersession_status"] == "SOURCE_UNAVAILABLE_HOLD"
    assert held_tracker["new_entry_allowed"] is False
    assert held_tracker["exit_allowed"] is False
    assert held_tracker["open_trade"] is None
    assert exiting_tracker["supersession_status"] == "SOURCE_UNAVAILABLE_EXIT_ONLY"
    assert exiting_tracker["new_entry_allowed"] is False
    assert exiting_tracker["exit_allowed"] is True
    assert exiting_tracker["open_trade"] is None


def test_pending_exit_retries_after_quote_recovers(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal(
        "EXIT_RETRY_PAPER",
        "TRX/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 0},
        exit_rule={"operator": "max_holding_bars", "value": 1},
        max_holding_bars=1,
    )
    _write_proposals(proposals_path, [proposal])
    cfg = _cfg(tmp_path, proposals_path)
    audit = SimpleNamespace(regime="NORMAL", quant_lab={})
    series = _series("TRX/USDT")

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "open",
        market_data_1h={"TRX/USDT": series},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=audit,
        now=NOW,
    )
    state_path = Path(cfg.quant_lab.paper_runtime.state_path)
    persisted = json.loads(state_path.read_text())
    tracker = persisted["trackers"][proposal.proposal_id]
    tracker["state"] = "PAPER_EXIT_PENDING"
    state_path.write_text(json.dumps(persisted), encoding="utf-8")

    missing_quote = run_generic_paper_runtime(
        run_dir=reports / "runs" / "pending",
        market_data_1h={
            "TRX/USDT": _series("TRX/USDT", last_ts=series.ts[-1] + 3_600_000)
        },
        top_of_book={},
        cfg=cfg,
        audit=audit,
        now=NOW + timedelta(hours=1),
    )
    recovered = run_generic_paper_runtime(
        run_dir=reports / "runs" / "closed",
        market_data_1h={
            "TRX/USDT": _series("TRX/USDT", last_ts=series.ts[-1] + 7_200_000)
        },
        top_of_book={"TRX/USDT": _quote(101.0, NOW + timedelta(hours=2))},
        cfg=cfg,
        audit=audit,
        now=NOW + timedelta(hours=2),
    )

    assert missing_quote["closed_trades"] == 0
    assert recovered["closed_trades"] == 1
    final_state = list(
        csv.DictReader((reports / "summaries/paper_strategy_state.csv").open())
    )[0]
    assert final_state["state"] == "WAITING_SIGNAL"


def test_daily_evidence_is_cumulative_and_proxy_cost_stays_paper_only(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal(
        "CUMULATIVE_DAILY_PAPER",
        "TRX/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 0},
        exit_rule={"operator": "max_holding_bars", "value": 1},
        max_holding_bars=1,
        required_cost_trust_level="CANARY",
    )
    _write_proposals(proposals_path, [proposal])
    cfg = _cfg(tmp_path, proposals_path)
    audit = SimpleNamespace(regime="NORMAL", quant_lab={})
    series = _series("TRX/USDT")

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "day-1",
        market_data_1h={"TRX/USDT": series},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=audit,
        now=NOW,
    )
    run_generic_paper_runtime(
        run_dir=reports / "runs" / "day-2",
        market_data_1h={
            "TRX/USDT": _series("TRX/USDT", last_ts=series.ts[-1] + 3_600_000)
        },
        top_of_book={"TRX/USDT": _quote(102.0, NOW + timedelta(days=1))},
        cfg=cfg,
        audit=audit,
        now=NOW + timedelta(days=1),
    )

    daily = list(
        csv.DictReader((reports / "summaries/paper_strategy_daily.csv").open())
    )
    latest = sorted(daily, key=lambda row: row["paper_date"])[-1]
    cost = list(
        csv.DictReader((reports / "summaries/paper_strategy_cost_evidence.csv").open())
    )[0]
    run = list(csv.DictReader((reports / "summaries/paper_strategy_runs.csv").open()))[
        0
    ]

    assert latest["paper_days"] == "2"
    assert latest["strategy_candidate"] == "generic_test"
    assert latest["heartbeat_day_count"] == "2"
    assert latest["entry_day_count"] == "1"
    assert latest["cumulative_would_enter_count"] == "1"
    assert latest["closed_entries"] == "1"
    assert latest["paper_pnl_day_count"] == "1"
    assert latest["spread_observation_coverage"] == "1.0"
    assert cost["required_cost_trust_level"] == "CANARY"
    assert cost["cost_trust_level"] == "PAPER_ONLY"
    assert cost["valid_for_live_coverage"] == "False"
    assert run["cost_trust_level"] == "PAPER_ONLY"


def test_cooldown_skips_full_bar_before_reentry(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal(
        "COOLDOWN_PAPER",
        "TRX/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 0},
        exit_rule={"operator": "max_holding_bars", "value": 1},
        max_holding_bars=1,
        cooldown_bars=1,
    )
    _write_proposals(proposals_path, [proposal])
    cfg = _cfg(tmp_path, proposals_path)
    audit = SimpleNamespace(regime="NORMAL", quant_lab={})
    series = _series("TRX/USDT")

    for index in range(4):
        last_ts = series.ts[-1] + index * 3_600_000
        run_generic_paper_runtime(
            run_dir=reports / "runs" / f"run-{index}",
            market_data_1h={"TRX/USDT": _series("TRX/USDT", last_ts=last_ts)},
            top_of_book={
                "TRX/USDT": _quote(100.0 + index, NOW + timedelta(hours=index))
            },
            cfg=cfg,
            audit=audit,
            now=NOW + timedelta(hours=index),
        )
        state = list(
            csv.DictReader((reports / "summaries/paper_strategy_state.csv").open())
        )[0]
        if index == 2:
            assert state["state"] == "WAITING_SIGNAL"
            assert state["open_paper_position"] == "False"

    final_state = list(
        csv.DictReader((reports / "summaries/paper_strategy_state.csv").open())
    )[0]
    assert final_state["state"] == "PAPER_OPEN"
    assert final_state["open_paper_position"] == "True"


def test_disabled_runtime_and_version_conflict_return_standard_rejections(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    original = _proposal("LOCKED_RULE_PAPER", "TRX/USDT")
    _write_proposals(proposals_path, [original])
    cfg = _cfg(tmp_path, proposals_path)
    cfg.quant_lab.paper_runtime.enabled = False

    disabled = run_generic_paper_runtime(
        run_dir=reports / "runs" / "disabled",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )
    disabled_ack = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack.csv").open())
    )[0]
    assert disabled["trackers"] == 0
    assert disabled_ack["accepted"] == "False"
    assert disabled_ack["reject_reason"] == "config_disabled"

    cfg.quant_lab.paper_runtime.enabled = True
    run_generic_paper_runtime(
        run_dir=reports / "runs" / "accepted",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )
    changed = _proposal(
        "LOCKED_RULE_PAPER",
        "TRX/USDT",
        entry_rule={"operator": "gt", "field": "close", "value": 999999},
    )
    _write_proposals(proposals_path, [changed])
    conflict = run_generic_paper_runtime(
        run_dir=reports / "runs" / "conflict",
        market_data_1h={
            "TRX/USDT": _series(
                "TRX/USDT",
                last_ts=_series("TRX/USDT").ts[-1] + 3_600_000,
            )
        },
        top_of_book={"TRX/USDT": _quote(101.0, NOW + timedelta(hours=1))},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW + timedelta(hours=1),
    )
    conflict_ack = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack.csv").open())
    )[-1]
    assert conflict["rejected"] == 1
    assert conflict_ack["reject_reason"] == "duplicate_version_conflict"


def test_runtime_tracker_and_history_limits_bound_large_proposal_batch(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposals = [
        _proposal(f"BOUNDED_PAPER_{index:03d}", "TRX/USDT") for index in range(120)
    ]
    _write_proposals(proposals_path, proposals)
    cfg = _cfg(tmp_path, proposals_path)
    cfg.quant_lab.paper_runtime.max_trackers = 100
    cfg.quant_lab.paper_runtime.max_history_records = 100

    result = run_generic_paper_runtime(
        run_dir=reports / "runs" / "bounded",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )
    state = json.loads(Path(cfg.quant_lab.paper_runtime.state_path).read_text())
    ack_rows = list(
        csv.DictReader((reports / "summaries/paper_strategy_proposal_ack.csv").open())
    )

    assert result["trackers"] == 100
    assert result["accepted"] == 100
    assert result["rejected"] == 20
    assert len(state["trackers"]) == 100
    assert len(state["signals"]) == 100
    assert {row["reject_reason"] for row in ack_rows if row["accepted"] == "False"} == {
        "tracker_capacity_exceeded"
    }


def test_dsl_rejects_arbitrary_operator_and_never_evaluates_source_text():
    with pytest.raises(ValidationError):
        PaperRule.model_validate({"operator": "__import__", "value": "os"})
    interpreter = PaperRuleInterpreter()
    assert interpreter.evaluate(
        PaperRule(operator="gt", field="close", value=1), {"close": 2}
    )
    with pytest.raises(ValidationError):
        PaperRule.model_validate(
            {
                "operator": "consecutive",
                "periods": 513,
                "children": [{"operator": "gt", "field": "close", "value": 1}],
            }
        )


def test_contract_hash_matches_quant_lab_canonical_vector():
    payload = {
        "contract_version": "quant_lab.paper_strategy.v1",
        "strategy_id": "CONTRACT_TEST",
        "strategy_version": "1.0.0",
        "strategy_family": "contract",
        "symbol": "TRX/USDT",
        "timeframe": "1h",
        "direction": "long",
        "entry_rule": {"operator": "momentum_gt", "field": "momentum_24", "value": 0},
        "exit_rule": {"operator": "max_holding_bars", "value": 48},
        "max_holding_bars": 48,
        "min_holding_bars": 1,
        "cooldown_bars": 2,
        "signal_confirmation_bars": 1,
        "cost_quantile": "p75",
        "minimum_expected_edge_bps": 10.0,
        "paper_notional_usdt": 20.0,
        "paper_only": True,
        "live_order_effect": "none",
        "max_live_notional_usdt": 0.0,
        "created_at": "2026-07-10T00:00:00Z",
        "expires_at": "2026-08-10T00:00:00Z",
        "source_pack_sha256": "",
        "source_dataset_versions": {"alpha_discovery_board": "v1"},
        "required_market_fields": ["bid", "ask", "mid", "momentum_24"],
        "required_cost_trust_level": "PAPER_ONLY",
        "lifecycle_state": "PAPER_PROPOSAL_READY",
        "lifecycle_reason": "ignored",
        "blocked_reasons": ["ignored"],
        "next_required_actions": ["ignored"],
    }

    assert paper_proposal_hash(payload) == (
        "6d922297dfdd33019d720d5491e276382d49c710e0823997f78e44a21dd29acb"
    )


def test_bundle_contains_generic_paper_evidence(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    _write_proposals(proposals_path, [_proposal("BUNDLE_PAPER", "TRX/USDT")])
    cfg = _cfg(tmp_path, proposals_path)
    run_generic_paper_runtime(
        run_dir=reports / "runs" / "run-1",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    bundle = export_v5_bundle(
        reports_dir=reports,
        out_dir=tmp_path / "bundles",
        include_logs=False,
        include_config=False,
        refresh_cost_probe_preflight=False,
    )
    with tarfile.open(bundle, "r:gz") as archive:
        names = set(archive.getnames())

    assert "summaries/paper_strategy_registry.csv" in names
    assert "summaries/paper_strategy_state.csv" in names
    assert "summaries/paper_strategy_signals.csv" in names
    assert "summaries/paper_strategy_quote_coverage.csv" in names
    assert "summaries/paper_strategy_cost_evidence.csv" in names
    assert "summaries/paper_strategy_exit_quality.csv" in names
    assert "summaries/paper_strategy_errors.csv" in names
    assert "summaries/paper_strategy_restart_recovery.csv" in names
    assert "summaries/quant_lab_contract_status.json" in names
    assert "summaries/trade_opportunity_funnel.csv" in names


def test_corrupt_state_fails_closed_and_preserves_last_published_evidence(tmp_path):
    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    _write_proposals(proposals_path, [_proposal("STATE_LOAD_GUARD", "TRX/USDT")])
    cfg = _cfg(tmp_path, proposals_path)
    first = run_generic_paper_runtime(
        run_dir=reports / "runs" / "first",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )
    summaries = reports / "summaries"
    ack_before = (summaries / "paper_strategy_proposal_ack.csv").read_text()
    signals_before = (summaries / "paper_strategy_signals.csv").read_text()
    Path(cfg.quant_lab.paper_runtime.state_path).write_text("{broken", encoding="utf-8")

    failed = run_generic_paper_runtime(
        run_dir=reports / "runs" / "corrupt",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(101.0, NOW + timedelta(hours=1))},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW + timedelta(hours=1),
    )

    status = json.loads((summaries / "quant_lab_contract_status.json").read_text())
    errors = list(csv.DictReader((summaries / "paper_strategy_errors.csv").open()))
    assert first["fail_closed"] is False
    assert failed["fail_closed"] is True
    assert failed["failure_stage"] == "state_load_failed"
    assert failed["signals"] == 0
    assert failed["closed_trades"] == 0
    assert (summaries / "paper_strategy_proposal_ack.csv").read_text() == ack_before
    assert (summaries / "paper_strategy_signals.csv").read_text() == signals_before
    assert status["state_loaded"] is False
    assert status["state_persisted"] is False
    assert status["real_order_calls"] == 0
    assert errors[-1]["error_code"] == "state_load_failed"


def test_state_save_failure_publishes_no_uncommitted_paper_evidence(
    tmp_path,
    monkeypatch,
):
    from src.paper_runtime.store import PaperRuntimeStore

    reports = tmp_path / "reports"
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    _write_proposals(proposals_path, [_proposal("STATE_SAVE_GUARD", "TRX/USDT")])
    cfg = _cfg(tmp_path, proposals_path)

    def fail_save(self, payload):
        raise OSError("simulated durable state failure")

    monkeypatch.setattr(PaperRuntimeStore, "save", fail_save)
    failed = run_generic_paper_runtime(
        run_dir=reports / "runs" / "save-failed",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    summaries = reports / "summaries"
    status = json.loads((summaries / "quant_lab_contract_status.json").read_text())
    errors = list(csv.DictReader((summaries / "paper_strategy_errors.csv").open()))
    assert failed["fail_closed"] is True
    assert failed["failure_stage"] == "state_write_failed"
    assert failed["signals"] == 0
    assert failed["closed_trades"] == 0
    assert not (summaries / "paper_strategy_proposal_ack.csv").exists()
    assert not (summaries / "paper_strategy_signals.csv").exists()
    assert not (summaries / "paper_strategy_runs.csv").exists()
    assert status["state_loaded"] is True
    assert status["state_persisted"] is False
    assert status["real_position_mutations"] == 0
    assert errors[-1]["error_code"] == "state_write_failed"


def test_generic_runtime_replaces_legacy_rows_in_canonical_contract_files(tmp_path):
    reports = tmp_path / "reports"
    summaries = reports / "summaries"
    summaries.mkdir(parents=True)
    (summaries / "paper_strategy_proposal_ack.csv").write_text(
        "proposal_id,proposal_hash,accepted,proposal_source\n"
        "legacy,wrong,False,legacy_tracker\n",
        encoding="utf-8",
    )
    (summaries / "paper_strategy_runs.csv").write_text(
        "paper_trade_id,strategy_id\nlegacy-trade,legacy\n",
        encoding="utf-8",
    )
    proposals_path = tmp_path / "paper_strategy_proposals.csv"
    proposal = _proposal("CANONICAL_OWNER", "TRX/USDT")
    _write_proposals(proposals_path, [proposal])
    cfg = _cfg(tmp_path, proposals_path)

    run_generic_paper_runtime(
        run_dir=reports / "runs" / "canonical",
        market_data_1h={"TRX/USDT": _series("TRX/USDT")},
        top_of_book={"TRX/USDT": _quote(100.0)},
        cfg=cfg,
        audit=SimpleNamespace(regime="NORMAL", quant_lab={}),
        now=NOW,
    )

    ack = list(csv.DictReader((summaries / "paper_strategy_proposal_ack.csv").open()))
    runs = list(csv.DictReader((summaries / "paper_strategy_runs.csv").open()))
    assert [(row["proposal_id"], row["proposal_hash"]) for row in ack] == [
        (proposal.proposal_id, proposal.proposal_hash)
    ]
    assert runs == []
