from __future__ import annotations

import json

import scripts.consistency_checker as consistency_checker


def test_consistency_checker_build_paths_anchor_to_workspace(tmp_path) -> None:
    paths = consistency_checker.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == (tmp_path / "reports").resolve()


def test_consistency_checker_reads_and_writes_under_workspace_reports(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    cost_dir = reports_dir / "cost_stats_real"
    cost_dir.mkdir(parents=True, exist_ok=True)
    latest_cost = cost_dir / "latest.json"
    latest_cost.write_text(json.dumps({"avg_cost_bps": 12.5}), encoding="utf-8")

    checker = consistency_checker.BacktestLiveConsistencyChecker(workspace=tmp_path)

    assert checker.load_backtest_config()["avg_cost_bps"] == 12.5

    checker.generate_report()

    reports = list(reports_dir.glob("consistency_check_*.json"))
    assert len(reports) == 1
    payload = json.loads(reports[0].read_text(encoding="utf-8"))
    assert payload["results"] == checker.results
