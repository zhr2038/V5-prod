from __future__ import annotations

import json
from pathlib import Path

import scripts.equity_anomaly_detector as detector_mod


def test_build_paths_anchor_equity_anomaly_detector_to_repo_root(tmp_path) -> None:
    paths = detector_mod.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports"
    assert paths.runs_dir == tmp_path / "reports" / "runs"


def test_load_equity_data_reads_workspace_runs_dir(tmp_path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260406_010000"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": "2026-04-06T00:00:00",
                        "equity": 100.0,
                        "cash": 40.0,
                        "positions_value": 60.0,
                    }
                ),
                json.dumps(
                    {
                        "ts": "2026-04-06T01:00:00",
                        "equity": 120.0,
                        "cash": 50.0,
                        "positions_value": 70.0,
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    detector = detector_mod.EquityAnomalyDetector(workspace=tmp_path)
    points = detector.load_equity_data(days=30)

    assert [(point["equity"], point["cash"], point["positions_value"]) for point in points] == [
        (100.0, 40.0, 60.0),
        (120.0, 50.0, 70.0),
    ]


def test_run_detection_uses_workspace_data(tmp_path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260406_020000"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-04-06T00:00:00", "equity": 100.0}),
                json.dumps({"ts": "2026-04-06T01:00:00", "equity": 130.0}),
                json.dumps({"ts": "2026-04-06T05:30:00", "equity": 131.0}),
            ]
        ),
        encoding="utf-8",
    )

    detector = detector_mod.EquityAnomalyDetector(workspace=tmp_path)
    anomalies = detector.run_detection(days=30)

    assert [item["type"] for item in anomalies] == ["jump", "stale"]
    assert detector.stats["total_points"] == 3
    assert detector.stats["anomalies"] == 2


def test_save_report_writes_into_workspace_reports_dir(tmp_path) -> None:
    detector = detector_mod.EquityAnomalyDetector(workspace=tmp_path)
    detector.stats = {"total_points": 2, "anomalies": 1}

    report_path = detector.save_report([{"type": "jump"}])

    assert report_path.parent == tmp_path / "reports"
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["stats"] == {"total_points": 2, "anomalies": 1}
    assert payload["anomalies"] == [{"type": "jump"}]
