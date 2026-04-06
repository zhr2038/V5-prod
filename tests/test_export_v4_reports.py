from __future__ import annotations

import json
from pathlib import Path

import scripts.export_v4_reports as export_v4_reports


def test_export_v4_reports_writes_no_data_summary_when_source_missing(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"

    summary_path = export_v4_reports.export_v4_reports(
        v4_reports_dir=tmp_path / "missing",
        out_dir=out_dir,
        start_ts=1700000000,
        end_ts=1700003600,
    )

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "v4"
    assert payload["data_quality"] == "no_data"
    assert payload["window_start_ts"] == 1700000000
    assert payload["window_end_ts"] == 1700003600
    assert (out_dir / "equity.jsonl").exists()
    assert (out_dir / "trades.csv").exists()


def test_export_v4_reports_reuses_matching_summary_and_artifacts(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    out_dir = tmp_path / "out"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "v4-run",
                "window_start_ts": 1700000000,
                "window_end_ts": 1700003600,
                "num_trades": 7,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (source_dir / "equity.jsonl").write_text('{"ts":"2026-01-01T00:00:00Z"}\n', encoding="utf-8")
    (source_dir / "trades.csv").write_text("symbol\nBTC/USDT\n", encoding="utf-8")

    summary_path = export_v4_reports.export_v4_reports(
        v4_reports_dir=source_dir,
        out_dir=out_dir,
        start_ts=1700000000,
        end_ts=1700003600,
    )

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "v4-run"
    assert payload["num_trades"] == 7
    assert (out_dir / "equity.jsonl").read_text(encoding="utf-8") == '{"ts":"2026-01-01T00:00:00Z"}\n'
    assert (out_dir / "trades.csv").read_text(encoding="utf-8") == "symbol\nBTC/USDT\n"


def test_export_v4_reports_rejects_same_source_and_output_dir(tmp_path: Path) -> None:
    source_dir = tmp_path / "same"
    source_dir.mkdir(parents=True, exist_ok=True)

    try:
        export_v4_reports.export_v4_reports(
            v4_reports_dir=source_dir,
            out_dir=source_dir,
            start_ts=1700000000,
            end_ts=1700003600,
        )
    except ValueError as exc:
        assert str(exc) == "out_dir must differ from v4_reports_dir"
    else:
        raise AssertionError("expected export_v4_reports() to reject identical source/output dirs")
