import subprocess
import sys
from pathlib import Path


def test_v5_bundle_export_script_avoids_shell_subprocess() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "generate_v5_bundle_remote.sh").read_text(encoding="utf-8")

    assert "shell=True" not in script


def test_v5_bundle_export_script_writes_lightweight_report_index() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "generate_v5_bundle_remote.sh").read_text(encoding="utf-8")

    assert "def write_static_report_index(" in script
    assert 'write_text("reports/index.json"' in script
    assert 'write_text("reports/index.html"' in script
    assert 'write_text("raw/large/.noindex"' in script


def test_v5_bundle_export_script_includes_bottom_zone_paper_summaries() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "generate_v5_bundle_remote.sh").read_text(encoding="utf-8")

    assert "REPORT_BOTTOM_ZONE_PAPER_RUN_FIELDS" in script
    assert "REPORT_BOTTOM_ZONE_PAPER_DAILY_FIELDS" in script
    assert '"reports/summaries/bottom_zone_probe_paper_runs.csv"' in script
    assert '"summaries/bottom_zone_probe_paper_runs.csv"' in script
    assert '"reports/summaries/bottom_zone_probe_paper_daily.csv"' in script
    assert '"summaries/bottom_zone_probe_paper_daily.csv"' in script


def test_v5_bundle_export_script_includes_fast_microstructure_shadow_summary() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "generate_v5_bundle_remote.sh").read_text(encoding="utf-8")

    assert "FAST_MICROSTRUCTURE_STRATEGY_SHADOW_FIELDS" in script
    assert '"reports/summaries/fast_microstructure_strategy_shadow.csv"' in script
    assert '"summaries/fast_microstructure_strategy_shadow.csv"' in script


def test_v5_bundle_export_script_includes_cost_probe_artifacts() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "generate_v5_bundle_remote.sh").read_text(encoding="utf-8")

    assert "COST_PROBE_BUNDLE_ARTIFACTS" in script
    assert '"cost_probe_plan.csv", "summaries/cost_probe_plan.csv"' in script
    assert '"cost_probe_order_events.jsonl", "summaries/cost_probe_order_events.jsonl"' in script
    assert '"cost_probe_roundtrip_events.jsonl", "summaries/cost_probe_roundtrip_events.jsonl"' in script
    assert '"cost_probe_summary.json", "summaries/cost_probe_summary.json"' in script
    assert '"cost_probe_p3_preflight.json", "summaries/cost_probe_p3_preflight.json"' in script
    assert '"manual_authorization_required"' in script
    assert "NON_SECRET_KEYS" in script
    assert '"runtime_cost_guard.csv", "summaries/cost_probe_runtime_cost_guard.csv"' in script
    assert '"cost_probe_artifact_count"' in script
    assert '"cost_probe_p3_preflight"' in script


def test_v5_bundle_export_script_regression() -> None:
    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "scripts/test_v5_bundle_export.py"],
        cwd=root,
        text=True,
        capture_output=True,
        # This helper exercises dozens of bundle-export fixtures; keep the
        # wrapper above the normal full-script runtime but below a real hang.
        timeout=600,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
