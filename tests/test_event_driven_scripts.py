from pathlib import Path


def test_deploy_event_driven_script_uses_repo_root_and_renderer() -> None:
    text = Path("deploy_event_driven.sh").read_text(encoding="utf-8")

    assert 'ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' in text
    assert 'CONFIG_FILE="${V5_CONFIG:-}"' in text
    assert 'render_systemd_units.py' in text
    assert "/home/admin/clawd/v5-prod" not in text


def test_monitor_event_driven_script_uses_runtime_paths() -> None:
    text = Path("monitor_event_driven.sh").read_text(encoding="utf-8")

    assert 'ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' in text
    assert 'derive_runtime_named_artifact_path' in text
    assert 'derive_runtime_named_json_path' in text
    assert 'LOG_FILE=' in text
    assert 'COOLDOWN_FILE=' in text
    assert "Path(sys.argv[1]).read_text()" in text
    assert "/home/admin/clawd/v5-prod" not in text
