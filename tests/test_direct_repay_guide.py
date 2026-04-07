from __future__ import annotations

import json

import scripts.direct_repay_guide as direct_repay_guide


def test_direct_repay_guide_build_paths_anchor_to_workspace(tmp_path) -> None:
    paths = direct_repay_guide.build_paths(workspace=tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.output_path == (tmp_path / "reports" / "direct_repay_guide.json")


def test_direct_repay_guide_writes_into_workspace_reports_when_cwd_differs(tmp_path, monkeypatch) -> None:
    other_dir = tmp_path / "elsewhere"
    workspace = tmp_path / "workspace"
    other_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(other_dir)

    output_path = direct_repay_guide.direct_repay_guide(workspace=workspace)

    assert output_path == (workspace / "reports" / "direct_repay_guide.json")
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["pepe_needed"] == 4353
    assert not (other_dir / "reports" / "direct_repay_guide.json").exists()
