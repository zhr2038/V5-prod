from __future__ import annotations

import pytest

from src.research import dependency_guard


def test_research_dependency_guard_points_to_research_requirements(monkeypatch) -> None:
    def fake_find_spec(name: str):
        if name == "xgboost":
            return None
        return object()

    monkeypatch.setattr(dependency_guard.importlib.util, "find_spec", fake_find_spec)

    with pytest.raises(SystemExit) as excinfo:
        dependency_guard.require_research_dependencies(("sklearn", "xgboost"))

    message = str(excinfo.value)
    assert "xgboost" in message
    assert "pip install -r requirements-research.txt" in message
