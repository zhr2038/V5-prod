from __future__ import annotations

import importlib.util
from collections.abc import Iterable


RESEARCH_DEPENDENCY_HINT = (
    "Missing optional ML/research dependencies: {missing}. "
    "Install them with: pip install -r requirements-research.txt"
)


def missing_research_dependencies(modules: Iterable[str] = ("sklearn", "xgboost")) -> list[str]:
    missing: list[str] = []
    for module_name in modules:
        if importlib.util.find_spec(str(module_name)) is None:
            missing.append(str(module_name))
    return missing


def research_dependency_error(missing: Iterable[str]) -> str:
    missing_text = ", ".join(str(item) for item in missing)
    return RESEARCH_DEPENDENCY_HINT.format(missing=missing_text or "not_observable")


def require_research_dependencies(modules: Iterable[str] = ("sklearn", "xgboost")) -> None:
    missing = missing_research_dependencies(modules)
    if missing:
        raise SystemExit(research_dependency_error(missing))
