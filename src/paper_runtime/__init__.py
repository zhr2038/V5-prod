"""Generic paper-only runtime with no dependency on live order execution."""

from src.paper_runtime.runtime import (
    paper_runtime_observation_symbols,
    run_generic_paper_runtime,
)

__all__ = ["paper_runtime_observation_symbols", "run_generic_paper_runtime"]
