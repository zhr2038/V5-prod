from __future__ import annotations

from types import SimpleNamespace

import main


class _Engine:
    def __init__(self) -> None:
        self.run_id = ""

    def set_run_id(self, run_id: str) -> None:
        self.run_id = run_id


def test_prime_pipeline_run_context_sets_alpha_and_portfolio_run_ids() -> None:
    alpha_engine = _Engine()
    portfolio_engine = _Engine()
    pipe = SimpleNamespace(alpha_engine=alpha_engine, portfolio_engine=portfolio_engine)

    main._prime_pipeline_run_context(pipe, " 20260427_19 ")

    assert alpha_engine.run_id == "20260427_19"
    assert portfolio_engine.run_id == "20260427_19"


def test_prime_pipeline_run_context_ignores_empty_run_id() -> None:
    alpha_engine = _Engine()
    pipe = SimpleNamespace(alpha_engine=alpha_engine)

    main._prime_pipeline_run_context(pipe, " ")

    assert alpha_engine.run_id == ""
