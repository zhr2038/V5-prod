from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds


@dataclass
class _Account:
    cash_usdt: float


class _AccountStore:
    def get(self) -> _Account:
        return _Account(cash_usdt=100.0)


class _PositionStore:
    def list(self) -> List[Any]:
        return []


class _OKXResponse:
    http_status = 200
    okx_code = "0"
    okx_msg = ""

    def __init__(self, details: List[Dict[str, str]]) -> None:
        self.data = {"data": [{"details": details}]}


class _OKX:
    def __init__(self, details: List[Dict[str, str]]) -> None:
        self._details = details

    def get_balance(self, ccy: Optional[str] = None) -> _OKXResponse:
        return _OKXResponse(self._details)


def test_reconcile_marks_non_universe_residual_as_ignored_dust(tmp_path: Path) -> None:
    engine = ReconcileEngine(
        okx=_OKX(
            [
                {"ccy": "USDT", "cashBal": "100", "ordFrozen": "0", "eqUsd": "100", "uTime": "1"},
                {"ccy": "BASED", "cashBal": "0.00971", "ordFrozen": "0", "eqUsd": "0.001275894", "uTime": "1"},
            ]
        ),
        position_store=_PositionStore(),
        account_store=_AccountStore(),
        thresholds=ReconcileThresholds(abs_usdt_tol=1.0, abs_base_tol=1e-8, dust_usdt_ignore=1.0),
    )

    status = engine.reconcile(
        out_path=str(tmp_path / "reconcile_status.json"),
        universe_bases=["BTC"],
        ccy_mode="universe_only",
    )

    based_diff = next(diff for diff in status["diffs"] if diff["ccy"] == "BASED")
    assert status["ok"] is True
    assert based_diff["enforced"] is False
    assert based_diff["ignored_as_dust"] is True
    assert status["stats"]["ignored_dust_count"] == 1
    assert status["ignored_dust"]["ccys"] == [
        {"ccy": "BASED", "delta": "0.00971", "estimated_delta_usdt": "0.001275894"}
    ]
