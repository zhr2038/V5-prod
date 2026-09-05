"""Run the fixed V5 decision pipeline against a research account, never an executor."""
from __future__ import annotations

import copy
import importlib
import json
import os
from contextlib import ExitStack, contextmanager, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.core.pipeline import V5Pipeline
from src.execution.position_store import Position
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
from src.research.quote_portfolio import number
from src.research.window_diagnostics import _sandbox_reports_dir


def _bind_paths(model, root):
    for key, value in model.__dict__.items():
        if hasattr(value, "model_dump"):
            _bind_paths(value, root)
        elif isinstance(value, str) and key.endswith(("_path", "_dir")) and value:
            path = Path(value)
            # Runtime files belong to this experiment; immutable config inputs are copied separately.
            parts = path.parts if not path.is_absolute() else ("inputs", path.name)
            setattr(model, key, str(root.joinpath(*parts)))


class OriginalV5Adapter:
    def __init__(self, cfg, *, sandbox: Path):
        self.root = sandbox.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.cfg = cfg.model_copy(deep=True)
        if self.cfg.alpha.ml_factor.enabled or self.cfg.alpha.dynamic_ic_weighting.enabled or self.cfg.alpha.dynamic_weights_by_regime_enabled:
            raise ValueError("baseline requires a frozen static weight/model version")
        _bind_paths(self.cfg, self.root)
        self.cfg.execution.mode, self.cfg.execution.dry_run = "dry_run", True
        self.cfg.execution.collect_ml_training_data = False
        self.cfg.participation.enabled = False
        self.cfg.decision_reference.enabled = False
        self.cfg.quant_lab.enabled = False  # Live reference is record-only; no research network fallbacks.
        self.cfg.exchange.api_key = self.cfg.exchange.api_secret = self.cfg.exchange.passphrase = ""
        self.current_time, self.pipeline = None, None
        self.last_observation = None

    @contextmanager
    def _environment(self, now):
        attempted = []

        def no_network(*args, **kwargs):
            attempted.append(True)
            raise RuntimeError("original V5 research adapter forbids network access")

        self.current_time = datetime.fromtimestamp(float(now), timezone.utc)
        old_cwd = Path.cwd()
        try:
            os.chdir(self.root)
            with ExitStack() as stack, patch.dict(os.environ, {"V5_WORKSPACE": str(self.root)}), patch("time.time", lambda: float(now)), patch("socket.socket.connect", no_network), _sandbox_reports_dir(self.root), redirect_stdout(StringIO()):
                # These constructors resolve input/cache roots from their module location.
                # Bind them before construction, including HMM's import-time root constant.
                for name in ("src.alpha.alpha_engine", "src.strategy.multi_strategy_system", "src.regime.regime_engine", "src.regime.ensemble_regime_engine", "src.regime.short_term_override", "src.regime.hmm_regime_detector"):
                    module = importlib.import_module(name)
                    stack.enter_context(patch.object(module, "__file__", str(self.root.joinpath(*name.split(".")).with_suffix(".py"))))
                    if hasattr(module, "PROJECT_ROOT"):
                        stack.enter_context(patch.object(module, "PROJECT_ROOT", self.root))
                yield
            if attempted:
                raise ValueError("baseline_input_incomplete_network_attempted")
        finally:
            os.chdir(old_cwd)

    def decide(self, *, snapshot, market_data, portfolio, regime_result: RegimeResult | None = None):
        now = snapshot["now_ts"]
        if self.last_observation is not None and now <= self.last_observation:
            raise ValueError("V5 baseline observations must increase strictly")
        positions = []
        for symbol, value in portfolio.positions.items():
            row = snapshot["symbols"][symbol]
            price = float(row["close"])
            qty = float(value["qty"])
            if qty <= 0:
                continue
            avg = float(value.get("entry_price", value["cash_cost"] / value["qty"]))
            highest = max(float(value.get("highest_px", avg)), price)
            value["highest_px"] = number(highest)
            iso = datetime.fromtimestamp(float(value["entry_ts"]), timezone.utc).isoformat()
            positions.append(Position(symbol=symbol, qty=qty, avg_px=avg, entry_ts=iso,
                                      highest_px=highest, last_update_ts=iso, last_mark_px=price,
                                      unrealized_pnl_pct=price / avg - 1,
                                      tags_json=json.dumps(value.get("metadata", {}))))
        with self._environment(now):
            if self.pipeline is None:
                self.pipeline = V5Pipeline(self.cfg, clock=SimpleNamespace(now=lambda: self.current_time))
                self.pipeline.alpha_engine.repo_root = self.root
                self.pipeline.regime_engine.repo_root = self.root
            equity_path = Path(self.cfg.execution.order_store_path).parent / "equity_validation.json"
            equity_path.parent.mkdir(parents=True, exist_ok=True)
            equity_path.write_text(json.dumps({"timestamp": now, "equity_valid": True,
                                               "equity_raw": float(portfolio.mark(snapshot["symbols"], now=now)["equity_usdt"])}), encoding="utf-8")
            audit = DecisionAudit(run_id=f"research-{int(now)}", now_ts=int(now), window_end_ts=snapshot["bar_ts"])
            result = self.pipeline.run(market_data_1h=market_data, positions=positions,
                                       cash_usdt=float(portfolio.cash), equity_peak_usdt=float(portfolio.peak),
                                       audit=audit, precomputed_regime=regime_result)
        self.last_observation = now
        return copy.deepcopy(result.orders), audit.to_dict()
