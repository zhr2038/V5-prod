from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import main as main_module
from configs.loader import load_config
from configs.schema import AppConfig, RegimeState
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline
from src.execution.fill_store import derive_runtime_auto_risk_eval_path
from src.regime.regime_engine import RegimeResult
from src.reporting.decision_audit import DecisionAudit
import src.core.pipeline as pipeline_module


def _write_config(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def _ms(ts_s: int) -> int:
    return ts_s * 1000


def _series(sym: str, close: float) -> MarketSeries:
    ts = [_ms(1_700_000_000 + i * 3600) for i in range(60)]
    close_arr = [close for _ in range(60)]
    vol = [1000.0 for _ in range(60)]
    return MarketSeries(
        symbol=sym,
        timeframe="1h",
        ts=ts,
        open=close_arr,
        high=close_arr,
        low=close_arr,
        close=close_arr,
        volume=vol,
    )


def _regime() -> RegimeResult:
    return RegimeResult(
        state=RegimeState.TRENDING,
        atr_pct=0.01,
        ma20=100.0,
        ma60=95.0,
        multiplier=1.0,
    )


def _write_auto_risk_level(order_store_path: str, level: str) -> None:
    path = derive_runtime_auto_risk_eval_path(order_store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"current_level": level}, ensure_ascii=False), encoding="utf-8")


def _build_pipe(cfg: AppConfig, tmp_path: Path) -> V5Pipeline:
    pipeline_module.REPORTS_DIR = tmp_path
    pipe = V5Pipeline(cfg)
    pipe.exit_policy.evaluate = lambda **kwargs: []
    pipe.stop_loss_manager.register_position = lambda *args, **kwargs: None
    pipe.stop_loss_manager.evaluate_stop = lambda *args, **kwargs: (False, 0.0, "", 0.0)
    pipe.fixed_stop_loss.register_position = lambda *args, **kwargs: None
    pipe.fixed_stop_loss.should_stop_loss = lambda *args, **kwargs: (False, 0.0, 0.0)
    pipe.portfolio_engine._load_fused_signals = lambda: {}
    pipe.data_collector.collect_features = lambda **kwargs: None
    pipe.data_collector.fill_labels = lambda current_ts: 0
    pipe.alpha_engine.get_latest_strategy_signal_payload = lambda: {"strategies": []}
    pipe.alpha_engine.strategy_signals_path = lambda: tmp_path / "reports" / "runs" / "test" / "strategy_signals.json"
    pipe.profit_taking.positions = {}
    return pipe


def test_runtime_reads_false_values_from_yaml(tmp_path: Path) -> None:
    cfg_path = _write_config(
        tmp_path / "configs" / "protect_false.yaml",
        """
symbols:
  - BTC/USDT
execution:
  mode: live
  order_store_path: reports/orders.sqlite
  protect_entry_require_alpha6_confirmation: false
  protect_entry_block_trend_only: false
  protect_entry_require_alpha6_rsi_confirm_positive: false
  protect_entry_alpha6_min_score: 0.05
""".strip()
        + "\n",
    )

    cfg = load_config(str(cfg_path))

    assert cfg.execution.protect_entry_require_alpha6_confirmation is False
    assert cfg.execution.protect_entry_block_trend_only is False
    assert cfg.execution.protect_entry_require_alpha6_rsi_confirm_positive is False
    assert cfg.execution.protect_entry_alpha6_min_score == 0.05


def test_effective_live_config_writes_explicit_true_values(tmp_path: Path) -> None:
    cfg_path = _write_config(
        tmp_path / "configs" / "protect_true.yaml",
        f"""
symbols:
  - BTC/USDT
execution:
  mode: live
  order_store_path: {str((tmp_path / 'reports' / 'orders.sqlite').resolve())}
  protect_entry_require_alpha6_confirmation: true
  protect_entry_block_trend_only: true
  protect_entry_require_alpha6_rsi_confirm_positive: true
  protect_entry_alpha6_min_score: 0.10
""".strip()
        + "\n",
    )

    cfg = load_config(str(cfg_path))
    out_path = main_module._write_effective_live_config(cfg)
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    assert payload["execution"]["protect_entry_require_alpha6_confirmation"] is True
    assert payload["execution"]["protect_entry_block_trend_only"] is True
    assert payload["execution"]["protect_entry_require_alpha6_rsi_confirm_positive"] is True
    assert payload["execution"]["protect_entry_alpha6_min_score"] == 0.10


def test_effective_live_config_writes_final_defaults_when_yaml_omits_keys(tmp_path: Path) -> None:
    cfg_path = _write_config(
        tmp_path / "configs" / "protect_default.yaml",
        f"""
symbols:
  - BTC/USDT
execution:
  mode: live
  order_store_path: {str((tmp_path / 'reports' / 'orders.sqlite').resolve())}
""".strip()
        + "\n",
    )

    cfg = load_config(str(cfg_path))
    out_path = main_module._write_effective_live_config(cfg)
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    assert payload["execution"]["protect_entry_require_alpha6_confirmation"] is True
    assert payload["execution"]["protect_entry_block_trend_only"] is True
    assert payload["execution"]["protect_entry_require_alpha6_rsi_confirm_positive"] is True
    assert payload["execution"]["protect_entry_alpha6_min_score"] == 0.40


def test_decision_audit_records_protect_entry_gate_configuration_each_round(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.order_store_path = str((tmp_path / "reports" / "orders.sqlite").resolve())
    cfg.execution.protect_entry_require_alpha6_confirmation = False
    cfg.execution.protect_entry_block_trend_only = False
    cfg.execution.protect_entry_require_alpha6_rsi_confirm_positive = True
    cfg.execution.protect_entry_alpha6_min_score = 0.25
    cfg.execution.negative_expectancy_cooldown_enabled = False
    cfg.execution.negative_expectancy_score_penalty_enabled = False
    cfg.execution.negative_expectancy_open_block_enabled = False
    cfg.execution.negative_expectancy_fast_fail_open_block_enabled = False

    _write_auto_risk_level(cfg.execution.order_store_path, "PROTECT")

    pipe = _build_pipe(cfg, tmp_path)
    pipe.portfolio_engine.allocate = lambda scores, market_data, regime_mult, audit=None: SimpleNamespace(
        target_weights={},
        selected=[],
        entry_candidates=[],
        volatilities={},
        notes="",
    )
    audit = DecisionAudit(run_id="protect-config-audit")

    pipe.run(
        market_data_1h={"BTC/USDT": _series("BTC/USDT", 50000.0)},
        positions=[],
        cash_usdt=100.0,
        equity_peak_usdt=100.0,
        audit=audit,
        precomputed_alpha=AlphaSnapshot(raw_factors={}, z_factors={}, scores={"BTC/USDT": 1.0}),
        precomputed_regime=_regime(),
    )

    assert audit.protect_entry_gate_active is True
    assert audit.protect_entry_require_alpha6_confirmation is False
    assert audit.protect_entry_block_trend_only is False
    assert audit.protect_entry_require_alpha6_rsi_confirm_positive is True
    assert audit.protect_entry_alpha6_min_score == 0.25
