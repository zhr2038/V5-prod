import json
from pathlib import Path

from configs.schema import AlphaConfig, RiskConfig
import src.portfolio.portfolio_engine as portfolio_engine_module
from src.portfolio.portfolio_engine import PortfolioEngine
from src.core.models import MarketSeries


def test_portfolio_caps_single_weight():
    pe = PortfolioEngine(alpha_cfg=AlphaConfig(long_top_pct=0.5), risk_cfg=RiskConfig(max_single_weight=0.25))
    scores = {"A/USDT": 10.0, "B/USDT": 9.0, "C/USDT": 1.0, "D/USDT": 0.0}

    md = {}
    for s in scores.keys():
        md[s] = MarketSeries(symbol=s, timeframe="1h", ts=list(range(200)), open=[1.0]*200, high=[1.0]*200, low=[1.0]*200, close=[1.0 + i*0.0001 for i in range(200)], volume=[1000.0]*200)

    snap = pe.allocate(scores=scores, market_data=md, regime_mult=1.0)
    assert snap.target_weights
    assert all(w <= 0.25 + 1e-9 for w in snap.target_weights.values())


def test_topk_dropout_reorders_before_cap_and_persists_final_selection(tmp_path: Path):
    alpha_cfg = AlphaConfig(long_top_pct=0.8, optimizer_enabled=False)
    alpha_cfg.dynamic_ic_weighting.enabled = False
    alpha_cfg.topk_dropout.state_path = str(tmp_path / "topk_dropout_state.json")
    pe = PortfolioEngine(alpha_cfg=alpha_cfg, risk_cfg=RiskConfig(max_single_weight=0.25, max_positions_override=3))

    state_path = Path(alpha_cfg.topk_dropout.state_path)
    state_path.write_text(
        json.dumps(
            {
                "selected": ["ETH/USDT", "BNB/USDT", "HYPE/USDT", "OKB/USDT"],
                "hold_cycles": {
                    "ETH/USDT": 3,
                    "BNB/USDT": 3,
                    "HYPE/USDT": 3,
                    "OKB/USDT": 3,
                },
                "updated_ts": 0,
            }
        ),
        encoding="utf-8",
    )

    scores = {
        "OKB/USDT": 1.57,
        "HYPE/USDT": 1.31,
        "SUI/USDT": 1.00,
        "BNB/USDT": 0.86,
        "ETH/USDT": 0.46,
    }
    md = {}
    for sym in scores:
        md[sym] = MarketSeries(
            symbol=sym,
            timeframe="1h",
            ts=list(range(200)),
            open=[1.0] * 200,
            high=[1.0] * 200,
            low=[1.0] * 200,
            close=[1.0 + i * 0.0001 for i in range(200)],
            volume=[1000.0] * 200,
        )

    snap = pe.allocate(scores=scores, market_data=md, regime_mult=1.0)

    assert snap.entry_candidates == ["OKB/USDT", "HYPE/USDT", "SUI/USDT"]
    assert snap.selected == ["OKB/USDT", "HYPE/USDT", "SUI/USDT"]
    assert "ETH/USDT" not in snap.target_weights

    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["selected"] == ["OKB/USDT", "HYPE/USDT", "SUI/USDT"]


def test_portfolio_fused_selection_respects_lower_alpha_adjusted_score(tmp_path: Path, monkeypatch):
    alpha_cfg = AlphaConfig(long_top_pct=0.5, use_fused_score_for_weighting=True)
    alpha_cfg.topk_dropout.enabled = False
    pe = PortfolioEngine(alpha_cfg=alpha_cfg, risk_cfg=RiskConfig(max_single_weight=0.5))
    pe.set_run_id("fused-adjusted")

    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "live_prod.yaml").write_text(
        "execution:\n  order_store_path: reports/orders.sqlite\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("V5_WORKSPACE", str(tmp_path))

    run_dir = tmp_path / "reports" / "runs" / "fused-adjusted"
    run_dir.mkdir(parents=True)
    (run_dir / "strategy_signals.json").write_text(
        json.dumps(
            {
                "fused": {
                    "OKB/USDT": {"direction": "buy", "score": 1.20},
                    "HYPE/USDT": {"direction": "buy", "score": 0.90},
                }
            }
        ),
        encoding="utf-8",
    )

    md = {}
    for sym in ("OKB/USDT", "HYPE/USDT"):
        md[sym] = MarketSeries(
            symbol=sym,
            timeframe="1h",
            ts=list(range(200)),
            open=[1.0] * 200,
            high=[1.0] * 200,
            low=[1.0] * 200,
            close=[1.0 + i * 0.0001 for i in range(200)],
            volume=[1000.0] * 200,
        )

    snap = pe.allocate(
        scores={"OKB/USDT": 0.05, "HYPE/USDT": 0.80},
        market_data=md,
        regime_mult=1.0,
    )

    assert snap.selected == ["HYPE/USDT"]


def test_portfolio_optimizer_respects_zero_prev_weight_penalty(tmp_path: Path):
    alpha_cfg = AlphaConfig(long_top_pct=1.0, optimizer_enabled=True, optimizer_prev_weight_penalty=0.0)
    alpha_cfg.optimizer_state_path = str(tmp_path / "optimizer_state.json")
    pe = PortfolioEngine(alpha_cfg=alpha_cfg, risk_cfg=RiskConfig(max_single_weight=1.0))

    state_path = Path(alpha_cfg.optimizer_state_path)
    state_path.write_text(
        json.dumps(
            {
                "weights": {
                    "A/USDT": 0.0,
                    "B/USDT": 1.0,
                },
                "updated_ts": 0,
            }
        ),
        encoding="utf-8",
    )

    md = {}
    for sym in ("A/USDT", "B/USDT"):
        md[sym] = MarketSeries(
            symbol=sym,
            timeframe="1h",
            ts=list(range(200)),
            open=[1.0] * 200,
            high=[1.0] * 200,
            low=[1.0] * 200,
            close=[1.0 + i * 0.0001 for i in range(200)],
            volume=[1000.0] * 200,
        )

    snap = pe.allocate(
        scores={"A/USDT": 10.0, "B/USDT": 1.0},
        market_data=md,
        regime_mult=1.0,
    )

    assert snap.target_weights["A/USDT"] > 0.99
    assert snap.target_weights["B/USDT"] < 0.01


def test_portfolio_dynamic_max_positions_uses_active_runtime_eval(monkeypatch, tmp_path: Path):
    fake_root = tmp_path / "repo"
    reports_dir = fake_root / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    configs_dir = fake_root / "configs"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)

    (reports_dir / "auto_risk_eval.json").write_text(
        json.dumps({"current_level": "ATTACK"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (runtime_dir / "auto_risk_eval.json").write_text(
        json.dumps({"current_level": "PROTECT"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(portfolio_engine_module, "RUNTIME_PROJECT_ROOT", fake_root)

    pe = PortfolioEngine(alpha_cfg=AlphaConfig(), risk_cfg=RiskConfig())

    assert pe._get_dynamic_max_positions() == 1


def test_portfolio_runtime_defaults_follow_active_order_store(monkeypatch, tmp_path: Path):
    workspace = tmp_path / "workspace"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_runs = reports_dir / "runs" / "runtime-run"
    runtime_runs = runtime_dir / "runs" / "runtime-run"
    configs_dir = workspace / "configs"
    runtime_runs.mkdir(parents=True, exist_ok=True)
    root_runs.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)

    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (root_runs / "strategy_signals.json").write_text(
        json.dumps({"fused": {"ROOT/USDT": {"direction": "buy", "score": 9.9}}}),
        encoding="utf-8",
    )
    (runtime_runs / "strategy_signals.json").write_text(
        json.dumps({"fused": {"RUNTIME/USDT": {"direction": "buy", "score": 0.7}}}),
        encoding="utf-8",
    )
    (reports_dir / "portfolio_optimizer_state.json").write_text(
        json.dumps({"weights": {"ROOT/USDT": 0.1}, "updated_ts": 1}),
        encoding="utf-8",
    )
    (runtime_dir / "portfolio_optimizer_state.json").write_text(
        json.dumps({"weights": {"RUNTIME/USDT": 0.9}, "updated_ts": 2}),
        encoding="utf-8",
    )
    (reports_dir / "topk_dropout_state.json").write_text(
        json.dumps({"selected": ["ROOT/USDT"], "hold_cycles": {"ROOT/USDT": 3}, "updated_ts": 1}),
        encoding="utf-8",
    )
    (runtime_dir / "topk_dropout_state.json").write_text(
        json.dumps({"selected": ["RUNTIME/USDT"], "hold_cycles": {"RUNTIME/USDT": 4}, "updated_ts": 2}),
        encoding="utf-8",
    )

    monkeypatch.setenv("V5_WORKSPACE", str(workspace))

    pe = PortfolioEngine(alpha_cfg=AlphaConfig(), risk_cfg=RiskConfig())
    pe.set_run_id("runtime-run")

    fused = pe._load_fused_signals()
    optimizer_state = pe._load_optimizer_state()
    topk_state = pe._load_topk_state()

    assert fused == {"RUNTIME/USDT": 0.7}
    assert optimizer_state["weights"] == {"RUNTIME/USDT": 0.9}
    assert topk_state["selected"] == ["RUNTIME/USDT"]

    pe._save_optimizer_state({"SAVED/USDT": 0.5})
    pe._save_topk_state(["SAVED/USDT"], {"SAVED/USDT": 1})

    runtime_optimizer = json.loads((runtime_dir / "portfolio_optimizer_state.json").read_text(encoding="utf-8"))
    runtime_topk = json.loads((runtime_dir / "topk_dropout_state.json").read_text(encoding="utf-8"))
    root_optimizer = json.loads((reports_dir / "portfolio_optimizer_state.json").read_text(encoding="utf-8"))
    root_topk = json.loads((reports_dir / "topk_dropout_state.json").read_text(encoding="utf-8"))

    assert runtime_optimizer["weights"] == {"SAVED/USDT": 0.5}
    assert runtime_topk["selected"] == ["SAVED/USDT"]
    assert root_optimizer["weights"] == {"ROOT/USDT": 0.1}
    assert root_topk["selected"] == ["ROOT/USDT"]
