from pathlib import Path


def test_v5_prod_service_runs_auto_sync_from_prod_workspace():
    text = Path("deploy/systemd/v5-prod.user.service").read_text(encoding="utf-8")
    assert "WorkingDirectory=/home/admin/clawd/v5-prod" in text
    assert "Environment=V5_WORKSPACE=/home/admin/clawd/v5-prod" in text
    assert "ExecStartPre=-/bin/bash -lc 'cd /home/admin/clawd/v5-prod && PYTHONPATH=. /home/admin/clawd/v5-prod/.venv/bin/python scripts/auto_sync_before_trade.py'" in text
    assert "ExecStart=/usr/bin/flock -n /tmp/v5_live_prod_hourly.lock -c \"/bin/bash -lc '/home/admin/clawd/v5-prod/scripts/run_hourly_live_window.sh'\"" in text


def test_v5_daily_ml_training_service_targets_prod_workspace():
    text = Path("deploy/systemd/v5-daily-ml-training.service").read_text(encoding="utf-8")
    assert "WorkingDirectory=/home/admin/clawd/v5-prod" in text
    assert "Environment=PYTHONPATH=/home/admin/clawd/v5-prod" in text
    assert "Environment=V5_WORKSPACE=/home/admin/clawd/v5-prod" in text
    assert "Environment=V5_ML_TARGET_MODE=forward_edge_rank" in text
    assert "Environment=V5_ML_CANDIDATES=ridge" in text
    assert "Environment=V5_ML_RIDGE_ALPHA=50" in text
    assert "Environment=V5_ML_MIN_SYMBOL_SAMPLES=48" in text
    assert "Environment=V5_ML_MIN_GROUP_SIZE=2" in text
    assert "Environment=V5_ML_MIN_GROUP_COVERAGE_RATIO=0.9" in text
    assert "Environment=V5_ML_FEATURE_SELECTOR=stable" in text
    assert "Environment=V5_ML_ROLLING_WINDOW_DAYS=10" in text
    assert "Environment=V5_ML_RECENCY_HALFLIFE_DAYS=5" in text
    assert "Environment=V5_ML_RECENCY_MAX_WEIGHT=3.0" in text
    assert "ExecStartPre=-/bin/bash -lc 'cd /home/admin/clawd/v5-prod && source .venv/bin/activate && python scripts/backfill_ml_multihorizon_labels.py'" in text
    assert "ExecStart=/bin/bash -lc 'cd /home/admin/clawd/v5-prod && source .venv/bin/activate && python scripts/daily_ml_training.py'" in text


def test_v5_daily_ml_training_timer_points_to_service():
    text = Path("deploy/systemd/v5-daily-ml-training.timer").read_text(encoding="utf-8")
    assert "OnCalendar=*-*-* 00:30:00" in text
    assert "Unit=v5-daily-ml-training.service" in text


def test_v5_model_promotion_gate_service_targets_prod_workspace():
    text = Path("deploy/systemd/v5-model-promotion-gate.service").read_text(encoding="utf-8")
    assert "WorkingDirectory=/home/admin/clawd/v5-prod" in text
    assert "SuccessExitStatus=2" in text
    assert "Environment=PYTHONPATH=/home/admin/clawd/v5-prod" in text
    assert "ExecStart=/bin/bash -lc 'cd /home/admin/clawd/v5-prod && source .venv/bin/activate && python scripts/model_promotion_gate.py'" in text


def test_v5_model_promotion_gate_timer_points_to_service():
    text = Path("deploy/systemd/v5-model-promotion-gate.timer").read_text(encoding="utf-8")
    assert "OnCalendar=*-*-* 00:40:00" in text
    assert "Unit=v5-model-promotion-gate.service" in text


def test_v5_web_dashboard_service_enables_live_okx_health_check():
    text = Path("deploy/systemd/v5-web-dashboard.service").read_text(encoding="utf-8")
    assert "Environment=V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT=1" in text


def test_v5_reconcile_service_targets_live_prod_config():
    text = Path("deploy/systemd/v5-reconcile.user.service").read_text(encoding="utf-8")
    assert "WorkingDirectory=/home/admin/clawd/v5-trading-bot" in text
    assert "scripts/reconcile_guard_once.py --config configs/live_prod.yaml --env .env --out reports/reconcile_status.json" in text


def test_v5_ledger_service_targets_live_prod_config():
    text = Path("deploy/systemd/v5-ledger.user.service").read_text(encoding="utf-8")
    assert "WorkingDirectory=/home/admin/clawd/v5-trading-bot" in text
    assert "scripts/bills_sync.py --config configs/live_prod.yaml --env .env --db reports/bills.sqlite" in text
    assert "scripts/ledger_once.py --config configs/live_prod.yaml --env .env --bills-db reports/bills.sqlite --out reports/ledger_status.json" in text


def test_install_systemd_production_only_disables_shadow_timers():
    text = Path("deploy/install_systemd.sh").read_text(encoding="utf-8")
    assert "disable --now v5-shadow-tuned-xgboost.user.timer v5-shadow-tuned-xgboost.user.service" in text
    assert "disable --now v5-shadow-regime.user.timer v5-shadow-regime.user.service" in text


def test_install_systemd_production_only_enables_trade_monitor_timer():
    text = Path("deploy/install_systemd.sh").read_text(encoding="utf-8")
    assert "--mapping v5-trade-monitor.service=v5-trade-monitor.service" in text
    assert "--mapping v5-trade-monitor.timer=v5-trade-monitor.timer" in text
    assert "systemctl --user enable --now v5-trade-monitor.timer" in text


def test_install_systemd_production_only_restarts_web_dashboard_service():
    text = Path("deploy/install_systemd.sh").read_text(encoding="utf-8")
    assert "systemctl --user enable --now v5-web-dashboard.service" in text
    assert "systemctl --user restart v5-web-dashboard.service" in text


def test_install_systemd_production_only_enables_spread_rollup_timer():
    text = Path("deploy/install_systemd.sh").read_text(encoding="utf-8")
    assert "--mapping v5-spread-rollup.user.service=v5-spread-rollup.service" in text
    assert "--mapping v5-spread-rollup.timer=v5-spread-rollup.timer" in text
    assert "systemctl --user enable --now v5-spread-rollup.timer" in text
