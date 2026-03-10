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
    assert "Environment=V5_ML_FEATURE_SELECTOR=stable" in text
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
