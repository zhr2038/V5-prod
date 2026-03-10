from pathlib import Path


def test_v5_prod_service_runs_auto_sync_from_prod_workspace():
    text = Path("deploy/systemd/v5-prod.user.service").read_text(encoding="utf-8")
    assert "WorkingDirectory=/home/admin/clawd/v5-prod" in text
    assert "Environment=V5_WORKSPACE=/home/admin/clawd/v5-prod" in text
    assert "ExecStartPre=/bin/bash -lc 'cd /home/admin/clawd/v5-prod && PYTHONPATH=. /home/admin/clawd/v5-prod/.venv/bin/python scripts/auto_sync_before_trade.py'" in text
    assert "ExecStart=/usr/bin/flock -n /tmp/v5_live_prod_hourly.lock -c \"/bin/bash -lc '/home/admin/clawd/v5-prod/scripts/run_hourly_live_window.sh'\"" in text
