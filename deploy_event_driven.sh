#!/usr/bin/env bash
#
# Safe deployment script for event-driven trading
# Phase 1: Parallel mode (event-driven logs, standard V5 executes)
#

set -e

ROOT="/home/admin/clawd/v5-trading-bot"
cd "$ROOT"

echo "=================================="
echo "Event-Driven Trading Deployment"
echo "=================================="
echo ""

# Backup current config
echo "[1/5] Backing up current configuration..."
cp configs/live_20u_real.yaml configs/live_20u_real.yaml.backup.$(date +%Y%m%d_%H%M%S)
echo "✅ Backup created"

# Add event-driven config (disabled by default)
echo ""
echo "[2/5] Adding event-driven configuration..."
if ! grep -q "event_driven:" configs/live_20u_real.yaml; then
    cat >> configs/live_20u_real.yaml << 'EOF'

# Event-Driven Trading Configuration
event_driven:
  enabled: false  # Set to true to enable (Phase 1: log only)
  check_interval_minutes: 15
  global_cooldown_p2_minutes: 30
  symbol_cooldown_minutes: 60
  signal_confirmation_periods: 2
  score_change_threshold: 0.30
  rank_jump_threshold: 3
  breakout_enabled: true
  breakout_lookback_hours: 24
  breakout_threshold_pct: 0.5
  heartbeat_interval_hours: 4
EOF
    echo "✅ Event-driven config added (disabled)"
else
    echo "✅ Event-driven config already exists"
fi

# Create event-driven timer (disabled initially)
echo ""
echo "[3/5] Setting up event-driven timer..."
cp deploy/systemd/v5-event-driven.timer ~/.config/systemd/user/
cp deploy/systemd/v5-reconcile.user.service ~/.config/systemd/user/v5-event-driven.service 2>/dev/null || true

# Modify the service to run our check script
cat > ~/.config/systemd/user/v5-event-driven.service << 'EOF'
[Unit]
Description=V5 Event-Driven Trading Check (15min)

[Service]
Type=oneshot
WorkingDirectory=/home/admin/clawd/v5-trading-bot
Environment=PYTHONPATH=/home/admin/clawd/v5-trading-bot
EnvironmentFile=/home/admin/clawd/v5-trading-bot/.env
ExecStart=/home/admin/clawd/v5-trading-bot/.venv/bin/python /home/admin/clawd/v5-trading-bot/event_driven_check.py
EOF

echo "✅ Timer configured"

# Reload systemd
echo ""
echo "[4/5] Reloading systemd..."
systemctl --user daemon-reload
echo "✅ Systemd reloaded"

# Start event-driven timer (parallel with existing)
echo ""
echo "[5/5] Starting event-driven timer..."
systemctl --user enable v5-event-driven.timer
systemctl --user start v5-event-driven.timer
echo "✅ Event-driven timer started"

echo ""
echo "=================================="
echo "Deployment Complete!"
echo "=================================="
echo ""
echo "Current Status:"
echo "  - Standard V5: $(systemctl --user is-active v5-live-20u.user.timer)"
echo "  - Event-Driven: $(systemctl --user is-active v5-event-driven.timer)"
echo ""
echo "Mode: PARALLEL (Phase 1)"
echo "  - Event-driven logs to: reports/event_driven_log.jsonl"
echo "  - Standard V5 continues to execute trades"
echo "  - Event-driven config: enabled=false (log only)"
echo ""
echo "To enable active mode (Phase 2):"
echo "  1. Edit configs/live_20u_real.yaml"
echo "  2. Set event_driven.enabled: true"
echo "  3. Restart timer: systemctl --user restart v5-event-driven.timer"
echo ""
echo "To rollback:"
echo "  ./rollback_event_driven.sh"
