#!/usr/bin/env bash
#
# Safe deployment script for event-driven trading
# Phase 1: Parallel mode (event-driven logs, standard V5 executes)
#

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

CONFIG_FILE="${V5_CONFIG:-}"
if [[ -z "$CONFIG_FILE" ]]; then
  if [[ -f "$ROOT/configs/live_prod.yaml" ]]; then
    CONFIG_FILE="$ROOT/configs/live_prod.yaml"
  elif [[ -f "$ROOT/configs/config.yaml" ]]; then
    CONFIG_FILE="$ROOT/configs/config.yaml"
  else
    CONFIG_FILE="$ROOT/configs/live_prod.yaml"
  fi
elif [[ "$CONFIG_FILE" != /* ]]; then
  CONFIG_FILE="$ROOT/$CONFIG_FILE"
fi

STANDARD_TIMER_UNIT="v5-prod.user.timer"

if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
else
  PYTHON_BIN="$(command -v python)"
fi

echo "=================================="
echo "Event-Driven Trading Deployment"
echo "=================================="
echo ""

# Backup current config
echo "[1/5] Backing up current configuration..."
cp "$CONFIG_FILE" "$CONFIG_FILE.backup.$(date +%Y%m%d_%H%M%S)"
echo "✅ Backup created"

# Add event-driven config (disabled by default)
echo ""
echo "[2/5] Adding event-driven configuration..."
if ! grep -q "event_driven:" "$CONFIG_FILE"; then
    cat >> "$CONFIG_FILE" << 'EOF'

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
mkdir -p ~/.config/systemd/user
"$PYTHON_BIN" "$ROOT/deploy/render_systemd_units.py" \
    --src-dir "$ROOT/deploy/systemd" \
    --dst-dir "$HOME/.config/systemd/user" \
    --root "$ROOT" \
    --mapping v5-event-driven.service=v5-event-driven.service \
    --mapping v5-event-driven.timer=v5-event-driven.timer

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
echo "  - Standard V5: $(systemctl --user is-active "$STANDARD_TIMER_UNIT" 2>/dev/null || echo 'unknown')"
echo "  - Event-Driven: $(systemctl --user is-active v5-event-driven.timer)"
echo ""
echo "Mode: PARALLEL (Phase 1)"
echo "  - Event-driven logs to: reports/event_driven_log.jsonl"
echo "  - Standard V5 continues to execute trades"
echo "  - Event-driven config: enabled=false (log only)"
echo "  - Config file: $CONFIG_FILE"
echo ""
echo "To enable active mode (Phase 2):"
echo "  1. Edit $(basename "$CONFIG_FILE")"
echo "  2. Set event_driven.enabled: true"
echo "  3. Restart timer: systemctl --user restart v5-event-driven.timer"
echo ""
echo "To rollback:"
echo "  ./rollback_event_driven.sh"
