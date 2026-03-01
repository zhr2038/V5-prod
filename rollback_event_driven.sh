#!/usr/bin/env bash
#
# Rollback script for event-driven trading
#

set -e

echo "=================================="
echo "Event-Driven Trading Rollback"
echo "=================================="
echo ""

echo "[1/3] Stopping event-driven timer..."
systemctl --user stop v5-event-driven.timer 2>/dev/null || true
systemctl --user disable v5-event-driven.timer 2>/dev/null || true
echo "✅ Event-driven timer stopped"

echo ""
echo "[2/3] Restoring configuration..."
# Remove event-driven config section
CONFIG_FILE="/home/admin/clawd/v5-trading-bot/configs/live_20u_real.yaml"
if grep -q "event_driven:" "$CONFIG_FILE"; then
    # Create temp file without event_driven section
    head -n $(grep -n "event_driven:" "$CONFIG_FILE" | head -1 | cut -d: -f1) "$CONFIG_FILE" > /tmp/config_restore.yaml
    mv /tmp/config_restore.yaml "$CONFIG_FILE"
    echo "✅ Event-driven config removed"
fi

echo ""
echo "[3/3] Cleaning up..."
rm -f ~/.config/systemd/user/v5-event-driven.timer
rm -f ~/.config/systemd/user/v5-event-driven.service
systemctl --user daemon-reload
echo "✅ Cleanup complete"

echo ""
echo "=================================="
echo "Rollback Complete!"
echo "=================================="
echo ""
echo "Current Status:"
echo "  - Standard V5: $(systemctl --user is-active v5-live-20u.user.timer)"
echo "  - Event-Driven: stopped"
echo ""
echo "System restored to original state."
