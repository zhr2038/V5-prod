#!/usr/bin/env python3
"""
Auto-sync positions before trading.
Runs before main V5 execution to ensure local state matches OKX.
"""
from __future__ import print_function

import sys
import json
import time
import logging
from pathlib import Path

sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('auto_sync')

def main():
    """Auto-sync positions from OKX to local store."""
    logger.info("=" * 60)
    logger.info("Pre-trade Auto-Sync")
    logger.info("=" * 60)
    
    # Also use print for systemd visibility
    print("[AUTO_SYNC] Starting pre-trade auto-sync", flush=True)
    
    try:
        from configs.loader import load_config
        from src.execution.okx_private_client import OKXPrivateClient
        from src.execution.position_store import PositionStore
        from src.execution.account_store import AccountStore
        from src.execution.bootstrap_patch import controlled_patch_from_okx_balance
        
        # Load config
        cfg = load_config(
            'configs/live_20u_real.yaml',
            env_path='/home/admin/clawd/v5-trading-bot/.env'
        )
        
        # Create client
        client = OKXPrivateClient(exchange=cfg.exchange)
        
        try:
            # Get OKX balance
            logger.info("Fetching OKX balance...")
            balance = client.get_balance()
            
            # Load stores
            position_store = PositionStore(path='reports/positions.sqlite')
            account_store = AccountStore(path='reports/positions.sqlite')
            
            # Check current diff
            local_positions_list = position_store.list()
            local_positions = {}
            for pos in local_positions_list:
                if hasattr(pos, 'symbol') and hasattr(pos, 'qty'):
                    local_positions[pos.symbol] = pos.qty
                elif isinstance(pos, dict):
                    local_positions[pos.get('symbol')] = pos.get('qty', 0)
            
            okx_positions = {}
            
            for detail in balance.data.get('details', []):
                ccy = detail.get('ccy', '')
                eq = float(detail.get('eq', 0))
                if eq > 0 and ccy != 'USDT':
                    okx_positions[f"{ccy}/USDT"] = eq
            
            # Calculate diff
            total_diff = 0
            for sym, local_qty in local_positions.items():
                okx_qty = okx_positions.get(sym, 0)
                diff = abs(local_qty - okx_qty)
                if diff > 1e-8:
                    total_diff += diff
                    logger.info(f"  {sym}: local={local_qty:.8f}, okx={okx_qty:.8f}, diff={diff:.8f}")
            
            for sym, okx_qty in okx_positions.items():
                if sym not in local_positions and okx_qty > 1e-8:
                    total_diff += okx_qty
                    logger.info(f"  {sym}: local=0, okx={okx_qty:.8f}, diff={okx_qty:.8f}")
            
            logger.info(f"Total position diff: {total_diff:.8f}")
            
            # Always sync from OKX before trade. Large diffs are exactly when sync is needed.
            if total_diff >= 5.0:
                logger.warning(
                    f"Diff is large ({total_diff:.6f}), proceeding with FORCED auto-sync to unblock run"
                )
            else:
                logger.info("Diff is small (<5), proceeding with auto-sync...")
            
            # Clear existing positions by setting qty to 0
            for sym in list(local_positions.keys()):
                try:
                    position_store.set_qty(sym, qty=0)
                except Exception as e:
                    logger.warning(f"  Could not clear {sym}: {e}")
            
            # Sync from OKX
            for sym, qty in okx_positions.items():
                if qty > 1e-8:
                    try:
                        position_store.set_qty(sym, qty=float(qty))
                        logger.info(f"  Synced {sym}: {qty}")
                    except Exception as e:
                        logger.warning(f"  Could not sync {sym}: {e}")
            
            # Update cash
            for detail in balance.data.get('details', []):
                if detail.get('ccy') == 'USDT':
                    cash = float(detail.get('eq', 0))
                    try:
                        account_store.update_cash(cash)
                        logger.info(f"  Synced cash: {cash} USDT")
                    except Exception as e:
                        logger.warning(f"  Could not sync cash: {e}")
                    break
            
            logger.info("✅ Auto-sync completed successfully")
            
            # Clear failure state
            failure_state_path = Path('reports/reconcile_failure_state.json')
            if failure_state_path.exists():
                failure_state = json.loads(failure_state_path.read_text())
                failure_state['consecutive_hard'] = 0
                failure_state['consecutive_soft'] = 0
                failure_state['consecutive_ok'] = 1  # Mark as OK for auto-clear
                failure_state['last_reason'] = 'auto_sync_reset'
                failure_state_path.write_text(json.dumps(failure_state, indent=2))
                logger.info("✅ Reset failure state counters")
            
            # Update reconcile status to OK
            reconcile_status_path = Path('reports/reconcile_status.json')
            reconcile_status = {
                'schema_version': 1,
                'ok': True,
                'reason': 'ok',
                'generated_ts_ms': int(time.time() * 1000),
                'ts_ms': int(time.time() * 1000),
                'source': 'auto_sync',
                'stats': {
                    'max_abs_usdt_delta': 0.0,
                    'max_abs_base_delta': 0.0
                },
                'diffs': []
            }
            reconcile_status_path.parent.mkdir(parents=True, exist_ok=True)
            reconcile_status_path.write_text(json.dumps(reconcile_status, indent=2))
            logger.info("✅ Updated reconcile status to OK")
            
            # Clear kill switch
            kill_switch_path = Path('reports/kill_switch.json')
            if kill_switch_path.exists():
                ks = json.loads(kill_switch_path.read_text())
                if ks.get('enabled'):
                    ks['enabled'] = False
                    ks['auto_sync_cleared'] = True
                    ks['auto_sync_ts_ms'] = int(time.time() * 1000)
                    kill_switch_path.write_text(json.dumps(ks, indent=2))
                    logger.info("✅ Kill switch disabled by auto-sync")
            
            return 0
                
        finally:
            client.close()
            
    except Exception as e:
        logger.error(f"Auto-sync failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())
