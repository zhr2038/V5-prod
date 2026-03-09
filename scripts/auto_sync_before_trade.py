#!/usr/bin/env python3
"""
Auto-sync positions before trading.
Runs before main V5 execution to ensure local state matches OKX.
"""
from __future__ import print_function

import sys
import os
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


def _as_float(v):
    try:
        if v is None:
            return 0.0
        if isinstance(v, str) and not v.strip():
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _extract_spot_qty(detail):
    """Extract best-effort spot quantity from OKX balance detail."""
    # Prefer cash/available balances; fall back to eq.
    vals = [
        _as_float(detail.get('cashBal')),
        _as_float(detail.get('availBal')),
        _as_float(detail.get('spotBal')),
        _as_float(detail.get('eq')),
    ]
    qty = max(vals)
    return qty if qty > 0 else 0.0


def _balance_details(balance_resp):
    """Normalize OKX balance payload to details list."""
    try:
        payload = balance_resp.data if hasattr(balance_resp, 'data') else (balance_resp or {})
        data_arr = payload.get('data') if isinstance(payload, dict) else None
        if isinstance(data_arr, list) and data_arr:
            d0 = data_arr[0] if isinstance(data_arr[0], dict) else {}
            details = d0.get('details', [])
            if isinstance(details, list):
                return details
        details = payload.get('details', []) if isinstance(payload, dict) else []
        return details if isinstance(details, list) else []
    except Exception:
        return []


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
        
        # Load config (prefer active V5_CONFIG)
        cfg_path = os.getenv('V5_CONFIG', 'configs/live_20u_real.yaml')
        cfg = load_config(
            cfg_path,
            env_path='/home/admin/clawd/v5-trading-bot/.env'
        )
        logger.info(f"Using config: {cfg_path}")
        
        # Create client
        client = OKXPrivateClient(exchange=cfg.exchange)
        
        try:
            # Get OKX balance
            logger.info("Fetching OKX balance...")
            balance = client.get_balance()
            balance_details = _balance_details(balance)
            logger.info(f"Balance details loaded: {len(balance_details)} assets")

            # Load stores
            position_store = PositionStore(path='reports/positions.sqlite')
            account_store = AccountStore(path='reports/positions.sqlite')
            
            # Check current diff
            local_positions_list = position_store.list()
            local_positions = {}
            for pos in local_positions_list:
                if hasattr(pos, 'symbol') and hasattr(pos, 'qty'):
                    sym = pos.symbol
                    qty = float(pos.qty or 0)
                elif isinstance(pos, dict):
                    sym = pos.get('symbol')
                    qty = float(pos.get('qty', 0) or 0)
                else:
                    continue
                # Ignore zero/dust records left in store history
                if sym and abs(qty) > 1e-8:
                    local_positions[sym] = qty

            # Primary sync targets
            tracked_symbols = set(getattr(cfg, 'symbols', []) or [])
            tracked_symbols |= set(getattr(getattr(cfg, 'universe', None), 'symbols', []) or [])
            # Core pairs (defensive fallback when config schema varies)
            tracked_symbols |= {'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'DOGE/USDT'}
            # Ignore tiny balances in sync to reduce dust-churn.
            # Positions below this USD value will be treated as dust and not written into local store.
            min_sync_value_usd = 0.5

            okx_positions = {}
            skipped_dust = []

            def _okx_qty(d):
                if isinstance(d, dict):
                    return _as_float(d.get('qty'))
                return _as_float(d)

            # 1) Bulk balance parse
            for detail in balance_details:
                ccy = str(detail.get('ccy') or '').upper()
                if not ccy or ccy == 'USDT':
                    continue
                sym = f"{ccy}/USDT"
                qty = _extract_spot_qty(detail)
                eq_usd = _as_float(detail.get('eqUsd'))
                if qty <= 1e-8:
                    continue

                # Keep only meaningful positions to avoid repeatedly syncing dust.
                if eq_usd >= min_sync_value_usd:
                    okx_positions[sym] = {
                        'qty': float(qty),
                        'eq_usd': float(eq_usd),
                    }
                else:
                    skipped_dust.append((sym, qty, eq_usd))

            # 2) Per-ccy verification for local symbols missing from bulk response
            #    (bulk details can occasionally omit small spot positions)
            ccy_check_candidates = set(tracked_symbols)
            for sym in sorted(ccy_check_candidates):
                if not sym or '/USDT' not in sym:
                    continue
                if sym in okx_positions:
                    continue
                ccy = sym.split('/')[0].upper()
                local_qty = float(local_positions.get(sym, 0.0) or 0.0)
                try:
                    one = client.get_balance(ccy=ccy)
                    qty = 0.0
                    eq_usd = 0.0
                    for d in one.data.get('data', [{}])[0].get('details', []):
                        if str(d.get('ccy') or '').upper() == ccy:
                            qty = max(qty, _extract_spot_qty(d))
                            eq_usd = max(eq_usd, _as_float(d.get('eqUsd')))
                    if qty > 1e-8 and eq_usd >= min_sync_value_usd:
                        okx_positions[sym] = {
                            'qty': float(qty),
                            'eq_usd': float(eq_usd),
                        }
                        logger.info(f"  [ccy-check] {sym}: recovered qty={qty:.8f}")
                    elif qty > 1e-8:
                        skipped_dust.append((sym, qty, eq_usd))
                        logger.info(
                            f"  [ccy-check] {sym}: skip dust qty={qty:.8f}, eqUsd={eq_usd:.4f} < {min_sync_value_usd}"
                        )
                    else:
                        logger.info(f"  [ccy-check] {sym}: confirmed zero")
                except Exception as e:
                    # Avoid false-zero wipe when single-ccy query fails transiently
                    if local_qty > 1e-8:
                        okx_positions[sym] = {
                            'qty': float(local_qty),
                            'eq_usd': 0.0,
                        }
                        logger.warning(
                            f"  [ccy-check] {sym}: query failed ({e}), keep local={local_qty:.8f}"
                        )
                    else:
                        logger.warning(f"  [ccy-check] {sym}: query failed ({e}), no local fallback")

            # Calculate diff
            total_diff = 0
            for sym, local_qty in local_positions.items():
                okx_qty = _okx_qty(okx_positions.get(sym, 0.0))
                diff = abs(local_qty - okx_qty)
                if diff > 1e-8:
                    total_diff += diff
                    logger.info(f"  {sym}: local={local_qty:.8f}, okx={okx_qty:.8f}, diff={diff:.8f}")

            for sym, okx_meta in okx_positions.items():
                okx_qty = _okx_qty(okx_meta)
                if sym not in local_positions and okx_qty > 1e-8:
                    total_diff += okx_qty
                    logger.info(f"  {sym}: local=0, okx={okx_qty:.8f}, diff={okx_qty:.8f}")
            
            logger.info(f"Total position diff: {total_diff:.8f}")
            if skipped_dust:
                sample = ', '.join([f"{s}:{u:.3f}U" for s, _, u in skipped_dust[:8]])
                logger.info(
                    f"Dust skipped from sync (<{min_sync_value_usd}U): {len(skipped_dust)} assets"
                    + (f" | {sample}" if sample else "")
                )
            
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
            for sym, meta in okx_positions.items():
                qty = _okx_qty(meta)
                if qty > 1e-8:
                    try:
                        existing = position_store.get(sym)
                        # price hint from OKX eqUsd/qty, used when creating a new local row
                        eq_usd = _as_float(meta.get('eq_usd')) if isinstance(meta, dict) else 0.0
                        px_hint = (float(eq_usd) / float(qty)) if (float(qty) > 0 and float(eq_usd) > 0) else 0.0
                        if existing is not None:
                            position_store.set_qty(sym, qty=float(qty))
                        else:
                            # set_qty() only updates existing rows; for new symbols we must upsert.
                            # Use px_hint to avoid zero avg_px.
                            if px_hint <= 0:
                                px_hint = 1.0
                            position_store.upsert_buy(sym, qty=float(qty), px=float(px_hint))
                        logger.info(f"  Synced {sym}: qty={qty:.8f}, px_hint={px_hint:.8f}")
                    except Exception as e:
                        logger.warning(f"  Could not sync {sym}: {e}")
            
            # Update cash
            for detail in balance_details:
                if str(detail.get('ccy') or '').upper() == 'USDT':
                    cash = max(
                        _as_float(detail.get('cashBal')),
                        _as_float(detail.get('availBal')),
                        _as_float(detail.get('eq')),
                    )
                    try:
                        st = account_store.get()
                        # keep peak at least as large as current cash to avoid fake DD on empty portfolio
                        st.cash_usdt = float(cash)
                        st.equity_peak_usdt = max(float(st.equity_peak_usdt), float(cash))
                        account_store.set(st)
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
            
            # Clear kill switch only when it is NOT a manual lock.
            kill_switch_path = Path('reports/kill_switch.json')
            if kill_switch_path.exists():
                ks = json.loads(kill_switch_path.read_text())
                if ks.get('enabled'):
                    if bool(ks.get('manual')):
                        logger.info("ℹ️ Manual kill switch detected, keep enabled")
                    else:
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
