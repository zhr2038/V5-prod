#!/usr/bin/env python3
"""
Event-driven trading wrapper for V5.
Integrates event-driven logic with existing V5 system.
Phase 1: Parallel mode (log only, don't trade)
Phase 2: Active mode (event-driven trading)
"""
import sys
import json
import time
import logging
import subprocess
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('event_driven_wrapper')

# Add project path
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

# Import event-driven components
try:
    from src.execution.event_types import MarketState, SignalState
    from src.execution.event_driven_integration import create_event_driven_trader
    logger.info("✅ Event-driven modules loaded")
except Exception as e:
    logger.error(f"❌ Failed to load event-driven modules: {e}")
    sys.exit(1)


def load_current_state(cfg=None):
    """Load current market state from V5 reports."""
    try:
        # Load config if not provided
        if cfg is None:
            from configs.loader import load_config
            cfg = load_config('configs/live_20u_real.yaml', env_path='/home/admin/clawd/v5-trading-bot/.env')
        
        # Load regime
        regime_path = Path('/home/admin/clawd/v5-trading-bot/reports/regime.json')
        regime = 'SIDEWAYS'
        if regime_path.exists():
            with open(regime_path) as f:
                regime_data = json.load(f)
                regime = regime_data.get('regime', 'SIDEWAYS')
        
        # Load portfolio (positions)
        portfolio_path = Path('/home/admin/clawd/v5-trading-bot/reports/portfolio.json')
        positions = {}
        if portfolio_path.exists():
            with open(portfolio_path) as f:
                portfolio = json.load(f)
                for sym, data in portfolio.get('positions', {}).items():
                    positions[sym] = {
                        'entry_price': data.get('avg_price', 0),
                        'quantity': data.get('quantity', 0)
                    }
        
        # Build tradeable symbol universe (only strategy-tradeable symbols)
        cfg_symbols = cfg.get('symbols', []) if isinstance(cfg, dict) else getattr(cfg, 'symbols', [])
        tradeable_symbols = set(str(s) for s in (cfg_symbols or []))
        try:
            uni_cfg = cfg.get('universe', {}) if isinstance(cfg, dict) else getattr(cfg, 'universe', None)
            uni_enabled = uni_cfg.get('enabled', False) if isinstance(uni_cfg, dict) else bool(getattr(uni_cfg, 'enabled', False))
            uni_use = uni_cfg.get('use_universe_symbols', False) if isinstance(uni_cfg, dict) else bool(getattr(uni_cfg, 'use_universe_symbols', False))
            if uni_enabled and uni_use:
                cache_rel = uni_cfg.get('cache_path', 'reports/universe_cache.json') if isinstance(uni_cfg, dict) else getattr(uni_cfg, 'cache_path', 'reports/universe_cache.json')
                cache_path = Path(cache_rel)
                if not cache_path.is_absolute():
                    cache_path = Path('/home/admin/clawd/v5-trading-bot') / cache_path
                if cache_path.exists():
                    cache_obj = json.loads(cache_path.read_text(encoding='utf-8'))
                    tradeable_symbols = set(str(s) for s in (cache_obj.get('symbols') or []))
        except Exception as e:
            logger.warning(f"Could not load universe cache, fallback to cfg.symbols: {e}")

        # Load prices from OKX API (filtered to tradeable universe only)
        prices = {}
        try:
            from src.execution.price_fetcher import fetch_prices
            all_prices = fetch_prices()
            prices = {sym: px for sym, px in all_prices.items() if sym in tradeable_symbols}
            logger.info(f"Loaded {len(prices)}/{len(all_prices)} prices (tradeable universe)")
            for sym, px in list(prices.items())[:5]:
                logger.info(f"  {sym}: {px}")
        except Exception as e:
            logger.error(f"Failed to fetch prices: {e}")

        if not prices:
            logger.warning("No prices available in tradeable universe - breakout detection disabled")
        
        # Load signals - PRIORITY: fused signals > alpha snapshot
        signals = {}
        
        # 1. Try to load FUSED signals from strategy_signals.json (highest priority)
        runs_dir = Path('/home/admin/clawd/v5-trading-bot/reports/runs')
        if runs_dir.exists():
            # Sort by modification time (newest first) instead of name
            run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()], 
                             key=lambda x: x.stat().st_mtime, reverse=True)
            logger.info(f"Found {len(run_dirs)} run directories")
            if run_dirs:
                latest = run_dirs[0]
                signals_path = latest / 'strategy_signals.json'
                logger.info(f"Looking for signals at: {signals_path}")
                if signals_path.exists():
                    try:
                        with open(signals_path) as f:
                            sig_data = json.load(f)
                            fused_signals = sig_data.get("fused", {})
                            logger.info(f"Found {len(fused_signals)} fused signals in file")
                            if fused_signals:
                                for sym, data in fused_signals.items():
                                    if sym not in tradeable_symbols:
                                        continue
                                    signals[sym] = SignalState(
                                        symbol=sym,
                                        direction=data.get('direction', 'hold'),
                                        score=data.get('score', 0),
                                        rank=data.get('rank', 99),
                                        timestamp_ms=int(datetime.now().timestamp() * 1000)
                                    )
                                logger.info(f"Loaded {len(signals)} FUSED signals from {latest.name} (tradeable filtered)")
                            else:
                                logger.warning("fused_signals is empty")
                    except Exception as e:
                        logger.error(f"Could not load fused signals: {e}")
                        import traceback
                        traceback.print_exc()
                else:
                    logger.warning(f"Signals file not found: {signals_path}")
        else:
            logger.warning(f"Runs directory not found: {runs_dir}")
        
        # 2. Fallback to alpha snapshot if no fused signals
        if not signals:
            alpha_path = Path('/home/admin/clawd/v5-trading-bot/reports/alpha_snapshot.json')
            if alpha_path.exists():
                with open(alpha_path) as f:
                    alpha = json.load(f)
                    for sym, score in alpha.get('scores', {}).items():
                        if sym not in tradeable_symbols:
                            continue
                        direction = 'buy' if score > 0 else 'sell' if score < 0 else 'hold'
                        rank = 50 - int(score * 50)
                        signals[sym] = SignalState(
                            symbol=sym,
                            direction=direction,
                            score=abs(score),
                            rank=max(1, min(99, rank)),
                            timestamp_ms=int(datetime.now().timestamp() * 1000)
                        )
                    logger.info(f"Loaded {len(signals)} signals from alpha snapshot (fallback)")
        
        # Load selected symbols
        selected = list(signals.keys())[:5]  # Top 5
        
        return {
            'timestamp_ms': int(datetime.now().timestamp() * 1000),
            'regime': regime,
            'prices': prices,
            'positions': positions,
            'signals': signals,
            'selected_symbols': selected
        }
    
    except Exception as e:
        logger.error(f"Failed to load state: {e}")
        import traceback
        traceback.print_exc()
        return None


def trigger_live_execution_service():
    """Start full live execution service (active mode)."""
    try:
        # Skip if service is already running/starting (avoid overlap starts)
        st = subprocess.run(
            ['systemctl', '--user', 'is-active', 'v5-live-20u.user.service'],
            capture_output=True,
            text=True,
            timeout=10,
        )
        state = (st.stdout or '').strip().lower()
        if state in ('active', 'activating'):
            return {
                'ok': True,
                'returncode': 0,
                'stdout': state,
                'stderr': '',
                'cmd': 'systemctl --user start v5-live-20u.user.service',
                'skipped_already_running': True,
            }

        cmd = ['systemctl', '--user', 'start', 'v5-live-20u.user.service']
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
        ok = proc.returncode == 0
        return {
            'ok': ok,
            'returncode': proc.returncode,
            'stdout': (proc.stdout or '').strip(),
            'stderr': (proc.stderr or '').strip(),
            'cmd': ' '.join(cmd),
            'skipped_already_running': False,
        }
    except Exception as e:
        return {
            'ok': False,
            'returncode': -1,
            'stdout': '',
            'stderr': str(e),
            'cmd': 'systemctl --user start v5-live-20u.user.service',
            'skipped_already_running': False,
        }


def get_last_live_run_age_sec():
    """Return (age_seconds, run_name) for latest run with decision_audit.json."""
    try:
        runs_dir = Path('/home/admin/clawd/v5-trading-bot/reports/runs')
        if not runs_dir.exists():
            return None, None
        cands = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
        if not cands:
            return None, None
        latest = max(cands, key=lambda d: d.stat().st_mtime)
        age = max(0.0, time.time() - latest.stat().st_mtime)
        return age, latest.name
    except Exception:
        return None, None


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Event-Driven Trading Check")
    logger.info("=" * 60)
    
    # Check if event-driven is enabled in config
    config_path = Path('/home/admin/clawd/v5-trading-bot/configs/live_20u_real.yaml')
    event_driven_enabled = False
    active_mode = False
    force_full_mode = False
    force_full_min_interval_minutes = 12
    ev_cfg = {}
    cfg = {}
    
    try:
        import yaml
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}
            ev_cfg = cfg.get('event_driven', {}) or {}
            event_driven_enabled = bool(ev_cfg.get('enabled', False))
            mode = str(ev_cfg.get('mode', '')).strip().lower()
            active_mode = bool(ev_cfg.get('active_mode', mode == 'active'))
            force_full_mode = bool(ev_cfg.get('force_full_run', mode in ('force_full', 'full')))
            force_full_min_interval_minutes = int(ev_cfg.get('force_full_min_interval_minutes', 12) or 12)
    except Exception as e:
        logger.warning(f"Could not read config: {e}")
    
    if not event_driven_enabled:
        logger.info("Event-driven trading is disabled in config")
        logger.info("Using standard V5 execution")
        return 0
    
    # Load current state
    logger.info("Loading current market state...")
    state = load_current_state(cfg)
    
    if not state:
        logger.error("Failed to load state, falling back to standard execution")
        return 0
    
    # Load last signal history for comparison
    last_state = None
    history_path = Path('/home/admin/clawd/v5-trading-bot/reports/event_driven_signals.json')
    if history_path.exists():
        try:
            with open(history_path) as f:
                last_data = json.load(f)
                last_state = {
                    'timestamp_ms': last_data.get('timestamp', 0),
                    'regime': last_data.get('regime', 'SIDEWAYS'),
                    'prices': last_data.get('prices', {}),
                    'signals': last_data.get('signals', {}),
                    'selected_symbols': list(last_data.get('signals', {}).keys())[:5]
                }
                logger.info(f"Loaded signal history from {last_data.get('timestamp', 0)}")
        except Exception as e:
            logger.warning(f"Could not load signal history: {e}")
    
    logger.info(f"Regime: {state['regime']}")
    logger.info(f"Positions: {list(state['positions'].keys())}")
    logger.info(f"Selected: {state['selected_symbols']}")
    
    if force_full_mode:
        mode_text = 'FORCE_FULL'
    elif active_mode:
        mode_text = 'ACTIVE'
    else:
        mode_text = 'PASSIVE'
    logger.info(f"Event-driven mode: {mode_text}")

    # Build trader config from YAML (with safe defaults)
    trader = create_event_driven_trader({
        'enabled': True,
        'check_interval_minutes': int(ev_cfg.get('check_interval_minutes', 15) or 15),
        'global_cooldown_p2_minutes': int(ev_cfg.get('global_cooldown_p2_minutes', 30) or 30),
        'symbol_cooldown_minutes': int(ev_cfg.get('symbol_cooldown_minutes', 60) or 60),
        'signal_confirmation_periods': int(ev_cfg.get('signal_confirmation_periods', 2) or 2),
        'score_change_threshold': float(ev_cfg.get('score_change_threshold', 0.30) or 0.30),
        'rank_jump_threshold': int(ev_cfg.get('rank_jump_threshold', 3) or 3),
        'breakout_enabled': bool(ev_cfg.get('breakout_enabled', True)),
        'breakout_lookback_hours': int(ev_cfg.get('breakout_lookback_hours', 24) or 24),
        'breakout_threshold_pct': float(ev_cfg.get('breakout_threshold_pct', 0.5) or 0.5),
        'heartbeat_interval_hours': int(ev_cfg.get('heartbeat_interval_hours', 4) or 4),
    })
    
    # Check if should trade (with last state for comparison)
    logger.info("Checking for trading events...")
    result = trader.should_trade(state, last_state)
    
    logger.info(f"Should trade: {result['should_trade']}")
    logger.info(f"Reason: {result['reason']}")
    logger.info(f"Events processed: {result.get('events_processed', 0)}")
    logger.info(f"Events blocked: {result.get('events_blocked', 0)}")
    
    if result['actions']:
        logger.info(f"Actions ({len(result['actions'])}):")
        for action in result['actions']:
            logger.info(f"  - {action['symbol']}: {action['action']} ({action['reason']})")
    
    # Active-mode execution (trigger full live run)
    execution = {
        'active_mode': active_mode,
        'force_full_mode': force_full_mode,
        'force_full_min_interval_minutes': force_full_min_interval_minutes,
        'live_service_triggered': False,
        'live_service_ok': None,
        'live_service_returncode': None,
        'live_service_stderr': '',
        'trigger_reason': None,
        'last_run_age_sec': None,
        'last_run_id': None,
    }

    if force_full_mode:
        age_sec, last_run_id = get_last_live_run_age_sec()
        execution['last_run_age_sec'] = age_sec
        execution['last_run_id'] = last_run_id

        min_interval_sec = max(0, int(force_full_min_interval_minutes) * 60)
        if age_sec is not None and age_sec < min_interval_sec:
            logger.info(
                f"FORCE_FULL throttled: last run {last_run_id} {age_sec:.1f}s ago < {min_interval_sec}s"
            )
            execution.update({
                'trigger_reason': 'force_full_throttled',
            })
        else:
            logger.info("FORCE_FULL mode: starting v5-live-20u.user.service")
            exec_res = trigger_live_execution_service()
            execution.update({
                'live_service_triggered': True,
                'live_service_ok': exec_res.get('ok'),
                'live_service_returncode': exec_res.get('returncode'),
                'live_service_stderr': exec_res.get('stderr', ''),
                'trigger_reason': 'force_full_run',
            })
            if exec_res.get('ok'):
                logger.info("FORCE_FULL mode: live service start request accepted")
            else:
                logger.error(
                    f"FORCE_FULL mode: failed to start live service rc={exec_res.get('returncode')} err={exec_res.get('stderr')}"
                )
    elif result['should_trade'] and result['actions']:
        logger.info("Event-driven trading triggered - actions generated")
        if active_mode:
            logger.info("ACTIVE mode: starting v5-live-20u.user.service")
            exec_res = trigger_live_execution_service()
            execution.update({
                'live_service_triggered': True,
                'live_service_ok': exec_res.get('ok'),
                'live_service_returncode': exec_res.get('returncode'),
                'live_service_stderr': exec_res.get('stderr', ''),
                'trigger_reason': 'event_actions',
            })
            if exec_res.get('ok'):
                logger.info("ACTIVE mode: live service start request accepted")
            else:
                logger.error(
                    f"ACTIVE mode: failed to start live service rc={exec_res.get('returncode')} err={exec_res.get('stderr')}"
                )
        else:
            logger.info("PASSIVE mode: actions logged only, no execution")
    else:
        logger.info("No event-driven actions - standard V5 may skip if no signals")

    # Log to file for monitoring
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'should_trade': result['should_trade'],
        'reason': result['reason'],
        'actions': result['actions'],
        'regime': state['regime'],
        'events_processed': result.get('events_processed', 0),
        'events_blocked': result.get('events_blocked', 0),
        'execution': execution,
    }
    
    log_path = Path('/home/admin/clawd/v5-trading-bot/reports/event_driven_log.jsonl')
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(log_path, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    
    # Save signal history for next comparison
    history_path = Path('/home/admin/clawd/v5-trading-bot/reports/event_driven_signals.json')
    signal_history = {
        'timestamp': int(datetime.now().timestamp() * 1000),
        'signals': {sym: sig.to_dict() if hasattr(sig, 'to_dict') else sig 
                   for sym, sig in state['signals'].items()},
        'prices': state['prices'],
        'regime': state['regime']
    }
    history_path.write_text(json.dumps(signal_history, indent=2))
    logger.info(f"Saved signal history ({len(signal_history['signals'])} signals)")

    return 0


if __name__ == '__main__':
    sys.exit(main())
