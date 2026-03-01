#!/usr/bin/env python3
"""
Event-driven trading wrapper for V5.
Integrates event-driven logic with existing V5 system.
Phase 1: Parallel mode (log only, don't trade)
Phase 2: Active mode (event-driven trading)
"""
import sys
import json
import logging
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
        
        # Load prices from OKX API
        prices = {}
        try:
            from src.execution.price_fetcher import fetch_prices
            prices = fetch_prices()
            logger.info(f"Loaded {len(prices)} prices")
            # Show sample
            for sym, px in list(prices.items())[:3]:
                logger.info(f"  {sym}: {px}")
        except Exception as e:
            logger.error(f"Failed to fetch prices: {e}")
        
        if not prices:
            # Fallback: empty prices (disable breakout detection)
            logger.warning("No prices available - breakout detection disabled")
        
        # Load signals from alpha snapshot (more reliable)
        signals = {}
        alpha_path = Path('/home/admin/clawd/v5-trading-bot/reports/alpha_snapshot.json')
        if alpha_path.exists():
            with open(alpha_path) as f:
                alpha = json.load(f)
                for sym, score in alpha.get('scores', {}).items():
                    # Convert score to signal direction
                    # Negative score = sell (lower rank), Positive = buy (higher rank)
                    direction = 'buy' if score > 0 else 'sell' if score < 0 else 'hold'
                    # Estimate rank based on score magnitude
                    rank = 50 - int(score * 50)  # Simple ranking approximation
                    signals[sym] = SignalState(
                        symbol=sym,
                        direction=direction,
                        score=abs(score),
                        rank=max(1, min(99, rank)),
                        timestamp_ms=int(datetime.now().timestamp() * 1000)
                    )
                logger.info(f"Loaded {len(signals)} signals from alpha snapshot")
        
        # Also try strategy signals as backup
        if not signals:
            runs_dir = Path('/home/admin/clawd/v5-trading-bot/reports/runs')
            if runs_dir.exists():
                run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()], reverse=True)
                if run_dirs:
                    latest = run_dirs[0]
                    signals_path = latest / 'strategy_signals.json'
                    if signals_path.exists():
                        with open(signals_path) as f:
                            sig_data = json.load(f)
                            for sym, data in sig_data.get('fused', {}).items():
                                signals[sym] = SignalState(
                                    symbol=sym,
                                    direction=data.get('direction', 'hold'),
                                    score=data.get('score', 0),
                                    rank=data.get('rank', 99),
                                    timestamp_ms=int(datetime.now().timestamp() * 1000)
                                )
        
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


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Event-Driven Trading Check")
    logger.info("=" * 60)
    
    # Check if event-driven is enabled in config
    config_path = Path('/home/admin/clawd/v5-trading-bot/configs/live_20u_real.yaml')
    event_driven_enabled = False
    
    try:
        import yaml
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
            event_driven_enabled = cfg.get('event_driven', {}).get('enabled', False)
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
    
    # Create event-driven trader with breakout disabled (price data unreliable)
    trader = create_event_driven_trader({
        'enabled': True,
        'check_interval_minutes': 15,
        'global_cooldown_p2_minutes': 30,
        'symbol_cooldown_minutes': 60,
        'breakout_enabled': False  # Disable breakout detection - no reliable price source
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
    
    # Log to file for monitoring
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'should_trade': result['should_trade'],
        'reason': result['reason'],
        'actions': result['actions'],
        'regime': state['regime'],
        'events_processed': result.get('events_processed', 0),
        'events_blocked': result.get('events_blocked', 0)
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
    
    # Return exit code
    # 0 = Continue with standard V5 (or no action needed)
    # 1 = Event-driven handled (skip standard)
    if result['should_trade'] and result['actions']:
        logger.info("Event-driven trading triggered - actions generated")
        # For Phase 1, we still return 0 to let standard V5 run
        # In Phase 2, we would return 1 and execute actions here
        return 0
    else:
        logger.info("No event-driven actions - standard V5 may skip if no signals")
        return 0


if __name__ == '__main__':
    sys.exit(main())
