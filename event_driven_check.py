#!/usr/bin/env python3
"""
Event-driven trading wrapper for V5.
Integrates event-driven logic with existing V5 system.
Phase 1: Parallel mode (log only, don't trade)
Phase 2: Active mode (event-driven trading)
"""
import sys
import os
import json
import time
import logging
import subprocess
import tempfile
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('event_driven_wrapper')

PROJECT_ROOT = Path(__file__).resolve().parent
REPORTS_DIR = PROJECT_ROOT / 'reports'

# Add project path
sys.path.insert(0, str(PROJECT_ROOT))

# Import event-driven components
try:
    from src.execution.event_types import MarketState, SignalState, top_selected_symbols
    from src.execution.event_driven_integration import create_event_driven_trader
    from src.execution.event_action_bridge import persist_event_actions
    from src.execution.fill_store import (
        derive_position_store_path,
        derive_runtime_named_artifact_path,
        derive_runtime_named_json_path,
        derive_runtime_reports_dir,
        derive_runtime_runs_dir,
    )
    from configs.runtime_config import resolve_runtime_path
    logger.info("✅ Event-driven modules loaded")
except Exception as e:
    logger.error(f"❌ Failed to load event-driven modules: {e}")
    sys.exit(1)


@dataclass(frozen=True)
class EventDrivenPaths:
    order_store_path: Path
    reports_dir: Path
    runs_dir: Path
    positions_db: Path
    portfolio_path: Path
    regime_path: Path
    alpha_snapshot_path: Path
    equity_validation_path: Path
    event_adaptive_state_path: Path
    event_driven_signals_path: Path
    event_candidates_path: Path
    riskoff_shadow_plan_path: Path
    event_param_scan_path: Path
    event_driven_log_path: Path


def build_paths(cfg=None) -> EventDrivenPaths:
    execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else getattr(cfg, "execution", None)
    raw_order_store_path = (
        execution_cfg.get("order_store_path")
        if isinstance(execution_cfg, dict)
        else getattr(execution_cfg, "order_store_path", None)
    )
    order_store_path = Path(
        resolve_runtime_path(
            raw_order_store_path,
            default="reports/orders.sqlite",
            project_root=PROJECT_ROOT,
        )
    ).resolve()
    reports_dir = derive_runtime_reports_dir(order_store_path).resolve()
    runs_dir = derive_runtime_runs_dir(order_store_path).resolve()
    return EventDrivenPaths(
        order_store_path=order_store_path,
        reports_dir=reports_dir,
        runs_dir=runs_dir,
        positions_db=derive_position_store_path(order_store_path).resolve(),
        portfolio_path=(reports_dir / "portfolio.json").resolve(),
        regime_path=(reports_dir / "regime.json").resolve(),
        alpha_snapshot_path=(reports_dir / "alpha_snapshot.json").resolve(),
        equity_validation_path=(reports_dir / "equity_validation.json").resolve(),
        event_adaptive_state_path=derive_runtime_named_json_path(order_store_path, "event_adaptive_state").resolve(),
        event_driven_signals_path=derive_runtime_named_json_path(order_store_path, "event_driven_signals").resolve(),
        event_candidates_path=derive_runtime_named_json_path(order_store_path, "event_candidates").resolve(),
        riskoff_shadow_plan_path=derive_runtime_named_json_path(order_store_path, "riskoff_shadow_plan").resolve(),
        event_param_scan_path=derive_runtime_named_json_path(order_store_path, "event_param_scan").resolve(),
        event_driven_log_path=derive_runtime_named_artifact_path(order_store_path, "event_driven_log", ".jsonl").resolve(),
    )


def resolve_config_path() -> Path:
    """Resolve active V5 config path with sensible priority.

    Priority:
    1) V5_CONFIG env
    2) configs/live_prod.yaml
    3) configs/live_20u_real.yaml
    4) configs/config.yaml
    """
    env_cfg = os.getenv('V5_CONFIG', '').strip()
    if env_cfg:
        p = Path(env_cfg)
        if not p.is_absolute():
            p = PROJECT_ROOT / p
        if p.exists():
            return p

    candidates = [
        PROJECT_ROOT / 'configs/live_prod.yaml',
        PROJECT_ROOT / 'configs/live_20u_real.yaml',
        PROJECT_ROOT / 'configs/config.yaml',
    ]
    for p in candidates:
        if p.exists():
            return p

    return PROJECT_ROOT / 'configs/live_prod.yaml'


def resolve_live_service_unit(ev_cfg: dict) -> str:
    """Resolve live service unit for event-driven trigger."""
    explicit = str((ev_cfg or {}).get('live_service_unit', '') or '').strip()
    if explicit:
        return explicit

    env_unit = os.getenv('V5_LIVE_SERVICE', '').strip()
    if env_unit:
        return env_unit

    # Prefer production service if present
    for unit in ('v5-prod.user.service', 'v5-live-20u.user.service'):
        p = Path.home() / '.config/systemd/user' / unit
        if p.exists():
            return unit

    return 'v5-prod.user.service'


def find_latest_fused_signals_file(runs_dir: Path, max_age_minutes: int = 90):
    """Find newest strategy_signals.json under runs/ within freshness window."""
    try:
        files = sorted(
            [p for p in runs_dir.glob('*/strategy_signals.json') if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not files:
            return None, None

        newest = files[0]
        age_sec = max(0.0, time.time() - newest.stat().st_mtime)
        age_min = age_sec / 60.0
        if age_min > float(max_age_minutes):
            return None, {'path': str(newest), 'age_min': age_min, 'fresh': False, 'count': len(files)}

        return newest, {'path': str(newest), 'age_min': age_min, 'fresh': True, 'count': len(files)}
    except Exception:
        return None, None


def _load_positions_snapshot(
    positions_db_path: Optional[Path] = None,
    portfolio_path: Optional[Path] = None,
):
    """Load live positions, preferring positions.sqlite over legacy portfolio.json."""
    positions: dict[str, dict] = {}
    position_symbols: set[str] = set()
    source = 'missing'

    db_path = positions_db_path or (REPORTS_DIR / 'positions.sqlite')
    try:
        from src.execution.position_store import PositionStore

        store = PositionStore(str(db_path))
        for pos in store.list():
            qty = float(getattr(pos, 'qty', 0.0) or 0.0)
            if qty <= 0:
                continue
            sym = str(getattr(pos, 'symbol', '') or '')
            if not sym:
                continue
            positions[sym] = {
                'entry_price': float(getattr(pos, 'avg_px', 0.0) or 0.0),
                'quantity': qty,
            }
            position_symbols.add(sym)
        if positions:
            return positions, position_symbols, 'position_store'
    except Exception as e:
        logger.warning(f"Could not load positions from sqlite store: {e}")

    legacy_portfolio_path = portfolio_path or (REPORTS_DIR / 'portfolio.json')
    if legacy_portfolio_path.exists():
        try:
            with open(legacy_portfolio_path) as f:
                portfolio = json.load(f)
            for sym, data in (portfolio.get('positions', {}) or {}).items():
                qty = float((data or {}).get('quantity', 0.0) or 0.0)
                if qty <= 0:
                    continue
                sym = str(sym or '')
                if not sym:
                    continue
                positions[sym] = {
                    'entry_price': float((data or {}).get('avg_price', 0.0) or 0.0),
                    'quantity': qty,
                }
                position_symbols.add(sym)
            if positions:
                source = 'portfolio_json'
        except Exception as e:
            logger.warning(f"Could not load legacy portfolio.json positions: {e}")

    return positions, position_symbols, source


def load_current_state(cfg=None, config_path: Path = None):
    """Load current market state from V5 reports."""
    try:
        # Load config if not provided
        if cfg is None:
            from configs.loader import load_config
            cfg_path = config_path or resolve_config_path()
            cfg = load_config(str(cfg_path), env_path=str(PROJECT_ROOT / '.env'))

        paths = build_paths(cfg)

        # Load regime
        regime_path = paths.regime_path
        regime = 'SIDEWAYS'
        if regime_path.exists():
            with open(regime_path) as f:
                regime_data = json.load(f)
                regime = regime_data.get('regime', 'SIDEWAYS')
        
        # Load live positions from sqlite first; fallback to legacy portfolio.json only when needed
        positions, position_symbols, position_source = _load_positions_snapshot(
            positions_db_path=paths.positions_db,
            portfolio_path=paths.portfolio_path,
        )
        logger.info(f"Loaded {len(positions)} positions from {position_source}")
        
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
                    cache_path = PROJECT_ROOT / cache_path
                if cache_path.exists():
                    cache_obj = json.loads(cache_path.read_text(encoding='utf-8'))
                    tradeable_symbols = set(str(s) for s in (cache_obj.get('symbols') or []))
        except Exception as e:
            logger.warning(f"Could not load universe cache, fallback to cfg.symbols: {e}")

        # Always keep current holdings inside event-driven watch scope so existing positions remain manageable.
        tradeable_symbols.update(position_symbols)

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
        runs_dir = paths.runs_dir
        if runs_dir.exists():
            run_dirs = [d for d in runs_dir.iterdir() if d.is_dir()]
            logger.info(f"Found {len(run_dirs)} run directories")

            ev_cfg = cfg.get('event_driven', {}) if isinstance(cfg, dict) else getattr(cfg, 'event_driven', None)
            max_age = ev_cfg.get('fused_signal_max_age_minutes', 90) if isinstance(ev_cfg, dict) else getattr(ev_cfg, 'fused_signal_max_age_minutes', 90)
            signals_path, meta = find_latest_fused_signals_file(runs_dir, max_age_minutes=int(max_age or 90))

            if signals_path is not None:
                logger.info(f"Using fused signals file: {signals_path} (age={meta.get('age_min', 0):.1f}m, files={meta.get('count', 0)})")
                try:
                    with open(signals_path) as f:
                        sig_data = json.load(f)
                        signals = _load_fused_signal_states(sig_data, tradeable_symbols)
                        logger.info(f"Found {len(sig_data.get('fused', {}))} fused signals in file")
                        if signals:
                            logger.info(f"Loaded {len(signals)} FUSED signals (tradeable filtered)")
                        else:
                            logger.warning("fused_signals is empty")
                except Exception as e:
                    logger.error(f"Could not load fused signals: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                if meta and meta.get('path'):
                    logger.warning(
                        f"No fresh fused signals file (latest stale: {meta.get('path')}, age={meta.get('age_min', 0):.1f}m > {int(max_age or 90)}m)"
                    )
                else:
                    logger.warning("No strategy_signals.json found under runs/")

            if not signals:
                audit_path, audit_meta = find_latest_decision_audit_file(runs_dir, max_age_minutes=int(max_age or 90))
                if audit_path is not None:
                    try:
                        with open(audit_path) as f:
                            audit_data = json.load(f)
                        signals = _load_decision_audit_signal_states(audit_data, tradeable_symbols)
                        if signals:
                            logger.info(
                                f"Loaded {len(signals)} signals from decision audit top_scores: "
                                f"{audit_path} (age={audit_meta.get('age_min', 0):.1f}m)"
                            )
                    except Exception as e:
                        logger.error(f"Could not load decision audit signals: {e}")
        else:
            logger.warning(f"Runs directory not found: {runs_dir}")
        
        # 2. Fallback to alpha snapshot if no fused signals
        if not signals:
            alpha_path = paths.alpha_snapshot_path
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
        
        # Resolve selected symbols by actual rank/signal quality rather than dict order.
        selected = top_selected_symbols(signals, limit=5)
        
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


def trigger_live_execution_service(service_unit: str):
    """Start full live execution service (active mode)."""
    try:
        # Skip if service is already running/starting (avoid overlap starts)
        st = subprocess.run(
            ['systemctl', '--user', 'is-active', service_unit],
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
                'cmd': f'systemctl --user start {service_unit}',
                'service_unit': service_unit,
                'skipped_already_running': True,
            }

        cmd = ['systemctl', '--user', 'start', service_unit]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
        ok = proc.returncode == 0
        return {
            'ok': ok,
            'returncode': proc.returncode,
            'stdout': (proc.stdout or '').strip(),
            'stderr': (proc.stderr or '').strip(),
            'cmd': ' '.join(cmd),
            'service_unit': service_unit,
            'skipped_already_running': False,
        }
    except Exception as e:
        return {
            'ok': False,
            'returncode': -1,
            'stdout': '',
            'stderr': str(e),
            'cmd': f'systemctl --user start {service_unit}',
            'service_unit': service_unit,
            'skipped_already_running': False,
        }


def get_last_live_run_age_sec(runs_dir: Path | None = None):
    """Return (age_seconds, run_name) for latest run with decision_audit.json."""
    try:
        runs_dir = runs_dir or (REPORTS_DIR / 'runs')
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


def find_latest_decision_audit_file(runs_dir: Path, max_age_minutes: int = 90):
    """Find newest decision_audit.json under runs/ within freshness window."""
    try:
        files = sorted(
            [p for p in runs_dir.glob('*/decision_audit.json') if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not files:
            return None, None

        newest = files[0]
        age_sec = max(0.0, time.time() - newest.stat().st_mtime)
        age_min = age_sec / 60.0
        if age_min > float(max_age_minutes):
            return None, {'path': str(newest), 'age_min': age_min, 'fresh': False, 'count': len(files)}

        return newest, {'path': str(newest), 'age_min': age_min, 'fresh': True, 'count': len(files)}
    except Exception:
        return None, None


def _load_fused_signal_states(sig_data: dict, tradeable_symbols: set[str]):
    signals = {}
    fused_signals = sig_data.get("fused", {})
    for sym, data in (fused_signals or {}).items():
        if sym not in tradeable_symbols:
            continue
        signals[sym] = SignalState(
            symbol=sym,
            direction=data.get('direction', 'hold'),
            score=data.get('score', 0),
            rank=data.get('rank', 99),
            timestamp_ms=int(datetime.now().timestamp() * 1000)
        )
    return signals


def _load_decision_audit_signal_states(audit_data: dict, tradeable_symbols: set[str]):
    signals = {}
    for row in (audit_data.get('top_scores') or []):
        sym = str(row.get('symbol') or '')
        if not sym or sym not in tradeable_symbols:
            continue
        try:
            score = float(row.get('score', row.get('display_score', 0)) or 0)
        except Exception:
            score = 0.0
        try:
            rank = int(row.get('rank', 99) or 99)
        except Exception:
            rank = 99
        direction = 'buy' if score > 0 else 'sell' if score < 0 else 'hold'
        signals[sym] = SignalState(
            symbol=sym,
            direction=direction,
            score=abs(score),
            rank=rank,
            timestamp_ms=int(datetime.now().timestamp() * 1000)
        )
    return signals


def get_current_live_window_run_id(now: datetime = None) -> str:
    """Return the run_id used by the hourly live wrapper for the current hour."""
    dt = now or datetime.now()
    return dt.strftime('%Y%m%d_%H')


def evaluate_live_trigger_throttle(
    *,
    last_run_age_sec,
    last_run_id,
    current_run_id: str,
    min_interval_minutes: int,
):
    """Decide whether a live trigger should be throttled."""
    min_interval_sec = max(0, int(min_interval_minutes or 0) * 60)
    current_id = str(current_run_id or '')
    last_id = str(last_run_id or '')

    if current_id and last_id and current_id == last_id:
        return {
            'throttled': True,
            'reason': 'same_window_already_ran',
            'min_interval_sec': min_interval_sec,
        }

    if last_run_age_sec is not None and float(last_run_age_sec) < float(min_interval_sec):
        return {
            'throttled': True,
            'reason': 'min_interval',
            'min_interval_sec': min_interval_sec,
        }

    return {
        'throttled': False,
        'reason': None,
        'min_interval_sec': min_interval_sec,
    }


def should_bypass_live_trigger_throttle(actions) -> bool:
    """Allow urgent risk-close actions to trigger a fresh live run immediately."""
    for action in actions or []:
        try:
            priority = int(action.get('priority', 99))
        except Exception:
            priority = 99
        side_action = str(action.get('action') or '').lower()
        reason = str(action.get('reason') or '').lower()
        if priority == 0 and side_action == 'close':
            return True
        if side_action == 'close' and (
            reason.startswith('take_profit')
            or reason == 'stop_loss'
            or reason == 'trailing_stop'
            or reason.startswith('rank_exit')
            or reason == 'regime_risk_off'
        ):
            return True
    return False


def build_candidate_watchlist(state: dict, breakout_threshold_pct: float = 0.5, top_n: int = 10):
    """Build top candidate watchlist with rough trigger prices."""
    out = []
    signals = state.get('signals', {}) or {}
    prices = state.get('prices', {}) or {}
    bps = max(0.0, float(breakout_threshold_pct)) / 100.0

    for sym, sig in signals.items():
        direction = getattr(sig, 'direction', None)
        score = getattr(sig, 'score', None)
        rank = getattr(sig, 'rank', None)

        if isinstance(sig, dict):
            direction = sig.get('direction', direction)
            score = sig.get('score', score)
            rank = sig.get('rank', rank)

        direction = str(direction or 'hold').lower()
        if direction == 'hold':
            continue

        px = float(prices.get(sym, 0.0) or 0.0)
        trigger_up = float(px * (1.0 + bps)) if px > 0 else None
        trigger_down = float(px * (1.0 - bps)) if px > 0 else None

        out.append({
            'symbol': str(sym),
            'direction': direction,
            'score': float(score or 0.0),
            'rank': int(rank or 99),
            'price': float(px) if px > 0 else None,
            'trigger_up': trigger_up,
            'trigger_down': trigger_down,
        })

    out.sort(key=lambda x: (-x['score'], x['rank']))
    return out[:max(1, int(top_n))]


def estimate_live_equity(eq_file: Path | None = None) -> float:
    """Best-effort live equity for shadow sizing."""
    eq_file = eq_file or (REPORTS_DIR / 'equity_validation.json')
    if eq_file.exists():
        try:
            obj = json.loads(eq_file.read_text(encoding='utf-8'))
            v = obj.get('okx_total_eq')
            if v is not None:
                return float(v)
        except Exception:
            pass
    return 0.0


def build_riskoff_shadow_plan(state: dict, cfg: dict, watchlist: list, *, equity_file: Path | None = None):
    """Build shadow plan for Risk-Off probe scenarios (no execution)."""
    regime = str(state.get('regime', 'SIDEWAYS'))
    risk_cfg = (cfg or {}).get('risk', {}) or {}
    reg_cfg = (cfg or {}).get('regime', {}) or {}

    equity = estimate_live_equity(equity_file)
    max_single = float(risk_cfg.get('max_single_weight', 0.25) or 0.25)
    max_gross = float(risk_cfg.get('max_gross_exposure', 1.0) or 1.0)
    current_mult = float(reg_cfg.get('pos_mult_risk_off', 0.0) or 0.0)

    buy_pool = [x for x in (watchlist or []) if str(x.get('direction')).lower() == 'buy']

    scenarios = [
        {'name': 'strict_close_only', 'pos_mult': 0.0, 'max_positions': 0},
        {'name': 'probe_1', 'pos_mult': 0.15, 'max_positions': 1},
        {'name': 'probe_2', 'pos_mult': 0.25, 'max_positions': 2},
    ]

    plans = []
    for sc in scenarios:
        mult = float(sc['pos_mult'])
        max_pos = int(sc['max_positions'])
        if mult <= 0 or max_pos <= 0 or equity <= 0 or not buy_pool:
            plans.append({
                'name': sc['name'],
                'pos_mult': mult,
                'max_positions': max_pos,
                'gross_notional': 0.0,
                'candidates': [],
            })
            continue

        n = min(max_pos, len(buy_pool))
        gross = equity * max_gross * mult
        per_weight = min(max_single, (max_gross * mult) / max(1, n))
        per_notional = equity * per_weight

        picks = []
        for c in buy_pool[:n]:
            picks.append({
                'symbol': c['symbol'],
                'score': c['score'],
                'price': c.get('price'),
                'shadow_target_usdt': round(per_notional, 4),
            })

        plans.append({
            'name': sc['name'],
            'pos_mult': mult,
            'max_positions': n,
            'gross_notional': round(gross, 4),
            'candidates': picks,
        })

    return {
        'timestamp': datetime.now().isoformat(),
        'regime': regime,
        'current_pos_mult_risk_off': current_mult,
        'equity_estimate_usdt': round(equity, 6),
        'enabled_for_current_regime': regime.upper() in ('RISK_OFF', 'RISK-OFF', 'RISK_OFF'),
        'plans': plans,
        'note': 'Shadow only. No execution triggered by this report.',
    }


def _read_recent_event_stats(log_path: Path, lookback: int = 12):
    """Read recent event-driven log stats."""
    items = []
    try:
        if not log_path.exists():
            return items
        with log_path.open('r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()[-max(1, int(lookback)):]
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            try:
                items.append(json.loads(ln))
            except Exception:
                continue
    except Exception:
        return []
    return items


def compute_adaptive_event_cfg(
    ev_cfg: dict,
    state: dict,
    *,
    log_path: Path | None = None,
    adaptive_state_path: Path | None = None,
):
    """Adaptive cooldown tuning for event-driven engine.

    Only adjusts cooldown/confirmation knobs, does not force trading.
    """
    base = {
        'check_interval_minutes': int(ev_cfg.get('check_interval_minutes', 15) or 15),
        'global_cooldown_p2_minutes': int(ev_cfg.get('global_cooldown_p2_minutes', 30) or 30),
        'symbol_cooldown_minutes': int(ev_cfg.get('symbol_cooldown_minutes', 60) or 60),
        'signal_confirmation_periods': int(ev_cfg.get('signal_confirmation_periods', 2) or 2),
    }

    acfg = (ev_cfg or {}).get('adaptive_cooldown', {}) or {}
    enabled = bool(acfg.get('enabled', True))
    if not enabled:
        return dict(base), {'enabled': False, 'applied': False, 'reason': 'disabled'}

    lookback = int(acfg.get('lookback_runs', 12) or 12)
    high_block_ratio = float(acfg.get('high_block_ratio', 0.75) or 0.75)
    min_events_for_action = int(acfg.get('min_events_for_action', 8) or 8)

    p2_min = int(acfg.get('p2_min_minutes', 8) or 8)
    p2_max = int(acfg.get('p2_max_minutes', 60) or 60)
    symbol_min = int(acfg.get('symbol_min_minutes', 15) or 15)
    symbol_max = int(acfg.get('symbol_max_minutes', 120) or 120)
    confirm_min = int(acfg.get('confirm_min', 1) or 1)
    confirm_max = int(acfg.get('confirm_max', 4) or 4)

    recent = _read_recent_event_stats(log_path or (REPORTS_DIR / 'event_driven_log.jsonl'), lookback=lookback)
    processed = sum(int((x or {}).get('events_processed', 0) or 0) for x in recent)
    blocked = sum(int((x or {}).get('events_blocked', 0) or 0) for x in recent)
    block_ratio = (blocked / processed) if processed > 0 else 0.0

    regime = str((state or {}).get('regime', 'SIDEWAYS')).upper()

    out = dict(base)
    reason = []

    if regime in ('SIDEWAYS', 'TRENDING', 'TRENDING_UP') and processed >= min_events_for_action and block_ratio >= high_block_ratio:
        out['global_cooldown_p2_minutes'] = max(p2_min, int(round(base['global_cooldown_p2_minutes'] * 0.5)))
        out['symbol_cooldown_minutes'] = max(symbol_min, int(round(base['symbol_cooldown_minutes'] * 0.5)))
        out['signal_confirmation_periods'] = max(confirm_min, int(base['signal_confirmation_periods']) - 1)
        reason.append('high_block_ratio_relax')
    elif regime in ('RISK_OFF', 'RISK-OFF'):
        out['global_cooldown_p2_minutes'] = min(p2_max, int(round(base['global_cooldown_p2_minutes'] * 1.2)))
        out['symbol_cooldown_minutes'] = min(symbol_max, int(round(base['symbol_cooldown_minutes'] * 1.2)))
        out['signal_confirmation_periods'] = min(confirm_max, int(base['signal_confirmation_periods']) + 1)
        reason.append('risk_off_harden')

    # clamp
    out['global_cooldown_p2_minutes'] = max(p2_min, min(p2_max, int(out['global_cooldown_p2_minutes'])))
    out['symbol_cooldown_minutes'] = max(symbol_min, min(symbol_max, int(out['symbol_cooldown_minutes'])))
    out['signal_confirmation_periods'] = max(confirm_min, min(confirm_max, int(out['signal_confirmation_periods'])))

    applied = out != base
    meta = {
        'enabled': True,
        'applied': applied,
        'reason': ','.join(reason) if reason else 'no_change',
        'regime': regime,
        'lookback_runs': lookback,
        'recent_events_processed': processed,
        'recent_events_blocked': blocked,
        'recent_block_ratio': round(block_ratio, 4),
        'base': base,
        'effective': out,
    }

    try:
        (adaptive_state_path or (REPORTS_DIR / 'event_adaptive_state.json')).write_text(
            json.dumps({'timestamp': datetime.now().isoformat(), **meta}, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
    except Exception:
        pass

    return out, meta


def run_event_param_scan(state: dict, last_state: dict, ev_cfg: dict):
    """Run lightweight one-shot parameter scan on current snapshot pair."""
    base = {
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
    }

    grid = []
    # Suppress noisy internal logs during parameter grid search
    noisy_names = [
        'src.execution.event_decision_engine',
        'src.execution.event_monitor',
        'src.execution.cooldown_manager',
        'src.execution.event_driven_integration',
    ]
    old_levels = {}
    for n in noisy_names:
        lg = logging.getLogger(n)
        old_levels[n] = lg.level
        lg.setLevel(logging.WARNING)

    try:
        with tempfile.TemporaryDirectory(prefix="v5_event_param_scan_") as temp_dir:
            temp_root = Path(temp_dir)
            candidate_idx = 0
            for sct in [0.25, 0.30, 0.35, 0.45]:
                for rjt in [3, 4, 5]:
                    for scp in [2, 3]:
                        for btp in [0.3, 0.5, 0.8]:
                            candidate_idx += 1
                            cfg_i = dict(base)
                            cfg_i.update({
                                'score_change_threshold': float(sct),
                                'rank_jump_threshold': int(rjt),
                                'signal_confirmation_periods': int(scp),
                                'breakout_threshold_pct': float(btp),
                                # Keep exploratory scans isolated from live persisted state.
                                'monitor_state_path': str(temp_root / f"event_monitor_state_{candidate_idx}.json"),
                                'cooldown_state_path': str(temp_root / f"cooldown_state_{candidate_idx}.json"),
                            })
                            trader_i = create_event_driven_trader(cfg_i)
                            res_i = trader_i.should_trade(state, last_state)
                            actions_n = len(res_i.get('actions') or [])
                            events_n = int(res_i.get('events_processed', 0) or 0)
                            blocked_n = int(res_i.get('events_blocked', 0) or 0)
                            score = actions_n * 5 + events_n - blocked_n * 2
                            # soft penalty for over-loose settings
                            score -= abs(float(sct) - float(base['score_change_threshold'])) * 5
                            score -= abs(float(btp) - float(base['breakout_threshold_pct'])) * 2

                            grid.append({
                                'params': {
                                    'score_change_threshold': sct,
                                    'rank_jump_threshold': rjt,
                                    'signal_confirmation_periods': scp,
                                    'breakout_threshold_pct': btp,
                                },
                                'actions': actions_n,
                                'events_processed': events_n,
                                'events_blocked': blocked_n,
                                'should_trade': bool(res_i.get('should_trade', False)),
                                'fitness': round(score, 4),
                            })
    finally:
        for n in noisy_names:
            logging.getLogger(n).setLevel(old_levels.get(n, logging.INFO))

    grid.sort(key=lambda x: (x['fitness'], x['actions'], x['events_processed']), reverse=True)
    best = grid[0] if grid else None

    return {
        'timestamp': datetime.now().isoformat(),
        'base': base,
        'best': best,
        'top5': grid[:5],
        'count': len(grid),
        'note': 'One-shot scan on current+previous snapshot, for no-trade tuning guidance.',
    }


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Event-Driven Trading Check")
    logger.info("=" * 60)
    
    # Check if event-driven is enabled in config
    config_path = resolve_config_path()
    event_driven_enabled = False
    active_mode = False
    force_full_mode = False
    force_full_min_interval_minutes = 12
    active_min_interval_minutes = 60
    ev_cfg = {}
    cfg = {}

    try:
        import yaml
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}

        # If current config has no event_driven block, fallback to dedicated event_driven.yaml
        ev_cfg = cfg.get('event_driven', {}) or {}
        if not ev_cfg:
            fallback_path = PROJECT_ROOT / 'configs/event_driven.yaml'
            if fallback_path.exists():
                with open(fallback_path) as f:
                    fb = yaml.safe_load(f) or {}
                    ev_cfg = fb.get('event_driven', {}) or {}

        event_driven_enabled = bool(ev_cfg.get('enabled', False))
        mode = str(ev_cfg.get('mode', '')).strip().lower()
        active_mode = bool(ev_cfg.get('active_mode', mode == 'active'))
        force_full_mode = bool(ev_cfg.get('force_full_run', mode in ('force_full', 'full')))
        force_full_min_interval_minutes = int(ev_cfg.get('force_full_min_interval_minutes', 12) or 12)
        active_min_interval_minutes = int(ev_cfg.get('active_min_interval_minutes', 60) or 60)
    except Exception as e:
        logger.warning(f"Could not read config: {e}")
    
    if not event_driven_enabled:
        logger.info("Event-driven trading is disabled in config")
        logger.info("Using standard V5 execution")
        return 0
    
    # Load current state
    logger.info("Loading current market state...")
    state = load_current_state(cfg, config_path=config_path)
    
    if not state:
        logger.error("Failed to load state, falling back to standard execution")
        return 0

    paths = build_paths(cfg)
    
    # Load last signal history for comparison
    last_state = None
    history_path = paths.event_driven_signals_path
    if history_path.exists():
        try:
            with open(history_path) as f:
                last_data = json.load(f)
                last_state = {
                    'timestamp_ms': last_data.get('timestamp', 0),
                    'regime': last_data.get('regime', 'SIDEWAYS'),
                    'prices': last_data.get('prices', {}),
                    'signals': last_data.get('signals', {}),
                    'selected_symbols': top_selected_symbols(last_data.get('signals', {}) or {}, limit=5)
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

    live_service_unit = resolve_live_service_unit(ev_cfg)
    logger.info(f"Live trigger service: {live_service_unit}")

    # No-trade period utilities: candidate watchlist + risk-off shadow + one-shot param scan
    watchlist = build_candidate_watchlist(
        state,
        breakout_threshold_pct=float(ev_cfg.get('breakout_threshold_pct', 0.5) or 0.5),
        top_n=10,
    )
    paths.event_candidates_path.write_text(
        json.dumps(
            {
                'timestamp': datetime.now().isoformat(),
                'regime': state.get('regime'),
                'count': len(watchlist),
                'candidates': watchlist,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding='utf-8',
    )

    shadow_plan = build_riskoff_shadow_plan(state, cfg, watchlist, equity_file=paths.equity_validation_path)
    paths.riskoff_shadow_plan_path.write_text(
        json.dumps(shadow_plan, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )

    param_scan = run_event_param_scan(state, last_state, ev_cfg)
    paths.event_param_scan_path.write_text(
        json.dumps(param_scan, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )

    # Build trader config from YAML (with adaptive cooldown)
    adaptive_cd, adaptive_meta = compute_adaptive_event_cfg(
        ev_cfg,
        state,
        log_path=paths.event_driven_log_path,
        adaptive_state_path=paths.event_adaptive_state_path,
    )
    logger.info(
        f"Adaptive cooldown: applied={adaptive_meta.get('applied')} reason={adaptive_meta.get('reason')} "
        f"p2={adaptive_cd.get('global_cooldown_p2_minutes')}m "
        f"symbol={adaptive_cd.get('symbol_cooldown_minutes')}m "
        f"confirm={adaptive_cd.get('signal_confirmation_periods')}"
    )

    trader = create_event_driven_trader({
        'enabled': True,
        'order_store_path': str(paths.order_store_path),
        'check_interval_minutes': int(ev_cfg.get('check_interval_minutes', adaptive_cd.get('check_interval_minutes', 15)) or 15),
        'global_cooldown_p2_minutes': int(adaptive_cd.get('global_cooldown_p2_minutes', ev_cfg.get('global_cooldown_p2_minutes', 30)) or 30),
        'symbol_cooldown_minutes': int(adaptive_cd.get('symbol_cooldown_minutes', ev_cfg.get('symbol_cooldown_minutes', 60)) or 60),
        'signal_confirmation_periods': int(adaptive_cd.get('signal_confirmation_periods', ev_cfg.get('signal_confirmation_periods', 2)) or 2),
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
        'active_min_interval_minutes': active_min_interval_minutes,
        'adaptive_cooldown': adaptive_meta,
        'live_service_unit': live_service_unit,
        'live_service_triggered': False,
        'live_service_ok': None,
        'live_service_returncode': None,
        'live_service_stderr': '',
        'trigger_reason': None,
        'last_run_age_sec': None,
        'last_run_id': None,
        'current_target_run_id': get_current_live_window_run_id(),
    }

    if force_full_mode:
        age_sec, last_run_id = get_last_live_run_age_sec(paths.runs_dir)
        execution['last_run_age_sec'] = age_sec
        execution['last_run_id'] = last_run_id

        throttle = evaluate_live_trigger_throttle(
            last_run_age_sec=age_sec,
            last_run_id=last_run_id,
            current_run_id=execution['current_target_run_id'],
            min_interval_minutes=force_full_min_interval_minutes,
        )
        if throttle['throttled']:
            logger.info(
                f"FORCE_FULL throttled: reason={throttle['reason']} last_run_id={last_run_id} "
                f"current_run_id={execution['current_target_run_id']} age_sec={age_sec}"
            )
            execution.update({
                'trigger_reason': f"force_full_throttled:{throttle['reason']}",
            })
        else:
            logger.info(f"FORCE_FULL mode: starting {live_service_unit}")
            exec_res = trigger_live_execution_service(live_service_unit)
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
            age_sec, last_run_id = get_last_live_run_age_sec(paths.runs_dir)
            execution['last_run_age_sec'] = age_sec
            execution['last_run_id'] = last_run_id
            bypass_throttle = should_bypass_live_trigger_throttle(result['actions'])
            throttle = {
                'throttled': False,
                'reason': 'risk_close_bypass',
                'min_interval_sec': max(0, int(active_min_interval_minutes or 0) * 60),
            } if bypass_throttle else evaluate_live_trigger_throttle(
                last_run_age_sec=age_sec,
                last_run_id=last_run_id,
                current_run_id=execution['current_target_run_id'],
                min_interval_minutes=active_min_interval_minutes,
            )
            if bypass_throttle:
                logger.info(
                    f"ACTIVE mode bypassing throttle for urgent risk actions: "
                    f"last_run_id={last_run_id} current_run_id={execution['current_target_run_id']}"
                )
                execution['trigger_reason'] = 'event_actions_risk_bypass'
            if throttle['throttled']:
                logger.info(
                    f"ACTIVE mode throttled: reason={throttle['reason']} last_run_id={last_run_id} "
                    f"current_run_id={execution['current_target_run_id']} age_sec={age_sec}"
                )
                execution.update({
                    'trigger_reason': f"active_throttled:{throttle['reason']}",
                })
            else:
                persisted = persist_event_actions(
                    actions=result['actions'],
                    target_run_id=execution['current_target_run_id'],
                    order_store_path=paths.order_store_path,
                )
                if persisted:
                    logger.info(
                        "ACTIVE mode: persisted close override actions for run %s",
                        execution['current_target_run_id'],
                    )
                logger.info(f"ACTIVE mode: starting {live_service_unit}")
                exec_res = trigger_live_execution_service(live_service_unit)
                execution.update({
                    'live_service_triggered': True,
                    'live_service_ok': exec_res.get('ok'),
                    'live_service_returncode': exec_res.get('returncode'),
                    'live_service_stderr': exec_res.get('stderr', ''),
                    'trigger_reason': execution.get('trigger_reason') or 'event_actions',
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
        'watchlist_top3': watchlist[:3],
        'param_scan_best': (param_scan or {}).get('best'),
        'execution': execution,
    }
    
    log_path = paths.event_driven_log_path
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(log_path, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    
    # Save signal history for next comparison
    history_path = paths.event_driven_signals_path
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
