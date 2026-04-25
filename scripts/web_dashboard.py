#!/usr/bin/env python3
"""
V5 Web Dashboard - 交易可视化界面

功能：
- 账户总览
- 交易历史
- 币种评分
- K线图表
- 系统状态
"""

import os
import json
import math
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import copy
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure repo-local imports work even when the script is launched outside the repo root.
_BOOTSTRAP_WORKSPACE = Path(__file__).resolve().parents[1]
_BOOTSTRAP_WORKSPACE_STR = str(_BOOTSTRAP_WORKSPACE)
if _BOOTSTRAP_WORKSPACE_STR not in sys.path:
    sys.path.insert(0, _BOOTSTRAP_WORKSPACE_STR)

from flask import Flask, g, has_app_context, has_request_context, render_template, jsonify, request, send_from_directory
import pandas as pd
import yaml
import requests
from configs.loader import load_config as load_app_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path

from src.core.models import MarketSeries
from src.data.okx_ccxt_provider import OKXCCXTProvider
from src.execution.fill_store import (
    derive_fill_store_path,
    derive_position_store_path,
    derive_runtime_auto_risk_guard_path,
    derive_runtime_auto_risk_eval_path,
    derive_runtime_cost_events_dir,
    derive_runtime_named_artifact_path,
    derive_runtime_reports_dir,
    derive_runtime_runs_dir,
)
from src.regime.funding_vote_utils import build_funding_vote, summarize_funding_rows
from src.regime.rss_vote_utils import build_rss_vote
from src.research.cache_loader import load_cached_market_data
from src.risk.auto_risk_guard import extract_risk_level

SLIPPAGE_HISTOGRAM_BINS = (
    (None, -10.0, '≤-10'),
    (-10.0, -5.0, '-10~-5'),
    (-5.0, 0.0, '-5~0'),
    (0.0, 5.0, '0~5'),
    (5.0, 10.0, '5~10'),
    (10.0, 20.0, '10~20'),
    (20.0, 40.0, '20~40'),
    (40.0, 80.0, '40~80'),
    (80.0, None, '≥80'),
)


def _detect_workspace() -> Path:
    candidates: List[Path] = []

    env_workspace = os.getenv('V5_WORKSPACE')
    if env_workspace:
        candidates.append(Path(env_workspace).expanduser())

    script_workspace = Path(__file__).resolve().parents[1]
    candidates.append(script_workspace)

    cwd_workspace = Path.cwd()
    if cwd_workspace not in candidates:
        candidates.append(cwd_workspace)

    for candidate in candidates:
        if (candidate / 'web' / 'templates' / 'monitor_v2.html').exists():
            return candidate

    return script_workspace


WORKSPACE = _detect_workspace()
WORKSPACE_STR = str(WORKSPACE)
if WORKSPACE_STR not in sys.path:
    sys.path.insert(0, WORKSPACE_STR)

WEB_DIR = WORKSPACE / 'web'
REPORTS_DIR = WORKSPACE / 'reports'
CACHE_DIR = WORKSPACE / 'data' / 'cache'
CHINA_TZ = timezone(timedelta(hours=8))


def _resolve_workspace_env_path() -> Path:
    return Path(resolve_runtime_env_path(project_root=WORKSPACE)).resolve()


def _resolve_react_build_path() -> Path:
    candidates: List[Path] = []

    env_dist = os.getenv('V5_DASHBOARD_DIST')
    if env_dist:
        candidates.append(Path(env_dist).expanduser())

    candidates.extend([
        WORKSPACE / 'web' / 'dist',
        WORKSPACE / 'dist',
        WORKSPACE / 'frontend' / 'dist',
    ])

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


REACT_BUILD_PATH = _resolve_react_build_path()


def _dashboard_renderer_mode() -> str:
    raw = str(os.getenv("V5_DASHBOARD_RENDERER") or "").strip().lower()
    if raw in {"react", "spa", "dist"}:
        return "react"
    if raw in {"template", "jinja", "legacy"}:
        return "template"
    return "template"


SYSTEMCTL_BIN = shutil.which('systemctl')
TIMER_TS_RE = re.compile(r'(\w{3}\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})')

app = Flask(
    __name__,
    template_folder=str(WEB_DIR / 'templates'),
    static_folder=str(WEB_DIR / 'static'),
)


@dataclass(frozen=True)
class DashboardRuntimePaths:
    reports_dir: Path
    orders_db: Path
    fills_db: Path
    positions_db: Path
    kill_switch_path: Path
    reconcile_status_path: Path
    runs_dir: Path
    auto_risk_guard_path: Path
    auto_risk_eval_path: Path
    telemetry_db: Path


_DASHBOARD_API_CACHE_MISS = object()
_OKX_ACCOUNT_BALANCE_CACHE: Dict[tuple[str, str, str], tuple[float, Any]] = {}
_OKX_ACCOUNT_BALANCE_CACHE_LOCK = threading.Lock()
_OKX_PUBLIC_TICKER_CACHE: Dict[str, tuple[float, float]] = {}
_OKX_PUBLIC_TICKER_CACHE_LOCK = threading.Lock()
_OKX_HEALTH_CHECK_CACHE: Dict[tuple[str, str, str], tuple[float, Any, float]] = {}
_OKX_HEALTH_CHECK_CACHE_LOCK = threading.Lock()
_DASHBOARD_ROUTE_CACHE: Dict[tuple[str, tuple[tuple[str, str], ...]], tuple[float, Any, int]] = {}
_DASHBOARD_ROUTE_CACHE_LOCK = threading.Lock()

# 注册健康检查蓝图
try:
    from src.reporting.health import health_bp
    app.register_blueprint(health_bp)
    print("[WebDashboard] Health check endpoints registered: /health, /ready, /liveness")
except Exception as e:
    print(f"[WebDashboard] Failed to register health blueprint: {e}")

try:
    from src.monitoring.prometheus_exporter import render_prometheus_metrics
except Exception as e:
    render_prometheus_metrics = None
    print(f"[WebDashboard] Failed to import prometheus exporter: {e}")


@app.after_request
def add_no_cache_headers(resp):
    """避免移动端缓存旧前端，确保样式/图表脚本及时生效。"""
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


def _log_dashboard_exception(label: str, exc: BaseException) -> None:
    print(f"[web_dashboard] {label}: {exc}", file=sys.stderr)
    traceback.print_exception(type(exc), exc, exc.__traceback__)


def _sanitize_public_error_text(value: Any, *, default: str = 'internal error') -> str:
    text = str(value or '').strip()
    if not text:
        return default
    lowered = text.lower()
    if 'traceback' in lowered:
        return default
    if re.search(r'[A-Za-z]:\\', text):
        return default
    if re.search(r'(^|[\s(])/(?:[^/\s]+/?)+', text):
        return default
    return text


def _json_internal_error_response(
    exc: BaseException,
    *,
    status_code: int = 500,
    error: str = 'internal server error',
    **extra: Any,
):
    _log_dashboard_exception('api error', exc)
    payload = dict(extra)
    payload['error'] = error
    return jsonify(payload), status_code


@app.route('/metrics')
def prometheus_metrics():
    if render_prometheus_metrics is None:
        body = "v5_metrics_exporter_up 0\n"
        return body, 500, {'Content-Type': 'text/plain; version=0.0.4; charset=utf-8'}

    try:
        body = render_prometheus_metrics(workspace=WORKSPACE, config=load_config())
        return body, 200, {'Content-Type': 'text/plain; version=0.0.4; charset=utf-8'}
    except Exception as exc:
        _log_dashboard_exception('metrics export error', exc)
        body = "v5_metrics_exporter_up 0\n"
        return body, 500, {'Content-Type': 'text/plain; version=0.0.4; charset=utf-8'}


def _extract_endpoint_json(result: Any) -> tuple[Any, int]:
    response = result
    status_code = 200

    if isinstance(result, tuple):
        if not result:
            return None, 500
        response = result[0]
        for item in result[1:]:
            if isinstance(item, int):
                status_code = item
                break
    else:
        try:
            status_code = int(getattr(response, 'status_code', 200) or 200)
        except Exception:
            status_code = 200

    if hasattr(response, 'get_json'):
        try:
            payload = response.get_json(silent=True)
        except TypeError:
            payload = response.get_json()
        return payload, status_code

    if isinstance(response, (dict, list)):
        return response, status_code

    return None, status_code


def _call_dashboard_api(api_func, *, default: Any, label: str, errors: Optional[List[str]] = None) -> Any:
    try:
        if has_app_context():
            cache = getattr(g, '_dashboard_api_cache', None)
            if cache is None:
                cache = {}
                g._dashboard_api_cache = cache
            cached = cache.get(api_func)
            if cached is None:
                cached = _extract_endpoint_json(api_func())
                cache[api_func] = cached
            payload, status_code = cached
        else:
            payload, status_code = _extract_endpoint_json(api_func())
    except Exception as exc:
        _log_dashboard_exception(f'dashboard child {label}', exc)
        if errors is not None:
            errors.append(f'{label}: {_sanitize_public_error_text(exc, default=str(exc))}')
        return default

    if status_code >= 400:
        message = None
        if isinstance(payload, dict):
            message = payload.get('error') or payload.get('message')
        message = _sanitize_public_error_text(message, default='internal error')
        if errors is not None:
            errors.append(f'{label}: {message}')
        return default

    return payload if payload is not None else default


def _dashboard_route_cache_key(name: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    context = (
        ('workspace', str(WORKSPACE)),
        ('reports_dir', str(REPORTS_DIR)),
        ('load_config_id', str(id(load_config))),
        ('runtime_paths_id', str(id(_resolve_dashboard_runtime_paths))),
        ('systemctl', str(SYSTEMCTL_BIN)),
    )
    if not has_request_context():
        return (name, context)
    args = tuple(sorted((key, str(value)) for key, value in request.args.items() if key != '_'))
    return (name, context + args)


def _cache_json_response(ttl_seconds):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ttl = float(ttl_seconds() if callable(ttl_seconds) else ttl_seconds or 0)
            if ttl <= 0:
                return func(*args, **kwargs)

            cache_key = _dashboard_route_cache_key(func.__name__)
            now = time.time()
            with _DASHBOARD_ROUTE_CACHE_LOCK:
                cached = _DASHBOARD_ROUTE_CACHE.get(cache_key)
                if cached and cached[0] > now:
                    cached_response = jsonify(copy.deepcopy(cached[1]))
                    cached_response.status_code = int(cached[2])
                    return cached_response

            response = func(*args, **kwargs)
            payload, status_code = _extract_endpoint_json(response)
            if status_code < 400 and payload is not None:
                with _DASHBOARD_ROUTE_CACHE_LOCK:
                    _DASHBOARD_ROUTE_CACHE[cache_key] = (now + ttl, copy.deepcopy(payload), int(status_code))
            return response

        return wrapper

    return decorator


def _dashboard_view_cache_ttl_seconds() -> float:
    view = str(request.args.get('view', 'full') or 'full').strip().lower() if has_request_context() else 'full'
    if view == 'primary':
        return 8.0
    if view == 'deferred':
        return 20.0
    return 10.0

def _resolve_config_path() -> Path:
    """Resolve config path via the shared runtime config helper."""
    return Path(resolve_runtime_config_path(project_root=WORKSPACE)).resolve()


CONFIG_PATH = _resolve_config_path()
POSITION_KLINE_TIMEFRAMES: Dict[str, Dict[str, Any]] = {
    '1h': {'source_timeframe': '1h', 'resample_rule': None, 'source_limit_multiplier': 1, 'fresh_for_seconds': 6 * 3600},
    '4h': {'source_timeframe': '1h', 'resample_rule': '4H', 'source_limit_multiplier': 4, 'fresh_for_seconds': 24 * 3600},
    '1d': {'source_timeframe': '1h', 'resample_rule': '1D', 'source_limit_multiplier': 24, 'fresh_for_seconds': 7 * 24 * 3600},
}
POSITION_KLINE_DEFAULT_LIMIT = 96
_OKX_PUBLIC_PROVIDER: Optional[OKXCCXTProvider] = None
_WORKSPACE_PYTHON_BIN: Optional[str] = None


def _normalize_dashboard_symbol(symbol: str) -> str:
    raw = str(symbol or '').strip().upper()
    if not raw:
        raise ValueError('symbol is required')

    cleaned = raw.replace('_', '/').replace('-', '/')
    cleaned = re.sub(r'/+', '/', cleaned)
    if cleaned.endswith('/USDT'):
        return cleaned
    if cleaned.endswith('USDT') and '/' not in cleaned:
        return f"{cleaned[:-4]}/USDT"
    if '/' in cleaned:
        base = cleaned.split('/', 1)[0]
    else:
        base = cleaned
    if not base:
        raise ValueError(f'invalid symbol: {symbol}')
    return f"{base}/USDT"


def _trim_market_series(series: MarketSeries, limit: int) -> MarketSeries:
    normalized = _normalize_market_series(series)
    target = max(int(limit or 0), 0)
    if target <= 0 or len(normalized.ts) <= target:
        return normalized
    return MarketSeries(
        symbol=normalized.symbol,
        timeframe=normalized.timeframe,
        ts=normalized.ts[-target:],
        open=normalized.open[-target:],
        high=normalized.high[-target:],
        low=normalized.low[-target:],
        close=normalized.close[-target:],
        volume=normalized.volume[-target:],
    )


def _market_series_to_frame(series: MarketSeries) -> pd.DataFrame:
    frame = pd.DataFrame({
        'timestamp_ms': list(series.ts or []),
        'open': list(series.open or []),
        'high': list(series.high or []),
        'low': list(series.low or []),
        'close': list(series.close or []),
        'volume': list(series.volume or []),
    })
    if frame.empty:
        return frame
    frame['timestamp'] = pd.to_datetime(frame['timestamp_ms'], unit='ms', utc=True)
    frame = frame.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp')
    return frame.set_index('timestamp')[['open', 'high', 'low', 'close', 'volume']]


def _frame_to_market_series(frame: pd.DataFrame, *, symbol: str, timeframe: str) -> MarketSeries:
    cleaned = frame.dropna(subset=['open', 'high', 'low', 'close']).sort_index()
    if cleaned.empty:
        return MarketSeries(symbol=symbol, timeframe=timeframe, ts=[], open=[], high=[], low=[], close=[], volume=[])
    timestamp_ms = (cleaned.index.astype('int64') // 1_000_000).astype(int).tolist()
    return MarketSeries(
        symbol=symbol,
        timeframe=timeframe,
        ts=timestamp_ms,
        open=cleaned['open'].astype(float).tolist(),
        high=cleaned['high'].astype(float).tolist(),
        low=cleaned['low'].astype(float).tolist(),
        close=cleaned['close'].astype(float).tolist(),
        volume=cleaned['volume'].fillna(0).astype(float).tolist(),
    )


def _normalize_market_series(series: MarketSeries) -> MarketSeries:
    points = []
    for idx, values in enumerate(
        zip(
            series.ts or [],
            series.open or [],
            series.high or [],
            series.low or [],
            series.close or [],
            series.volume or [],
        )
    ):
        ts_ms, open_px, high_px, low_px, close_px, volume = values
        try:
            ts_value = int(ts_ms)
        except (TypeError, ValueError):
            continue
        if abs(ts_value) < 10_000_000_000:
            ts_value *= 1000
        points.append((ts_value, idx, open_px, high_px, low_px, close_px, volume))

    if not points:
        return MarketSeries(symbol=series.symbol, timeframe=series.timeframe, ts=[], open=[], high=[], low=[], close=[], volume=[])

    points.sort(key=lambda item: (item[0], item[1]))
    deduped: List[tuple[int, int, Any, Any, Any, Any, Any]] = []
    for point in points:
        if deduped and deduped[-1][0] == point[0]:
            deduped[-1] = point
        else:
            deduped.append(point)

    return MarketSeries(
        symbol=series.symbol,
        timeframe=series.timeframe,
        ts=[int(item[0]) for item in deduped],
        open=[item[2] for item in deduped],
        high=[item[3] for item in deduped],
        low=[item[4] for item in deduped],
        close=[item[5] for item in deduped],
        volume=[item[6] for item in deduped],
    )


def _latest_market_series_ts_ms(series: Optional[MarketSeries]) -> Optional[int]:
    if series is None or not series.ts:
        return None
    latest_ts_ms = None
    for ts_ms in series.ts:
        try:
            ts_value = int(ts_ms)
        except (TypeError, ValueError):
            continue
        if abs(ts_value) < 10_000_000_000:
            ts_value *= 1000
        latest_ts_ms = ts_value if latest_ts_ms is None else max(latest_ts_ms, ts_value)
    return latest_ts_ms


def _load_cached_position_market_series(symbol: str, timeframe: str, limit: int) -> Optional[MarketSeries]:
    tf_config = POSITION_KLINE_TIMEFRAMES[timeframe]
    source_timeframe = str(tf_config['source_timeframe'])
    source_limit = max(int(limit or 0) * int(tf_config['source_limit_multiplier']), int(limit or 0))
    source_limit = max(source_limit, int(limit or 0))
    market_data = load_cached_market_data(CACHE_DIR, [symbol], source_timeframe, limit=source_limit)
    series = market_data.get(symbol)
    if series is None or not series.ts:
        return None

    resample_rule = tf_config.get('resample_rule')
    if not resample_rule:
        return _trim_market_series(series, limit)

    frame = _market_series_to_frame(series)
    if frame.empty:
        return None

    resampled = frame.resample(str(resample_rule)).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    })
    resampled = resampled.dropna(subset=['open', 'high', 'low', 'close'])
    if resampled.empty:
        return None
    return _frame_to_market_series(resampled.tail(int(limit or 0)), symbol=symbol, timeframe=timeframe)


def _position_market_series_is_fresh(series: Optional[MarketSeries], timeframe: str) -> bool:
    last_ts = _latest_market_series_ts_ms(series)
    if last_ts is None:
        return False
    if last_ts <= 0:
        return False
    max_age_seconds = float(POSITION_KLINE_TIMEFRAMES.get(timeframe, {}).get('fresh_for_seconds') or 0)
    if max_age_seconds <= 0:
        return False
    age_seconds = (time.time() * 1000 - last_ts) / 1000.0
    return age_seconds <= max_age_seconds


def _get_okx_public_provider() -> OKXCCXTProvider:
    global _OKX_PUBLIC_PROVIDER
    if _OKX_PUBLIC_PROVIDER is None:
        _OKX_PUBLIC_PROVIDER = OKXCCXTProvider(rate_limit=True)
    return _OKX_PUBLIC_PROVIDER


def _load_position_market_series(symbol: str, timeframe: str, limit: int) -> tuple[MarketSeries, str]:
    normalized_symbol = _normalize_dashboard_symbol(symbol)
    stale_cached_series: Optional[MarketSeries] = None

    try:
        cached_series = _load_cached_position_market_series(normalized_symbol, timeframe, limit)
        if cached_series is not None and cached_series.ts:
            if _position_market_series_is_fresh(cached_series, timeframe):
                return _trim_market_series(cached_series, limit), 'cache'
            stale_cached_series = cached_series
    except Exception:
        pass

    provider = _get_okx_public_provider()
    try:
        series = provider.fetch_ohlcv([normalized_symbol], timeframe=timeframe, limit=int(limit or 0)).get(normalized_symbol)
        if series is not None and series.ts:
            return _trim_market_series(series, limit), 'okx'
    except Exception:
        pass

    if stale_cached_series is not None and stale_cached_series.ts:
        return _trim_market_series(stale_cached_series, limit), 'cache_stale'

    raise FileNotFoundError(f'no OHLCV data for {normalized_symbol}')


def _load_multi_strategy_score_transform() -> tuple[str, float]:
    mode = "tanh"
    scale = 1.0
    try:
        cfg = load_app_config(str(_resolve_config_path()), env_path=None)
        alpha_cfg = getattr(cfg, "alpha", None)
        if alpha_cfg is not None:
            mode = str(getattr(alpha_cfg, "multi_strategy_score_transform", mode) or mode).strip().lower()
            scale = float(getattr(alpha_cfg, "multi_strategy_score_transform_scale", scale) or scale)
    except Exception:
        pass
    if mode not in {"none", "clip", "tanh"}:
        mode = "tanh"
    return mode, max(scale, 1e-6)


def _legacy_display_score(score: float) -> float:
    raw = float(score or 0.0)
    if abs(raw) <= 1.0:
        return raw
    mode, scale = _load_multi_strategy_score_transform()
    magnitude = abs(raw)
    if mode == "none":
        return raw
    if mode == "clip":
        return math.copysign(min(magnitude, 1.0), raw)
    return math.copysign(math.tanh(magnitude / scale), raw)


def _load_recent_scan_limit(env_name: str) -> Optional[int]:
    raw = str(os.getenv(env_name, '') or '').strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _iter_decision_audits(
    reports_dir: Path,
    scan_limit: Optional[int] = None,
    *,
    include_parse_errors: bool = False,
) -> List[Dict[str, Any]]:
    runs_dir = reports_dir / 'runs'
    if not runs_dir.exists():
        return []

    def _candidate_sort_epoch(run_dir: Path) -> float:
        epoch = _run_id_epoch(run_dir.name)
        if epoch is not None:
            return epoch
        try:
            return (run_dir / 'decision_audit.json').stat().st_mtime
        except OSError:
            return run_dir.stat().st_mtime

    run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
    run_dirs.sort(key=_candidate_sort_epoch, reverse=True)
    if scan_limit is not None:
        run_dirs = run_dirs[:scan_limit]

    audits: List[Dict[str, Any]] = []
    for run_dir in run_dirs:
        audit_path = run_dir / 'decision_audit.json'
        try:
            audit = json.loads(audit_path.read_text(encoding='utf-8'))
        except Exception:
            if not include_parse_errors:
                continue
            audit = {
                'run_id': run_dir.name,
                '_parse_error': True,
                'error': 'internal parse error',
            }
        if not isinstance(audit, dict) or (not audit and not include_parse_errors):
            continue
        audits.append({
            'run_dir': run_dir,
            'audit': audit,
            'sort_epoch': _decision_audit_sort_epoch(run_dir, audit),
        })
    audits.sort(key=lambda item: float(item.get('sort_epoch', 0.0) or 0.0), reverse=True)
    if scan_limit is not None:
        audits = audits[:scan_limit]
    return audits


def _sorted_run_dirs_by_artifact_mtime(runs_dir: Path, artifact_name: str, limit: Optional[int] = None) -> List[Path]:
    if not runs_dir.exists():
        return []

    run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / artifact_name).exists()]
    def _sort_epoch(run_dir: Path) -> float:
        try:
            return datetime.strptime(run_dir.name, "%Y%m%d_%H").timestamp()
        except Exception:
            return (run_dir / artifact_name).stat().st_mtime

    run_dirs.sort(key=_sort_epoch, reverse=True)
    if limit is not None:
        run_dirs = run_dirs[:limit]
    return run_dirs


def _normalize_top_scores(raw_scores: Any, limit: int = 20) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    if not isinstance(raw_scores, list):
        return items

    for idx, item in enumerate(raw_scores[:limit]):
        if not isinstance(item, dict):
            continue
        try:
            raw_score = round(float(item.get('raw_score', item.get('score', 0)) or 0), 4)
            if item.get('display_score') is None:
                display_score = round(float(_legacy_display_score(raw_score)), 4)
            else:
                display_score = round(float(item.get('display_score', item.get('score', 0)) or 0), 4)
            items.append({
                'symbol': item.get('symbol', 'Unknown'),
                'score': display_score,
                'display_score': display_score,
                'raw_score': raw_score,
                'rank': int(item.get('rank', idx + 1) or (idx + 1)),
            })
        except Exception:
            continue
    return items


def _decision_note_text(audit: Dict[str, Any]) -> str:
    notes = audit.get('notes', [])
    if isinstance(notes, list):
        return ' '.join(str(item) for item in notes if item)
    return str(notes or '')


def _decision_counts(audit: Dict[str, Any]) -> Dict[str, int]:
    counts = audit.get('counts', {})
    if not isinstance(counts, dict):
        return {}
    normalized: Dict[str, int] = {}
    for key in ('universe', 'scored', 'selected', 'executed'):
        try:
            normalized[key] = int(counts.get(key, 0) or 0)
        except Exception:
            normalized[key] = 0
    return normalized


def _is_failed_decision_audit(audit: Dict[str, Any]) -> bool:
    details = audit.get('regime_details', {})
    regime = str(audit.get('regime') or '').strip().upper()
    note_text = _decision_note_text(audit).lower()
    counts = _decision_counts(audit)
    has_top_scores = bool(_normalize_top_scores(audit.get('top_scores', [])))
    has_details = isinstance(details, dict) and bool(details)
    return (
        'no market data returned from provider' in note_text
        and not has_top_scores
        and not has_details
        and counts.get('universe', 0) <= 0
        and counts.get('scored', 0) <= 0
        and regime in {'', 'UNKNOWN'}
    )


def _has_usable_market_state(audit: Dict[str, Any]) -> bool:
    if _is_failed_decision_audit(audit):
        return False
    details = audit.get('regime_details', {})
    if isinstance(details, dict) and details:
        return True
    regime = str(audit.get('regime') or '').strip()
    return bool(regime and regime.upper() != 'UNKNOWN')


def _load_regime_json_snapshot(reports_dir: Path) -> Dict[str, Any]:
    regime_json_path = reports_dir / 'regime.json'
    payload = _load_json_payload(regime_json_path)
    if not payload:
        return {}

    regime = str(payload.get('state') or payload.get('regime') or '').strip()
    if not regime:
        return {}

    votes = payload.get('votes', {})
    alerts = payload.get('alerts', [])
    monitor = payload.get('monitor', {})
    ts = (
        _coerce_timestamp_epoch(payload.get('ts'))
        or _coerce_timestamp_epoch(payload.get('timestamp'))
        or _coerce_timestamp_epoch(payload.get('last_update'))
    )
    if ts is None:
        try:
            ts = regime_json_path.stat().st_mtime
        except OSError:
            ts = None
    return {
        'state': regime,
        'position_multiplier': float(payload.get('position_multiplier', payload.get('multiplier', 0.0)) or 0.0),
        'final_score': float(payload.get('final_score', 0.0) or 0.0),
        'method': 'regime_json',
        'ts': ts,
        'votes': votes if isinstance(votes, dict) else {},
        'alerts': alerts if isinstance(alerts, list) else [],
        'monitor': monitor if isinstance(monitor, dict) else {},
    }


def _load_alpha_snapshot_scores(reports_dir: Path, limit: int = 20) -> Dict[str, Any]:
    alpha_snapshot_path = reports_dir / 'alpha_snapshot.json'
    payload = _load_json_payload(alpha_snapshot_path)
    raw_scores = payload.get('scores', {})
    if not isinstance(raw_scores, dict) or not raw_scores:
        return {}

    items: List[Dict[str, Any]] = []
    ranked = sorted(raw_scores.items(), key=lambda kv: float(kv[1] or 0.0), reverse=True)
    for rank, (symbol, raw_value) in enumerate(ranked[:limit], start=1):
        try:
            raw_score = round(float(raw_value or 0.0), 4)
            display_score = round(float(_legacy_display_score(raw_score)), 4)
        except Exception:
            continue
        items.append({
            'symbol': str(symbol or 'Unknown'),
            'score': display_score,
            'display_score': display_score,
            'raw_score': raw_score,
            'rank': rank,
        })

    if not items:
        return {}

    regime_snapshot = _load_regime_json_snapshot(reports_dir)
    ml_runtime = payload.get('ml_runtime', {}) if isinstance(payload.get('ml_runtime', {}), dict) else {}
    snapshot_ts = (
        _coerce_timestamp_epoch(payload.get('timestamp'))
        or _coerce_timestamp_epoch(payload.get('ts'))
        or _coerce_timestamp_epoch(ml_runtime.get('ts'))
    )
    if snapshot_ts is None:
        try:
            snapshot_ts = alpha_snapshot_path.stat().st_mtime
        except OSError:
            snapshot_ts = None
    return {
        'regime': str(regime_snapshot.get('state') or 'Unknown'),
        'current_run': 'alpha_snapshot',
        'scores': items,
        'timestamp': snapshot_ts,
    }

# 生产环境显示的 timer 列表
def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    if text in {'1', 'true', 'yes', 'on'}:
        return True
    if text in {'0', 'false', 'no', 'off', 'none', 'null'}:
        return False
    return bool(default)


def _normalize_symbol_key(symbol: Any) -> str:
    return str(symbol or '').strip().upper().replace('-', '/')


def _score_rank_map(scores: Any) -> Dict[str, int]:
    if not isinstance(scores, dict):
        return {}
    ranked = sorted(
        (
            (str(sym), _coerce_float(score))
            for sym, score in scores.items()
        ),
        key=lambda item: (item[1], item[0]),
        reverse=True,
    )
    return {sym: idx + 1 for idx, (sym, _) in enumerate(ranked)}


def _build_ml_signal_overview(
    reports_dir: Path,
    orders_db: Optional[Path] = None,
    preferred_symbols: Optional[List[str]] = None,
    limit: int = 3,
) -> Dict[str, Any]:
    runtime = _load_json_payload(
        derive_runtime_named_artifact_path(orders_db or (reports_dir / 'orders.sqlite'), 'ml_runtime_status', '.json')
    )
    promotion = _load_json_payload(
        derive_runtime_named_artifact_path(orders_db or (reports_dir / 'orders.sqlite'), 'model_promotion_decision', '.json')
    )
    snapshot = _load_json_payload(reports_dir / 'alpha_snapshot.json')
    impact_summary = _load_json_payload(
        derive_runtime_named_artifact_path(orders_db or (reports_dir / 'orders.sqlite'), 'ml_overlay_impact', '.json')
    )

    raw_factors = snapshot.get('raw_factors', {})
    z_factors = snapshot.get('z_factors', {})
    score_map = snapshot.get('scores', {})
    base_score_map = snapshot.get('base_scores', {})
    overlay_score_map = snapshot.get('ml_overlay_scores', {})
    base_rank_map = _score_rank_map(base_score_map)
    final_rank_map = _score_rank_map(score_map)

    factor_rows: List[Dict[str, Any]] = []
    if isinstance(z_factors, dict):
        for symbol, z_bucket in z_factors.items():
            if not isinstance(z_bucket, dict):
                continue
            raw_bucket = raw_factors.get(symbol, {}) if isinstance(raw_factors, dict) else {}
            if raw_bucket and not isinstance(raw_bucket, dict):
                raw_bucket = {}
            ml_zscore = _coerce_float(z_bucket.get('ml_pred_zscore', 0.0))
            ml_raw = _coerce_float(raw_bucket.get('ml_pred_raw', 0.0))
            overlay_score = _coerce_float(
                raw_bucket.get('ml_overlay_score', overlay_score_map.get(symbol, z_bucket.get('ml_overlay_score', 0.0)))
            )
            base_score = _coerce_float(raw_bucket.get('ml_base_score', base_score_map.get(symbol, 0.0)))
            final_score = _coerce_float(score_map.get(symbol, 0.0)) if isinstance(score_map, dict) else 0.0
            base_rank = int(base_rank_map.get(str(symbol), 0) or 0)
            final_rank = int(final_rank_map.get(str(symbol), 0) or 0)
            factor_rows.append({
                'symbol': str(symbol or 'Unknown'),
                'symbol_key': _normalize_symbol_key(symbol),
                'ml_zscore': round(ml_zscore, 4),
                'ml_raw': round(ml_raw, 6),
                'ml_overlay_score': round(overlay_score, 4),
                'base_score': round(base_score, 4),
                'final_score': round(final_score, 4),
                'score_delta': round(final_score - base_score, 4),
                'base_rank': base_rank,
                'final_rank': final_rank,
                'rank_delta': int(base_rank - final_rank) if base_rank and final_rank else 0,
            })

    nonzero_rows = [
        row for row in factor_rows
        if abs(float(row.get('ml_zscore', 0.0) or 0.0)) > 1e-9 or abs(float(row.get('ml_raw', 0.0) or 0.0)) > 1e-12
    ]
    nonzero_rows.sort(
        key=lambda item: (
            abs(float(item.get('ml_zscore', 0.0) or 0.0)),
            abs(float(item.get('ml_raw', 0.0) or 0.0)),
            str(item.get('symbol') or ''),
        ),
        reverse=True,
    )

    factor_by_symbol = {str(row['symbol_key']): row for row in nonzero_rows}
    contributors: List[Dict[str, Any]] = []
    seen_symbols = set()

    for symbol in preferred_symbols or []:
        row = factor_by_symbol.get(_normalize_symbol_key(symbol))
        if row is None:
            continue
        symbol_key = str(row['symbol_key'])
        if symbol_key in seen_symbols:
            continue
        contributors.append({k: v for k, v in row.items() if k != 'symbol_key'})
        seen_symbols.add(symbol_key)
        if len(contributors) >= limit:
            break

    if len(contributors) < limit:
        for row in nonzero_rows:
            symbol_key = str(row['symbol_key'])
            if symbol_key in seen_symbols:
                continue
            contributors.append({k: v for k, v in row.items() if k != 'symbol_key'})
            seen_symbols.add(symbol_key)
            if len(contributors) >= limit:
                break

    promoted_rows = [
        {k: v for k, v in row.items() if k != 'symbol_key'}
        for row in factor_rows
        if int(row.get('rank_delta', 0) or 0) > 0
    ]
    promoted_rows.sort(
        key=lambda item: (
            int(item.get('rank_delta', 0) or 0),
            abs(float(item.get('score_delta', 0.0) or 0.0)),
            str(item.get('symbol') or ''),
        ),
        reverse=True,
    )
    suppressed_rows = [
        {k: v for k, v in row.items() if k != 'symbol_key'}
        for row in factor_rows
        if int(row.get('rank_delta', 0) or 0) < 0
    ]
    suppressed_rows.sort(
        key=lambda item: (
            abs(int(item.get('rank_delta', 0) or 0)),
            abs(float(item.get('score_delta', 0.0) or 0.0)),
            str(item.get('symbol') or ''),
        ),
        reverse=True,
    )

    try:
        prediction_count = int(runtime.get('prediction_count', 0) or 0)
    except Exception:
        prediction_count = 0

    configured_enabled = _coerce_bool(runtime.get('configured_enabled', False))
    promoted = _coerce_bool(runtime.get('promotion_passed', promotion.get('passed', False)))
    live_active = _coerce_bool(runtime.get('used_in_latest_snapshot', False))
    overlay_mode = str(runtime.get('overlay_mode') or impact_summary.get('overlay_mode') or 'disabled')
    coverage_count = prediction_count if prediction_count > 0 else len(nonzero_rows)
    reason = str(runtime.get('reason') or runtime.get('error') or '')
    last_step = impact_summary.get('last_step', {}) if isinstance(impact_summary, dict) else {}
    rolling_24h = impact_summary.get('rolling_24h', {}) if isinstance(impact_summary, dict) else {}
    rolling_48h = impact_summary.get('rolling_48h', {}) if isinstance(impact_summary, dict) else {}
    impact_status = str((rolling_24h.get('status') or last_step.get('status') or 'insufficient'))

    if not configured_enabled and (promoted or live_active or coverage_count > 0):
        configured_enabled = True
    if overlay_mode in {'', 'disabled'} and configured_enabled:
        if live_active:
            overlay_mode = 'live'
        elif coverage_count > 0:
            overlay_mode = 'observe'

    return {
        'configured_enabled': configured_enabled,
        'promoted': promoted,
        'live_active': live_active,
        'overlay_mode': overlay_mode,
        'prediction_count': prediction_count,
        'active_symbols': coverage_count,
        'coverage_count': coverage_count,
        'ml_weight': _coerce_float(runtime.get('ml_weight', 0.0)),
        'configured_ml_weight': _coerce_float(runtime.get('configured_ml_weight', runtime.get('ml_weight', 0.0))),
        'effective_ml_weight': _coerce_float(runtime.get('effective_ml_weight', runtime.get('ml_weight', 0.0))),
        'online_control_reason': str(runtime.get('online_control_reason') or impact_summary.get('online_control_reason') or ''),
        'reason': reason,
        'last_update': runtime.get('ts'),
        'overlay_transform': runtime.get('overlay_transform'),
        'overlay_transform_scale': _coerce_float(runtime.get('overlay_transform_scale', 0.0)),
        'overlay_transform_max_abs': _coerce_float(runtime.get('overlay_transform_max_abs', 0.0)),
        'overlay_score_max_abs': _coerce_float(runtime.get('overlay_score_max_abs', 0.0)),
        'impact_status': impact_status,
        'last_step': last_step,
        'rolling_24h': rolling_24h,
        'rolling_48h': rolling_48h,
        'top_contributors': contributors,
        'top_promoted': promoted_rows[:limit],
        'top_suppressed': suppressed_rows[:limit],
    }


def _resolve_shadow_workspace() -> Optional[Path]:
    candidates: List[Path] = []

    env_shadow = os.getenv('V5_SHADOW_WORKSPACE')
    if env_shadow:
        candidates.append(Path(env_shadow).expanduser())

    candidates.append(WORKSPACE)
    candidates.append(WORKSPACE.parent / 'v5-shadow-tuned-xgboost')
    candidates.append(WORKSPACE.parent / 'v5-shadow-xgboost')

    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = str(candidate.resolve())
        except Exception:
            resolved = str(candidate)
        if resolved in seen:
            continue
        seen.add(resolved)

        reports_dir = candidate / 'reports'
        runtime_dir = reports_dir / 'shadow_tuned_xgboost'
        runs_dir = reports_dir / 'runs'
        if runtime_dir.exists() and runs_dir.exists():
            return candidate
    return None


def _coerce_timestamp_epoch(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value or '').strip()
    if not text:
        return None

    if text.endswith('Z'):
        text = text[:-1] + '+00:00'

    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _risk_state_epoch(payload: Any, *, primary_keys: tuple[str, ...]) -> Optional[float]:
    if not isinstance(payload, dict):
        return None

    for key in primary_keys:
        epoch = _coerce_timestamp_epoch(payload.get(key))
        if epoch is not None:
            return epoch

    history = payload.get('history')
    if isinstance(history, list):
        latest_history = max(
            (item for item in history if isinstance(item, dict)),
            key=lambda item: float(_coerce_timestamp_epoch(item.get('ts')) or float('-inf')),
            default=None,
        )
        if isinstance(latest_history, dict):
            epoch = _coerce_timestamp_epoch(latest_history.get('ts'))
            if epoch is not None:
                return epoch

    return None


def _sorted_risk_history_tail(history: Any, limit: int = 5) -> List[Dict[str, Any]]:
    if not isinstance(history, list):
        return []
    ordered = sorted(
        [item for item in history if isinstance(item, dict)],
        key=lambda item: float(_coerce_timestamp_epoch(item.get('ts')) or float('-inf')),
    )
    return ordered[-max(1, int(limit)) :]


def _latest_risk_history_ts(history: Any) -> str:
    if not isinstance(history, list):
        return ''
    latest_history = max(
        (item for item in history if isinstance(item, dict)),
        key=lambda item: float(_coerce_timestamp_epoch(item.get('ts')) or float('-inf')),
        default=None,
    )
    if not isinstance(latest_history, dict):
        return ''
    return str(latest_history.get('ts') or '').strip()


def _reflection_report_sort_epoch(path: Path) -> float:
    match = re.search(r"reflection_(\d{8}_\d{4,6})$", path.stem)
    if match:
        stamp = match.group(1)
        for fmt in ("%Y%m%d_%H%M%S", "%Y%m%d_%H%M"):
            try:
                return datetime.strptime(stamp, fmt).timestamp()
            except Exception:
                continue
    return path.stat().st_mtime


def _run_id_epoch(run_id: str) -> Optional[float]:
    match = re.search(r'(\d{8})_(\d{2})(\d{2})?(\d{2})?$', str(run_id or '').strip())
    if not match:
        return None

    date_part, hour_part, minute_part, second_part = match.groups()
    timestamp = f"{date_part}{hour_part}{minute_part or '00'}{second_part or '00'}"
    try:
        return datetime.strptime(timestamp, '%Y%m%d%H%M%S').timestamp()
    except ValueError:
        return None


def _decision_audit_sort_epoch(run_dir: Path, audit: Dict[str, Any]) -> float:
    if isinstance(audit, dict):
        for key in ('timestamp', 'now_ts', 'ts'):
            epoch = _coerce_timestamp_epoch(audit.get(key))
            if epoch is not None:
                return epoch

    if isinstance(audit, dict):
        epoch = _run_id_epoch(str(audit.get('run_id') or ''))
        if epoch is not None:
            return epoch

    epoch = _run_id_epoch(run_dir.name)
    if epoch is not None:
        return epoch

    try:
        return (run_dir / 'decision_audit.json').stat().st_mtime
    except OSError:
        return run_dir.stat().st_mtime


def _pick_latest_shadow_audit(reports_dir: Path) -> tuple[Optional[Path], Dict[str, Any]]:
    entries = _iter_decision_audits(reports_dir, scan_limit=1)
    if not entries:
        return None, {}
    latest = entries[0]
    audit = latest.get('audit', {})
    return latest.get('run_dir'), audit if isinstance(audit, dict) else {}


def _load_shadow_ml_overlay_summary(shadow_workspace: Path) -> Dict[str, Any]:
    reports_dir = shadow_workspace / 'reports'
    runtime_dir = reports_dir / 'shadow_tuned_xgboost'
    run_dir, audit = _pick_latest_shadow_audit(reports_dir)
    runtime = _load_json_payload(runtime_dir / 'ml_runtime_status.json')
    impact_summary = _load_json_payload(runtime_dir / 'ml_overlay_impact.json')

    overview = {}
    if isinstance(audit, dict):
        stored = audit.get('ml_signal_overview', {})
        if isinstance(stored, dict):
            overview.update(stored)

    if isinstance(runtime, dict) and runtime:
        overview['configured_enabled'] = _coerce_bool(runtime.get('configured_enabled', False))
        overview['promoted'] = _coerce_bool(runtime.get('promotion_passed', False))
        overview['live_active'] = _coerce_bool(runtime.get('used_in_latest_snapshot', False))
        overview['prediction_count'] = int(runtime.get('prediction_count', 0) or 0)
        active_symbols = int(runtime.get('prediction_count', 0) or 0)
        overview['active_symbols'] = active_symbols
        overview['coverage_count'] = active_symbols
        overview['ml_weight'] = _coerce_float(runtime.get('ml_weight', 0.0))
        overview['reason'] = str(runtime.get('reason') or runtime.get('error') or '')
        overview['last_update'] = runtime.get('ts')
        overview['overlay_transform'] = runtime.get('overlay_transform')
        overview['overlay_transform_scale'] = _coerce_float(runtime.get('overlay_transform_scale', 0.0))
        overview['overlay_transform_max_abs'] = _coerce_float(runtime.get('overlay_transform_max_abs', 0.0))
        overview['overlay_score_max_abs'] = _coerce_float(runtime.get('overlay_score_max_abs', 0.0))

    if isinstance(impact_summary, dict) and impact_summary:
        last_step = impact_summary.get('last_step', {}) if isinstance(impact_summary.get('last_step', {}), dict) else {}
        rolling_24h = impact_summary.get('rolling_24h', {}) if isinstance(impact_summary.get('rolling_24h', {}), dict) else {}
        rolling_48h = impact_summary.get('rolling_48h', {}) if isinstance(impact_summary.get('rolling_48h', {}), dict) else {}
        overview['last_step'] = last_step
        overview['rolling_24h'] = rolling_24h
        overview['rolling_48h'] = rolling_48h
        overview['impact_status'] = str(
            rolling_24h.get('status') or rolling_48h.get('status') or last_step.get('status') or 'insufficient'
        )

    overview.setdefault('top_contributors', [])
    overview.setdefault('top_promoted', [])
    overview.setdefault('top_suppressed', [])
    overview.setdefault('impact_status', 'insufficient')

    ts = overview.get('last_update')
    if ts is None and isinstance(audit, dict):
        ts = audit.get('timestamp') or audit.get('now_ts')
    if ts is None and run_dir is not None:
        ts = _decision_audit_sort_epoch(run_dir, audit if isinstance(audit, dict) else {})
    run_id = str(audit.get('run_id') or (run_dir.name if run_dir is not None else '')) if isinstance(audit, dict) else ''
    if run_id and _coerce_timestamp_epoch(ts) is None:
        run_ts = _run_id_epoch(run_id)
        if run_ts is not None:
            ts = run_ts

    return {
        'available': bool(overview),
        'workspace': str(shadow_workspace),
        'reports_dir': str(reports_dir),
        'run_id': run_id,
        'timestamp': ts,
        'as_of': overview.get('last_update'),
        'last_updated': overview.get('last_update'),
        'prediction_count': overview.get('prediction_count'),
        'overlay_score_max_abs': overview.get('overlay_score_max_abs'),
        'impact_status': overview.get('impact_status'),
        'ml_signal_overview': overview,
        'rolling_24h': overview.get('rolling_24h'),
        'rolling_48h': overview.get('rolling_48h'),
        'top_scores': audit.get('top_scores', []) if isinstance(audit, dict) else [],
        'notes': audit.get('notes', [])[:8] if isinstance(audit, dict) and isinstance(audit.get('notes', []), list) else [],
    }


TIMER_CANDIDATES = ['v5-prod.user.timer']
PRODUCTION_TIMER_CONFIGS = [
    {'name': 'v5-prod.user.timer', 'desc': '实盘主循环', 'icon': 'LIVE'},
    {'name': 'v5-event-driven.timer', 'desc': '事件驱动检查', 'icon': 'EVENT'},
    {'name': 'v5-sentiment-collect.timer', 'desc': '情绪采集', 'icon': 'SENT'},
    {'name': 'v5-reconcile.timer', 'desc': '对账状态刷新', 'icon': 'RECON'},
    {'name': 'v5-ledger.timer', 'desc': '账本状态刷新', 'icon': 'LEDGER'},
    {'name': 'v5-cost-rollup-real.user.timer', 'desc': '真实成本汇总', 'icon': 'COST'},
]


def _run_systemctl_user(*args: str, timeout: int = 5) -> subprocess.CompletedProcess:
    if not SYSTEMCTL_BIN:
        raise FileNotFoundError('systemctl is not available')
    return subprocess.run(
        [SYSTEMCTL_BIN, '--user', *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _parse_systemctl_properties(stdout: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    for line in stdout.splitlines():
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        props[key.strip()] = value.strip()
    return props


def _parse_timer_datetime(value: str) -> Optional[datetime]:
    value = str(value or '').strip()
    if not value or value.lower() == 'n/a':
        return None

    match = TIMER_TS_RE.search(value)
    if match:
        try:
            return datetime.strptime(match.group(1), '%a %Y-%m-%d %H:%M:%S')
        except ValueError:
            pass

    cleaned = re.sub(r'\s+[A-Z]{2,5}$', '', value)
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def _parse_time_left_seconds(value: Optional[str]) -> int:
    text = str(value or '').strip().lower()
    if not text:
        return 0

    total = 0
    for pattern, multiplier in (
        (r'(\d+)\s*h\b', 3600),
        (r'(\d+)\s*min\b', 60),
        (r'(\d+)\s*s\b', 1),
    ):
        for match in re.finditer(pattern, text):
            total += int(match.group(1)) * multiplier
    return total


def _parse_timer_interval_minutes(on_calendar: str) -> int:
    calendar = str(on_calendar or '').lower()
    if not calendar:
        return 60
    if 'hourly' in calendar or '0/1' in calendar:
        return 60
    if '0/2' in calendar or '00/2' in calendar:
        return 120

    match = re.search(r'/(\d+)', calendar)
    if match:
        try:
            return max(1, int(match.group(1))) * 60
        except ValueError:
            return 60
    return 60


def _timer_enabled(unit_file_state: str) -> bool:
    return str(unit_file_state or '').startswith('enabled')


def _get_timer_state(timer_name: str) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        'name': timer_name,
        'active': False,
        'enabled': False,
        'active_state': 'unknown',
        'unit_file_state': 'unknown',
        'error': None,
    }
    try:
        result = _run_systemctl_user(
            'show',
            timer_name,
            '--property=UnitFileState',
            '--property=ActiveState',
            timeout=5,
        )
        props = _parse_systemctl_properties(result.stdout)
        unit_file_state = props.get('UnitFileState', 'unknown')
        active_state = props.get('ActiveState', 'unknown')
        state['unit_file_state'] = unit_file_state
        state['active_state'] = active_state
        state['enabled'] = _timer_enabled(unit_file_state)
        state['active'] = active_state == 'active'
    except Exception as exc:
        state['error'] = str(exc)
    return state


def _get_timer_runtime(timer_name: str) -> Dict[str, Any]:
    runtime = _get_timer_state(timer_name)
    runtime.update({
        'next_run': None,
        'countdown_seconds': 0,
        'interval_minutes': 60,
        'time_left': None,
    })

    try:
        result = _run_systemctl_user(
            'show',
            timer_name,
            '--property=OnCalendar',
            '--property=Trigger',
            timeout=5,
        )
        props = _parse_systemctl_properties(result.stdout)
        runtime['interval_minutes'] = _parse_timer_interval_minutes(props.get('OnCalendar', ''))
        trigger_dt = _parse_timer_datetime(props.get('Trigger', ''))
        if trigger_dt:
            runtime['next_run'] = trigger_dt.strftime('%Y-%m-%d %H:%M:%S')
            runtime['countdown_seconds'] = max(0, int((trigger_dt - datetime.now()).total_seconds()))
    except Exception as exc:
        if not runtime.get('error'):
            runtime['error'] = str(exc)

    if runtime['next_run'] is not None:
        return runtime

    try:
        result = _run_systemctl_user('list-timers', timer_name, '--no-pager', timeout=5)
        for line in result.stdout.splitlines():
            if timer_name not in line:
                continue

            matches = list(TIMER_TS_RE.finditer(line))
            if matches:
                next_run_dt = _parse_timer_datetime(matches[0].group(1))
                if next_run_dt:
                    runtime['next_run'] = next_run_dt.strftime('%Y-%m-%d %H:%M:%S')
                    runtime['countdown_seconds'] = max(0, int((next_run_dt - datetime.now()).total_seconds()))

            if len(matches) >= 2:
                left_str = line[matches[0].end():matches[1].start()].strip()
                left_str = re.sub(r'^[A-Z]{2,5}\s+', '', left_str)
                left_str = re.sub(r'\bleft\b', '', left_str).strip()
                if left_str:
                    runtime['time_left'] = left_str
                    parsed_seconds = _parse_time_left_seconds(left_str)
                    if parsed_seconds > 0:
                        runtime['countdown_seconds'] = parsed_seconds
            break
    except Exception as exc:
        if not runtime.get('error'):
            runtime['error'] = str(exc)

    return runtime


def _pick_timer_name() -> str:
    """Pick active/enabled timer name, fallback to production timer."""
    for name in TIMER_CANDIDATES:
        if _get_timer_state(name).get('active'):
            return name
    for name in TIMER_CANDIDATES:
        if _get_timer_state(name).get('enabled'):
            return name
    return TIMER_CANDIDATES[0]

# 排除测试/异常数据
EXCLUDED_SYMBOLS = ['PEPE-USDT', 'MERL-USDT', 'SPACE-USDT']
POSITION_HIDDEN_BASE_SYMBOLS = {
    'PROMPT', 'XAUT', 'WLFI', 'SPACE', 'KITE', 'AGLD', 'MERL', 'USDG', 'J', 'PEPE'
}
MIN_VISIBLE_POSITION_VALUE_USD = 0.5


def _resolve_workspace_relative_path(raw_path: object, default: str) -> Path:
    raw = str(raw_path or default).strip()
    path = Path(raw)
    if not path.is_absolute():
        parts = path.parts
        if parts and parts[0] == 'reports':
            remainder = Path(*parts[1:]) if len(parts) > 1 else Path()
            path = REPORTS_DIR / remainder
        else:
            path = WORKSPACE / path
    return path.resolve()


def _resolve_dashboard_runtime_artifact_path(
    orders_db: Path,
    raw_path: object,
    legacy_default: str,
) -> Path:
    raw = str(raw_path or "").strip()
    if not raw or raw == legacy_default:
        name = Path(legacy_default).name
        suffix = ".jsonl" if name.endswith(".jsonl") else Path(name).suffix
        base_name = name[: -len(suffix)] if suffix else name
        return derive_runtime_named_artifact_path(orders_db, base_name, suffix).resolve()
    return _resolve_workspace_relative_path(raw, legacy_default)


def get_db_connection():
    """获取数据库连接"""
    db_path = _resolve_dashboard_runtime_paths(load_config()).orders_db
    if db_path.exists():
        return sqlite3.connect(db_path)
    return None


def _to_inst_id(symbol: str, quote_ccy: str = 'USDT') -> str:
    raw = str(symbol or '').strip().upper()
    if not raw:
        return ''
    if '-' in raw:
        return raw
    if '/' in raw:
        base, quote = raw.split('/', 1)
        return f'{base}-{quote}'
    return f'{raw}-{quote_ccy}'


def _load_avg_cost_from_fills(symbol: str, current_qty: float, reports_dir: Optional[Path] = None, fills_db: Optional[Path] = None) -> Optional[float]:
    if float(current_qty or 0.0) <= 0:
        return None

    base_symbol = str(symbol or '').split('/')[0].split('-')[0].upper()
    inst_id = _to_inst_id(base_symbol)
    if fills_db is None:
        fills_db = (reports_dir or REPORTS_DIR) / 'fills.sqlite'
    if not fills_db.exists() or not inst_id:
        return None

    try:
        conn = sqlite3.connect(str(fills_db))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT side, fill_px, fill_sz, fill_notional, fee, fee_ccy
            FROM fills
            WHERE inst_id = ?
            ORDER BY ts_ms ASC, created_ts_ms ASC, trade_id ASC
            """,
            (inst_id,),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception:
        return None

    if not rows:
        return None

    quote_symbol = inst_id.split('-', 1)[1].upper() if '-' in inst_id else 'USDT'
    queue: List[List[float]] = []

    for side, fill_px, fill_sz, fill_notional, fee, fee_ccy in rows:
        side = str(side or '').lower()
        qty = float(fill_sz or 0.0)
        if qty <= 0:
            continue

        px = float(fill_px or 0.0)
        notional = float(fill_notional or 0.0)
        if notional <= 0 and px > 0:
            notional = px * qty
        fee_val = float(fee or 0.0)
        fee_ccy_norm = str(fee_ccy or '').upper()

        if side == 'buy':
            net_base_qty = qty + (fee_val if fee_ccy_norm == base_symbol else 0.0)
            if net_base_qty <= 1e-12:
                continue
            total_quote_cost = notional
            if fee_ccy_norm == quote_symbol:
                total_quote_cost += abs(fee_val)
            queue.append([net_base_qty, total_quote_cost / net_base_qty])
            continue

        if side != 'sell':
            continue

        remove_qty = qty
        if fee_ccy_norm == base_symbol:
            remove_qty += abs(fee_val)
        while remove_qty > 1e-12 and queue:
            head_qty, _ = queue[0]
            if head_qty <= remove_qty + 1e-12:
                remove_qty -= head_qty
                queue.pop(0)
            else:
                queue[0][0] = head_qty - remove_qty
                remove_qty = 0.0

    remaining_qty = sum(qty for qty, _ in queue)
    if remaining_qty <= 1e-12:
        return None

    trim_qty = remaining_qty - float(current_qty)
    if trim_qty > 1e-8:
        while trim_qty > 1e-12 and queue:
            head_qty, _ = queue[0]
            if head_qty <= trim_qty + 1e-12:
                trim_qty -= head_qty
                queue.pop(0)
            else:
                queue[0][0] = head_qty - trim_qty
                trim_qty = 0.0

    remaining_qty = sum(qty for qty, _ in queue)
    if remaining_qty <= 1e-12:
        return None

    qty_gap = float(current_qty) - remaining_qty
    if qty_gap > max(1e-4, float(current_qty) * 0.02):
        return None

    total_cost = sum(qty * cost for qty, cost in queue)
    if total_cost <= 0:
        return None
    return total_cost / remaining_qty


def load_config():
    """加载配置"""
    cfg = load_app_config(
        str(_resolve_config_path()),
        env_path=str(_resolve_workspace_env_path()),
    )
    return cfg.model_dump(mode='python')


def _dashboard_execution_mode(config: Dict[str, Any]) -> str:
    execution_cfg = config.get('execution', {}) if isinstance(config, dict) else {}
    mode = str(execution_cfg.get('mode') or '').strip().lower()
    if mode in {'live', 'dry_run'}:
        return mode
    return 'dry_run' if _dashboard_to_bool(execution_cfg.get('dry_run', True)) else 'live'


def _dashboard_dry_run(config: Dict[str, Any]) -> bool:
    return _dashboard_execution_mode(config) != 'live'


def _dashboard_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _sanitize_peak_equity(total_equity: float, initial_capital: float, peak_equity: float) -> float:
    total_equity = float(total_equity or 0.0)
    initial_capital = float(initial_capital or 0.0)
    peak_equity = float(peak_equity or 0.0)
    sane_floor = max(total_equity, initial_capital)

    if peak_equity <= 0:
        return sane_floor
    if peak_equity < sane_floor:
        return sane_floor
    if total_equity > 0 and peak_equity > total_equity * 2:
        return sane_floor
    return peak_equity


def _maybe_float(value: Any) -> Optional[float]:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except Exception:
        return None


def _load_reconcile_cash_balance(runtime_paths: Optional[DashboardRuntimePaths] = None) -> tuple[bool, float]:
    reconcile_file = (runtime_paths or _resolve_dashboard_runtime_paths()).reconcile_status_path
    if not reconcile_file.exists():
        return False, 0.0

    try:
        with open(reconcile_file, 'r', encoding='utf-8') as f:
            reconcile = json.load(f)
    except Exception:
        return False, 0.0

    exchange_cash = _maybe_float(
        reconcile.get('exchange_snapshot', {}).get('ccy_cashBal', {}).get('USDT')
    )
    if exchange_cash is not None:
        return True, exchange_cash

    local_cash = _maybe_float(reconcile.get('local_snapshot', {}).get('cash_usdt'))
    if local_cash is not None:
        return True, local_cash

    return False, 0.0


def _account_equity_from_balance_details(
    details: Any,
    *,
    source: str,
    hidden_symbols: Optional[set[str]] = None,
    min_visible_position_value_usd: float = 0.0,
) -> Dict[str, Any]:
    if not isinstance(details, list):
        return {}

    hidden = set()
    for raw_symbol in hidden_symbols or set():
        symbol = str(raw_symbol or '').strip().upper()
        if not symbol:
            continue
        hidden.add(symbol)
        hidden.add(symbol.replace('/', '-').split('-')[0])

    cash_usdt: Optional[float] = None
    total_equity = 0.0
    positions_value = 0.0
    seen_equity = False

    for detail in details:
        if not isinstance(detail, dict):
            continue
        ccy = str(detail.get('ccy') or '').strip().upper()
        if not ccy:
            continue
        eq_usd = _maybe_float(detail.get('eqUsd'))
        cash_bal = _maybe_float(detail.get('cashBal'))
        eq_qty = _maybe_float(detail.get('eq'))
        if ccy == 'USDT':
            cash_usdt = cash_bal if cash_bal is not None else (eq_qty if eq_qty is not None else eq_usd)
        if eq_usd is None:
            continue
        total_equity += eq_usd
        seen_equity = True
        if eq_usd <= 0:
            continue
        if ccy != 'USDT' and ccy not in hidden and eq_usd >= min_visible_position_value_usd:
            positions_value += eq_usd

    payload: Dict[str, Any] = {'source': source}
    if cash_usdt is not None:
        payload['cash_usdt'] = cash_usdt
    if seen_equity:
        payload['total_equity_usdt'] = total_equity
        payload['positions_value_usdt'] = positions_value
    return payload


def _load_reconcile_account_equity(runtime_paths: Optional[DashboardRuntimePaths] = None) -> Dict[str, Any]:
    reconcile_file = (runtime_paths or _resolve_dashboard_runtime_paths()).reconcile_status_path
    if not reconcile_file.exists():
        return {}

    try:
        reconcile = json.loads(reconcile_file.read_text(encoding='utf-8', errors='ignore'))
    except Exception:
        return {}

    exchange_snapshot = reconcile.get('exchange_snapshot') or {}
    ccy_eq_usd = exchange_snapshot.get('ccy_eqUsd') or {}
    ccy_cash_bal = exchange_snapshot.get('ccy_cashBal') or {}
    if not isinstance(ccy_eq_usd, dict) and not isinstance(ccy_cash_bal, dict):
        return {}

    ccys = set()
    if isinstance(ccy_eq_usd, dict):
        ccys.update(str(ccy) for ccy in ccy_eq_usd)
    if isinstance(ccy_cash_bal, dict):
        ccys.update(str(ccy) for ccy in ccy_cash_bal)
    details = [
        {
            'ccy': ccy,
            'eqUsd': ccy_eq_usd.get(ccy) if isinstance(ccy_eq_usd, dict) else None,
            'cashBal': ccy_cash_bal.get(ccy) if isinstance(ccy_cash_bal, dict) else None,
        }
        for ccy in sorted(ccys)
    ]
    return _account_equity_from_balance_details(
        details,
        source='reconcile',
        hidden_symbols=set(EXCLUDED_SYMBOLS) | POSITION_HIDDEN_BASE_SYMBOLS,
        min_visible_position_value_usd=MIN_VISIBLE_POSITION_VALUE_USD,
    )


def _load_reconcile_total_equity(runtime_paths: Optional[DashboardRuntimePaths] = None) -> tuple[bool, float]:
    reconcile_file = (runtime_paths or _resolve_dashboard_runtime_paths()).reconcile_status_path
    if not reconcile_file.exists():
        return False, 0.0

    try:
        reconcile = json.loads(reconcile_file.read_text(encoding='utf-8'))
    except Exception:
        return False, 0.0

    exchange_snapshot = reconcile.get('exchange_snapshot', {})
    ccy_eq_usd = exchange_snapshot.get('ccy_eqUsd', {})
    if isinstance(ccy_eq_usd, dict) and ccy_eq_usd:
        total = 0.0
        seen = False
        for value in ccy_eq_usd.values():
            eq_usd = _maybe_float(value)
            if eq_usd is None:
                continue
            total += eq_usd
            seen = True
        if seen:
            return True, total

    local_total = _maybe_float(reconcile.get('local_snapshot', {}).get('total_equity_usdt'))
    if local_total is not None:
        return True, local_total

    return False, 0.0


def _env_flag_enabled(name: str) -> bool:
    value = str(os.getenv(name, '') or '').strip().lower()
    return value in {'1', 'true', 'yes', 'on'}


def _dashboard_live_account_enabled() -> bool:
    return _env_flag_enabled('V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT') or _env_flag_enabled('V5_DASHBOARD_ALLOW_LIVE_OKX')


def _load_local_account_state(runtime_paths: Optional[DashboardRuntimePaths] = None) -> Dict[str, float]:
    db_path = (runtime_paths or _resolve_dashboard_runtime_paths()).positions_db
    if not db_path.exists():
        return {}

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT cash_usdt, equity_peak_usdt FROM account_state WHERE k='default'")
        row = cursor.fetchone()
        conn.close()
    except Exception:
        return {}

    if not row:
        return {}

    return {
        'cash_usdt': float(row[0] or 0.0),
        'equity_peak_usdt': float(row[1] or 0.0),
    }


def _split_inst_id_base_quote(inst_id: str) -> tuple[str, str]:
    inst = str(inst_id or '').upper()
    if '-' in inst:
        return tuple(inst.split('-', 1))
    if '/' in inst:
        return tuple(inst.split('/', 1))
    return inst, 'USDT'


def _signed_fee_usdt_from_fee_fields(inst_id: str, px: Any, fee_amount: Any, fee_ccy: Any = None) -> float:
    try:
        fee_val = float(fee_amount or 0.0)
    except Exception:
        return 0.0

    fee_ccy_norm = str(fee_ccy or '').strip().upper()
    if not fee_ccy_norm:
        return fee_val

    base_ccy, quote_ccy = _split_inst_id_base_quote(inst_id)
    if fee_ccy_norm == quote_ccy:
        return fee_val
    if fee_ccy_norm != base_ccy:
        return 0.0

    try:
        px_val = float(px or 0.0)
    except Exception:
        return 0.0
    if px_val <= 0:
        return 0.0
    return fee_val * px_val


def _signed_fee_usdt_from_order_fee(inst_id: str, avg_px: Any, raw_fee: Any) -> float:
    raw = str(raw_fee or '').strip()
    if not raw:
        return 0.0

    try:
        numeric_fee = float(raw)
    except Exception:
        numeric_fee = None
    if numeric_fee is not None:
        return _signed_fee_usdt_from_fee_fields(inst_id, avg_px, numeric_fee)

    try:
        fee_map = json.loads(raw)
    except Exception:
        return 0.0
    if not isinstance(fee_map, dict):
        return 0.0

    px = float(avg_px or 0.0)
    total_fee_usdt = 0.0
    for ccy, value in fee_map.items():
        total_fee_usdt += _signed_fee_usdt_from_fee_fields(inst_id, px, value, ccy)
    return total_fee_usdt


def _load_total_fees_from_orders(*, excluded_inst_ids: Optional[List[str]] = None, max_notional_usdt: float = 1000.0, reports_dir: Optional[Path] = None, orders_db: Optional[Path] = None) -> float:
    if orders_db is None:
        orders_db = (reports_dir or REPORTS_DIR) / 'orders.sqlite'
    if not orders_db.exists():
        return 0.0

    excluded = [str(x) for x in (excluded_inst_ids or [])]
    placeholders = ','.join(['?' for _ in excluded]) if excluded else ''
    sql = """
        SELECT inst_id, avg_px, fee
        FROM orders
        WHERE state='FILLED' AND notional_usdt < ?
    """
    params: List[Any] = [float(max_notional_usdt)]
    if excluded:
        sql += f" AND inst_id NOT IN ({placeholders})"
        params.extend(excluded)

    try:
        conn = sqlite3.connect(str(orders_db))
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        conn.close()
    except Exception:
        return 0.0

    total = 0.0
    for inst_id, avg_px, fee in rows:
        total += _signed_fee_usdt_from_order_fee(str(inst_id or ''), avg_px, fee)
    return total


def _load_workspace_exchange_creds() -> tuple[str, str, str]:
    key = str(os.getenv('EXCHANGE_API_KEY') or '')
    sec = str(os.getenv('EXCHANGE_API_SECRET') or '')
    pp = str(os.getenv('EXCHANGE_PASSPHRASE') or '')
    envp = _resolve_workspace_env_path()

    try:
        from dotenv import load_dotenv
        load_dotenv(str(envp))
        key = key or str(os.getenv('EXCHANGE_API_KEY') or '')
        sec = sec or str(os.getenv('EXCHANGE_API_SECRET') or '')
        pp = pp or str(os.getenv('EXCHANGE_PASSPHRASE') or '')
    except Exception:
        pass

    if key and sec and pp:
        return key, sec, pp

    if envp.exists():
        try:
            for ln in envp.read_text(encoding='utf-8', errors='ignore').splitlines():
                if not ln or ln.strip().startswith('#') or '=' not in ln:
                    continue
                env_key, env_val = ln.split('=', 1)
                env_key = env_key.strip()
                env_val = env_val.strip().strip('"').strip("'")
                if env_key == 'EXCHANGE_API_KEY' and not key:
                    key = env_val
                elif env_key == 'EXCHANGE_API_SECRET' and not sec:
                    sec = env_val
                elif env_key == 'EXCHANGE_PASSPHRASE' and not pp:
                    pp = env_val
        except Exception:
            pass
    return key, sec, pp


def _okx_account_balance_cache_ttl_seconds() -> float:
    try:
        ttl = float(os.getenv('V5_DASHBOARD_OKX_BALANCE_CACHE_TTL_SECONDS', '2') or '2')
    except Exception:
        ttl = 2.0
    return max(0.0, min(ttl, 10.0))


def _okx_public_ticker_cache_ttl_seconds() -> float:
    try:
        ttl = float(os.getenv('V5_DASHBOARD_PUBLIC_TICKER_CACHE_TTL_SECONDS', '10') or '10')
    except Exception:
        ttl = 10.0
    return max(0.0, min(ttl, 60.0))


def _load_okx_account_balance(key: str, sec: str, pp: str) -> Dict[str, Any]:
    ttl = _okx_account_balance_cache_ttl_seconds()
    cache_key = (key, sec, pp)
    now = time.time()
    if ttl > 0:
        with _OKX_ACCOUNT_BALANCE_CACHE_LOCK:
            cached = _OKX_ACCOUNT_BALANCE_CACHE.get(cache_key)
            if cached and cached[0] > now:
                payload = cached[1]
                return payload if isinstance(payload, dict) else {}

    import hmac, hashlib, base64

    ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
    path = '/api/v5/account/balance'
    msg = ts + 'GET' + path
    sig = base64.b64encode(hmac.new(sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
    headers = {
        'OK-ACCESS-KEY': key,
        'OK-ACCESS-SIGN': sig,
        'OK-ACCESS-TIMESTAMP': ts,
        'OK-ACCESS-PASSPHRASE': pp,
    }
    data = requests.get('https://www.okx.com' + path, headers=headers, timeout=8).json()
    if ttl > 0:
        with _OKX_ACCOUNT_BALANCE_CACHE_LOCK:
            _OKX_ACCOUNT_BALANCE_CACHE[cache_key] = (time.time() + ttl, data)
    return data if isinstance(data, dict) else {}


def _load_okx_public_ticker_last_price(symbol: str) -> float:
    symbol_key = str(symbol or '').strip().upper()
    if not symbol_key:
        return 0.0

    ttl = _okx_public_ticker_cache_ttl_seconds()
    now = time.time()
    if ttl > 0:
        with _OKX_PUBLIC_TICKER_CACHE_LOCK:
            cached = _OKX_PUBLIC_TICKER_CACHE.get(symbol_key)
            if cached and cached[0] > now:
                return float(cached[1] or 0.0)

    response = requests.get(f"https://www.okx.com/api/v5/market/ticker?instId={symbol_key}-USDT", timeout=5)
    payload = response.json()
    if payload.get('code') == '0' and payload.get('data'):
        price = float(payload['data'][0].get('last') or 0)
        if price > 0 and ttl > 0:
            with _OKX_PUBLIC_TICKER_CACHE_LOCK:
                _OKX_PUBLIC_TICKER_CACHE[symbol_key] = (time.time() + ttl, price)
        return price
    return 0.0


def _resolve_dashboard_runtime_paths(cfg: Optional[Dict[str, Any]] = None) -> DashboardRuntimePaths:
    config = cfg if isinstance(cfg, dict) else load_config()
    execution_cfg = config.get('execution', {}) if isinstance(config, dict) else {}
    orders_db = _resolve_workspace_relative_path(
        execution_cfg.get('order_store_path'),
        'reports/orders.sqlite',
    )
    reports_dir = derive_runtime_reports_dir(orders_db)
    return DashboardRuntimePaths(
        reports_dir=reports_dir,
        orders_db=orders_db,
        fills_db=derive_fill_store_path(orders_db),
        positions_db=derive_position_store_path(orders_db),
        kill_switch_path=_resolve_dashboard_runtime_artifact_path(
            orders_db,
            execution_cfg.get('kill_switch_path'),
            'reports/kill_switch.json',
        ),
        reconcile_status_path=_resolve_dashboard_runtime_artifact_path(
            orders_db,
            execution_cfg.get('reconcile_status_path'),
            'reports/reconcile_status.json',
        ),
        runs_dir=derive_runtime_runs_dir(orders_db),
        auto_risk_guard_path=derive_runtime_auto_risk_guard_path(orders_db),
        auto_risk_eval_path=derive_runtime_auto_risk_eval_path(orders_db),
        telemetry_db=derive_runtime_named_artifact_path(orders_db, 'api_telemetry', '.sqlite').resolve(),
    )


def _format_dashboard_ts_ms(ts_ms: Any) -> str:
    try:
        ts_f = float(ts_ms or 0)
    except Exception:
        return ''
    if ts_f <= 0:
        return ''
    return datetime.fromtimestamp(ts_f / 1000.0, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')


def _dashboard_kill_switch_enabled(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding='utf-8', errors='ignore'))
    except Exception:
        return False

    if isinstance(payload, dict):
        if 'enabled' in payload:
            return _dashboard_to_bool(payload.get('enabled'))
        nested = payload.get('kill_switch')
        if isinstance(nested, dict):
            if 'enabled' in nested:
                return _dashboard_to_bool(nested.get('enabled'))
            if 'active' in nested:
                return _dashboard_to_bool(nested.get('active'))
            return False
        if 'active' in payload:
            return _dashboard_to_bool(payload.get('active'))
    return _dashboard_to_bool(payload)


def _dashboard_status_mode(status_data: Dict[str, Any], errors: Optional[List[str]] = None) -> str:
    mode = str(status_data.get('mode') or '').strip().lower() if isinstance(status_data, dict) else ''
    if mode in {'live', 'dry_run', 'unknown'}:
        return mode
    if any(str(err).startswith('status:') for err in (errors or [])):
        return 'unknown'
    return 'live' if not _dashboard_to_bool((status_data or {}).get('dry_run', True)) else 'dry_run'


def _build_dashboard_system_status(
    status_data: Dict[str, Any],
    *,
    account_data: Dict[str, Any],
    errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        'isRunning': _dashboard_to_bool(status_data.get('timer_active', False)),
        'mode': _dashboard_status_mode(status_data, errors),
        'lastUpdate': account_data.get('last_update', ''),
        'killSwitch': _dashboard_to_bool(status_data.get('kill_switch', False)),
        'errors': (
            ([
                _sanitize_public_error_text(status_data['timer_error'], default='internal error')
            ] if status_data.get('timer_error') else [])
            + list(errors or [])
        ),
    }


def _percentile_value(values: List[float], quantile: float) -> Optional[float]:
    xs: List[float] = []
    for value in values or []:
        try:
            numeric = float(value)
        except Exception:
            continue
        if math.isfinite(numeric):
            xs.append(numeric)
    xs.sort()
    if not xs:
        return None
    if len(xs) == 1:
        return float(xs[0])
    q = min(max(float(quantile), 0.0), 1.0)
    pos = q * (len(xs) - 1)
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return float(xs[lower])
    frac = pos - lower
    return float(xs[lower] + (xs[upper] - xs[lower]) * frac)


def _load_api_telemetry_summary(
    runtime_paths: Optional[DashboardRuntimePaths] = None,
    *,
    lookback_hours: int = 24,
) -> Dict[str, Any]:
    paths = runtime_paths or _resolve_dashboard_runtime_paths(load_config())
    telemetry_db = Path(
        getattr(
            paths,
            'telemetry_db',
            derive_runtime_named_artifact_path(paths.orders_db, 'api_telemetry', '.sqlite'),
        )
    ).resolve()
    summary: Dict[str, Any] = {
        'status': 'missing',
        'lookbackHours': int(lookback_hours),
        'totalRequests': 0,
        'successRate': None,
        'errorCount': 0,
        'rateLimitedCount': 0,
        'p50LatencyMs': None,
        'p95LatencyMs': None,
        'lastRequestAt': '',
        'lastErrorAt': '',
        'latestError': None,
        'note': '暂无 API 遥测数据',
    }
    if not telemetry_db.exists():
        return summary

    since_ts_ms = int((time.time() - max(1, int(lookback_hours)) * 3600) * 1000)
    conn = None
    try:
        conn = sqlite3.connect(str(telemetry_db))
        conn.row_factory = sqlite3.Row
        window_row = conn.execute(
            """
            SELECT
              COUNT(*) AS total_requests,
              SUM(CASE WHEN status_class = '2xx' THEN 1 ELSE 0 END) AS success_count,
              SUM(CASE WHEN status_class != '2xx' THEN 1 ELSE 0 END) AS error_count,
              SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END) AS rate_limited_count
            FROM api_request_log
            WHERE ts_ms >= ?
            """,
            (since_ts_ms,),
        ).fetchone()
        latency_rows = conn.execute(
            """
            SELECT duration_ms
            FROM api_request_log
            WHERE ts_ms >= ?
              AND duration_ms IS NOT NULL
            ORDER BY duration_ms ASC
            """,
            (since_ts_ms,),
        ).fetchall()
        latest_request = conn.execute(
            """
            SELECT ts_ms
            FROM api_request_log
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
        latest_error = conn.execute(
            """
            SELECT ts_ms, method, endpoint, status_class, http_status, okx_code, okx_msg
            FROM api_request_log
            WHERE status_class != '2xx'
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.Error as exc:
        summary['status'] = 'error'
        summary['note'] = 'API 遥测读取失败'
        summary['latestError'] = {'message': _sanitize_public_error_text(str(exc), default='internal error')}
        return summary
    finally:
        if conn is not None:
            conn.close()

    total_requests = int((window_row['total_requests'] or 0) if window_row else 0)
    success_count = int((window_row['success_count'] or 0) if window_row else 0)
    error_count = int((window_row['error_count'] or 0) if window_row else 0)
    rate_limited_count = int((window_row['rate_limited_count'] or 0) if window_row else 0)
    success_rate = (float(success_count) / float(total_requests)) if total_requests > 0 else None
    durations = [float(row['duration_ms']) for row in latency_rows or [] if row['duration_ms'] is not None]
    p50_latency_ms = _percentile_value(durations, 0.50)
    p95_latency_ms = _percentile_value(durations, 0.95)

    summary.update(
        {
            'totalRequests': total_requests,
            'successRate': success_rate,
            'errorCount': error_count,
            'rateLimitedCount': rate_limited_count,
            'p50LatencyMs': p50_latency_ms,
            'p95LatencyMs': p95_latency_ms,
            'lastRequestAt': _format_dashboard_ts_ms(latest_request['ts_ms'] if latest_request else None),
            'lastErrorAt': _format_dashboard_ts_ms(latest_error['ts_ms'] if latest_error else None),
            'latestError': (
                {
                    'method': str(latest_error['method'] or ''),
                    'endpoint': str(latest_error['endpoint'] or ''),
                    'statusClass': str(latest_error['status_class'] or ''),
                    'httpStatus': (int(latest_error['http_status']) if latest_error['http_status'] is not None else None),
                    'okxCode': (str(latest_error['okx_code']) if latest_error['okx_code'] is not None else None),
                    'message': _sanitize_public_error_text(latest_error['okx_msg'], default='error'),
                }
                if latest_error
                else None
            ),
        }
    )

    if total_requests <= 0:
        summary['status'] = 'missing'
        summary['note'] = f'近{int(lookback_hours)}h 未采集到 API 请求'
        return summary

    error_rate = float(error_count) / float(total_requests)
    if rate_limited_count >= 10 or error_rate >= 0.10 or (p95_latency_ms or 0.0) >= 2500.0:
        summary['status'] = 'critical'
        summary['note'] = 'API 延迟或错误率偏高'
    elif rate_limited_count > 0 or error_rate >= 0.03 or (p95_latency_ms or 0.0) >= 1200.0:
        summary['status'] = 'warning'
        summary['note'] = 'API 出现限流或延迟抬升'
    else:
        summary['status'] = 'healthy'
        summary['note'] = 'API 遥测稳定'
    return summary


def _load_backtest_slippage_baseline(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = cfg if isinstance(cfg, dict) else load_config()
    backtest_cfg = config.get('backtest', {}) if isinstance(config, dict) else {}

    try:
        default_bps = float(backtest_cfg.get('slippage_bps', 5.0) or 5.0)
    except Exception:
        default_bps = 5.0

    mode = str(backtest_cfg.get('cost_model', 'default') or 'default').lower()
    quantile = str(backtest_cfg.get('slippage_quantile', 'p90') or 'p90')
    baseline = {
        'valueBps': float(default_bps),
        'label': f'回测默认 {default_bps:.1f}bps',
        'mode': 'default',
        'quantile': quantile,
        'sourceDay': None,
    }
    if mode != 'calibrated':
        return baseline

    try:
        from src.backtest.cost_calibration import load_latest_cost_stats
    except Exception:
        return baseline

    raw_stats_dir = backtest_cfg.get('cost_stats_dir')
    stats_dir = _resolve_workspace_relative_path(raw_stats_dir, 'reports/cost_stats')
    max_age_days = int(backtest_cfg.get('max_stats_age_days', 7) or 7)
    min_fills_global = int(backtest_cfg.get('min_fills_global', 30) or 30)
    stats, _stats_path = load_latest_cost_stats(str(stats_dir), max_age_days=max_age_days)
    if not isinstance(stats, dict):
        return baseline

    try:
        global_fills = int(((stats.get('coverage') or {}).get('fills')) or 0)
    except Exception:
        global_fills = 0
    if global_fills < min_fills_global:
        return baseline

    bucket = ((stats.get('buckets') or {}).get('ALL|ALL|ALL|ALL') or {})
    slippage_bps = ((bucket.get('slippage_bps') or {}).get(quantile))
    try:
        baseline_value = float(slippage_bps)
    except Exception:
        return baseline

    source_day = str(stats.get('day') or '').strip() or None
    return {
        'valueBps': baseline_value,
        'label': f'回测校准 {quantile.upper()}',
        'mode': 'calibrated',
        'quantile': quantile,
        'sourceDay': source_day,
    }


def _build_slippage_histogram(values: List[float]) -> List[Dict[str, Any]]:
    histogram: List[Dict[str, Any]] = []
    for lower, upper, label in SLIPPAGE_HISTOGRAM_BINS:
        count = 0
        for value in values or []:
            numeric = float(value)
            if lower is None:
                if numeric <= float(upper):
                    count += 1
                continue
            if upper is None:
                if numeric >= float(lower):
                    count += 1
                continue
            if numeric >= float(lower) and numeric < float(upper):
                count += 1
        histogram.append({
            'label': label,
            'startBps': lower,
            'endBps': upper,
            'count': count,
        })
    return histogram


def _load_slippage_insights(
    runtime_paths: Optional[DashboardRuntimePaths] = None,
    *,
    cfg: Optional[Dict[str, Any]] = None,
    lookback_days: int = 14,
) -> Dict[str, Any]:
    config = cfg if isinstance(cfg, dict) else load_config()
    paths = runtime_paths or _resolve_dashboard_runtime_paths(config)
    events_dir = derive_runtime_cost_events_dir(paths.orders_db).resolve()
    baseline = _load_backtest_slippage_baseline(config)
    summary: Dict[str, Any] = {
        'status': 'missing',
        'lookbackDays': int(lookback_days),
        'sampleCount': 0,
        'actualAvgBps': None,
        'actualP50Bps': None,
        'actualP90Bps': None,
        'actualP95Bps': None,
        'actualMinBps': None,
        'actualMaxBps': None,
        'baselineBps': baseline.get('valueBps'),
        'baselineLabel': baseline.get('label'),
        'baselineMode': baseline.get('mode'),
        'baselineSourceDay': baseline.get('sourceDay'),
        'bins': _build_slippage_histogram([]),
        'lastFillAt': '',
        'note': '暂无实测滑点数据',
    }
    if not events_dir.exists():
        return summary

    values: List[float] = []
    last_fill_ts = 0
    event_files = sorted(
        (
            path
            for path in events_dir.glob('*.jsonl')
            if re.fullmatch(r'\d{8}\.jsonl', path.name)
        )
    )[-max(1, int(lookback_days)):]
    for event_file in event_files:
        try:
            lines = event_file.read_text(encoding='utf-8').splitlines()
        except Exception:
            continue
        for line in lines:
            raw = line.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except Exception:
                continue
            if str(event.get('event_type') or '').lower() != 'fill':
                continue
            slip = event.get('slippage_bps')
            if slip is None:
                notional = float(event.get('notional_usdt') or 0.0)
                slip_usdt = event.get('slippage_usdt')
                if slip_usdt is not None and notional > 0:
                    try:
                        slip = float(slip_usdt) / notional * 10000.0
                    except Exception:
                        slip = None
            try:
                slip_f = float(slip)
            except Exception:
                continue
            if not math.isfinite(slip_f):
                continue
            values.append(slip_f)
            try:
                ts_val = int((event.get('ts') or 0)) * 1000
                last_fill_ts = max(last_fill_ts, ts_val)
            except Exception:
                pass

    if not values:
        return summary

    values.sort()
    sample_count = len(values)
    p50_bps = _percentile_value(values, 0.50)
    p90_bps = _percentile_value(values, 0.90)
    p95_bps = _percentile_value(values, 0.95)
    avg_bps = float(sum(values) / sample_count) if sample_count > 0 else None

    summary.update(
        {
            'sampleCount': sample_count,
            'actualAvgBps': avg_bps,
            'actualP50Bps': p50_bps,
            'actualP90Bps': p90_bps,
            'actualP95Bps': p95_bps,
            'actualMinBps': float(values[0]),
            'actualMaxBps': float(values[-1]),
            'bins': _build_slippage_histogram(values),
            'lastFillAt': _format_dashboard_ts_ms(last_fill_ts),
        }
    )

    baseline_bps = baseline.get('valueBps')
    if sample_count < 5:
        summary['status'] = 'warning'
        summary['note'] = '滑点样本偏少，先观察趋势'
    elif baseline_bps is not None and avg_bps is not None and avg_bps > float(baseline_bps) * 1.8:
        summary['status'] = 'critical'
        summary['note'] = '实测滑点显著高于回测基线'
    elif baseline_bps is not None and p90_bps is not None and p90_bps > float(baseline_bps) * 1.4:
        summary['status'] = 'warning'
        summary['note'] = '滑点尾部偏高，需关注执行质量'
    else:
        summary['status'] = 'healthy'
        summary['note'] = '实测滑点基本贴近回测基线'
    return summary


def _runtime_ic_diagnostic_pattern(orders_db: Path) -> str:
    if orders_db.name == "orders.sqlite":
        return "ic_diagnostics_*.json"
    if "orders" in orders_db.stem:
        return orders_db.stem.replace("orders", "ic_diagnostics_*", 1) + ".json"
    return "ic_diagnostics_*.json"


def _history_entry_sort_epoch(entry: Dict[str, Any]) -> float:
    if isinstance(entry, dict):
        raw_ts = entry.get("timestamp") or entry.get("ts")
        if raw_ts:
            try:
                return datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00")).timestamp()
            except Exception:
                pass
        run_id = str(entry.get("run_id") or "")
        if run_id:
            try:
                return datetime.strptime(run_id, "%Y%m%d_%H").timestamp()
            except Exception:
                pass
    return 0.0


def _ic_diagnostic_sort_epoch(path: Path) -> float:
    match = re.search(r"(?<!\d)(20\d{6})(?!\d)", path.stem)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d").timestamp()
        except Exception:
            pass
    return path.stat().st_mtime


def _can_execute_python(candidate: str) -> bool:
    try:
        result = subprocess.run(
            [candidate, '-c', 'import sys'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
        return result.returncode == 0
    except Exception:
        return False


def _resolve_workspace_python() -> str:
    global _WORKSPACE_PYTHON_BIN
    if _WORKSPACE_PYTHON_BIN:
        return _WORKSPACE_PYTHON_BIN

    candidates = []
    env_python = os.getenv('V5_PYTHON_BIN')
    if env_python:
        candidates.append(env_python)
    candidates.extend([
        str(WORKSPACE / '.venv' / 'bin' / 'python'),
        'python3',
        'python',
    ])

    for candidate in candidates:
        if _can_execute_python(candidate):
            _WORKSPACE_PYTHON_BIN = candidate
            return candidate

    _WORKSPACE_PYTHON_BIN = 'python3'
    return _WORKSPACE_PYTHON_BIN


def _load_live_okx_balance_snapshot() -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        'ok': False,
        'cash_usdt': 0.0,
        'total_equity_usdt': 0.0,
        'error': '',
    }
    try:
        import base64
        import hashlib
        import hmac
        import time
        from dotenv import load_dotenv

        envp = _resolve_workspace_env_path()
        load_dotenv(str(envp))
        key = os.getenv('EXCHANGE_API_KEY')
        sec = os.getenv('EXCHANGE_API_SECRET')
        pp = os.getenv('EXCHANGE_PASSPHRASE')
        if not (key and sec and pp):
            if envp.exists():
                for ln in envp.read_text(encoding='utf-8', errors='ignore').splitlines():
                    if not ln or ln.strip().startswith('#') or '=' not in ln:
                        continue
                    k, v = ln.split('=', 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k == 'EXCHANGE_API_KEY' and not key:
                        key = v
                    elif k == 'EXCHANGE_API_SECRET' and not sec:
                        sec = v
                    elif k == 'EXCHANGE_PASSPHRASE' and not pp:
                        pp = v

        if not (key and sec and pp):
            snapshot['error'] = 'missing_credentials'
            return snapshot

        ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
        path = '/api/v5/account/balance'
        msg = ts + 'GET' + path
        sig = base64.b64encode(hmac.new(sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
        headers = {
            'OK-ACCESS-KEY': key,
            'OK-ACCESS-SIGN': sig,
            'OK-ACCESS-TIMESTAMP': ts,
            'OK-ACCESS-PASSPHRASE': pp,
        }
        response = requests.get('https://www.okx.com' + path, headers=headers, timeout=8)
        payload = response.json()
        if payload.get('code') != '0' or not payload.get('data'):
            snapshot['error'] = str(payload.get('msg') or payload.get('code') or 'balance_request_failed')
            return snapshot

        account_payload = payload['data'][0] if payload.get('data') else {}
        details = account_payload.get('details', [])
        cash_usdt = 0.0
        total_equity = 0.0
        eq_usd_seen = False
        for detail in details:
            ccy = str(detail.get('ccy') or '')
            if ccy == 'USDT':
                usdt_eq = _maybe_float(detail.get('eq'))
                if usdt_eq is not None:
                    cash_usdt = usdt_eq
            eq_usd = _maybe_float(detail.get('eqUsd'))
            if eq_usd is not None:
                total_equity += eq_usd
                eq_usd_seen = True

        if not eq_usd_seen:
            total_equity = float(
                _maybe_float(account_payload.get('totalEqUsd'))
                or _maybe_float(account_payload.get('totalEq'))
                or 0.0
            )

        snapshot.update({
            'ok': True,
            'cash_usdt': float(cash_usdt),
            'total_equity_usdt': float(total_equity),
            'error': '',
        })
        return snapshot
    except Exception as e:
        snapshot['error'] = str(e)
        return snapshot


def _static_asset_version(filename: str) -> str:
    asset_path = WEB_DIR / 'static' / Path(filename)
    try:
        return str(asset_path.stat().st_mtime_ns)
    except OSError:
        return '0'


def _render_monitor_v2():
    template_path = WEB_DIR / 'templates' / 'monitor_v2.html'
    index_path = REACT_BUILD_PATH / 'index.html'
    renderer_mode = _dashboard_renderer_mode()

    if renderer_mode == "react" and index_path.exists():
        return send_from_directory(str(REACT_BUILD_PATH), 'index.html')

    if template_path.exists():
        return render_template(
            'monitor_v2.html',
            monitor_v2_js_version=_static_asset_version('js/monitor_v2.js'),
            ml_status_panel_js_version=_static_asset_version('js/ml_status_panel.js'),
        )

    if index_path.exists():
        return send_from_directory(str(REACT_BUILD_PATH), 'index.html')

    return 'Not found', 404


def _resolve_safe_react_asset(filename: str) -> Optional[Path]:
    raw = str(filename or '').strip()
    if not raw:
        return None

    candidate = (REACT_BUILD_PATH / raw).resolve()
    build_root = REACT_BUILD_PATH.resolve()
    if candidate == build_root:
        return None
    if build_root not in candidate.parents:
        return None
    return candidate


def _send_react_asset(path: str):
    if str(path).lower().endswith('.js'):
        return send_from_directory(str(REACT_BUILD_PATH), path, mimetype='application/javascript')
    return send_from_directory(str(REACT_BUILD_PATH), path)


@app.route('/')
def index():
    """主页面 - 新版监控面板"""
    return _render_monitor_v2()


@app.route('/monitor')
def monitor():
    """旧版监控页面（保留兼容）"""
    return _render_monitor_v2()


@app.route('/simple')
def simple_dashboard():
    """简洁版监控页"""
    return _render_monitor_v2()


@app.route('/<path:filename>')
def static_files(filename):
    """提供React静态文件"""
    file_path = _resolve_safe_react_asset(filename)
    if file_path is None:
        return 'Not found', 404
    
    # 检查文件是否存在
    if file_path.exists() and file_path.is_file():
        rel_path = file_path.relative_to(REACT_BUILD_PATH.resolve()).as_posix()
        return _send_react_asset(rel_path)
    
    # 如果文件不存在，返回index.html（支持React Router）
    index_path = REACT_BUILD_PATH / 'index.html'
    if index_path.exists():
        return _send_react_asset('index.html')
    
    return 'Not found', 404


@app.route('/api/account')
@_cache_json_response(5.0)
def api_account():
    """账户信息API - 优先OKX实时数据"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)
        latest_update_epoch: Optional[float] = None
        reconcile_path = runtime_paths.reconcile_status_path
        reconcile_mtime = None
        if reconcile_path.exists():
            try:
                reconcile_mtime = reconcile_path.stat().st_mtime
            except OSError:
                reconcile_mtime = None
        try:
            has_reconcile_cash, cash = _load_reconcile_cash_balance(runtime_paths=runtime_paths)
        except TypeError:
            has_reconcile_cash, cash = _load_reconcile_cash_balance()
        try:
            local_account_state = _load_local_account_state(runtime_paths=runtime_paths)
        except TypeError:
            local_account_state = _load_local_account_state()

        authoritative_equity: Optional[float] = None
        authoritative_positions_value: Optional[float] = None
        equity_source = 'local'
        try:
            key, sec, pp = _load_workspace_exchange_creds()
            if _dashboard_live_account_enabled() and key and sec and pp:
                data = _load_okx_account_balance(key, sec, pp)
                if data.get('code') == '0' and data.get('data'):
                    account_equity = _account_equity_from_balance_details(
                        data['data'][0].get('details', []),
                        source='okx_live',
                        hidden_symbols=set(EXCLUDED_SYMBOLS) | POSITION_HIDDEN_BASE_SYMBOLS,
                        min_visible_position_value_usd=MIN_VISIBLE_POSITION_VALUE_USD,
                    )
                    if 'cash_usdt' in account_equity:
                        cash = float(account_equity['cash_usdt'] or 0.0)
                        has_reconcile_cash = True
                    if 'total_equity_usdt' in account_equity:
                        authoritative_equity = float(account_equity['total_equity_usdt'] or 0.0)
                        authoritative_positions_value = float(account_equity.get('positions_value_usdt') or 0.0)
                        equity_source = str(account_equity.get('source') or 'okx_live')
                        latest_update_epoch = time.time()
        except Exception:
            pass

        if authoritative_equity is None:
            account_equity = _load_reconcile_account_equity(runtime_paths=runtime_paths)
            if 'cash_usdt' in account_equity:
                cash = float(account_equity['cash_usdt'] or 0.0)
                has_reconcile_cash = True
            if 'total_equity_usdt' in account_equity:
                authoritative_equity = float(account_equity['total_equity_usdt'] or 0.0)
                authoritative_positions_value = float(account_equity.get('positions_value_usdt') or 0.0)
                equity_source = str(account_equity.get('source') or 'reconcile')
                if reconcile_mtime is not None:
                    latest_update_epoch = reconcile_mtime

        if (not has_reconcile_cash) and cash <= 0:
            cash = float(local_account_state.get('cash_usdt') or 0.0)
        elif latest_update_epoch is None and has_reconcile_cash and reconcile_mtime is not None:
            latest_update_epoch = reconcile_mtime

        conn = None
        if runtime_paths.orders_db.exists():
            try:
                conn = sqlite3.connect(str(runtime_paths.orders_db))
            except Exception:
                conn = None
        if conn:
            cursor = conn.cursor()
            placeholders = ','.join(['?' for _ in EXCLUDED_SYMBOLS])
            query = f"""
                SELECT 
                    SUM(CASE WHEN state='FILLED' THEN 1 ELSE 0 END) as total_trades,
                    SUM(CASE WHEN side='buy' AND state='FILLED' THEN notional_usdt ELSE 0 END) as total_buy,
                    SUM(CASE WHEN side='sell' AND state='FILLED' THEN notional_usdt ELSE 0 END) as total_sell
                FROM orders
                WHERE inst_id NOT IN ({placeholders})
                AND notional_usdt < 1000
            """
            cursor.execute(query, EXCLUDED_SYMBOLS)
            row = cursor.fetchone()
            conn.close()

            total_trades = row[0] or 0
            total_buy = row[1] or 0
            total_sell = row[2] or 0
            total_fees = _load_total_fees_from_orders(
                excluded_inst_ids=EXCLUDED_SYMBOLS,
                max_notional_usdt=1000.0,
                orders_db=runtime_paths.orders_db,
            )
            realized_pnl = float(total_sell) - float(total_buy) + float(total_fees)
        else:
            total_trades = total_buy = total_sell = total_fees = realized_pnl = 0

        positions_value = 0.0
        positions_rows = []
        pos_payload = _call_dashboard_api(api_positions, default={"positions": []}, label="positions_for_account") or {}
        if isinstance(pos_payload, dict):
            positions_rows = pos_payload.get('positions', []) or []
        elif isinstance(pos_payload, list):
            positions_rows = pos_payload
        positions_value = sum(float(x.get('value_usdt') or x.get('value') or 0.0) for x in positions_rows)

        if authoritative_positions_value is not None:
            positions_value = authoritative_positions_value
        total_equity = authoritative_equity if authoritative_equity is not None else float(cash or 0) + positions_value
        initial_capital = 120.0
        equity_delta = total_equity - initial_capital
        total_pnl_pct = equity_delta / initial_capital if initial_capital > 0 else 0

        positions_count = 0
        try:
            rows = positions_rows if isinstance(positions_rows, list) else []
            positions_count = len([p for p in rows if float(p.get('value_usdt') or p.get('value') or 0) > 1])
        except Exception:
            pass

        budget_cap = float(config.get('budget', {}).get('live_equity_cap_usdt', 0) or 0)
        drawdown_pct = 0.0
        peak_equity = initial_capital

        if budget_cap > 0:
            peak_equity = max(float(budget_cap), float(total_equity))
            drawdown_pct = (peak_equity - total_equity) / peak_equity if peak_equity > 0 else 0
            if total_equity > peak_equity:
                drawdown_pct = 0.0
        else:
            try:
                local_peak = float(local_account_state.get('equity_peak_usdt') or 0.0)
                if local_peak > 0:
                    peak_equity = _sanitize_peak_equity(total_equity, initial_capital, local_peak)
                else:
                    peak_equity = max(total_equity, initial_capital)
            except Exception:
                peak_equity = max(total_equity, initial_capital)

            drawdown_pct = (peak_equity - total_equity) / peak_equity if peak_equity > 0 else 0

        drawdown_pct = max(0.0, min(1.0, drawdown_pct))
        if latest_update_epoch is None and runtime_paths.positions_db.exists():
            try:
                latest_update_epoch = runtime_paths.positions_db.stat().st_mtime
            except OSError:
                latest_update_epoch = None

        return jsonify({
            'cash_usdt': round(float(cash), 2),
            'positions_value_usdt': round(float(positions_value), 4),
            'total_equity_usdt': round(float(total_equity), 4),
            'equity_source': equity_source,
            'initial_capital_usdt': round(float(initial_capital), 4),
            'equity_delta_usdt': round(float(equity_delta), 4),
            'total_pnl_pct': round(float(total_pnl_pct), 4),
            'drawdown_pct': round(float(drawdown_pct), 4),
            'peak_equity_usdt': round(float(peak_equity), 2),
            'budget_cap_usdt': round(float(budget_cap), 2) if budget_cap > 0 else None,
            'positions_count': positions_count,
            'total_trades': int(total_trades),
            'total_buy': round(float(total_buy), 2),
            'total_sell': round(float(total_sell), 2),
            'total_fees': round(float(total_fees), 4),
            'realized_pnl': round(float(realized_pnl), 2),
            'last_update': datetime.fromtimestamp(latest_update_epoch).strftime('%Y-%m-%d %H:%M:%S') if latest_update_epoch is not None else ''
        })
    except Exception as e:
        return _json_internal_error_response(
            e,
            cash_usdt=0.0,
            positions_value_usdt=0.0,
            total_equity_usdt=0.0,
            initial_capital_usdt=120.0,
            equity_delta_usdt=0.0,
            total_pnl_pct=0.0,
            drawdown_pct=0.0,
            peak_equity_usdt=120.0,
            budget_cap_usdt=None,
            positions_count=0,
            total_trades=0,
            total_buy=0.0,
            total_sell=0.0,
            total_fees=0.0,
            realized_pnl=0.0,
            last_update='',
        )


@app.route('/api/trades')
@_cache_json_response(10.0)
def api_trades():
    """交易历史API（优先OKX实时成交，回退DB，再回退runs/*/trades.csv）"""
    try:
        runtime_paths = _resolve_dashboard_runtime_paths(load_config())
        trades = []

        # 0) 优先OKX实时成交
        try:
            import hmac, hashlib, base64

            key, sec, pp = _load_workspace_exchange_creds()
            if _dashboard_live_account_enabled() and key and sec and pp:
                ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
                path = '/api/v5/trade/fills?limit=100'
                msg = ts + 'GET' + path
                sig = base64.b64encode(hmac.new(sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
                headers = {
                    'OK-ACCESS-KEY': key,
                    'OK-ACCESS-SIGN': sig,
                    'OK-ACCESS-TIMESTAMP': ts,
                    'OK-ACCESS-PASSPHRASE': pp,
                }
                resp = requests.get('https://www.okx.com' + path, headers=headers, timeout=8)
                data = resp.json()
                if data.get('code') == '0':
                    for r in data.get('data', []):
                        try:
                            inst = str(r.get('instId', ''))
                            if (not inst) or (inst in EXCLUDED_SYMBOLS):
                                continue
                            ts_ms = int(r.get('ts') or 0)
                            t = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(CHINA_TZ)
                            px = float(r.get('fillPx') or 0)
                            sz = float(r.get('fillSz') or 0)
                            amount = px * sz
                            fee = _signed_fee_usdt_from_fee_fields(
                                inst,
                                px,
                                r.get('fee'),
                                r.get('feeCcy') or r.get('fillFeeCcy'),
                            )
                            trades.append({
                                'symbol': inst,
                                'side': str(r.get('side', '')),
                                'price': round(px, 6),
                                'qty': round(sz, 8),
                                'amount': round(amount, 4),
                                'fee': round(fee, 6),
                                'state': 'FILLED',
                                'time': t.strftime('%Y-%m-%d %H:%M:%S')
                            })
                        except Exception:
                            continue
        except Exception:
            pass

        # 1) 回退订单库
        if not trades:
            conn = None
            if runtime_paths.orders_db.exists():
                conn = sqlite3.connect(str(runtime_paths.orders_db))
            if conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in EXCLUDED_SYMBOLS])
                try:
                    cursor.execute(f"""
                        SELECT 
                            inst_id, side, notional_usdt, fee, state, avg_px,
                            datetime(COALESCE(NULLIF(updated_ts, 0), created_ts)/1000, 'unixepoch', '+8 hours') as time
                        FROM orders 
                        WHERE state='FILLED'
                        AND inst_id NOT IN ({placeholders})
                        AND notional_usdt < 1000
                        ORDER BY COALESCE(NULLIF(updated_ts, 0), created_ts) DESC
                        LIMIT 100
                    """, EXCLUDED_SYMBOLS)
                except sqlite3.OperationalError:
                    cursor.execute(f"""
                        SELECT 
                            inst_id, side, notional_usdt, fee, state, avg_px,
                            datetime(created_ts/1000, 'unixepoch', '+8 hours') as time
                        FROM orders 
                        WHERE state='FILLED'
                        AND inst_id NOT IN ({placeholders})
                        AND notional_usdt < 1000
                        ORDER BY created_ts DESC
                        LIMIT 100
                    """, EXCLUDED_SYMBOLS)

                for row in cursor.fetchall():
                    try:
                        price = float(row[5] or 0)
                        amount = float(row[2] or 0)
                        qty = (amount / price) if price > 0 else 0.0
                        trades.append({
                            'symbol': str(row[0]),
                            'side': str(row[1]),
                            'price': round(price, 6),
                            'qty': round(qty, 8),
                            'amount': round(amount, 4),
                            'fee': round(_signed_fee_usdt_from_order_fee(str(row[0]), row[5], row[3]), 6),
                            'state': str(row[4]),
                            'time': str(row[6])
                        })
                    except (TypeError, ValueError):
                        continue
                conn.close()

        # 2) 回退 runs/*/trades.csv
        if not trades:
            runs_dir = runtime_paths.runs_dir
            if runs_dir.exists():
                run_dirs = _sorted_run_dirs_by_artifact_mtime(runs_dir, 'trades.csv', limit=24)
                for run_dir in run_dirs:
                    p = run_dir / 'trades.csv'
                    if not p.exists():
                        continue
                    try:
                        import csv
                        with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                            reader = csv.DictReader(f)
                            for r in reader:
                                sym = str(r.get('symbol', '') or '')
                                if not sym:
                                    continue
                                if any(ex in sym for ex in EXCLUDED_SYMBOLS):
                                    continue
                                price = float(r.get('price') or r.get('avg_px') or 0)
                                amount = float(r.get('notional_usdt') or 0)
                                qty = float(r.get('qty') or 0)
                                if qty <= 0 and price > 0 and amount > 0:
                                    qty = amount / price
                                trades.append({
                                    'symbol': sym.replace('/USDT', '-USDT'),
                                    'side': str(r.get('side', '')),
                                    'price': round(price, 6),
                                    'qty': round(qty, 8),
                                    'amount': round(amount, 4),
                                    'fee': round(float(r.get('fee_usdt') or 0), 6),
                                    'state': 'FILLED',
                                    'time': str(r.get('ts', '')),
                                })
                    except Exception:
                        continue
                    if len(trades) >= 100:
                        break

        return jsonify({'trades': trades[:100]})
    except Exception as e:
        return _json_internal_error_response(e, trades=[])


@app.route('/api/positions')
@_cache_json_response(5.0)
def api_positions():
    """持仓信息API（优先 positions.sqlite，回退最新 run 的 positions.jsonl）"""
    try:
        runtime_paths = _resolve_dashboard_runtime_paths(load_config())
        hidden_symbols = POSITION_HIDDEN_BASE_SYMBOLS
        authoritative_snapshot_seen = False
        price_cache: Dict[str, float] = {}

        def get_last_price_usdt(symbol: str) -> float:
            symbol = str(symbol or '').strip().upper()
            if not symbol:
                return 0.0
            if symbol in price_cache:
                return price_cache[symbol]
            try:
                cache_dir = WORKSPACE / 'data' / 'cache'
                files = list(cache_dir.glob(f'{symbol}_USDT_1H_*.csv'))
                if files:
                    latest_file = max(files, key=_ohlcv_cache_file_epoch)
                    file_epoch = _ohlcv_cache_file_epoch(latest_file)
                    if time.time() - file_epoch < 900:  # 15分钟内
                        df = pd.read_csv(latest_file)
                        if len(df) > 0 and 'close' in df.columns:
                            ts_col = 'timestamp' if 'timestamp' in df.columns else 'ts' if 'ts' in df.columns else None
                            if ts_col is not None:
                                parsed_ts = pd.to_datetime(df[ts_col], errors='coerce', utc=False)
                                if parsed_ts.notna().any():
                                    df = df.assign(_parsed_ts=parsed_ts).sort_values('_parsed_ts', kind='mergesort')
                            price = float(df.iloc[-1]['close'])
                            price_cache[symbol] = price
                            return price
            except Exception:
                pass

            try:
                price = _load_okx_public_ticker_last_price(symbol)
                price_cache[symbol] = price
                return price
            except Exception:
                pass

            price_cache[symbol] = 0.0
            return 0.0

        def choose_spot_qty(detail: Dict[str, Any]) -> float:
            cash_bal = float(detail.get('cashBal') or 0)
            eq_qty = float(detail.get('eq') or 0)
            avail_bal = float(detail.get('availBal') or 0)
            spot_bal = float(detail.get('spotBal') or 0)
            if cash_bal > 0:
                return cash_bal
            if eq_qty > 0:
                return eq_qty
            if avail_bal > 0:
                return avail_bal
            if spot_bal > 0:
                return spot_bal
            return 0.0

        def append_position(ccy: str, qty: float, eq_usd: float) -> None:
            ccy = str(ccy or '')
            if not ccy or ccy == 'USDT' or ccy in hidden_symbols:
                return
            qty = float(qty or 0)
            if qty <= 0:
                return

            px = get_last_price_usdt(ccy)
            eq_usd = float(eq_usd or 0)
            if eq_usd <= 0 and px > 0:
                eq_usd = qty * px
            if eq_usd <= 0 or eq_usd < 0.5:
                return

            effective_px = eq_usd / qty if qty > 0 else 0.0
            if effective_px > 0:
                px = effective_px
            if px <= 0:
                return

            positions.append({
                'symbol': ccy,
                'qty': round(qty, 8),
                'avg_px': round(avg_price_hints.get(ccy, 0.0), 6),
                'last_price': round(px, 6),
                'value_usdt': round(eq_usd, 4)
            })

        pos_db = runtime_paths.positions_db
        positions = []
        avg_price_hints: Dict[str, float] = {}

        if pos_db.exists():
            try:
                conn = sqlite3.connect(str(pos_db))
                cur = conn.cursor()
                cur.execute("SELECT symbol, avg_px FROM positions")
                for symbol_raw, avg_px in cur.fetchall():
                    base = str(symbol_raw or '').split('/')[0] if '/' in str(symbol_raw or '') else str(symbol_raw or '').split('-')[0]
                    avg_price = float(avg_px or 0)
                    if base and avg_price > 0:
                        avg_price_hints[base] = avg_price
                conn.close()
            except Exception:
                avg_price_hints = {}

        # 0) 优先实时OKX余额（与用户手动操作一致）
        try:
            key, sec, pp = _load_workspace_exchange_creds()
            if _dashboard_live_account_enabled() and key and sec and pp:
                data = _load_okx_account_balance(key, sec, pp)
                if data.get('code') == '0' and data.get('data'):
                    authoritative_snapshot_seen = True
                    details = data['data'][0].get('details', [])
                    for d in details:
                        try:
                            ccy = str(d.get('ccy') or '')
                            qty = choose_spot_qty(d)
                            eq_usd = float(d.get('eqUsd') or 0)
                            append_position(ccy, qty, eq_usd)
                        except Exception:
                            continue
        except Exception:
            pass

        # 1) 回退 positions.sqlite（仅当实时OKX不可用且positions为空）
        # 注意：如果OKX API成功调用但返回空持仓，说明真的没持仓，不应回退到缓存
        reconcile_file = runtime_paths.reconcile_status_path
        if not positions and reconcile_file.exists():
            try:
                reconcile = json.loads(reconcile_file.read_text(encoding='utf-8', errors='ignore'))
                exchange_snapshot = reconcile.get('exchange_snapshot') or {}
                ccy_cash_bal = exchange_snapshot.get('ccy_cashBal') or {}
                ccy_eq_usd = exchange_snapshot.get('ccy_eqUsd') or {}
                if isinstance(ccy_cash_bal, dict):
                    authoritative_snapshot_seen = True
                    for ccy, qty in ccy_cash_bal.items():
                        eq_usd = ccy_eq_usd.get(ccy) if isinstance(ccy_eq_usd, dict) else 0
                        append_position(ccy, qty, eq_usd)
            except Exception:
                pass

        fallback_source = None
        if not authoritative_snapshot_seen and not positions and pos_db.exists():
            fallback_source = "positions.sqlite"
            conn = sqlite3.connect(str(pos_db))
            cur = conn.cursor()
            cur.execute("SELECT symbol, qty, avg_px, last_mark_px FROM positions")
            rows = cur.fetchall()
            conn.close()

            for symbol_raw, qty, avg_px, last_mark_px in rows:
                try:
                    symbol_raw = str(symbol_raw or '')
                    base = symbol_raw.split('/')[0] if '/' in symbol_raw else symbol_raw.split('-')[0]
                    if base == 'USDT' or base in hidden_symbols:
                        continue
                    print(f"[positions] sqlite: {base}, qty={qty}")

                    qty_float = float(qty or 0)
                    if qty_float <= 0:
                        continue

                    px = float(last_mark_px or 0) if last_mark_px else 0.0
                    if px <= 0:
                        px = get_last_price_usdt(base)
                    if px <= 0 and avg_px:
                        px = float(avg_px)

                    value = qty_float * px if px > 0 else 0.0
                    if value < 0.5:
                        continue

                    positions.append({
                        'symbol': base,
                        'qty': round(qty_float, 8),
                        'avg_px': round(float(avg_px or 0), 6),
                        'last_price': round(px, 6),
                        'value_usdt': round(value, 4)
                    })
                except Exception:
                    continue

        # 2) 回退：DB为空且OKX不可用时读取最新 runs/*/positions.jsonl
        if not authoritative_snapshot_seen and not positions:
            runs_dir = runtime_paths.runs_dir
            if runs_dir.exists():
                run_dirs = _sorted_run_dirs_by_artifact_mtime(runs_dir, 'positions.jsonl', limit=12)
                for run_dir in run_dirs:
                    p = run_dir / 'positions.jsonl'
                    if not p.exists():
                        continue
                    try:
                        with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                try:
                                    row = json.loads(line)
                                    symbol_raw = str(row.get('symbol', '') or '')
                                    base = symbol_raw.split('/')[0] if '/' in symbol_raw else symbol_raw.split('-')[0]
                                    if base == 'USDT' or base in hidden_symbols:
                                        continue
                                    qty_float = float(row.get('qty') or 0)
                                    if qty_float <= 0:
                                        continue
                                    px = float(row.get('mark_px') or 0)
                                    if px <= 0:
                                        px = get_last_price_usdt(base)
                                    if px <= 0:
                                        px = float(row.get('avg_px') or 0)
                                    value = qty_float * px if px > 0 else 0.0
                                    if value < 0.5:
                                        continue
                                    positions.append({
                                        'symbol': base,
                                        'qty': round(qty_float, 8),
                                        'avg_px': round(float(row.get('avg_px') or 0), 6),
                                        'last_price': round(px, 6),
                                        'value_usdt': round(value, 4)
                                    })
                                except Exception:
                                    continue
                    except Exception:
                        continue
                    if positions:
                        break

        positions.sort(key=lambda x: x.get('value_usdt', 0), reverse=True)

        # 优先用 fills.sqlite 重建净持仓成本，避免 orders 聚合值、模糊匹配和 base fee 漂移。
        for p in positions:
            symbol = p.get('symbol', '')
            if not symbol:
                continue
            avg_cost = _load_avg_cost_from_fills(
                symbol,
                float(p.get('qty', 0) or 0.0),
                fills_db=runtime_paths.fills_db,
            )
            if avg_cost and avg_cost > 0:
                p['avg_px'] = round(avg_cost, 6)

        for p in positions:
            avg_px = float(p.get('avg_px', 0))
            last_px = float(p.get('last_price', 0))
            if avg_px > 0 and last_px > 0:
                p['pnl_pct'] = round((last_px - avg_px) / avg_px, 4)
                # 盈亏金额
                qty = float(p.get('qty', 0))
                p['pnl_value'] = round((last_px - avg_px) * qty, 4)
            else:
                p['pnl_pct'] = 0.0
                p['pnl_value'] = 0.0
            p['price'] = last_px
            p['value'] = p.get('value_usdt', 0)
            p['quantity'] = p.get('qty', 0)
        
        return jsonify({'positions': positions})
    except Exception as e:
        return _json_internal_error_response(e, positions=[])


@app.route('/api/position_kline')
@_cache_json_response(15.0)
def api_position_kline():
    """持仓币 K 线数据。"""
    try:
        symbol = str(request.args.get('symbol', '') or '').strip()
        if not symbol:
            return jsonify({'error': 'symbol is required', 'candles': []}), 400

        timeframe = str(request.args.get('timeframe', '1h') or '1h').strip().lower()
        if timeframe not in POSITION_KLINE_TIMEFRAMES:
            timeframe = '1h'

        try:
            limit = int(request.args.get('limit', POSITION_KLINE_DEFAULT_LIMIT))
        except (TypeError, ValueError):
            limit = POSITION_KLINE_DEFAULT_LIMIT
        limit = max(24, min(limit, 240))

        normalized_symbol = _normalize_dashboard_symbol(symbol)
        series, source = _load_position_market_series(normalized_symbol, timeframe, limit)
        series = _trim_market_series(series, limit)
        candles = []
        for ts_ms, open_px, high_px, low_px, close_px, volume in zip(
            series.ts,
            series.open,
            series.high,
            series.low,
            series.close,
            series.volume,
        ):
            ts_value = int(ts_ms)
            if abs(ts_value) < 10_000_000_000:
                ts_value *= 1000
            candles.append({
                'ts': ts_value,
                'time': datetime.fromtimestamp(ts_value / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M'),
                'open': round(float(open_px), 8),
                'high': round(float(high_px), 8),
                'low': round(float(low_px), 8),
                'close': round(float(close_px), 8),
                'volume': round(float(volume), 8),
            })

        if not candles:
            return jsonify({'error': f'no candles for {normalized_symbol}', 'candles': []}), 404

        first_open = float(candles[0]['open'] or 0)
        last_close = float(candles[-1]['close'] or 0)
        period_change_pct = ((last_close - first_open) / first_open) if first_open > 0 else 0.0

        return jsonify({
            'symbol': normalized_symbol,
            'timeframe': timeframe,
            'source': source,
            'candles': candles,
            'summary': {
                'bars': len(candles),
                'open': round(first_open, 8),
                'close': round(last_close, 8),
                'high': round(max(float(item['high']) for item in candles), 8),
                'low': round(min(float(item['low']) for item in candles), 8),
                'volume': round(sum(float(item['volume']) for item in candles), 8),
                'change_pct': round(period_change_pct, 6),
                'last_time': candles[-1]['time'],
            },
        })
    except Exception as exc:
        return _json_internal_error_response(exc, candles=[])


@app.route('/api/scores')
@_cache_json_response(20.0)
def api_scores():
    """币种评分API（当前run vs 上一个run 的排名变化）"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)
        current_run_id: Optional[str] = None
        previous_run_id: Optional[str] = None
        current_regime = 'Unknown'
        current_scores: List[Dict[str, Any]] = []
        previous_scores: List[Dict[str, Any]] = []

        usable_runs: List[Dict[str, Any]] = []
        for entry in _iter_decision_audits(
            runtime_paths.reports_dir,
            scan_limit=_load_recent_scan_limit('V5_DASHBOARD_SCORE_AUDIT_SCAN_LIMIT'),
        ):
            items = _normalize_top_scores(entry['audit'].get('top_scores', []))
            if not items:
                continue
            usable_runs.append({
                'run_id': entry['run_dir'].name,
                'regime': str(entry['audit'].get('regime') or 'Unknown'),
                'scores': items,
                'sort_epoch': float(entry.get('sort_epoch', 0.0) or 0.0),
            })

        current_run_epoch = None
        if usable_runs:
            current_run_id = usable_runs[0]['run_id']
            current_regime = usable_runs[0]['regime']
            current_scores = usable_runs[0]['scores']
            current_run_epoch = float(usable_runs[0].get('sort_epoch', 0.0) or 0.0)
            if len(usable_runs) > 1:
                previous_run_id = usable_runs[1]['run_id']
                previous_scores = usable_runs[1]['scores']
        else:
            alpha_snapshot = _load_alpha_snapshot_scores(runtime_paths.reports_dir)
            if alpha_snapshot:
                current_run_id = str(alpha_snapshot.get('current_run') or 'alpha_snapshot')
                current_regime = str(alpha_snapshot.get('regime') or 'Unknown')
                current_scores = alpha_snapshot.get('scores', [])
                current_run_epoch = _coerce_timestamp_epoch(alpha_snapshot.get('timestamp'))
            else:
                return jsonify({'regime': 'Unknown', 'scores': []})

        previous_ranking = {}
        for idx, s in enumerate(previous_scores):
            previous_ranking[s['symbol']] = {
                'rank': int(s.get('rank', idx + 1) or (idx + 1)),
                'score': s.get('score', 0),
                'raw_score': s.get('raw_score', s.get('score', 0)),
            }

        scores_with_trend = []
        for idx, s in enumerate(current_scores):
            symbol = s['symbol']
            current_rank = int(s.get('rank', idx + 1) or (idx + 1))
            prev_info = previous_ranking.get(symbol)
            if prev_info:
                rank_change = prev_info['rank'] - current_rank
                score_change = round(float(s['score']) - float(prev_info['score']), 4)
                raw_score_change = round(float(s.get('raw_score', s['score'])) - float(prev_info.get('raw_score', prev_info['score'])), 4)
                trend = 'up' if rank_change > 0 else 'down' if rank_change < 0 else 'stable'
                scores_with_trend.append({
                    **s,
                    'rank': current_rank,
                    'previous_rank': prev_info['rank'],
                    'rank_change': rank_change,
                    'score_change': score_change,
                    'raw_score_change': raw_score_change,
                    'trend': trend,
                })
            else:
                scores_with_trend.append({
                    **s,
                    'rank': current_rank,
                    'previous_rank': None,
                    'rank_change': None,
                    'score_change': None,
                    'raw_score_change': None,
                    'trend': 'new',
                })

        return jsonify({
            'regime': current_regime,
            'current_run': current_run_id,
            'previous_run': previous_run_id,
            'scores': scores_with_trend,
            'last_update': datetime.fromtimestamp(current_run_epoch).isoformat() if current_run_epoch is not None else ''
        })
    except Exception as e:
        return _json_internal_error_response(
            e,
            regime='Error',
            current_run=None,
            previous_run=None,
            scores=[],
            last_update='',
        )


@app.route('/api/sentiment')
def api_sentiment():
    """情绪分析API（优先读取本地缓存，避免阻塞UI）"""
    try:
        # 动态展示：主流币 + 当前评分Top币，避免TRX等未显示
        symbols = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']
        try:
            top_scores = api_scores().get_json().get('scores', [])[:8]
            for row in top_scores:
                sym = str(row.get('symbol', '')).replace('/USDT', '-USDT')
                if sym and sym not in symbols:
                    symbols.append(sym)
        except Exception:
            pass
        cache_dir = WORKSPACE / 'data/sentiment_cache'
        results = {}
        latest_update_epoch: Optional[float] = None

        for symbol in symbols:
            try:
                latest = _latest_signal_file(
                    cache_dir,
                    [
                        f'rss_{symbol}_*.json',
                        'rss_MARKET_*.json',
                        f'funding_{symbol}_*.json',
                        f'deepseek_{symbol}_*.json',
                        f'{symbol}_*.json',
                    ],
                )

                if latest is None:
                    results[symbol] = {'error': 'no_cache'}
                    continue

                with open(latest, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logical_epoch = _signal_file_epoch(latest)
                latest_update_epoch = logical_epoch if latest_update_epoch is None else max(latest_update_epoch, logical_epoch)

                results[symbol] = {
                    'sentiment': float(data.get('f6_sentiment', 0.0)),
                    'fear_greed': float(data.get('f6_fear_greed_index', 50.0)),
                    'stage': data.get('f6_market_stage', 'unknown'),
                    'summary': data.get('f6_sentiment_summary', ''),
                    'source': data.get('f6_sentiment_source', 'cache'),
                    'cache_file': latest.name,
                    'cache_mtime': datetime.fromtimestamp(logical_epoch).strftime('%Y-%m-%d %H:%M:%S')
                }
            except Exception as e:
                results[symbol] = {'error': 'cache_error'}

        valid_scores = [r['sentiment'] for r in results.values() if 'sentiment' in r]
        valid_fg = [r['fear_greed'] for r in results.values() if 'fear_greed' in r]
        avg_sentiment = sum(valid_scores) / len(valid_scores) if valid_scores else 0.0
        avg_fear_greed = sum(valid_fg) / len(valid_fg) if valid_fg else 50.0

        if avg_sentiment > 0.5:
            market_mood = '贪婪'
            mood_color = '#22c55e'
        elif avg_sentiment < -0.5:
            market_mood = '恐慌'
            mood_color = '#ef4444'
        else:
            market_mood = '中性'
            mood_color = '#64748b'

        return jsonify({
            'overall': {
                'sentiment': round(avg_sentiment, 4),
                'fear_greed': int(round(avg_fear_greed)),
                'mood': market_mood,
                'mood_color': mood_color
            },
            'by_symbol': results,
            'last_update': datetime.fromtimestamp(latest_update_epoch).strftime('%Y-%m-%d %H:%M:%S') if latest_update_epoch is not None else ''
        })
    except Exception as e:
        return _json_internal_error_response(
            e,
            overall={
                'sentiment': 0.0,
                'fear_greed': 50,
                'mood': '中性',
                'mood_color': '#64748b',
            },
            by_symbol={},
            last_update='',
        )


@app.route('/api/status')
@_cache_json_response(5.0)
def api_status():
    """系统状态API"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)
        timer_name = _pick_timer_name()
        timer_state = _get_timer_state(timer_name)
        mode = _dashboard_execution_mode(config)
        dry_run = _dashboard_dry_run(config)

        return jsonify({
            'timer_active': bool(timer_state.get('active')),
            'timer_name': timer_name,
            'timer_error': timer_state.get('error'),
            'mode': mode,
            'dry_run': dry_run,
            'kill_switch': _dashboard_kill_switch_enabled(runtime_paths.kill_switch_path),
            'equity_cap': config.get('budget', {}).get('live_equity_cap_usdt', 0),
            'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        })
    except Exception as e:
        return _json_internal_error_response(
            e,
            timer_active=False,
            timer_name='v5-prod.user.timer',
            timer_error=None,
            mode='unknown',
            dry_run=True,
            kill_switch=False,
            equity_cap=0,
            last_check='',
        )


def calculate_market_indicators():
    """从BTC K线数据计算市场指标"""
    try:
        # 读取BTC缓存数据
        cache_dir = CACHE_DIR
        btc_files = list(cache_dir.glob('BTC_USDT_1H_*.csv'))
        
        if not btc_files:
            return {'ma20': 0, 'ma60': 0, 'atr_percent': 1.0, 'price': 0}
        
        # 读取最新的BTC数据
        def _sort_epoch(path: Path) -> float:
            suffix = path.stem.removeprefix('BTC_USDT_1H_')
            try:
                return datetime.strptime(suffix, "%Y%m%d_%H").timestamp()
            except Exception:
                return path.stat().st_mtime

        latest_file = max(btc_files, key=_sort_epoch)
        df = pd.read_csv(latest_file)
        ts_col = 'timestamp' if 'timestamp' in df.columns else 'ts' if 'ts' in df.columns else None
        if ts_col is not None:
            parsed_ts = pd.to_datetime(df[ts_col], errors='coerce', utc=False)
            if parsed_ts.notna().any():
                df = df.assign(_parsed_ts=parsed_ts).sort_values('_parsed_ts', kind='mergesort')
        
        if len(df) < 60:
            return {'ma20': 0, 'ma60': 0, 'atr_percent': 1.0, 'price': 0}
        
        # 计算MA20和MA60
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma60'] = df['close'].rolling(window=60).mean()
        
        # 计算ATR
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = abs(df['high'] - df['close'].shift())
        df['low_close'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=14).mean()
        
        # 获取最新值
        latest = df.iloc[-1]
        price = latest['close']
        ma20 = latest['ma20']
        ma60 = latest['ma60']
        atr = latest['atr']
        atr_percent = (atr / price * 100) if price > 0 else 1.0
        
        return {
            'ma20': round(ma20, 2) if not pd.isna(ma20) else 0,
            'ma60': round(ma60, 2) if not pd.isna(ma60) else 0,
            'atr_percent': round(atr_percent, 2) if not pd.isna(atr_percent) else 1.0,
            'price': round(price, 2)
        }
    except Exception as e:
        print(f"计算市场指标失败: {e}")
        return {'ma20': 0, 'ma60': 0, 'atr_percent': 1.0, 'price': 0}


def _downsample_history(points: List[Dict[str, Any]], max_points: int = 24) -> List[Dict[str, Any]]:
    if len(points) <= max_points:
        return points
    if max_points <= 1:
        return [points[-1]]
    step = (len(points) - 1) / float(max_points - 1)
    out = []
    used = set()
    for idx in range(max_points):
        pos = int(round(idx * step))
        pos = max(0, min(len(points) - 1, pos))
        if pos in used:
            continue
        used.add(pos)
        out.append(points[pos])
    if out[-1] != points[-1]:
        out[-1] = points[-1]
    return out


def _load_market_vote_history(reports_dir: Path, hours: int = 24, max_points: int = 24) -> List[Dict[str, Any]]:
    db_path = reports_dir / 'regime_history.db'
    if not db_path.exists():
        return []

    cutoff_ms = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              ts_ms, final_state, final_score, confidence,
              hmm_state, hmm_confidence,
              funding_state, funding_confidence, funding_sentiment,
              rss_state, rss_confidence, rss_sentiment
            FROM regime_history
            WHERE ts_ms >= ?
            ORDER BY ts_ms ASC
            LIMIT 288
            """,
            (cutoff_ms,),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception:
        return []

    points = []
    for row in rows:
        ts_ms = int(row['ts_ms'] or 0)
        if ts_ms <= 0:
            continue
        points.append({
            'ts_ms': ts_ms,
            'label': datetime.fromtimestamp(ts_ms / 1000).strftime('%m-%d %H:%M'),
            'final': {
                'state': str(row['final_state'] or 'SIDEWAYS'),
                'confidence': float(row['confidence'] or 0.0),
                'score': float(row['final_score'] or 0.0),
            },
            'votes': {
                'hmm': {
                    'state': str(row['hmm_state'] or 'SIDEWAYS'),
                    'confidence': float(row['hmm_confidence'] or 0.0),
                },
                'funding': {
                    'state': str(row['funding_state'] or 'SIDEWAYS'),
                    'confidence': float(row['funding_confidence'] or 0.0),
                    'sentiment': float(row['funding_sentiment'] or 0.0),
                },
                'rss': {
                    'state': str(row['rss_state'] or 'SIDEWAYS'),
                    'confidence': float(row['rss_confidence'] or 0.0),
                    'sentiment': float(row['rss_sentiment'] or 0.0),
                },
            },
        })

    return _downsample_history(points, max_points=max_points)


def _ohlcv_cache_file_epoch(path: Path) -> float:
    stem = path.stem
    suffix = stem.split("_1H_", 1)[1] if "_1H_" in stem else stem

    hourly_match = re.search(r"(20\d{6}_\d{2})$", suffix)
    if hourly_match:
        try:
            return datetime.strptime(hourly_match.group(1), "%Y%m%d_%H").timestamp()
        except Exception:
            pass

    date_tokens = re.findall(r"(20\d{2}-\d{2}-\d{2}|20\d{6})", suffix)
    if date_tokens:
        token = date_tokens[-1]
        try:
            fmt = "%Y-%m-%d" if "-" in token else "%Y%m%d"
            return datetime.strptime(token, fmt).timestamp()
        except Exception:
            pass

    return path.stat().st_mtime


def _signal_file_epoch(path: Path) -> float:
    parts = path.stem.split('_')
    if len(parts) >= 2:
        try:
            return datetime.strptime('_'.join(parts[-2:]), "%Y%m%d_%H").timestamp()
        except Exception:
            pass
    return path.stat().st_mtime


def _latest_signal_file(cache_dir: Path, patterns: List[str]) -> Optional[Path]:

    latest: Optional[Path] = None
    latest_epoch = -1.0
    for pattern in patterns:
        for path in cache_dir.glob(pattern):
            try:
                epoch = _signal_file_epoch(path)
            except OSError:
                continue
            if epoch > latest_epoch:
                latest = path
                latest_epoch = epoch
    return latest


def _signal_health(cache_dir: Path, patterns: List[str], max_age_minutes: int, error_name: str) -> Dict[str, Any]:
    latest = _latest_signal_file(cache_dir, patterns)
    if latest is None:
        return {
            'status': 'missing',
            'is_fresh': False,
            'error': error_name,
            'last_file': None,
            'last_mtime': None,
            'age_minutes': None,
            'max_age_minutes': int(max_age_minutes),
        }

    signal_epoch = _signal_file_epoch(latest)
    age_minutes = max(0.0, (datetime.now().timestamp() - signal_epoch) / 60.0)
    is_fresh = age_minutes <= max(int(max_age_minutes), 1)
    return {
        'status': 'fresh' if is_fresh else 'stale',
        'is_fresh': bool(is_fresh),
        'error': None if is_fresh else error_name,
        'last_file': latest.name,
        'last_mtime': datetime.fromtimestamp(signal_epoch).strftime('%Y-%m-%d %H:%M:%S'),
        'age_minutes': round(age_minutes, 1),
        'max_age_minutes': int(max_age_minutes),
    }


def _load_json_payload(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _build_live_funding_vote(
    cache_dir: Path,
    max_age_minutes: int,
    weight: float,
    regime_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    regime_cfg = regime_cfg or {}
    funding_kwargs = {
        'trending_threshold': float(regime_cfg.get('funding_trending_threshold', 0.10) or 0.10),
        'risk_off_threshold': float(regime_cfg.get('funding_risk_off_threshold', -0.10) or -0.10),
        'breadth_threshold': float(regime_cfg.get('funding_breadth_threshold', 0.68) or 0.68),
        'extreme_sentiment_threshold': float(
            regime_cfg.get('funding_extreme_sentiment_threshold', 0.12) or 0.12
        ),
        'extreme_breadth_threshold': float(
            regime_cfg.get('funding_extreme_breadth_threshold', 0.55) or 0.55
        ),
    }
    composite_file = _latest_signal_file(cache_dir, ['funding_COMPOSITE_*.json'])
    if composite_file is not None:
        health = _signal_health(cache_dir, [composite_file.name], max_age_minutes, 'funding_signal_stale_or_missing')
        if health.get('is_fresh'):
            data = _load_json_payload(composite_file)
            return build_funding_vote(
                sentiment=float(data.get('f6_sentiment', 0.0) or 0.0),
                weight=float(weight),
                details=data.get('tier_breakdown', {}),
                composite=True,
                positive_weight_share=float(data.get('positive_weight_share', 0.0) or 0.0),
                negative_weight_share=float(data.get('negative_weight_share', 0.0) or 0.0),
                strongest_sentiment=float(data.get('strongest_sentiment', 0.0) or 0.0),
                max_abs_sentiment=float(data.get('max_abs_sentiment', 0.0) or 0.0),
                extreme_positive_weight_share=float(data.get('extreme_positive_weight_share', 0.0) or 0.0),
                extreme_negative_weight_share=float(data.get('extreme_negative_weight_share', 0.0) or 0.0),
                **funding_kwargs,
            )

    rows = []
    details: Dict[str, float] = {}
    for sym in ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']:
        latest = _latest_signal_file(cache_dir, [f'funding_{sym}_*.json'])
        if latest is None:
            continue
        health = _signal_health(cache_dir, [latest.name], max_age_minutes, 'funding_signal_stale_or_missing')
        if not health.get('is_fresh'):
            continue
        data = _load_json_payload(latest)
        sentiment = max(-1.0, min(1.0, float(data.get('f6_sentiment', 0.0) or 0.0)))
        rows.append({'symbol': sym, 'sentiment': sentiment, 'weight': 1.0})
        details[sym] = sentiment

    if not rows:
        return {}

    metrics = summarize_funding_rows(
        rows,
        extreme_sentiment_threshold=funding_kwargs['extreme_sentiment_threshold'],
    )
    return build_funding_vote(
        sentiment=metrics['sentiment'],
        weight=float(weight),
        details=details,
        composite=False,
        positive_weight_share=metrics['positive_weight_share'],
        negative_weight_share=metrics['negative_weight_share'],
        strongest_sentiment=metrics['strongest_sentiment'],
        max_abs_sentiment=metrics['max_abs_sentiment'],
        extreme_positive_weight_share=metrics['extreme_positive_weight_share'],
        extreme_negative_weight_share=metrics['extreme_negative_weight_share'],
        **funding_kwargs,
    )


def _build_live_rss_vote(cache_dir: Path, max_age_minutes: int, weight: float) -> Dict[str, Any]:
    latest = _latest_signal_file(cache_dir, ['rss_MARKET_*.json', 'rss_BTC-USDT_*.json'])
    if latest is None:
        return {}

    health = _signal_health(cache_dir, [latest.name], max_age_minutes, 'rss_signal_stale_or_missing')
    if not health.get('is_fresh'):
        return {}

    data = _load_json_payload(latest)
    return build_rss_vote(data, weight)


def _load_latest_regime_history_snapshot(reports_dir: Path) -> Dict[str, Any]:
    db_path = reports_dir / 'regime_history.db'
    if not db_path.exists():
        return {}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              ts_ms, final_state, final_score, confidence, multiplier,
              hmm_state, hmm_confidence, hmm_trending_up_prob, hmm_trending_down_prob, hmm_sideways_prob,
              funding_state, funding_confidence, funding_sentiment,
              rss_state, rss_confidence, rss_sentiment,
              alerts_json, weights_json
            FROM regime_history
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        conn.close()
        if row is None:
            return {}

        try:
            weights = json.loads(row['weights_json'] or '{}')
        except Exception:
            weights = {}
        try:
            alerts = json.loads(row['alerts_json'] or '[]')
        except Exception:
            alerts = []

        return {
            'state': str(row['final_state'] or 'SIDEWAYS'),
            'position_multiplier': float(row['multiplier'] or 0.0),
            'final_score': float(row['final_score'] or 0.0),
            'method': 'regime_history',
            'ts': (float(row['ts_ms']) / 1000.0) if row['ts_ms'] is not None else None,
            'votes': {
                'hmm': {
                    'state': row['hmm_state'],
                    'confidence': float(row['hmm_confidence'] or 0.0),
                    'weight': float(weights.get('hmm', 0.0) or 0.0),
                    'raw_state': row['hmm_state'],
                    'probs': {
                        'TrendingUp': float(row['hmm_trending_up_prob'] or 0.0),
                        'TrendingDown': float(row['hmm_trending_down_prob'] or 0.0),
                        'Sideways': float(row['hmm_sideways_prob'] or 0.0),
                    },
                },
                'funding': {
                    'state': row['funding_state'],
                    'confidence': float(row['funding_confidence'] or 0.0),
                    'weight': float(weights.get('funding', 0.0) or 0.0),
                    'sentiment': float(row['funding_sentiment'] or 0.0),
                },
                'rss': {
                    'state': row['rss_state'],
                    'confidence': float(row['rss_confidence'] or 0.0),
                    'weight': float(weights.get('rss', 0.0) or 0.0),
                    'sentiment': float(row['rss_sentiment'] or 0.0),
                },
            },
            'alerts': alerts if isinstance(alerts, list) else [],
            'monitor': {},
        }
    except Exception:
        return {}


def _load_market_state_snapshot(reports_dir: Path) -> Dict[str, Any]:
    try:
        audit_entries = _iter_decision_audits(
            reports_dir,
            scan_limit=_load_recent_scan_limit('V5_DASHBOARD_MARKET_AUDIT_SCAN_LIMIT'),
        )
        regime_json_snapshot = _load_regime_json_snapshot(reports_dir)
        if audit_entries:
            if _is_failed_decision_audit(audit_entries[0]['audit']) and regime_json_snapshot:
                return regime_json_snapshot

            for entry in audit_entries:
                audit = entry['audit']
                if not _has_usable_market_state(audit):
                    continue

                regime = str(audit.get('regime') or 'SIDEWAYS')
                details = audit.get('regime_details', {})
                if isinstance(details, dict) and details:
                    regime = str(details.get('final_state') or regime)
                else:
                    details = {}

                votes = details.get('votes', {}) if isinstance(details.get('votes', {}), dict) else {}
                alerts = []
                for source in (details.get('alerts', []), votes.get('alerts', [])):
                    if not isinstance(source, list):
                        continue
                    for item in source:
                        if item and item not in alerts:
                            alerts.append(item)

                return {
                    'state': regime,
                    'position_multiplier': float(audit.get('regime_multiplier', details.get('multiplier', 0.0)) or 0.0),
                    'final_score': float(details.get('final_score', audit.get('final_score', 0.0)) or 0.0),
                    'method': str(details.get('method', 'decision_audit')),
                    'ts': float(entry.get('sort_epoch', 0.0) or 0.0),
                    'votes': votes,
                    'alerts': alerts,
                    'monitor': details.get('monitor', {}) if isinstance(details.get('monitor', {}), dict) else {},
                }

        if regime_json_snapshot:
            return regime_json_snapshot
    except Exception:
        pass

    return _load_latest_regime_history_snapshot(reports_dir)


@app.route('/api/market_state')
@_cache_json_response(10.0)
def api_market_state():
    """市场状态 API，补齐投票详情和情绪缓存健康。"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)
        snapshot = _load_market_state_snapshot(runtime_paths.reports_dir)
        history_snapshot = _load_latest_regime_history_snapshot(runtime_paths.reports_dir)
        regime = str(snapshot.get('state') or 'SIDEWAYS')
        votes = snapshot.get('votes', {}) if isinstance(snapshot.get('votes', {}), dict) else {}
        history_votes = history_snapshot.get('votes', {}) if isinstance(history_snapshot.get('votes', {}), dict) else {}
        alerts = snapshot.get('alerts', []) if isinstance(snapshot.get('alerts', []), list) else []
        monitor = snapshot.get('monitor', {}) if isinstance(snapshot.get('monitor', {}), dict) else {}

        regime_cfg = config.get('regime', {}) if isinstance(config, dict) else {}
        cache_dir = WORKSPACE / 'data' / 'sentiment_cache'
        signal_health = {
            'funding': _signal_health(
                cache_dir,
                [
                    'funding_COMPOSITE_*.json',
                    'funding_BTC-USDT_*.json',
                    'funding_ETH-USDT_*.json',
                    'funding_SOL-USDT_*.json',
                    'funding_BNB-USDT_*.json',
                ],
                int(regime_cfg.get('funding_signal_max_age_minutes', 180) or 180),
                'funding_signal_stale_or_missing',
            ),
            'rss': _signal_health(
                cache_dir,
                [
                    'rss_MARKET_*.json',
                    'rss_BTC-USDT_*.json',
                ],
                int(regime_cfg.get('rss_signal_max_age_minutes', 180) or 180),
                'rss_signal_stale_or_missing',
            ),
        }

        configured_weights = {
            'hmm': float(regime_cfg.get('hmm_weight', 0.40) or 0.40),
            'funding': float(regime_cfg.get('funding_weight', 0.35) or 0.35),
            'rss': float(regime_cfg.get('rss_weight', 0.25) or 0.25),
        }
        live_votes = {
            'funding': _build_live_funding_vote(
                cache_dir,
                int(regime_cfg.get('funding_signal_max_age_minutes', 180) or 180),
                configured_weights['funding'],
                regime_cfg=regime_cfg,
            ),
            'rss': _build_live_rss_vote(
                cache_dir,
                int(regime_cfg.get('rss_signal_max_age_minutes', 180) or 180),
                configured_weights['rss'],
            ),
        }
        history_24h = _load_market_vote_history(runtime_paths.reports_dir, hours=24, max_points=24)
        hmm_history_vote = history_votes.get('hmm', {}) if isinstance(history_votes.get('hmm', {}), dict) else {}
        hmm_vote = votes.get('hmm', {})
        if not isinstance(hmm_vote, dict):
            hmm_vote = {}
        if hmm_history_vote.get('state') and (
            not hmm_vote.get('state')
            or not isinstance(hmm_vote.get('probs'), dict)
            or float(hmm_vote.get('confidence', 0) or 0) <= 0
        ):
            hmm_vote.update(hmm_history_vote)
            hmm_vote.pop('error', None)
        if hmm_vote:
            hmm_vote.setdefault('weight', configured_weights['hmm'])
        votes['hmm'] = hmm_vote
        stale_errors = {
            'funding': 'funding_signal_stale_or_missing',
            'rss': 'rss_signal_stale_or_missing',
        }
        for name in ('funding', 'rss'):
            vote = votes.get(name, {})
            if not isinstance(vote, dict):
                vote = {}
            live_vote = live_votes.get(name, {})
            if live_vote.get('state'):
                merged_vote = dict(vote)
                merged_vote.update(live_vote)
                merged_vote.pop('error', None)
                vote = merged_vote
            if signal_health[name].get('error'):
                vote.setdefault('error', signal_health[name]['error'])
            elif vote.get('error') == stale_errors[name]:
                vote.pop('error', None)
            votes[name] = vote

        merged_alerts: List[str] = []
        for item in list(alerts) + [signal_health['funding'].get('error'), signal_health['rss'].get('error')]:
            if not item or item in merged_alerts:
                continue
            if item == 'funding_signal_stale_or_missing' and signal_health['funding'].get('is_fresh'):
                continue
            if item == 'rss_signal_stale_or_missing' and signal_health['rss'].get('is_fresh'):
                continue
            merged_alerts.append(str(item))

        indicators = calculate_market_indicators()
        multiplier_map = {
            'Risk-Off': 0.0,
            'RISK_OFF': 0.0,
            'Trending': 1.2,
            'TRENDING': 1.2,
            'Sideways': 0.8,
            'SIDEWAYS': 0.8,
        }
        multiplier = float(snapshot.get('position_multiplier', multiplier_map.get(regime, 0.3)) or 0.0)

        descriptions = {
            'Risk-Off': '风险规避模式，空仓保护中',
            'RISK_OFF': '风险规避模式，空仓保护中',
            'Trending': '趋势行情，增加仓位暴露',
            'TRENDING': '趋势行情，增加仓位暴露',
            'Sideways': '震荡行情，正常仓位',
            'SIDEWAYS': '震荡行情，正常仓位',
        }
        latest_history_ts_ms = None
        if history_24h:
            try:
                latest_history_ts_ms = int(history_24h[-1].get('ts_ms') or 0)
            except Exception:
                latest_history_ts_ms = None
        snapshot_ts_epoch = _coerce_timestamp_epoch(snapshot.get('ts'))
        return jsonify({
            'state': regime.upper().replace('-', '_'),
            'position_multiplier': multiplier,
            'description': descriptions.get(regime, '市场状态监控中'),
            'method': snapshot.get('method', 'unknown'),
            'votes': {
                'hmm': votes.get('hmm', {'state': 'N/A', 'weight': 0}),
                'funding': votes.get('funding', {'state': 'N/A', 'weight': 0}),
                'rss': votes.get('rss', {'state': 'N/A', 'weight': 0}),
            },
            'alerts': merged_alerts,
            'monitor': monitor,
            'final_score': float(snapshot.get('final_score', 0.0) or 0.0),
            'price': indicators['price'],
            'signal_health': signal_health,
            'history_24h': history_24h,
            'last_update': (
                datetime.fromtimestamp(snapshot_ts_epoch).strftime('%Y-%m-%d %H:%M:%S')
                if snapshot_ts_epoch is not None
                else datetime.fromtimestamp(latest_history_ts_ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S') if latest_history_ts_ms else ''
            ),
        })
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            state='UNKNOWN',
            position_multiplier=0.0,
            votes={},
            alerts=[],
            history_24h=[],
        )


def _load_equity_points(limit: int = 800, runtime_paths: Optional[DashboardRuntimePaths] = None):
    """从 reports/runs/*/equity.jsonl 聚合权益点（真实口径：cash+持仓市值）。"""
    runs_dir = (runtime_paths or _resolve_dashboard_runtime_paths()).runs_dir
    if not runs_dir.exists():
        return []

    def _candidate_upper_epoch(run_dir: Path) -> Optional[float]:
        run_epoch = _run_id_epoch(run_dir.name)
        if run_epoch is not None:
            return run_epoch + 3600.0
        eq_file = run_dir / 'equity.jsonl'
        try:
            return eq_file.stat().st_mtime
        except OSError:
            return None

    dedup: Dict[str, float] = {}
    dedup_epoch: Dict[str, float] = {}
    run_dirs = _sorted_run_dirs_by_artifact_mtime(runs_dir, 'equity.jsonl')
    for run_dir in run_dirs:
        eq_file = run_dir / 'equity.jsonl'
        try:
            with open(eq_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                        ts = row.get('ts')
                        eq = row.get('equity')
                        if ts is None or eq is None:
                            continue
                        ts_str = str(ts)
                        if ts_str in dedup:
                            continue
                        dedup[ts_str] = float(eq)
                        ts_epoch = _coerce_timestamp_epoch(ts_str)
                        if ts_epoch is not None:
                            dedup_epoch[ts_str] = ts_epoch
                    except Exception:
                        continue
        except Exception:
            continue

        if len(dedup) >= limit and len(dedup_epoch) >= limit:
            candidate_upper = _candidate_upper_epoch(run_dir)
            if candidate_upper is not None and candidate_upper <= min(dedup_epoch.values()):
                break

    points = sorted(dedup.items(), key=lambda x: x[0])
    if len(points) > limit:
        points = points[-limit:]
    return points


@app.route('/api/equity_history')
def api_equity_history():
    """权益曲线历史（基于运行时equity快照）"""
    try:
        points = _load_equity_points(runtime_paths=_resolve_dashboard_runtime_paths(load_config()))
        data = [{'timestamp': ts, 'value': round(eq, 4)} for ts, eq in points]
        return jsonify(data)
    except Exception as exc:
        _log_dashboard_exception('equity_history error', exc)
        return jsonify([]), 500


@app.route('/api/equity_curve')
def api_equity_curve():
    """权益曲线 - 新版格式（基于运行时equity快照，默认展示最近48小时并做时间分桶）"""
    try:
        points = _load_equity_points(runtime_paths=_resolve_dashboard_runtime_paths(load_config()))
        if not points:
            return jsonify({'dates': [], 'values': [], 'pnl': [], 'initial': 0, 'current': 0, 'total_return': 0, 'days': 0})

        # 解析时间，保留最近48小时，避免全历史挤在一起
        parsed = []
        for ts, eq in points:
            try:
                dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
                parsed.append((dt, float(eq)))
            except Exception:
                continue
        if not parsed:
            return jsonify({'dates': [], 'values': [], 'pnl': [], 'initial': 0, 'current': 0, 'total_return': 0, 'days': 0})

        end_dt = parsed[-1][0]
        start_dt = end_dt - timedelta(hours=48)
        recent = [(dt, eq) for dt, eq in parsed if dt >= start_dt]
        if len(recent) < 10:
            recent = parsed[-200:]  # 回退：至少给一些点

        # 15分钟分桶，取每桶最后一个点
        bucketed = {}
        for dt, eq in recent:
            b = dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)
            bucketed[b] = eq
        series = sorted(bucketed.items(), key=lambda x: x[0])

        dates, values, pnls = [], [], []
        prev = None
        for dt, eq in series:
            dates.append(dt.isoformat())
            values.append(round(eq, 4))
            pnls.append(round(eq - prev, 4) if prev is not None else 0.0)
            prev = eq

        initial = values[0] if values else 0
        current = values[-1] if values else 0
        total_return = ((current - initial) / initial * 100) if initial else 0
        days = len({d.split('T')[0] for d in dates})

        return jsonify({
            'dates': dates,
            'values': values,
            'pnl': pnls,
            'initial': round(initial, 4),
            'current': round(current, 4),
            'total_return': round(total_return, 2),
            'days': int(days)
        })
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            dates=[],
            values=[],
            pnl=[],
            initial=0,
            current=0,
            total_return=0,
            days=0,
        )


@app.route('/api/dashboard')
@_cache_json_response(_dashboard_view_cache_ttl_seconds)
def api_dashboard():
    """Dashboard 完整数据API"""
    try:
        view = str(request.args.get('view', 'full') or 'full').strip().lower()
        errors: List[str] = []
        runtime_paths = _resolve_dashboard_runtime_paths(load_config())
        account_data = _call_dashboard_api(api_account, default={}, label='account', errors=errors)
        positions_payload = _call_dashboard_api(api_positions, default={'positions': []}, label='positions', errors=errors)
        status_data = _call_dashboard_api(api_status, default={}, label='status', errors=errors)
        market_state_data = _call_dashboard_api(api_market_state, default={}, label='market_state', errors=errors)
        ml_training = _call_dashboard_api(api_ml_training, default={'status': 'unknown'}, label='ml_training', errors=errors)
        positions_data = positions_payload
        if isinstance(positions_payload, dict):
            positions_data = positions_payload.get('positions', positions_payload.get('data', []))
        if not isinstance(positions_data, list):
            positions_data = []
        if not isinstance(status_data, dict):
            status_data = {}
        
        # 转换持仓格式
        positions = []
        for pos in positions_data:
            avg_price = float(pos.get('avg_px', 0) or 0)
            cur_price = float(pos.get('last_price', 0) or 0)
            qty = float(pos.get('qty', 0) or 0)
            value = float(pos.get('value_usdt', 0) or 0)
            pnl = float(pos.get('pnl_value', 0) or 0)
            raw_pnl_pct = pos.get('pnl_pct', None)
            if raw_pnl_pct is None:
                pnl_pct = ((cur_price - avg_price) / avg_price) if avg_price > 0 and cur_price > 0 else 0
            else:
                pnl_pct = float(raw_pnl_pct or 0)
            positions.append({
                'symbol': pos.get('symbol', ''),
                'qty': qty,
                'avgPrice': round(avg_price, 6),
                'currentPrice': round(cur_price, 6),
                'value': round(value, 4),
                'pnl': round(pnl, 4),
                # Keep ratios in decimal form; monitor_v2.html formats them as percentages.
                'pnlPercent': round(pnl_pct, 4)
            })
        
        positions_value = float(account_data.get('positions_value_usdt', 0) or 0)
        if positions_value <= 0:
            positions_value = sum(float(p.get('value', 0) or 0) for p in positions)
        cash_usdt = float(account_data.get('cash_usdt', 0) or 0)
        total_equity = float(account_data.get('total_equity_usdt', 0) or 0)
        if total_equity <= 0:
            total_equity = cash_usdt + positions_value
        initial_capital = float(account_data.get('initial_capital_usdt', 0) or 0)
        if initial_capital <= 0:
            initial_capital = 120.0
        total_pnl = account_data.get('equity_delta_usdt', None)
        if total_pnl is None:
            total_pnl = total_equity - initial_capital if initial_capital > 0 else account_data.get('realized_pnl', 0)
        total_pnl = float(total_pnl or 0)
        total_pnl_pct = float(account_data.get('total_pnl_pct', 0) or 0)
        drawdown_pct = float(account_data.get('drawdown_pct', 0) or 0)
        realized_pnl = float(account_data.get('realized_pnl', 0) or 0)
        trades: List[Dict[str, Any]] = []
        alpha_scores: List[Dict[str, Any]] = []

        dashboard_data = {
            'account': {
                'totalEquity': round(total_equity, 4),
                'cash': round(cash_usdt, 4),
                'positionsValue': round(positions_value, 4),
                'initialCapital': round(initial_capital, 4),
                'totalPnl': round(total_pnl, 4),
                'realizedPnl': round(realized_pnl, 4),
                # Keep ratios in decimal form; monitor_v2.html formats them as percentages.
                'totalPnlPercent': round(total_pnl_pct, 4),
                'todayPnl': 0,
                'todayPnlPercent': 0,
                'sharpeRatio': 0,
                'maxDrawdown': round(drawdown_pct, 4),
                'winRate': 0,
                'totalTrades': account_data.get('total_trades', 0)
            },
            'positions': positions,
            'trades': trades,
            'alphaScores': alpha_scores,
            'marketState': market_state_data,
            'systemStatus': _build_dashboard_system_status(status_data, account_data=account_data, errors=errors),
            'mlTraining': ml_training,
        }

        if view == 'primary':
            dashboard_data.pop('trades', None)
            dashboard_data.pop('alphaScores', None)
            return jsonify(dashboard_data)

        trades_payload = _call_dashboard_api(api_trades, default={'trades': []}, label='trades', errors=errors)
        scores_data = _call_dashboard_api(api_scores, default={'scores': []}, label='scores', errors=errors)
        timers_data = _call_dashboard_api(api_timers, default={'timers': []}, label='timers', errors=errors)
        api_telemetry = _load_api_telemetry_summary(runtime_paths=runtime_paths)
        slippage_insights = _load_slippage_insights(runtime_paths=runtime_paths, cfg=load_config())

        trades_data = trades_payload
        if isinstance(trades_payload, dict):
            trades_data = trades_payload.get('trades', trades_payload.get('data', []))
        if not isinstance(trades_data, list):
            trades_data = []
        if not isinstance(scores_data, dict):
            scores_data = {'scores': []}

        trades = []
        for i, trade in enumerate(trades_data[:20]):
            trades.append({
                'id': str(i),
                'timestamp': trade.get('time', '') if trade.get('time') else '',
                'symbol': trade.get('symbol', '').replace('-USDT', '/USDT'),
                'side': trade.get('side', 'buy'),
                'type': 'REBALANCE',
                'price': float(trade.get('price', 0) or 0),
                'qty': float(trade.get('qty', 0) or 0),
                'value': trade.get('amount', 0),
                'fee': abs(trade.get('fee', 0))
            })

        alpha_scores = []
        for i, score in enumerate(scores_data.get('scores', [])[:10]):
            alpha_scores.append({
                'symbol': score.get('symbol', '').replace('-USDT', '/USDT'),
                'score': score.get('score', 0),
                'f1_mom_5d': 0,
                'f2_mom_20d': 0,
                'f3_vol_adj': 0,
                'f4_volume': 0,
                'f5_rsi': 0,
                'weight': 0.1
            })

        dashboard_data.update({
            'trades': trades,
            'alphaScores': alpha_scores,
            'timers': timers_data,
            'apiTelemetry': api_telemetry,
            'slippageInsights': slippage_insights,
            'systemStatus': _build_dashboard_system_status(status_data, account_data=account_data, errors=errors),
        })

        if view == 'deferred':
            return jsonify({
                'trades': trades,
                'alphaScores': alpha_scores,
                'timers': timers_data,
                'apiTelemetry': api_telemetry,
                'slippageInsights': slippage_insights,
                'systemStatus': dashboard_data['systemStatus'],
            })

        equity_data = _call_dashboard_api(api_equity_history, default=[], label='equity_history', errors=errors)
        cost_calibration = _call_dashboard_api(api_cost_calibration, default={'status': 'unknown'}, label='cost_calibration', errors=errors)
        ic_diagnostics = _call_dashboard_api(api_ic_diagnostics, default={'status': 'no_data'}, label='ic_diagnostics', errors=errors)
        reflection_reports = _call_dashboard_api(api_reflection_reports, default={'reports': []}, label='reflection_reports', errors=errors)
        dashboard_data.update({
            'equityCurve': equity_data if isinstance(equity_data, list) else [],
            'costCalibration': cost_calibration,
            'icDiagnostics': ic_diagnostics,
            'reflectionReports': reflection_reports,
        })

        return jsonify(dashboard_data)
    except Exception as e:
        return _json_internal_error_response(
            e,
            account={
                'totalEquity': 0.0,
                'cash': 0.0,
                'positionsValue': 0.0,
                'initialCapital': 0.0,
                'totalPnl': 0.0,
                'realizedPnl': 0.0,
                'totalPnlPercent': 0.0,
                'todayPnl': 0.0,
                'todayPnlPercent': 0.0,
                'sharpeRatio': 0.0,
                'maxDrawdown': 0.0,
                'winRate': 0.0,
                'totalTrades': 0,
            },
            positions=[],
            trades=[],
            alphaScores=[],
            marketState={},
            systemStatus={
                'isRunning': False,
                'mode': 'dry_run',
                'lastUpdate': '',
                'killSwitch': False,
                'errors': ['internal server error'],
            },
            equityCurve=[],
            timers={'timers': []},
            costCalibration={'status': 'unknown'},
            icDiagnostics={'status': 'no_data'},
            mlTraining={'status': 'unknown'},
            reflectionReports={'reports': []},
            apiTelemetry={
                'status': 'error',
                'lookbackHours': 24,
                'totalRequests': 0,
                'successRate': None,
                'errorCount': 0,
                'rateLimitedCount': 0,
                'p50LatencyMs': None,
                'p95LatencyMs': None,
                'lastRequestAt': '',
                'lastErrorAt': '',
                'latestError': None,
                'note': 'API 遥测读取失败',
            },
            slippageInsights={
                'status': 'error',
                'lookbackDays': 14,
                'sampleCount': 0,
                'actualAvgBps': None,
                'actualP50Bps': None,
                'actualP90Bps': None,
                'actualP95Bps': None,
                'actualMinBps': None,
                'actualMaxBps': None,
                'baselineBps': None,
                'baselineLabel': '',
                'baselineMode': 'default',
                'baselineSourceDay': None,
                'bins': [],
                'lastFillAt': '',
                'note': '滑点数据读取失败',
            },
        )


@app.route('/api/timer')
def api_timer():
    """定时任务信息API"""
    try:
        timer_name = _pick_timer_name()
        runtime = _get_timer_runtime(timer_name)
        return jsonify({
            'timer_name': timer_name,
            'next_run': runtime.get('next_run'),
            'countdown_seconds': int(runtime.get('countdown_seconds') or 0),
            'interval_minutes': int(runtime.get('interval_minutes') or 60),
            'error': runtime.get('error'),
            'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        })
    except Exception as e:
        return _json_internal_error_response(
            e,
            timer_name='v5-prod.user.timer',
            next_run=None,
            countdown_seconds=0,
            interval_minutes=120,
            last_check='',
        )


@app.route('/api/timers')
@_cache_json_response(15.0)
def api_timers():
    """所有定时任务状态API"""
    try:
        timers = []

        for config in PRODUCTION_TIMER_CONFIGS:
            timer_name = config['name']
            runtime = _get_timer_runtime(timer_name)
            timers.append({
                'name': timer_name,
                'desc': config['desc'],
                'icon': config['icon'],
                'enabled': bool(runtime.get('enabled')),
                'active': bool(runtime.get('active')),
                'active_state': runtime.get('active_state'),
                'unit_file_state': runtime.get('unit_file_state'),
                'next_run': runtime.get('next_run'),
                'time_left': runtime.get('time_left'),
                'countdown_seconds': int(runtime.get('countdown_seconds') or 0),
                'interval_minutes': int(runtime.get('interval_minutes') or 60),
                'error': runtime.get('error'),
            })

        return jsonify({
            'timers': timers,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        })
    except Exception as e:
        return _json_internal_error_response(
            e,
            timers=[],
            last_update='',
        )


@app.route('/api/cost_calibration')
def api_cost_calibration():
    """F2成本校准进度API - 从定时任务生成的真实成本数据计算"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)

        # 优先读取定时任务生成的真实成本数据 (cost_stats_real)
        cost_dir = derive_runtime_named_artifact_path(
            runtime_paths.orders_db,
            'cost_stats_real',
            '',
        )
        if not cost_dir.exists():
            cost_dir = derive_runtime_named_artifact_path(
                runtime_paths.orders_db,
                'cost_stats',
                '',
            )  # 兼容旧路径
        events_dir = derive_runtime_cost_events_dir(runtime_paths.orders_db)
        
        calibration_data = []
        total_days = 0
        avg_slippage_bps = 0
        avg_fee_bps = 0
        total_trade_count = 0
        data_source = 'stats'
        
        # 优先从cost_stats_real读取已汇总的数据
        if cost_dir.exists():
            stats_files = sorted(
                (
                    path
                    for path in cost_dir.glob('daily_cost_stats_*.json')
                    if re.fullmatch(r'daily_cost_stats_\d{8}\.json', path.name)
                ),
                key=lambda path: path.name,
            )
            
            for stats_file in stats_files[-30:]:  # 最近30天
                try:
                    with open(stats_file, 'r') as f:
                        stats = json.load(f)
                    
                    day = stats_file.stem.replace('daily_cost_stats_', '')
                    
                    # 从嵌套的buckets计算平均值
                    buckets = stats.get('buckets', {})
                    day_slippage = []
                    day_fee = []
                    day_trade_count = 0
                    
                    for bucket_name, bucket_data in buckets.items():
                        slippage_data = bucket_data.get('slippage_bps', {})
                        fee_data = bucket_data.get('fee_bps', {})
                        
                        if slippage_data.get('count', 0) > 0 and slippage_data.get('mean') is not None:
                            day_slippage.append(slippage_data['mean'])
                        if fee_data.get('count', 0) > 0 and fee_data.get('mean') is not None:
                            day_fee.append(fee_data['mean'])
                        
                        day_trade_count += slippage_data.get('count', 0)
                    
                    if day_trade_count <= 0:
                        continue

                    avg_day_slippage = sum(day_slippage) / len(day_slippage) if day_slippage else 0
                    avg_day_fee = sum(day_fee) / len(day_fee) if day_fee else 0
                    total_day_cost = avg_day_slippage + avg_day_fee
                    
                    # 过滤异常值（成本 > 1000 bps 或 < 0 视为异常）
                    if total_day_cost > 1000 or total_day_cost < 0:
                        print(f"[CostCalibration] Skipping abnormal day {day}: cost={total_day_cost:.2f} bps")
                        continue
                    
                    calibration_data.append({
                        'date': day,
                        'slippage_bps': round(avg_day_slippage, 4),
                        'fee_bps': round(avg_day_fee, 4),
                        'total_cost_bps': round(total_day_cost, 4),
                        'trade_count': day_trade_count
                    })
                    
                    total_days += 1
                    avg_slippage_bps += avg_day_slippage
                    avg_fee_bps += avg_day_fee
                    total_trade_count += day_trade_count
                except Exception as e:
                    print(f"处理 {stats_file} 失败: {e}")
                    continue
        
        dated_event_files = []
        if events_dir.exists():
            dated_event_files = sorted(
                (
                    path
                    for path in events_dir.glob('*.jsonl')
                    if re.fullmatch(r'\d{8}\.jsonl', path.name)
                )
            )

        # 如果没有stats数据，从cost_events原始数据计算
        if total_days == 0 and dated_event_files:
            
            # 按日期分组统计
            daily_stats = {}
            
            for event_file in dated_event_files[-30:]:  # 最近30天
                try:
                    # 从文件名提取日期 (YYYYMMDD.jsonl)
                    day = event_file.stem
                    if not day.isdigit() or len(day) != 8:
                        continue
                    
                    slippage_list = []
                    fee_list = []
                    
                    with open(event_file, 'r') as f:
                        for line in f:
                            try:
                                event = json.loads(line.strip())
                                notional = float(event.get('notional_usdt') or 0.0)

                                slippage = event.get('slippage_bps')
                                if slippage is None and notional > 0:
                                    slip_usdt = event.get('slippage_usdt')
                                    if slip_usdt is not None:
                                        slippage = float(slip_usdt) / notional * 10000.0

                                fee = event.get('fee_bps')
                                if fee is None and notional > 0:
                                    fee_usdt = event.get('fee_usdt')
                                    if fee_usdt is None:
                                        cost_total = event.get('cost_usdt_total')
                                        slip_usdt = event.get('slippage_usdt')
                                        if cost_total is not None and slip_usdt is not None:
                                            fee_usdt = float(cost_total) - float(slip_usdt)
                                    if fee_usdt is None:
                                        raw_fee = event.get('fee')
                                        if raw_fee is not None:
                                            fee_usdt = raw_fee
                                    if fee_usdt is not None:
                                        fee = float(fee_usdt) / notional * 10000.0
                                
                                if slippage is not None and not isinstance(slippage, str):
                                    slippage_list.append(float(slippage))
                                if fee is not None and not isinstance(fee, str):
                                    fee_list.append(float(fee))
                            except:
                                continue
                    
                    if slippage_list or fee_list:
                        avg_s = sum(slippage_list) / len(slippage_list) if slippage_list else 0
                        avg_f = sum(fee_list) / len(fee_list) if fee_list else 0
                        
                        daily_stats[day] = {
                            'date': day,
                            'slippage_bps': round(avg_s, 4),
                            'fee_bps': round(avg_f, 4),
                            'total_cost_bps': round(avg_s + avg_f, 4),
                            'trade_count': len(slippage_list) + len(fee_list)
                        }
                except Exception as e:
                    print(f"处理 {event_file} 失败: {e}")
                    continue
            
            # 转换为列表并计算平均值
            calibration_data = list(daily_stats.values())
            calibration_data.sort(key=lambda x: x['date'])
            
            total_days = len(calibration_data)
            if total_days > 0:
                data_source = 'events'
            for d in calibration_data:
                avg_slippage_bps += d['slippage_bps']
                avg_fee_bps += d['fee_bps']
                total_trade_count += d['trade_count']
        
        # 计算平均值
        if total_days > 0:
            avg_slippage_bps /= total_days
            avg_fee_bps /= total_days

        latest_update = ''
        if calibration_data:
            latest_day = calibration_data[-1].get('date')
            if isinstance(latest_day, str) and re.fullmatch(r'\d{8}', latest_day):
                latest_update = datetime.strptime(latest_day, '%Y%m%d').strftime('%Y-%m-%d 00:00:00')
        
        # 获取事件文件数
        event_count = len(dated_event_files)
        
        return jsonify({
            'status': 'calibrated' if total_days >= 7 else 'calibrating',
            'total_days': total_days,
            'avg_slippage_bps': round(avg_slippage_bps, 4),
            'avg_fee_bps': round(avg_fee_bps, 4),
            'avg_total_cost_bps': round(avg_slippage_bps + avg_fee_bps, 4),
            'event_files': event_count,
            'total_trades': total_trade_count,
            'daily_stats': calibration_data[-7:],  # 最近7天
            'progress_percent': min(100, int(total_days / 7 * 100)),
            'data_source': data_source,
            'last_update': latest_update,
        })
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            status='error',
            daily_stats=[],
            last_update='',
        )


@app.route('/api/ic_diagnostics')
def api_ic_diagnostics():
    """IC诊断进度API"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)

        # 查找IC诊断文件。优先按文件名中的日期选最新，解析失败时再退回 mtime。
        ic_files = list(runtime_paths.reports_dir.glob(_runtime_ic_diagnostic_pattern(runtime_paths.orders_db)))

        if not ic_files:
            return jsonify({
                'status': 'no_data',
                'message': '暂无IC诊断数据'
            })

        ic_files.sort(key=_ic_diagnostic_sort_epoch, reverse=True)

        # 优先使用“有可用因子IC”的最新文件；否则回退到最近文件
        latest_ic = ic_files[0]
        ic_data = None
        fallback_reason = None
        for f in ic_files:
            try:
                with open(f, 'r', encoding='utf-8') as fh:
                    d = json.load(fh)
                
                # 检查新格式 (fresh文件)
                if 'factors' in d and isinstance(d['factors'], dict):
                    for factor_info in d['factors'].values():
                        if factor_info.get('count', 0) > 0:
                            latest_ic = f
                            ic_data = d
                            break
                    if ic_data:
                        break
                    continue
                
                # 检查旧格式
                ic_by_factor = (d.get('overall_tradable') or {}).get('ic', {})
                has_valid_data = False
                if isinstance(ic_by_factor, dict) and len(ic_by_factor) > 0:
                    for factor_data in ic_by_factor.values():
                        if isinstance(factor_data, dict) and factor_data.get('count', 0) > 0:
                            has_valid_data = True
                            break
                if has_valid_data:
                    latest_ic = f
                    ic_data = d
                    break
            except Exception:
                continue

        if ic_data is None:
            with open(latest_ic, 'r', encoding='utf-8') as f:
                ic_data = json.load(f)
            fallback_reason = 'latest_file_has_no_valid_factor_ic'
        
        # 检查是否是新的简化格式（fresh文件）
        if 'factors' in ic_data and isinstance(ic_data['factors'], dict):
            # 新格式：直接有factors字段
            factors_data = ic_data['factors']
            factors = []
            all_ic_values = []
            for factor_name, factor_info in factors_data.items():
                ic_val = factor_info.get('ic', 0)
                count = factor_info.get('count', 0)
                all_ic_values.append(ic_val)
                factors.append({
                    'name': factor_name,
                    'ic': round(ic_val, 4),
                    'ic_median': round(ic_val, 4),  # 简化为相同值
                    'ic_std': 0.1,
                    'ir': round(ic_val / 0.1, 4),
                    'sample_count': count
                })
            
            overall_ic_mean = sum(all_ic_values) / len(all_ic_values) if all_ic_values else 0
            overall_std = 0.1
            overall_ir = overall_ic_mean / overall_std
            
            # 处理by_regime
            regimes = []
            regime_data = ic_data.get('by_regime', {})
            for regime_name, regime_factors in regime_data.items():
                if isinstance(regime_factors, dict):
                    regime_ic_values = [v for v in regime_factors.values() if isinstance(v, (int, float))]
                    avg_ic = sum(regime_ic_values) / len(regime_ic_values) if regime_ic_values else 0
                    regimes.append({
                        'name': regime_name[:20],  # 截断长名称
                        'ic': round(avg_ic, 4),
                        'sample_count': 0
                    })
            
            return jsonify({
                'status': 'ready',
                'overall_ic': round(overall_ic_mean, 4),
                'overall_ir': round(overall_ir, 4),
                'sample_count': ic_data.get('total_samples', 0),
                'timestamps_count': 0,
                'lookback_days': 14,
                'factors': factors,
                'regimes': regimes,
                'source_file': latest_ic.name,
                'fallback_reason': 'fresh_format',
                'last_update': datetime.fromtimestamp(_ic_diagnostic_sort_epoch(latest_ic)).strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # 解析IC数据 - 旧版结构在overall_tradable.ic下
        overall_tradable = ic_data.get('overall_tradable', {})
        overall_raw = ic_data.get('overall_raw', {})
        
        # 获取IC数据
        ic_by_factor = overall_tradable.get('ic', {})
        
        # 计算整体IC（所有因子的平均）
        all_ic_values = []
        for factor_data in ic_by_factor.values():
            mean_ic = factor_data.get('mean')
            if mean_ic is not None:
                all_ic_values.append(mean_ic)
        
        overall_ic_mean = sum(all_ic_values) / len(all_ic_values) if all_ic_values else 0
        
        # 计算各因子IC
        def _num(v, default=0.0):
            try:
                if v is None:
                    return float(default)
                return float(v)
            except Exception:
                return float(default)

        factors = []
        for factor_name, factor_data in ic_by_factor.items():
            mean_ic = _num(factor_data.get('mean', 0.0), 0.0)
            p50_ic = _num(factor_data.get('p50', 0.0), 0.0)
            count = int(_num(factor_data.get('count', 0), 0))

            # 简化计算IR (IC / std)，如果std不可用则用近似值
            p75 = _num(factor_data.get('p75', 0.0), 0.0)
            p25 = _num(factor_data.get('p25', 0.0), 0.0)
            std_approx = ((p75 - p25) / 1.35) if (p75 != 0 or p25 != 0) else 0.1
            std_approx = std_approx if std_approx > 1e-9 else 0.1
            ir = mean_ic / std_approx

            factors.append({
                'name': factor_name,
                'ic': round(mean_ic, 4),
                'ic_median': round(p50_ic, 4),
                'ic_std': round(std_approx, 4),
                'ir': round(ir, 4),
                'sample_count': count
            })
        
        # 按Regime分组 - 从by_regime数据中提取
        regimes = []
        regime_data = ic_data.get('by_regime', {})
        for regime_name, regime_info in regime_data.items():
            regime_ic_data = regime_info.get('ic', {})
            if regime_ic_data:
                # 计算该regime下所有因子的平均IC
                regime_ic_values = []
                for factor_ic in regime_ic_data.values():
                    if isinstance(factor_ic, dict) and 'mean' in factor_ic:
                        regime_ic_values.append(factor_ic['mean'])
                    elif isinstance(factor_ic, (int, float)):
                        regime_ic_values.append(factor_ic)
                
                avg_regime_ic = sum(regime_ic_values) / len(regime_ic_values) if regime_ic_values else 0
                
                regimes.append({
                    'name': regime_name,
                    'ic': round(avg_regime_ic, 4),
                    'sample_count': regime_info.get('n', 0)
                })
        
        # 计算整体IR
        overall_std = 0.1  # 默认值
        if factors:
            overall_std = sum(f['ic_std'] for f in factors) / len(factors)
        overall_ir = overall_ic_mean / overall_std if overall_std > 0 else 0
        
        return jsonify({
            'status': 'ready',
            'overall_ic': round(overall_ic_mean, 4),
            'overall_ir': round(overall_ir, 4),
            'sample_count': overall_tradable.get('used_points', 0),
            'timestamps_count': overall_tradable.get('used_timestamps', 0),
            'lookback_days': ic_data.get('lookback_days', 30),
            'factors': factors,
            'regimes': regimes,
            'source_file': latest_ic.name,
            'fallback_reason': fallback_reason,
            'last_update': datetime.fromtimestamp(_ic_diagnostic_sort_epoch(latest_ic)).strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            status='error',
            factors=[],
            regimes=[],
            last_update='',
        )


def _api_ml_training_v2():
    config = load_config()
    runtime_paths = _resolve_dashboard_runtime_paths(config)

    def _resolve_workspace_path(raw_path: str | None, default: str) -> Path:
        p = Path(str(raw_path or default))
        if not p.is_absolute():
            p = WORKSPACE / p
        return p

    def _normalize_model_base_path(path: Path) -> Path:
        p = Path(path)
        if p.name.endswith('_config.json'):
            return p.with_name(p.name[:-len('_config.json')])
        if p.suffix in {'.txt', '.pkl'}:
            return p.with_suffix('')
        return p

    def _model_artifact_candidates(base_path: Path) -> List[Path]:
        return [
            Path(f'{base_path}.txt'),
            Path(f'{base_path}.pkl'),
            Path(f'{base_path}_config.json'),
        ]

    def _model_artifact_exists(base_path: Path) -> bool:
        return any(p.exists() for p in _model_artifact_candidates(base_path))

    def _latest_model_file(base_path: Path) -> Optional[Path]:
        existing = [p for p in _model_artifact_candidates(base_path) if p.exists()]
        if not existing:
            return None
        model_files = [p for p in existing if not p.name.endswith('_config.json')]
        preferred = model_files or existing
        return max(preferred, key=lambda p: p.stat().st_mtime)

    def _latest_model_file_mtime(base_path: Path) -> Optional[float]:
        latest = _latest_model_file(base_path)
        if latest is None:
            return None
        return latest.stat().st_mtime

    configured_enabled = False
    min_samples = 200
    model_base_path = WORKSPACE / 'models' / 'ml_factor_model'
    pointer_path = WORKSPACE / 'models' / 'ml_factor_model_active.txt'
    promotion_path = derive_runtime_named_artifact_path(
        runtime_paths.orders_db,
        'model_promotion_decision',
        '.json',
    )
    runtime_path = derive_runtime_named_artifact_path(
        runtime_paths.orders_db,
        'ml_runtime_status',
        '.json',
    )
    try:
        cfg = load_app_config(str(_resolve_config_path()), env_path=None)
        ml_cfg = getattr(getattr(cfg, 'alpha', None), 'ml_factor', None)
        if ml_cfg is not None:
            configured_enabled = _coerce_bool(getattr(ml_cfg, 'enabled', False))
            model_base_path = _normalize_model_base_path(
                _resolve_workspace_path(getattr(ml_cfg, 'model_path', 'models/ml_factor_model'), 'models/ml_factor_model')
            )
            pointer_path = _resolve_workspace_path(
                getattr(ml_cfg, 'active_model_pointer_path', 'models/ml_factor_model_active.txt'),
                'models/ml_factor_model_active.txt',
            )
            promotion_path = _resolve_dashboard_runtime_artifact_path(
                runtime_paths.orders_db,
                getattr(ml_cfg, 'promotion_decision_path', 'reports/model_promotion_decision.json'),
                'reports/model_promotion_decision.json',
            )
            runtime_path = _resolve_dashboard_runtime_artifact_path(
                runtime_paths.orders_db,
                getattr(ml_cfg, 'runtime_status_path', 'reports/ml_runtime_status.json'),
                'reports/ml_runtime_status.json',
            )
    except Exception:
        pass

    total_samples = 0
    labeled_samples = 0
    db_path = derive_runtime_named_artifact_path(
        runtime_paths.orders_db,
        'ml_training_data',
        '.db',
    )
    if db_path.exists():
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM feature_snapshots')
        total_samples = int(cur.fetchone()[0] or 0)
        cur.execute('SELECT COUNT(*) FROM feature_snapshots WHERE label_filled = 1')
        labeled_samples = int(cur.fetchone()[0] or 0)
        conn.close()

    latest_history = {}
    history_path = derive_runtime_named_artifact_path(
        runtime_paths.orders_db,
        'ml_training_history',
        '.json',
    )
    if history_path.exists():
        try:
            hist_obj = json.loads(history_path.read_text(encoding='utf-8'))
            if isinstance(hist_obj, list) and hist_obj:
                entries = [entry for entry in hist_obj if isinstance(entry, dict)]
                if entries:
                    latest_history = max(entries, key=_history_entry_sort_epoch)
        except Exception:
            pass

    decision = {}
    if promotion_path.exists():
        try:
            decision = json.loads(promotion_path.read_text(encoding='utf-8'))
        except Exception:
            decision = {}

    runtime = {}
    if runtime_path.exists():
        try:
            runtime = json.loads(runtime_path.read_text(encoding='utf-8'))
        except Exception:
            runtime = {}

    latest_model = _latest_model_file(model_base_path)
    latest_model_mtime = _latest_model_file_mtime(model_base_path)
    model_time = datetime.fromtimestamp(latest_model_mtime) if latest_model_mtime is not None else None

    def _display_update_value(value: Any) -> Optional[str]:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value)).strftime('%Y-%m-%d %H:%M:%S')
        text = str(value or '').strip()
        if not text:
            return None
        if text.endswith('Z'):
            text = text[:-1]
        if '+' in text:
            text = text.split('+', 1)[0]
        return text.replace('T', ' ')[:19]

    structured_update_candidates = [
        (
            _coerce_timestamp_epoch(latest_history.get('timestamp')) if isinstance(latest_history, dict) else None,
            _display_update_value(latest_history.get('timestamp')) if isinstance(latest_history, dict) else None,
        ),
        (
            _coerce_timestamp_epoch(decision.get('ts')) if isinstance(decision, dict) else None,
            _display_update_value(decision.get('ts')) if isinstance(decision, dict) else None,
        ),
        (
            _coerce_timestamp_epoch(runtime.get('ts')) if isinstance(runtime, dict) else None,
            _display_update_value(runtime.get('ts')) if isinstance(runtime, dict) else None,
        ),
    ]
    valid_structured_updates = [(ts, display) for ts, display in structured_update_candidates if ts is not None and display]
    latest_update_epoch = max((ts for ts, _ in valid_structured_updates), default=None)
    latest_update_display = ''
    if valid_structured_updates and latest_update_epoch is not None:
        for ts, display in valid_structured_updates:
            if ts == latest_update_epoch:
                latest_update_display = display
                break
    elif latest_model_mtime is not None:
        latest_update_epoch = float(latest_model_mtime)
        latest_update_display = _display_update_value(latest_model_mtime) or ''

    active_model_base = model_base_path
    if pointer_path.exists():
        try:
            pointer_value = pointer_path.read_text(encoding='utf-8').strip()
            if pointer_value:
                active_model_base = _normalize_model_base_path(
                    _resolve_workspace_path(pointer_value, pointer_value)
                )
        except Exception:
            pass

    effective_samples = labeled_samples if labeled_samples > 0 else total_samples
    stages = {
        'sampling': effective_samples > 0,
        'trained': _model_artifact_exists(model_base_path),
        'promoted': _coerce_bool(decision.get('passed')) and pointer_path.exists() and _model_artifact_exists(active_model_base),
        'liveActive': _coerce_bool(runtime.get('used_in_latest_snapshot')),
    }
    if stages['liveActive']:
        phase = 'live_active'
    elif stages['promoted']:
        phase = 'promoted'
    elif stages['trained']:
        phase = 'trained'
    elif stages['sampling']:
        phase = 'collecting'
    else:
        phase = 'no_data'
    stage_display = ' / '.join([
        f"采样中 {'是' if stages['sampling'] else '否'}",
        f"已训练 {'是' if stages['trained'] else '否'}",
        f"已通过门控 {'是' if stages['promoted'] else '否'}",
        f"已被实盘使用 {'是' if stages['liveActive'] else '否'}",
    ])

    return jsonify({
        'status': phase,
        'phase': phase,
        'display_status': stage_display,
        'configured_enabled': configured_enabled,
        'stages': stages,
        'total_samples': total_samples,
        'labeled_samples': labeled_samples,
        'samples_needed': min_samples,
        'progress_percent': min(100, int((effective_samples / min_samples) * 100)) if effective_samples else 0,
        'latest_model': latest_model.name if latest_model else None,
        'model_date': model_time.strftime('%Y-%m-%d %H:%M') if model_time else None,
        'last_ic': round(float(latest_history.get('valid_ic')), 4) if latest_history.get('valid_ic') is not None else None,
        'last_training_ts': latest_history.get('timestamp'),
        'last_training_gate_passed': _coerce_bool((latest_history.get('gate') or {}).get('passed')),
        'last_promotion_ts': decision.get('ts'),
        'promotion_fail_reasons': [str(x) for x in (decision.get('fail_reasons') or [])],
        'last_runtime_ts': runtime.get('ts'),
        'runtime_reason': runtime.get('reason'),
        'runtime_prediction_count': int(runtime.get('prediction_count') or 0),
        'model_path': str(model_base_path),
        'active_model_path': str(active_model_base) if pointer_path.exists() else None,
        'last_update': latest_update_display
    })


@app.route('/api/ml_training')
@_cache_json_response(20.0)
def api_ml_training():
    try:
        return _api_ml_training_v2()
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            status='error',
            phase='error',
            configured_enabled=False,
            stages={
                'sampling': False,
                'trained': False,
                'promoted': False,
                'liveActive': False,
            },
            runtime_prediction_count=0,
            last_update='',
        )


@app.route('/api/reflection_reports')
def api_reflection_reports():
    """反思Agent报告列表API（兼容V1/V2结构）"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)
        reflection_dir = derive_runtime_named_artifact_path(runtime_paths.orders_db, 'reflection', '')

        if not reflection_dir.exists():
            return jsonify({'reports': [], 'message': '暂无反思报告'})

        reports = []
        report_files = sorted(
            (
                path
                for path in reflection_dir.glob('reflection_*.json')
                if re.fullmatch(r"reflection_\d{8}_\d{4,6}\.json", path.name)
            ),
            key=_reflection_report_sort_epoch,
            reverse=True,
        )

        for report_file in report_files[:10]:
            try:
                with open(report_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # V2: summary/alerts; V1: overall_metrics/insights
                summary = data.get('summary', {})
                metrics = data.get('overall_metrics', {})
                alerts = data.get('alerts', [])
                insights = data.get('insights', [])

                total_pnl = summary.get('total_realized_pnl', metrics.get('total_pnl', 0))
                trade_count = summary.get('total_trades', metrics.get('total_trades', 0))
                symbols = summary.get('total_symbols', metrics.get('unique_symbols', 0))

                high_priority = sum(1 for a in alerts if str(a.get('level', '')).lower() in ('high', 'critical'))
                medium_priority = sum(1 for a in alerts if str(a.get('level', '')).lower() in ('medium', 'warning'))
                if not alerts and insights:
                    high_priority = sum(1 for i in insights if str(i.get('severity', '')).lower() == 'high')
                    medium_priority = sum(1 for i in insights if str(i.get('severity', '')).lower() == 'medium')

                reports.append({
                    'filename': report_file.name,
                    'date': report_file.stem.replace('reflection_', ''),
                    'total_pnl': round(float(total_pnl or 0), 2),
                    'trade_count': int(trade_count or 0),
                    'symbols': int(symbols or 0),
                    'insights_count': len(alerts) if alerts else len(insights),
                    'high_priority': high_priority,
                    'medium_priority': medium_priority
                })
            except Exception:
                continue

        latest_update = ''
        if report_files:
            latest_update = datetime.fromtimestamp(_reflection_report_sort_epoch(report_files[0])).strftime('%Y-%m-%d %H:%M:%S')

        return jsonify({
            'reports': reports,
            'total_reports': len(report_files),
            'last_update': latest_update,
        })
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            reports=[],
            total_reports=0,
            last_update='',
        )


@app.route('/api/decision_chain')
def api_decision_chain():
    """决策归因面板API - 展示策略信号到执行的完整链路"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)
        # 获取最近5轮决策记录
        if not runtime_paths.runs_dir.exists():
            return jsonify({'rounds': [], 'message': '暂无决策记录'})

        decision_chain_scan_limit = _load_recent_scan_limit('V5_DASHBOARD_DECISION_CHAIN_SCAN_LIMIT')
        try:
            audit_entries = _iter_decision_audits(
                runtime_paths.reports_dir,
                scan_limit=decision_chain_scan_limit,
                include_parse_errors=True,
            )
        except TypeError:
            audit_entries = _iter_decision_audits(runtime_paths.reports_dir, scan_limit=decision_chain_scan_limit)
        if not audit_entries:
            return jsonify({'rounds': [], 'message': '暂无决策记录'})
        latest_update_epoch = float(audit_entries[0].get('sort_epoch', 0.0) or 0.0) if audit_entries else 0.0

        rounds = []
        for entry in audit_entries[:5]:
            try:
                run_dir = entry['run_dir']
                data = entry.get('audit')
                if not isinstance(data, dict):
                    continue
                if data.get('_parse_error'):
                    rounds.append({
                        'run_id': run_dir.name,
                        'time': run_dir.name,
                        'strategy_signals': [],
                        'risk_state': {'regime': 'Error'},
                        'execution_result': {
                            'selected': 0,
                            'targets_pre_risk': 0,
                            'orders_rebalance': 0,
                            'orders_exit': 0,
                            'negative_expectancy_cooldown': 0,
                            'negative_expectancy_open_block': 0,
                            'negative_expectancy_fast_fail_open_block': 0,
                        },
                        'block_reasons': {'parse_error': 1},
                        'blocked_top': [],
                        'error': 'internal parse error',
                    })
                    continue

                # 提取决策链信息
                run_id = run_dir.name
                ts = data.get('now_ts') or data.get('window_start_ts')
                epoch = _coerce_timestamp_epoch(ts)
                if epoch is not None:
                    # 决策时间展示统一按逻辑时间戳转成 CST，避免文件 mtime 被补写后误导旧/新数据判断。
                    run_time = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    run_time = run_id

                # 1. 策略层信号
                selected_scores = data.get('top_scores', [])
                strategy_signals = []
                for item in selected_scores[:5]:
                    strategy_signals.append({
                        'symbol': item.get('symbol'),
                        'score': round(float(item.get('score', 0)), 4),
                        'rank': item.get('rank', 0)
                    })

                # 2. 风控层状态
                risk_state = {
                    'regime': data.get('regime', 'Unknown'),
                    'regime_multiplier': data.get('regime_multiplier', 1.0),
                    'dd_multiplier': None,
                    'deadband': data.get('rebalance_deadband_pct')
                }
                # 从notes中提取DD multiplier
                for note in data.get('notes', []):
                    if 'DD multiplier' in note:
                        try:
                            import re
                            m = re.search(r'DD multiplier:\s*([\d.]+)', note)
                            if m:
                                risk_state['dd_multiplier'] = float(m.group(1))
                        except:
                            pass
                    if 'drawdown' in note.lower():
                        try:
                            import re
                            m = re.search(r'drawdown:\s*([\d.]+)%', note, re.IGNORECASE)
                            if m:
                                risk_state['drawdown_pct'] = float(m.group(1))
                        except:
                            pass

                # 3. 执行层结果
                counts = data.get('counts', {})
                execution_result = {
                    'selected': int(counts.get('selected', 0) or 0),
                    'targets_pre_risk': int(counts.get('targets_pre_risk', 0) or 0),
                    'orders_rebalance': int(counts.get('orders_rebalance', 0) or 0),
                    'orders_exit': int(counts.get('orders_exit', 0) or 0),
                    'negative_expectancy_score_penalty': int(
                        counts.get('negative_expectancy_score_penalty', 0) or 0
                    ),
                    'negative_expectancy_cooldown': int(counts.get('negative_expectancy_cooldown', 0) or 0),
                    'negative_expectancy_open_block': int(counts.get('negative_expectancy_open_block', 0) or 0),
                    'negative_expectancy_fast_fail_open_block': int(
                        counts.get('negative_expectancy_fast_fail_open_block', 0) or 0
                    ),
                }

                # 4. 阻塞原因统计
                router_decisions = data.get('router_decisions', [])
                block_reasons = {}
                for rd in router_decisions:
                    reason = rd.get('reason', 'unknown')
                    block_reasons[reason] = block_reasons.get(reason, 0) + 1

                # 5. 被拦截的Top信号
                blocked_signals = []
                for rd in (router_decisions or []):
                    if rd.get('reason') == 'deadband':
                        try:
                            drift_v = float(rd.get('drift') or 0.0)
                        except Exception:
                            drift_v = 0.0
                        try:
                            deadband_v = float(rd.get('deadband') or 0.0)
                        except Exception:
                            deadband_v = 0.0
                        blocked_signals.append({
                            'symbol': rd.get('symbol'),
                            'drift': round(drift_v, 4),
                            'deadband': round(deadband_v, 4)
                        })
                # 按漂移排序
                blocked_signals.sort(key=lambda x: abs(float(x.get('drift', 0) or 0)), reverse=True)

                rounds.append({
                    'run_id': run_id,
                    'time': run_time,
                    'strategy_signals': strategy_signals,
                    'risk_state': risk_state,
                    'execution_result': execution_result,
                    'block_reasons': block_reasons,
                    'blocked_top': blocked_signals[:3]
                })
            except Exception as e:
                # 保留可观测性，避免静默失败导致前端长期显示空白
                rounds.append({
                    'run_id': run_dir.name,
                    'time': run_dir.name,
                    'strategy_signals': [],
                    'risk_state': {'regime': 'Error'},
                    'execution_result': {
                        'selected': 0,
                        'targets_pre_risk': 0,
                        'orders_rebalance': 0,
                        'orders_exit': 0,
                        'negative_expectancy_cooldown': 0,
                        'negative_expectancy_open_block': 0,
                        'negative_expectancy_fast_fail_open_block': 0,
                    },
                    'block_reasons': {'parse_error': 1},
                    'blocked_top': [],
                    'error': 'internal parse error'
                })
                continue

        return jsonify({
            'rounds': rounds,
            'last_update': datetime.fromtimestamp(latest_update_epoch).strftime('%Y-%m-%d %H:%M:%S') if latest_update_epoch > 0 else ''
        })
    except Exception as exc:
        return _json_internal_error_response(exc, rounds=[], last_update='')


@app.route('/api/shadow_test')
def api_shadow_test():
    """参数A/B影子测试API - 对比当前参数与候选参数的历史表现"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)
        import sys
        sys.path.insert(0, str(WORKSPACE))
        rebalance_cfg = config.get('rebalance', {}) if isinstance(config, dict) else {}
        try:
            current_deadband = float(rebalance_cfg.get('deadband_sideways', 0.04) or 0.04)
        except Exception:
            current_deadband = 0.04
        proposed_deadband = max(0.0, round(current_deadband - 0.01, 4))
        
        # 获取最近7天的运行数据用于对比
        if not runtime_paths.runs_dir.exists():
            return jsonify({'status': 'no_data', 'message': '暂无运行数据'})

        audit_entries = _iter_decision_audits(runtime_paths.reports_dir, scan_limit=50)
        recent_entries = audit_entries[:50]
        latest_update_epoch = float(audit_entries[0].get('sort_epoch', 0.0) or 0.0) if audit_entries else 0.0
        
        current_stats = {
            'rounds': 0,
            'total_selected': 0,
            'total_rebalance': 0,
            'total_exit': 0,
            'negative_expectancy_score_penalty_count': 0,
            'negative_expectancy_cooldown_count': 0,
            'negative_expectancy_open_block_count': 0,
            'negative_expectancy_fast_fail_open_block_count': 0,
            'deadband_blocks': 0,
            'avg_deadband_skip': 0
        }
        
        # 模拟：deadband_sideways 轻微下调后的预估影响
        # 实际实现需要重新跑历史数据，这里用启发式估算
        simulated_stats = {
            'rounds': 0,
            'total_selected': 0,
            'total_rebalance': 0,
            'total_exit': 0,
            'deadband_blocks': 0,
            'avg_deadband_skip': 0,
            'estimated_improvement': 0
        }
        
        deadband_skips = []
        
        for entry in recent_entries:
            try:
                data = entry.get('audit')
                if not isinstance(data, dict):
                    continue
                
                counts = data.get('counts', {})
                current_stats['rounds'] += 1
                current_stats['total_selected'] += counts.get('selected', 0)
                current_stats['total_rebalance'] += counts.get('orders_rebalance', 0)
                current_stats['total_exit'] += counts.get('orders_exit', 0)
                current_stats['negative_expectancy_score_penalty_count'] += int(
                    counts.get('negative_expectancy_score_penalty', 0) or 0
                )
                current_stats['negative_expectancy_cooldown_count'] += int(
                    counts.get('negative_expectancy_cooldown', 0) or 0
                )
                current_stats['negative_expectancy_open_block_count'] += int(
                    counts.get('negative_expectancy_open_block', 0) or 0
                )
                current_stats['negative_expectancy_fast_fail_open_block_count'] += int(
                    counts.get('negative_expectancy_fast_fail_open_block', 0) or 0
                )
                
                # 统计deadband拦截
                router_decisions = data.get('router_decisions', [])
                deadband_count = sum(1 for rd in router_decisions if rd.get('reason') == 'deadband')
                current_stats['deadband_blocks'] += deadband_count
                
                # 记录被拦漂移值用于模拟
                for rd in router_decisions:
                    if rd.get('reason') == 'deadband':
                        drift = abs(float(rd.get('drift', 0)))
                        deadband_skips.append(drift)
                        
                        # 模拟：如果 deadband 收紧到 proposed_deadband，有多少能成交
                        if drift > proposed_deadband:
                            simulated_stats['estimated_improvement'] += 1
                            
            except Exception:
                continue
        
        # 计算当前统计
        if current_stats['rounds'] > 0:
            current_stats['avg_selected'] = round(current_stats['total_selected'] / current_stats['rounds'], 2)
            current_stats['avg_rebalance'] = round(current_stats['total_rebalance'] / current_stats['rounds'], 2)
            current_stats['conversion_rate'] = round(
                (current_stats['total_rebalance'] / current_stats['total_selected'] * 100) if current_stats['total_selected'] > 0 else 0, 
                1
            )
        
        if deadband_skips:
            current_stats['avg_deadband_skip'] = round(sum(deadband_skips) / len(deadband_skips), 4)
        
        # 只读取 A/B gate 评估；请求内不做同步刷新，避免阻塞页面并污染测试/生产时序
        ab_gate = None
        ab_gate_status = 'missing'
        ab_gate_age_sec = None
        try:
            gate_path = derive_runtime_named_artifact_path(runtime_paths.orders_db, 'ab_gate_status', '.json')
            if gate_path.exists():
                with open(gate_path, 'r', encoding='utf-8') as f:
                    ab_gate = json.load(f)
                gate_epoch = _coerce_timestamp_epoch((ab_gate or {}).get('ts')) if isinstance(ab_gate, dict) else None
                if gate_epoch is None:
                    gate_epoch = gate_path.stat().st_mtime
                ab_gate_age_sec = max(0, int(datetime.now().timestamp() - gate_epoch))
                ab_gate_status = 'stale' if ab_gate_age_sec > 1800 else 'fresh'
        except Exception:
            ab_gate = None

        # 生成A/B对比报告
        ab_report = {
            'status': 'ready',
            'window_days': 7,
            'window_rounds': current_stats['rounds'],
            'current_params': {
                'deadband_sideways': current_deadband,
                'description': '当前参数'
            },
            'proposed_params': {
                'deadband_sideways': proposed_deadband,
                'description': '建议参数（更激进）'
            },
            'comparison': {
                'current': {
                    'avg_selected_per_round': current_stats.get('avg_selected', 0),
                    'avg_rebalance_per_round': current_stats.get('avg_rebalance', 0),
                    'conversion_rate': current_stats.get('conversion_rate', 0),
                    'total_deadband_blocks': current_stats['deadband_blocks'],
                    'avg_drift_when_blocked': current_stats['avg_deadband_skip'],
                    'negative_expectancy_score_penalty_count': current_stats['negative_expectancy_score_penalty_count'],
                    'negative_expectancy_cooldown_count': current_stats['negative_expectancy_cooldown_count'],
                    'negative_expectancy_open_block_count': current_stats['negative_expectancy_open_block_count'],
                    'negative_expectancy_fast_fail_open_block_count': current_stats['negative_expectancy_fast_fail_open_block_count'],
                },
                'estimated_with_proposed': {
                    'avg_rebalance_per_round': round(
                        current_stats.get('avg_rebalance', 0) + 
                        (simulated_stats['estimated_improvement'] / max(current_stats['rounds'], 1)), 
                        2
                    ),
                    'estimated_conversion_rate': round(
                        ((current_stats['total_rebalance'] + simulated_stats['estimated_improvement']) / 
                         max(current_stats['total_selected'], 1)) * 100,
                        1
                    ),
                    'additional_trades': simulated_stats['estimated_improvement'],
                    'risk_note': '成交增加，但可能包含更多弱信号'
                }
            },
            'recommendation': {
                'action': 'cautious_try' if simulated_stats['estimated_improvement'] > 5 else 'keep_current',
                'reason': f"过去{current_stats['rounds']}轮中，约{simulated_stats['estimated_improvement']}笔额外交易可成交" if simulated_stats['estimated_improvement'] > 0 else "当前参数下成交率已合理",
                'suggested_next_step': (
                    f'将 deadband_sideways 从 {current_deadband:.2f} 调至 {proposed_deadband:.2f}，观察24小时'
                    if simulated_stats['estimated_improvement'] > 5
                    else '保持当前参数'
                )
            },
            'matrix': [
                {'name': 'A(当前)', 'params': {'deadband_sideways': current_deadband, 'min_trade_notional_base': 2.0, 'pos_mult_sideways': 0.8}},
                {'name': 'B1', 'params': {'deadband_sideways': proposed_deadband}},
                {'name': 'B2', 'params': {'min_trade_notional_base': 2.5}},
                {'name': 'B3', 'params': {'pos_mult_sideways': 0.7}},
            ],
            'ab_gate': ab_gate,
            'ab_gate_status': ab_gate_status,
            'ab_gate_age_sec': ab_gate_age_sec,
            'last_update': datetime.fromtimestamp(latest_update_epoch).strftime('%Y-%m-%d %H:%M:%S') if latest_update_epoch > 0 else ''
        }
        
        return jsonify(ab_report)
        
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            status='error',
            window_rounds=0,
            ab_gate_status='error',
            ab_gate_error='internal error',
            matrix=[],
        )


@app.route('/api/smart_alerts')
def api_smart_alerts():
    """智能告警API - 返回当前活跃的告警"""
    try:
        from src.monitoring.smart_alert import SmartAlertEngine
        
        engine = SmartAlertEngine()
        alerts = engine.run_all_checks()
        
        return jsonify({
            'alerts': alerts,
            'count': len(alerts),
            'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'alert' if alerts else 'normal'
        })
    except Exception as exc:
        return _json_internal_error_response(exc, alerts=[], status='error')


@app.route('/api/auto_risk_guard')
@_cache_json_response(8.0)
def api_auto_risk_guard():
    """自动风险档位API - 显示当前风险档位和配置"""
    try:
        from src.risk.auto_risk_guard import AutoRiskGuard
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)

        eval_data = _load_json_payload(runtime_paths.auto_risk_eval_path)
        guard_state = _load_json_payload(runtime_paths.auto_risk_guard_path)
        eval_level = extract_risk_level(eval_data)
        guard_level = extract_risk_level(guard_state)
        eval_epoch = _risk_state_epoch(eval_data, primary_keys=('ts',))
        guard_epoch = _risk_state_epoch(guard_state, primary_keys=('last_update',))

        eval_path = runtime_paths.auto_risk_eval_path or (runtime_paths.reports_dir / 'auto_risk_eval.json')
        use_eval_snapshot = False
        if eval_level and eval_path.exists():
            use_eval_snapshot = not guard_level or guard_epoch is None or (eval_epoch is not None and eval_epoch >= guard_epoch)

        if use_eval_snapshot:
            data = eval_data if isinstance(eval_data, dict) else {}
            level = eval_level or 'NEUTRAL'
            risk_level = AutoRiskGuard.LEVELS.get(level, AutoRiskGuard.LEVELS['NEUTRAL'])
            config = data.get('config')
            if not isinstance(config, dict):
                config = asdict(risk_level)
            eval_history = data.get('history')

            return jsonify({
                'current_level': level,
                'config': config,
                'history': _sorted_risk_history_tail(eval_history, 5),
                'metrics': data.get('metrics', {}),
                'reason': data.get('reason', ''),
                'last_update': str(data.get('ts') or _latest_risk_history_ts(eval_history) or '').strip()
            })

        guard = AutoRiskGuard(state_path=str(runtime_paths.auto_risk_guard_path))
        guard_config = guard.get_current_config()
        guard_history = guard.history[-5:]
        guard_metrics = guard.metrics
        guard_reason = ''
        guard_last_update = ''
        latest_guard_history_ts = ''

        if isinstance(guard_state, dict):
            stored_config = guard_state.get('current_config')
            if isinstance(stored_config, dict):
                guard_config = stored_config

            stored_history = guard_state.get('history')
            if isinstance(stored_history, list):
                guard_history = _sorted_risk_history_tail(stored_history, 5)
                latest_guard_history_ts = _latest_risk_history_ts(stored_history)
                latest_history = max(
                    (
                        item
                        for item in stored_history
                        if isinstance(item, dict) and str(item.get('to') or '').strip().upper() == guard.current_level
                    ),
                    key=lambda item: float(_coerce_timestamp_epoch(item.get('ts')) or float('-inf')),
                    default=None,
                )
                if isinstance(latest_history, dict):
                    guard_reason = str(latest_history.get('reason') or '').strip()
                    if not guard_last_update:
                        guard_last_update = str(latest_history.get('ts') or '').strip()

            stored_metrics = guard_state.get('metrics')
            if isinstance(stored_metrics, dict):
                guard_metrics = stored_metrics

            guard_last_update = str(guard_state.get('last_update') or guard_last_update or '').strip()
            if not guard_last_update:
                guard_last_update = latest_guard_history_ts

        return jsonify({
            'current_level': guard.current_level,
            'config': guard_config,
            'history': guard_history,
            'metrics': guard_metrics,
            'reason': guard_reason,
            'last_update': guard_last_update or ''
        })
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            current_level='UNKNOWN',
            config={},
            history=[],
            metrics={},
            reason='',
            last_update='',
        )


@app.route('/api/decision_audit')
@_cache_json_response(15.0)
def api_decision_audit():
    """获取最新决策审计数据（策略信号带回退，避免前端空白）"""
    try:
        config = load_config()
        runtime_paths = _resolve_dashboard_runtime_paths(config)
        if not runtime_paths.runs_dir.exists():
            return jsonify({'error': 'No runs directory'}), 404

        decision_audit_scan_limit = _load_recent_scan_limit('V5_DASHBOARD_DECISION_AUDIT_SCAN_LIMIT')
        audit_entries = _iter_decision_audits(runtime_paths.reports_dir, scan_limit=decision_audit_scan_limit)
        if not audit_entries:
            return jsonify({'error': 'No audit files found'}), 404

        latest_run_dir = audit_entries[0]['run_dir']
        latest_audit_file = latest_run_dir / 'decision_audit.json'

        with open(latest_audit_file, 'r') as f:
            audit_data = json.load(f)

        # 默认时间戳：使用审计自身的排序时间，避免文件 mtime 被补写后误导前端。
        ts = float(audit_entries[0].get('sort_epoch', 0.0) or _decision_audit_sort_epoch(latest_run_dir, audit_data))

        def _load_strategy_signals(path: Path):
            """兼容多种 strategy_signals.json 结构。"""
            with open(path, 'r') as sf:
                strategy_data = json.load(sf)

            strategies = strategy_data.get('strategies')
            if isinstance(strategies, list) and strategies:
                return strategies

            # 兼容旧字段
            legacy = strategy_data.get('strategy_signals')
            if isinstance(legacy, list) and legacy:
                return legacy

            # 仅有 fused 时，合成一个摘要，避免前端显示空白
            fused = strategy_data.get('fused')
            if isinstance(fused, dict) and fused:
                rows = list(fused.values())
                buy_cnt = sum(1 for r in rows if str(r.get('direction', '')).lower() == 'buy')
                sell_cnt = sum(1 for r in rows if str(r.get('direction', '')).lower() == 'sell')
                synth_signals = []
                for sym, r in fused.items():
                    synth_signals.append({
                        'symbol': sym,
                        'side': r.get('direction', 'hold'),
                        'score': float(r.get('score', 0.0) or 0.0),
                        'confidence': float(r.get('confidence', r.get('score', 0.0)) or 0.0),
                        'metadata': {'strategy': r.get('strategy', 'FUSED')}
                    })
                return [{
                    'strategy': 'FUSED',
                    'type': 'fused',
                    'allocation': 1.0,
                    'total_signals': len(rows),
                    'buy_signals': buy_cnt,
                    'sell_signals': sell_cnt,
                    'signals': synth_signals
                }]

            return []

        strategy_signals = []
        strategy_source_run = None
        strategy_signal_source = 'missing'
        def _load_run_strategy_payload(run_dir: Path, audit_obj: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Optional[str], Optional[float]]:
            embedded = audit_obj.get('strategy_signals')
            if isinstance(embedded, list) and embedded:
                return embedded, 'decision_audit', None

            run_strategy_file = run_dir / 'strategy_signals.json'
            if run_strategy_file.exists():
                try:
                    loaded = _load_strategy_signals(run_strategy_file)
                    if loaded:
                        return loaded, 'strategy_file', run_strategy_file.stat().st_mtime
                except Exception:
                    return [], None, None
            return [], None, None

        embedded_strategy_signals = audit_data.get('strategy_signals')
        if isinstance(embedded_strategy_signals, list) and embedded_strategy_signals:
            strategy_signals = embedded_strategy_signals
            strategy_source_run = latest_run_dir.name
            strategy_signal_source = 'decision_audit'

        # 优先：同一run目录
        strategy_file = latest_run_dir / 'strategy_signals.json'
        if not strategy_signals and strategy_file.exists():
            try:
                strategy_signals = _load_strategy_signals(strategy_file)
                if strategy_signals:
                    strategy_source_run = latest_run_dir.name
                    strategy_signal_source = 'strategy_file'
            except Exception:
                strategy_signals = []

        # 回退：按时间倒序遍历，找到第一个可成功解析的 strategy_signals.json
        if not strategy_signals:
            iterable = audit_entries[1:] if audit_entries is not None else run_dirs[1:]
            for stale_item in iterable:
                if audit_entries is not None:
                    stale_run_dir = stale_item['run_dir']
                    stale_audit = stale_item.get('audit')
                    if not isinstance(stale_audit, dict):
                        continue
                    fallback_default_ts = float(stale_item.get('sort_epoch', 0.0) or 0.0)
                else:
                    stale_run_dir = stale_item
                    try:
                        with open(stale_run_dir / 'decision_audit.json', 'r') as f:
                            stale_audit = json.load(f)
                    except Exception:
                        continue
                    fallback_default_ts = (stale_run_dir / 'decision_audit.json').stat().st_mtime
                fallback_signals, fallback_source, fallback_ts = _load_run_strategy_payload(stale_run_dir, stale_audit)
                if not fallback_signals:
                    continue

                strategy_signals = fallback_signals
                strategy_source_run = stale_run_dir.name
                strategy_signal_source = f'previous_run_{fallback_source}'
                break

        # Build actionable signal view: sell only for held symbols; buy only for non-held symbols.
        held_symbols = set()
        try:
            con = sqlite3.connect(str(runtime_paths.positions_db))
            cur = con.cursor()
            cur.execute("SELECT symbol FROM positions WHERE qty > 0")
            held_symbols = {str(r[0]) for r in cur.fetchall()}
            con.close()
        except Exception:
            held_symbols = set()

        fused_rows = []
        for block in (strategy_signals or []):
            for s in (block.get('signals') or []):
                sym = str(s.get('symbol') or '')
                side = str(s.get('side') or s.get('direction') or '').lower()
                try:
                    score = float(s.get('score', 0.0) or 0.0)
                except Exception:
                    score = 0.0
                if sym and side in {'buy', 'sell'}:
                    fused_rows.append({'symbol': sym, 'side': side, 'score': score})

        actionable_buy = sorted(
            [r for r in fused_rows if r['side'] == 'buy' and r['symbol'] not in held_symbols],
            key=lambda x: x['score'],
            reverse=True,
        )
        actionable_sell = sorted(
            [r for r in fused_rows if r['side'] == 'sell' and r['symbol'] in held_symbols],
            key=lambda x: x['score'],
            reverse=True,
        )

        run_id = str(audit_data.get('run_id') or latest_run_dir.name)

        # Router decision summary (for why blocked / why passed)
        router_decisions = audit_data.get('router_decisions', []) or []
        router_reason_counts = {}
        for rd in router_decisions:
            reason = str(rd.get('reason') or 'unknown')
            router_reason_counts[reason] = int(router_reason_counts.get(reason, 0)) + 1

        # Execution outcomes (from orders.sqlite in this run_id)
        run_orders = []
        def _normalize_db_ts(raw_value: Any) -> int:
            try:
                value = int(raw_value or 0)
            except Exception:
                return 0
            if 0 < value < 10_000_000_000:
                return value * 1000
            return value

        def _effective_order_ts(created_ts: Any, updated_ts: Any) -> int:
            return max(_normalize_db_ts(created_ts), _normalize_db_ts(updated_ts))

        execution_summary = {
            'total': 0,
            'filled': 0,
            'rejected': 0,
            'open_or_partial': 0,
            'cancelled': 0,
            'other': 0,
            'reject_reasons': {},
            'negative_expectancy_penalty_count': int((audit_data.get('counts', {}) or {}).get('negative_expectancy_score_penalty', 0) or 0),
            'negative_expectancy_cooldown_count': int((audit_data.get('counts', {}) or {}).get('negative_expectancy_cooldown', 0) or 0),
            'negative_expectancy_open_block_count': int((audit_data.get('counts', {}) or {}).get('negative_expectancy_open_block', 0) or 0),
            'negative_expectancy_fast_fail_open_block_count': int(
                (audit_data.get('counts', {}) or {}).get('negative_expectancy_fast_fail_open_block', 0) or 0
            ),
            'negative_expectancy_probation_release_count': 0,
        }
        try:
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        """
                        SELECT
                          COUNT(*) AS total,
                          SUM(CASE WHEN state='FILLED' THEN 1 ELSE 0 END) AS filled,
                          SUM(CASE WHEN state='REJECTED' THEN 1 ELSE 0 END) AS rejected,
                          SUM(CASE WHEN state IN ('OPEN','PARTIAL','SENT','ACK','UNKNOWN') THEN 1 ELSE 0 END) AS open_like,
                          SUM(CASE WHEN state IN ('CANCELED','CANCELLED') THEN 1 ELSE 0 END) AS cancelled
                        FROM orders
                        WHERE run_id = ?
                        """,
                        (run_id,),
                    )
                    summary_row = cur.fetchone() or (0, 0, 0, 0, 0)
                    execution_summary.update({
                        'total': int(summary_row[0] or 0),
                        'filled': int(summary_row[1] or 0),
                        'rejected': int(summary_row[2] or 0),
                        'open_or_partial': int(summary_row[3] or 0),
                        'cancelled': int(summary_row[4] or 0),
                    })
                    execution_summary['other'] = max(
                        0,
                        execution_summary['total']
                        - execution_summary['filled']
                        - execution_summary['rejected']
                        - execution_summary['open_or_partial']
                        - execution_summary['cancelled'],
                    )
                except Exception:
                    pass

                try:
                    cur.execute(
                        """
                        SELECT COALESCE(NULLIF(last_error_code, ''), NULLIF(last_error_msg, ''), 'unknown') AS reason, COUNT(*)
                        FROM orders
                        WHERE run_id = ? AND state = 'REJECTED'
                        GROUP BY reason
                        """,
                        (run_id,),
                    )
                    execution_summary['reject_reasons'] = {
                        str(reason or 'unknown'): int(count or 0)
                        for reason, count in cur.fetchall()
                    }
                except Exception:
                    execution_summary['reject_reasons'] = {}

                try:
                    cur.execute(
                        """
                        SELECT created_ts, updated_ts, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id
                        FROM orders
                        WHERE run_id = ?
                        ORDER BY CASE WHEN COALESCE(updated_ts, 0) > 0 THEN updated_ts ELSE created_ts END DESC
                        LIMIT 100
                        """,
                        (run_id,),
                    )
                    preview_rows = cur.fetchall()
                    preview_has_updated_ts = True
                except Exception:
                    cur.execute(
                        """
                        SELECT created_ts, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id
                        FROM orders
                        WHERE run_id = ?
                        ORDER BY created_ts DESC
                        LIMIT 100
                        """,
                        (run_id,),
                    )
                    preview_rows = cur.fetchall()
                    preview_has_updated_ts = False

                for r in preview_rows:
                    if preview_has_updated_ts:
                        created_ts, updated_ts, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id = r
                    else:
                        created_ts, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id = r
                        updated_ts = created_ts
                    run_orders.append({
                        'created_ts': _effective_order_ts(created_ts, updated_ts),
                        'inst_id': str(inst_id or ''),
                        'side': str(side or ''),
                        'intent': str(intent or ''),
                        'state': str(state or 'UNKNOWN').upper(),
                        'notional_usdt': _coerce_float(notional_usdt, 0.0),
                        'last_error_code': str(last_error_code or ''),
                        'last_error_msg': str(last_error_msg or ''),
                        'ord_id': str(ord_id or ''),
                    })

                conn.close()
        except Exception:
            pass

        # Recent fill context + latest run with actual order attempts
        recent_fill_summary = {
            'count_60m': 0,
            'count_24h': 0,
            'latest_fill': None,
        }
        latest_ordered_run_summary = None
        try:
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                fill_ts_by_ord_id: Dict[str, int] = {}
                fill_ts_by_cl_ord_id: Dict[str, int] = {}
                fill_events: List[Dict[str, Any]] = []
                try:
                    if runtime_paths.fills_db.exists():
                        fills_conn = sqlite3.connect(str(runtime_paths.fills_db))
                        fills_cur = fills_conn.cursor()
                        try:
                            fills_cur.execute(
                                """
                                SELECT ts_ms, created_ts_ms, ord_id, cl_ord_id
                                FROM fills
                                """
                            )
                            fill_rows_raw = fills_cur.fetchall()
                        except Exception:
                            fill_rows_raw = []
                        finally:
                            fills_conn.close()

                        for fill_row in fill_rows_raw:
                            ts_ms, created_ts_ms, ord_id, cl_ord_id = fill_row
                            fill_ts = max(_normalize_db_ts(ts_ms), _normalize_db_ts(created_ts_ms))
                            if fill_ts <= 0:
                                continue
                            ord_key = str(ord_id or '')
                            cl_key = str(cl_ord_id or '')
                            fill_events.append({
                                'ts': fill_ts,
                                'ord_id': ord_key,
                                'cl_ord_id': cl_key,
                            })
                            if ord_key:
                                fill_ts_by_ord_id[ord_key] = max(fill_ts_by_ord_id.get(ord_key, 0), fill_ts)
                            if cl_key:
                                fill_ts_by_cl_ord_id[cl_key] = max(fill_ts_by_cl_ord_id.get(cl_key, 0), fill_ts)
                except Exception:
                    fill_ts_by_ord_id = {}
                    fill_ts_by_cl_ord_id = {}
                    fill_events = []

                # 1) Recent fills window
                try:
                    cur.execute(
                        """
                        SELECT created_ts, updated_ts, run_id, inst_id, side, intent, notional_usdt, ord_id, cl_ord_id
                        FROM orders
                        WHERE state = 'FILLED'
                        ORDER BY CASE WHEN COALESCE(updated_ts, 0) > 0 THEN updated_ts ELSE created_ts END DESC
                        LIMIT 200
                        """
                    )
                    fill_rows = cur.fetchall()
                    fill_rows_have_updated_ts = True
                except Exception:
                    try:
                        cur.execute(
                            """
                            SELECT created_ts, updated_ts, run_id, inst_id, side, intent, notional_usdt, ord_id
                            FROM orders
                            WHERE state = 'FILLED'
                            ORDER BY CASE WHEN COALESCE(updated_ts, 0) > 0 THEN updated_ts ELSE created_ts END DESC
                            LIMIT 200
                            """
                        )
                        fill_rows = cur.fetchall()
                        fill_rows_have_updated_ts = 'no_cl_ord_id'
                    except Exception:
                        cur.execute(
                            """
                            SELECT created_ts, run_id, inst_id, side, intent, notional_usdt, ord_id
                            FROM orders
                            WHERE state = 'FILLED'
                            ORDER BY created_ts DESC
                            LIMIT 200
                            """
                        )
                        fill_rows = cur.fetchall()
                        fill_rows_have_updated_ts = False

                now_ms = int(datetime.now().timestamp() * 1000)
                latest_fill_ts = 0
                order_event_ts_by_ord_id: Dict[str, int] = {}
                order_event_ts_by_cl_ord_id: Dict[str, int] = {}
                order_meta_by_ord_id: Dict[str, Dict[str, Any]] = {}
                order_meta_by_cl_ord_id: Dict[str, Dict[str, Any]] = {}
                for r in fill_rows:
                    if fill_rows_have_updated_ts is True:
                        created_ts, updated_ts, order_run_id, inst_id, side, intent, notional_usdt, ord_id, cl_ord_id = r
                    elif fill_rows_have_updated_ts == 'no_cl_ord_id':
                        created_ts, updated_ts, order_run_id, inst_id, side, intent, notional_usdt, ord_id = r
                        cl_ord_id = ''
                    else:
                        created_ts, order_run_id, inst_id, side, intent, notional_usdt, ord_id = r
                        updated_ts = created_ts
                        cl_ord_id = ''

                    order_event_ts = _effective_order_ts(created_ts, updated_ts)
                    ord_key = str(ord_id or '')
                    cl_key = str(cl_ord_id or '')
                    if ord_key:
                        order_event_ts_by_ord_id[ord_key] = order_event_ts
                    if cl_key:
                        order_event_ts_by_cl_ord_id[cl_key] = order_event_ts

                    order_meta = {
                        'run_id': str(order_run_id or ''),
                        'inst_id': str(inst_id or ''),
                        'side': str(side or ''),
                        'intent': str(intent or ''),
                        'notional_usdt': _coerce_float(notional_usdt, 0.0),
                        'ord_id': ord_key,
                    }
                    if ord_key:
                        order_meta_by_ord_id[ord_key] = order_meta
                    if cl_key:
                        order_meta_by_cl_ord_id[cl_key] = order_meta

                    order_age_ms = max(0, now_ms - order_event_ts)
                    if order_age_ms <= 60 * 60 * 1000:
                        recent_fill_summary['count_60m'] += 1
                    if order_age_ms <= 24 * 60 * 60 * 1000:
                        recent_fill_summary['count_24h'] += 1

                    fill_event_ts = max(
                        fill_ts_by_ord_id.get(ord_key, 0),
                        fill_ts_by_cl_ord_id.get(cl_key, 0),
                    )
                    latest_event_ts = max(order_event_ts, fill_event_ts)
                    if latest_event_ts >= latest_fill_ts:
                        latest_fill_ts = latest_event_ts
                        recent_fill_summary['latest_fill'] = {
                            'created_ts': latest_event_ts,
                            'run_id': str(order_run_id or ''),
                            'inst_id': str(inst_id or ''),
                            'side': str(side or ''),
                            'intent': str(intent or ''),
                            'notional_usdt': _coerce_float(notional_usdt, 0.0),
                            'ord_id': ord_key,
                        }

                for fill_event in fill_events:
                    fill_ts = int(fill_event.get('ts') or 0)
                    ord_key = str(fill_event.get('ord_id') or '')
                    cl_key = str(fill_event.get('cl_ord_id') or '')
                    matched_order_ts = max(
                        order_event_ts_by_ord_id.get(ord_key, 0),
                        order_event_ts_by_cl_ord_id.get(cl_key, 0),
                    )
                    if matched_order_ts and matched_order_ts == fill_ts:
                        continue

                    fill_age_ms = max(0, now_ms - fill_ts)
                    if fill_age_ms <= 60 * 60 * 1000:
                        recent_fill_summary['count_60m'] += 1
                    if fill_age_ms <= 24 * 60 * 60 * 1000:
                        recent_fill_summary['count_24h'] += 1

                    if fill_ts >= latest_fill_ts:
                        latest_fill_ts = fill_ts
                        order_meta = order_meta_by_ord_id.get(ord_key) or order_meta_by_cl_ord_id.get(cl_key) or {}
                        recent_fill_summary['latest_fill'] = {
                            'created_ts': fill_ts,
                            'run_id': str(order_meta.get('run_id') or ''),
                            'inst_id': str(order_meta.get('inst_id') or ''),
                            'side': str(order_meta.get('side') or ''),
                            'intent': str(order_meta.get('intent') or ''),
                            'notional_usdt': _coerce_float(order_meta.get('notional_usdt'), 0.0),
                            'ord_id': str(order_meta.get('ord_id') or ord_key),
                        }

                # 2) Latest run that has at least one order row (attempt)
                try:
                    cur.execute(
                        """
                        SELECT
                          run_id,
                          MAX(CASE WHEN COALESCE(updated_ts, 0) > 0 THEN updated_ts ELSE created_ts END) AS last_ts,
                          COUNT(*) AS total,
                          SUM(CASE WHEN state='FILLED' THEN 1 ELSE 0 END) AS filled,
                          SUM(CASE WHEN state='REJECTED' THEN 1 ELSE 0 END) AS rejected,
                          SUM(CASE WHEN state IN ('OPEN','PARTIAL','SENT','ACK','UNKNOWN') THEN 1 ELSE 0 END) AS open_like,
                          SUM(CASE WHEN state IN ('CANCELED','CANCELLED') THEN 1 ELSE 0 END) AS cancelled
                        FROM orders
                        GROUP BY run_id
                        ORDER BY last_ts DESC
                        LIMIT 20
                        """
                    )
                except Exception:
                    cur.execute(
                        """
                        SELECT
                          run_id,
                          MAX(created_ts) AS last_ts,
                          COUNT(*) AS total,
                          SUM(CASE WHEN state='FILLED' THEN 1 ELSE 0 END) AS filled,
                          SUM(CASE WHEN state='REJECTED' THEN 1 ELSE 0 END) AS rejected,
                          SUM(CASE WHEN state IN ('OPEN','PARTIAL','SENT','ACK','UNKNOWN') THEN 1 ELSE 0 END) AS open_like,
                          SUM(CASE WHEN state IN ('CANCELED','CANCELLED') THEN 1 ELSE 0 END) AS cancelled
                        FROM orders
                        GROUP BY run_id
                        ORDER BY last_ts DESC
                        LIMIT 20
                        """
                    )
                run_rows = cur.fetchall()
                if run_rows:
                    rr = run_rows[0]
                    latest_ordered_run_summary = {
                        'run_id': str(rr[0] or ''),
                        'last_ts': _normalize_db_ts(rr[1]),
                        'total': int(rr[2] or 0),
                        'filled': int(rr[3] or 0),
                        'rejected': int(rr[4] or 0),
                        'open_or_partial': int(rr[5] or 0),
                        'cancelled': int(rr[6] or 0),
                    }

                conn.close()
        except Exception:
            pass

        # Try to expose actual fused ranking used for selection (if available)
        fused_buy_rank = []
        strategy_source_file = None
        try:
            if strategy_source_run:
                p = runs_dir / str(strategy_source_run) / 'strategy_signals.json'
                if p.exists():
                    strategy_source_file = p
            if strategy_source_file is None and strategy_file.exists():
                strategy_source_file = strategy_file

            if strategy_source_file and strategy_source_file.exists():
                sobj = json.loads(strategy_source_file.read_text(encoding='utf-8'))
                fused = sobj.get('fused', {}) if isinstance(sobj, dict) else {}
                if isinstance(fused, dict):
                    buys = []
                    for sym, sig in fused.items():
                        if str((sig or {}).get('direction', '')).lower() != 'buy':
                            continue
                        try:
                            sc = float((sig or {}).get('score', 0.0) or 0.0)
                        except Exception:
                            sc = 0.0
                        buys.append({'symbol': str(sym), 'score': sc})
                    buys.sort(key=lambda x: x['score'], reverse=True)
                    for i, b in enumerate(buys, start=1):
                        fused_buy_rank.append({'rank': i, 'symbol': b['symbol'], 'score': b['score']})
        except Exception:
            fused_buy_rank = []

        # Route-level selected/blocked breakdown
        selected_orders = [
            {
                'symbol': str(rd.get('symbol') or ''),
                'side': str(rd.get('side') or ''),
                'reason': str(rd.get('reason') or ''),
                'notional': _coerce_float(rd.get('notional'), 0.0),
            }
            for rd in router_decisions
            if str(rd.get('action') or '').lower() == 'create'
        ]
        blocked_routes = [
            {
                'symbol': str(rd.get('symbol') or ''),
                'reason': str(rd.get('reason') or 'unknown'),
                'action': str(rd.get('action') or ''),
            }
            for rd in router_decisions
            if str(rd.get('action') or '').lower() != 'create'
        ]

        # Final target ranking from this run's own decision audit (most reliable for this run).
        target_rank = []
        try:
            tpr = audit_data.get('targets_pre_risk', {}) or {}
            if isinstance(tpr, dict):
                for sym, w in tpr.items():
                    try:
                        target_rank.append({'symbol': str(sym), 'target_weight': float(w)})
                    except Exception:
                        continue
                target_rank.sort(key=lambda x: float(x.get('target_weight', 0.0)), reverse=True)
        except Exception:
            target_rank = []

        fused_source_is_fallback = bool(strategy_source_run and strategy_source_run != latest_run_dir.name)
        preferred_ml_symbols: List[str] = []
        for item in audit_data.get('top_scores', []) or []:
            if isinstance(item, dict) and item.get('symbol'):
                preferred_ml_symbols.append(str(item.get('symbol')))
        for item in target_rank[:10]:
            if isinstance(item, dict) and item.get('symbol'):
                preferred_ml_symbols.append(str(item.get('symbol')))
        stored_ml_overview = audit_data.get('ml_signal_overview', {}) if isinstance(audit_data, dict) else {}
        ml_signal_overview = _build_ml_signal_overview(
            runtime_paths.reports_dir,
            orders_db=runtime_paths.orders_db,
            preferred_symbols=preferred_ml_symbols,
        )
        if isinstance(stored_ml_overview, dict) and stored_ml_overview:
            merged_ml_overview = dict(stored_ml_overview)
            merged_ml_overview.update({k: v for k, v in ml_signal_overview.items() if v not in (None, {}, [], "")})
            ml_signal_overview = merged_ml_overview

        return jsonify({
            'run_id': run_id,
            'strategy_run_id': strategy_source_run,
            'strategy_signal_source': strategy_signal_source,
            'strategy_signals_count': len(strategy_signals or []),
            'timestamp': ts,
            'regime': audit_data.get('regime'),
            'regime_details': audit_data.get('regime_details', {}),
            'counts': audit_data.get('counts', {}),
            'rejects': audit_data.get('rejects', {}),
            'top_scores': audit_data.get('top_scores', []),
            'selection_source': 'fused' if fused_buy_rank else 'alpha',
            'target_rank': target_rank[:20],
            'fused_buy_rank': fused_buy_rank[:20],
            'fused_rank_source_run': strategy_source_run,
            'fused_source_is_fallback': fused_source_is_fallback,
            'router_decisions': router_decisions,
            'router_reason_counts': router_reason_counts,
            'selected_orders': selected_orders,
            'blocked_routes': blocked_routes,
            'strategy_signals': strategy_signals,
            'actionable_signals': {
                'held_symbols': sorted(list(held_symbols)),
                'buy_candidates': actionable_buy,
                'sell_candidates': actionable_sell,
            },
            'execution_summary': execution_summary,
            'execution_scope': {
                'type': 'run_id_only',
                'run_id': run_id,
                'note': 'execution_summary/run_orders 仅统计本次run；recent_fill_summary统计跨run最近成交。',
            },
            'ml_signal_overview': ml_signal_overview,
            'recent_fill_summary': recent_fill_summary,
            'latest_ordered_run_summary': latest_ordered_run_summary,
            'run_orders': run_orders[:30],
            'notes': audit_data.get('notes', [])[:12]
        })
    except Exception as exc:
        return _json_internal_error_response(exc)


@app.route('/api/shadow_ml_overlay')
@_cache_json_response(30.0)
def api_shadow_ml_overlay():
    try:
        shadow_workspace = _resolve_shadow_workspace()
        if shadow_workspace is None:
            return jsonify({
                'available': False,
                'error': '未找到旁路调优版 XGBoost 工作区',
            })

        payload = _load_shadow_ml_overlay_summary(shadow_workspace)
        if not payload.get('available'):
            payload['error'] = '旁路调优版 XGBoost 归因数据未就绪'
        return jsonify(payload)
    except Exception as exc:
        return _json_internal_error_response(exc, available=False)


@app.route('/api/health')
def api_health():
    """系统健康检查API"""
    try:
        runtime_paths = _resolve_dashboard_runtime_paths(load_config())
        checks = []
        overall_status = 'healthy'
        
        # 1. 检查定时任务
        try:
            timer_name = _pick_timer_name()
            timer_state = _get_timer_state(timer_name)

            if timer_state.get('error'):
                checks.append({'name': '定时任务', 'status': 'warning', 'detail': 'timer warning'})
                if overall_status == 'healthy':
                    overall_status = 'warning'
            elif timer_state.get('active'):
                checks.append({'name': '定时任务', 'status': 'healthy', 'detail': f'{timer_name}运行中'})
            else:
                checks.append({'name': '定时任务', 'status': 'critical', 'detail': f'{timer_name}未运行'})
                overall_status = 'critical'
        except Exception:
            checks.append({'name': '定时任务', 'status': 'warning', 'detail': 'timer warning'})
            overall_status = 'warning'
        
        # 2. 检查数据库
        try:
            orders_db = runtime_paths.orders_db
            if orders_db.exists():
                conn = sqlite3.connect(str(orders_db))
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM orders")
                count = cursor.fetchone()[0]
                conn.close()
                checks.append({'name': '数据库', 'status': 'healthy', 'detail': f'{count}条订单记录'})
            else:
                checks.append({'name': '数据库', 'status': 'critical', 'detail': 'orders.sqlite不存在'})
                overall_status = 'critical'
        except Exception as exc:
            checks.append({
                'name': '数据库',
                'status': 'warning',
                'detail': _sanitize_public_error_text(exc, default='database status unavailable'),
            })
        
        # 3. 检查OKX API
        try:
            import time

            key, sec, pp = _load_workspace_exchange_creds()
            if not _dashboard_live_account_enabled():
                checks.append({'name': 'OKX API', 'status': 'warning', 'detail': 'live probe disabled'})
                if overall_status == 'healthy':
                    overall_status = 'warning'
            else:
                if key and sec and pp:
                    now = time.time()
                    ttl = max(0, int(str(os.getenv('V5_DASHBOARD_HEALTH_OKX_CACHE_TTL_SECONDS', '30') or '30')))
                    cache_key = (key, sec, pp)
                    cached_payload = None
                    cached_latency = 0.0
                    cache_hit = False
                    if ttl > 0:
                        with _OKX_HEALTH_CHECK_CACHE_LOCK:
                            cached = _OKX_HEALTH_CHECK_CACHE.get(cache_key)
                        if cached and cached[0] > now:
                            cached_payload = cached[1]
                            cached_latency = float(cached[2] or 0.0)
                            cache_hit = True

                    if cached_payload is None:
                        start = time.time()
                        cached_payload = _load_okx_account_balance(key, sec, pp)
                        cached_latency = (time.time() - start) * 1000
                        if ttl > 0:
                            with _OKX_HEALTH_CHECK_CACHE_LOCK:
                                _OKX_HEALTH_CHECK_CACHE[cache_key] = (now + ttl, cached_payload, cached_latency)

                    if isinstance(cached_payload, dict) and cached_payload.get('code') == '0':
                        detail = f'{cached_latency:.0f}ms'
                        if cache_hit:
                            detail += ' cached'
                        checks.append({'name': 'OKX API', 'status': 'healthy', 'detail': detail})
                    else:
                        checks.append({'name': 'OKX API', 'status': 'critical', 'detail': 'API响应异常'})
                        overall_status = 'critical'
                else:
                    checks.append({'name': 'OKX API', 'status': 'warning', 'detail': 'API密钥未配置'})
                    if overall_status == 'healthy':
                        overall_status = 'warning'
        except Exception:
            checks.append({'name': 'OKX API', 'status': 'warning', 'detail': 'okx api unavailable'})
            if overall_status == 'healthy':
                overall_status = 'warning'
        
        # 4. 检查磁盘空间
        try:
            import shutil
            total, used, free = shutil.disk_usage(WORKSPACE)
            free_gb = free / (1024**3)
            used_percent = used / total * 100
            
            if free_gb < 1:
                checks.append({'name': '磁盘空间', 'status': 'critical', 'detail': f'仅剩{free_gb:.1f}GB'})
                overall_status = 'critical'
            elif used_percent > 90:
                checks.append({'name': '磁盘空间', 'status': 'warning', 'detail': f'已用{used_percent:.1f}%'})
                if overall_status == 'healthy':
                    overall_status = 'warning'
            else:
                checks.append({'name': '磁盘空间', 'status': 'healthy', 'detail': f'{free_gb:.1f}GB可用'})
        except Exception:
            checks.append({'name': '磁盘空间', 'status': 'warning', 'detail': 'disk status unavailable'})
        
        warning_count = sum(1 for item in checks if item.get('status') == 'warning')
        critical_count = sum(1 for item in checks if item.get('status') == 'critical')
        if overall_status == 'healthy':
            if critical_count > 0:
                overall_status = 'critical'
            elif warning_count > 0:
                overall_status = 'warning'
        checked_at = datetime.now()
        return jsonify({
            'status': overall_status,
            'checks': checks,
            'timestamp': checked_at.isoformat(),
            'last_update': checked_at.strftime('%Y-%m-%d %H:%M:%S'),
            'warning_count': warning_count,
            'critical_count': critical_count,
        })
        
    except Exception as exc:
        return _json_internal_error_response(
            exc,
            status='error',
            checks=[],
            warning_count=0,
            critical_count=0,
        )


if __name__ == '__main__':
    print("="*60)
    print("V5 Web Dashboard 启动中...")
    print("="*60)
    print(f"访问地址: http://0.0.0.0:5000")
    print("="*60)
    host = "0.0.0.0"
    port = int(os.getenv("V5_WEB_PORT", "5000") or 5000)
    threads = int(os.getenv("V5_WEB_THREADS", "8") or 8)
    try:
        from waitress import serve

        serve(app, host=host, port=port, threads=threads)
    except Exception as exc:
        print(f"Waitress unavailable, fallback to Flask dev server: {exc}")
        app.run(host=host, port=port, debug=False)
