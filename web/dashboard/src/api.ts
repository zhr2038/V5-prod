import type {
  DashboardData,
  ApiTelemetrySeriesData,
  RiskGuardData,
  MarketStateData,
  DecisionAuditData,
  HealthData,
  MLTrainingData,
  PositionKlinePayload,
  Trade,
  LiveFollowupBundlesData,
  LiveFollowupBundleGenerateResult,
  QuantLabStatusData,
  QuantLabPermissionData,
  QuantLabCostEstimateData,
  QuantLabGateDecisionData,
} from './types';

const API_BASE = '';

type ApiTradePayload = Partial<Trade> & {
  time?: string;
  amount?: number;
  trade_id?: string;
  fill_id?: string;
  order_id?: string;
  ord_id?: string;
  client_order_id?: string;
  cl_ord_id?: string;
  orderId?: string;
  tradeId?: string;
  fill_count?: number;
};

type ApiPositionPayload = Partial<import('./types').Position> & {
  avg_px?: number;
  last_price?: number;
  pnl_value?: number;
  pnl_pct?: number;
  value_usdt?: number;
  price?: number;
};

export interface QuantLabCostEstimateParams {
  symbol: string;
  regime?: string;
  notional_usdt?: number;
  quantile?: string;
}

async function fetchJson<T>(url: string): Promise<T | null> {
  try {
    const res = await fetch(`${API_BASE}${url}${url.includes('?') ? '&' : '?'}_=${Date.now()}`, {
      cache: 'no-store',
      headers: { Accept: 'application/json' },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return (await res.json()) as T;
  } catch (err) {
    console.error('fetch failed', url, err);
    return null;
  }
}

async function postJson<T>(url: string): Promise<T | null> {
  try {
    const res = await fetch(`${API_BASE}${url}`, {
      method: 'POST',
      cache: 'no-store',
      headers: { Accept: 'application/json' },
    });
    const payload = (await res.json()) as T;
    if (!res.ok) {
      console.error('post failed', url, payload);
      return payload;
    }
    return payload;
  } catch (err) {
    console.error('post failed', url, err);
    return null;
  }
}

function normalizeTradeSymbol(symbol: unknown): string {
  const raw = String(symbol || '').trim();
  if (!raw) return '';
  if (raw.includes('/')) return raw;
  return raw.replace('-USDT', '/USDT');
}

function normalizePositionEntry(position: ApiPositionPayload) {
  const qty = Number(position.qty ?? 0) || 0;
  const avgPrice = Number(position.avgPrice ?? position.avg_px ?? 0) || 0;
  const currentPrice = Number(position.currentPrice ?? position.last_price ?? position.price ?? 0) || 0;
  const value = Number(position.value ?? position.value_usdt ?? 0) || 0;
  const pnl = Number(position.pnl ?? position.pnl_value ?? 0) || 0;
  const pnlPercent = Number(position.pnlPercent ?? position.pnl_pct ?? 0) || 0;

  return {
    symbol: String(position.symbol || ''),
    qty,
    avgPrice,
    currentPrice,
    value,
    pnl,
    pnlPercent,
  };
}

function normalizeTradeEntry(trade: ApiTradePayload, index: number): Trade {
  const symbol = normalizeTradeSymbol(trade.symbol);
  const timestamp = String(trade.timestamp || trade.time || '').trim();
  const side = String(trade.side || 'buy');
  const value = Number(trade.value ?? trade.amount ?? 0) || 0;
  const fee = Math.abs(Number(trade.fee ?? 0) || 0);
  const price = Number(trade.price ?? 0) || 0;
  const qty = Number(trade.qty ?? 0) || 0;
  const derivedQty = qty > 0 ? qty : (price > 0 && value > 0 ? value / price : 0);
  const sourceId = String(
    trade.id ||
    trade.trade_id ||
    trade.fill_id ||
    trade.order_id ||
    trade.ord_id ||
    trade.client_order_id ||
    trade.cl_ord_id ||
    ''
  ).trim();
  const fallbackId = [
    symbol || 'trade',
    side,
    timestamp || index,
    price.toFixed(8),
    derivedQty.toFixed(12),
    value.toFixed(8),
  ].join('|');

  return {
    id: sourceId || fallbackId,
    timestamp,
    symbol,
    side,
    type: String(trade.type || 'REBALANCE'),
    price,
    qty: derivedQty,
    value,
    fee,
    orderId: String(trade.orderId || trade.order_id || trade.ord_id || trade.client_order_id || trade.cl_ord_id || '').trim() || undefined,
    tradeId: String(trade.tradeId || trade.trade_id || trade.fill_id || '').trim() || undefined,
    fillCount: Number(trade.fillCount || trade.fill_count || 1) || 1,
    aggregated: Boolean(trade.aggregated),
  };
}

function tradeDedupeKey(trade: Trade) {
  return [
    String(trade.id || '').trim(),
    normalizeTradeSymbol(trade.symbol).toUpperCase().replace('/', '-'),
    String(trade.timestamp || '').trim(),
    String(trade.side || '').trim().toLowerCase(),
    Number(trade.price || 0).toFixed(8),
    Number(trade.qty || 0).toFixed(12),
    Number(trade.value || 0).toFixed(8),
  ].join('|');
}

export function dedupeTradeEntries(trades: Trade[] | undefined | null): Trade[] {
  if (!Array.isArray(trades)) return [];
  const seen = new Set<string>();
  const deduped: Trade[] = [];
  for (const trade of trades) {
    const key = tradeDedupeKey(trade);
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(trade);
  }
  return deduped;
}

function tradeOrderGroupKey(trade: Trade) {
  const symbol = normalizeTradeSymbol(trade.symbol).toUpperCase().replace('/', '-');
  const side = String(trade.side || '').trim().toLowerCase();
  const orderId = String(trade.orderId || '').trim();
  if (!orderId) return '';
  return ['order', symbol, side, orderId].join('|');
}

function tradeTimestampValue(trade: Trade) {
  const raw = String(trade.timestamp || '').trim();
  if (!raw) return 0;
  const parsed = Date.parse(raw.includes('T') ? raw : raw.replace(' ', 'T'));
  return Number.isFinite(parsed) ? parsed : 0;
}

export function summarizeTradeOrders(trades: Trade[] | undefined | null): Trade[] {
  const exact = dedupeTradeEntries(trades);
  const grouped = new Map<string, Trade[]>();
  const passthrough: Trade[] = [];

  for (const trade of exact) {
    const key = tradeOrderGroupKey(trade);
    if (!key) {
      passthrough.push(trade);
      continue;
    }
    const rows = grouped.get(key) || [];
    rows.push(trade);
    grouped.set(key, rows);
  }

  const summaries: Trade[] = [];
  for (const [key, rows] of grouped.entries()) {
    if (rows.length <= 1) {
      summaries.push(rows[0]);
      continue;
    }
    const latest = rows.reduce((best, row) => (tradeTimestampValue(row) > tradeTimestampValue(best) ? row : best), rows[0]);
    const qty = rows.reduce((sum, row) => sum + (Number(row.qty) || 0), 0);
    const value = rows.reduce((sum, row) => sum + (Number(row.value) || 0), 0);
    const fee = rows.reduce((sum, row) => sum + (Number(row.fee) || 0), 0);
    summaries.push({
      ...latest,
      id: key,
      price: qty > 0 && value > 0 ? value / qty : latest.price,
      qty,
      value,
      fee,
      fillCount: rows.reduce((sum, row) => sum + Math.max(1, Number(row.fillCount || 1) || 1), 0),
      aggregated: true,
    });
  }

  return [...passthrough, ...summaries].sort((a, b) => tradeTimestampValue(b) - tradeTimestampValue(a));
}

export const api = {
  dashboard: () => fetchJson<DashboardData>('/api/dashboard?view=primary'),
  dashboardDeferred: () => fetchJson<Partial<DashboardData>>('/api/dashboard?view=deferred'),
  positions: async () => {
    const payload = await fetchJson<{ positions?: ApiPositionPayload[] }>('/api/positions');
    if (!payload) return null;
    const positions = Array.isArray(payload?.positions)
      ? payload.positions.map((position) => normalizePositionEntry(position))
      : [];
    return { positions };
  },
  trades: async () => {
    const payload = await fetchJson<{ trades?: ApiTradePayload[] }>('/api/trades');
    if (!payload) return null;
    const trades = Array.isArray(payload?.trades)
      ? summarizeTradeOrders(payload.trades.map((trade, index) => normalizeTradeEntry(trade, index)))
      : [];
    return { trades };
  },
  riskGuard: () => fetchJson<RiskGuardData>('/api/auto_risk_guard'),
  marketState: () => fetchJson<MarketStateData>('/api/market_state'),
  decisionAudit: () => fetchJson<DecisionAuditData>('/api/decision_audit'),
  apiTelemetrySeries: (lookbackHours = 24, bucketMinutes = 5) =>
    fetchJson<ApiTelemetrySeriesData>(
      `/api/api_telemetry_series?lookback_hours=${encodeURIComponent(String(lookbackHours))}&bucket_minutes=${encodeURIComponent(
        String(bucketMinutes)
      )}`
    ),
  health: () => fetchJson<HealthData>('/api/health'),
  mlTraining: () => fetchJson<MLTrainingData>('/api/ml_training'),
  liveFollowupBundles: () => fetchJson<LiveFollowupBundlesData>('/api/live_followup_bundles?limit=5'),
  generateLiveFollowupBundle: () => postJson<LiveFollowupBundleGenerateResult>('/api/live_followup_bundles/generate'),
  quantLabStatus: () => fetchJson<QuantLabStatusData>('/api/quant_lab/status'),
  quantLabLivePermission: (strategy = 'v5', version = '5.0.0') =>
    fetchJson<QuantLabPermissionData>(
      `/api/quant_lab/live_permission?strategy=${encodeURIComponent(strategy)}&version=${encodeURIComponent(version)}`
    ),
  quantLabLivePermissionDetail: (strategy = 'v5', version = '5.0.0') =>
    fetchJson<QuantLabPermissionData>(
      `/api/quant_lab/live_permission_detail?strategy=${encodeURIComponent(strategy)}&version=${encodeURIComponent(version)}`
    ),
  quantLabCostEstimate: ({ symbol, regime = 'normal', notional_usdt = 0, quantile = 'p75' }: QuantLabCostEstimateParams) =>
    fetchJson<QuantLabCostEstimateData>(
      `/api/quant_lab/cost_estimate?symbol=${encodeURIComponent(symbol)}&regime=${encodeURIComponent(regime)}&notional_usdt=${encodeURIComponent(
        String(notional_usdt)
      )}&quantile=${encodeURIComponent(quantile)}`
    ),
  quantLabGateDecision: (alphaId = 'v5.core.momentum') =>
    fetchJson<QuantLabGateDecisionData>(`/api/quant_lab/gate_decision?alpha_id=${encodeURIComponent(alphaId)}`),
  positionKline: async (symbol: string, timeframe: string): Promise<PositionKlinePayload> => {
    const payload = await fetchJson<PositionKlinePayload>(
      `/api/position_kline?symbol=${encodeURIComponent(symbol)}&timeframe=${timeframe}`
    );
    return payload && Array.isArray(payload.candles) ? payload : { candles: [] };
  },
};
