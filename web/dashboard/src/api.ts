import type {
  DashboardData,
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
  const value = Number(trade.value ?? trade.amount ?? 0) || 0;
  const fee = Math.abs(Number(trade.fee ?? 0) || 0);
  const price = Number(trade.price ?? 0) || 0;
  const qty = Number(trade.qty ?? 0) || 0;
  const derivedQty = qty > 0 ? qty : (price > 0 && value > 0 ? value / price : 0);

  return {
    id: String(trade.id || `${symbol || 'trade'}-${timestamp || index}`),
    timestamp,
    symbol,
    side: String(trade.side || 'buy'),
    type: String(trade.type || 'REBALANCE'),
    price,
    qty: derivedQty,
    value,
    fee,
  };
}

export const api = {
  dashboard: () => fetchJson<DashboardData>('/api/dashboard?view=primary'),
  dashboardDeferred: () => fetchJson<Partial<DashboardData>>('/api/dashboard?view=deferred'),
  positions: async () => {
    const payload = await fetchJson<{ positions?: ApiPositionPayload[] }>('/api/positions');
    const positions = Array.isArray(payload?.positions)
      ? payload.positions.map((position) => normalizePositionEntry(position))
      : [];
    return { positions };
  },
  trades: async () => {
    const payload = await fetchJson<{ trades?: ApiTradePayload[] }>('/api/trades');
    const trades = Array.isArray(payload?.trades)
      ? payload.trades.map((trade, index) => normalizeTradeEntry(trade, index))
      : [];
    return { trades };
  },
  riskGuard: () => fetchJson<RiskGuardData>('/api/auto_risk_guard'),
  marketState: () => fetchJson<MarketStateData>('/api/market_state'),
  decisionAudit: () => fetchJson<DecisionAuditData>('/api/decision_audit'),
  health: () => fetchJson<HealthData>('/api/health'),
  mlTraining: () => fetchJson<MLTrainingData>('/api/ml_training'),
  liveFollowupBundles: () => fetchJson<LiveFollowupBundlesData>('/api/live_followup_bundles?limit=5'),
  generateLiveFollowupBundle: () => postJson<LiveFollowupBundleGenerateResult>('/api/live_followup_bundles/generate'),
  quantLabStatus: () => fetchJson<QuantLabStatusData>('/api/quant_lab/status'),
  quantLabLivePermission: (strategy = 'v5', version = 'v1') =>
    fetchJson<QuantLabPermissionData>(
      `/api/quant_lab/live_permission?strategy=${encodeURIComponent(strategy)}&version=${encodeURIComponent(version)}`
    ),
  quantLabLivePermissionDetail: (strategy = 'v5', version = 'v1') =>
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
