import type {
  DashboardData,
  RiskGuardData,
  MarketStateData,
  DecisionAuditData,
  HealthData,
  MLTrainingData,
  ShadowMLData,
  PositionKlinePayload,
  Trade,
} from './types';

const API_BASE = '';

type ApiTradePayload = Partial<Trade> & {
  time?: string;
  amount?: number;
};

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

function normalizeTradeSymbol(symbol: unknown): string {
  const raw = String(symbol || '').trim();
  if (!raw) return '';
  if (raw.includes('/')) return raw;
  return raw.replace('-USDT', '/USDT');
}

function normalizeTradeEntry(trade: ApiTradePayload, index: number): Trade {
  const symbol = normalizeTradeSymbol(trade.symbol);
  const timestamp = String(trade.timestamp || trade.time || '').trim();
  const value = Number(trade.value ?? trade.amount ?? 0) || 0;
  const fee = Math.abs(Number(trade.fee ?? 0) || 0);

  return {
    id: String(trade.id || `${symbol || 'trade'}-${timestamp || index}`),
    timestamp,
    symbol,
    side: String(trade.side || 'buy'),
    type: String(trade.type || 'REBALANCE'),
    price: Number(trade.price ?? 0) || 0,
    qty: Number(trade.qty ?? 0) || 0,
    value,
    fee,
  };
}

export const api = {
  dashboard: () => fetchJson<DashboardData>('/api/dashboard?view=primary'),
  dashboardDeferred: () => fetchJson<Partial<DashboardData>>('/api/dashboard?view=deferred'),
  positions: () => fetchJson<{ positions?: import('./types').Position[] }>('/api/positions'),
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
  shadowMl: () => fetchJson<ShadowMLData>('/api/shadow_ml_overlay'),
  positionKline: async (symbol: string, timeframe: string): Promise<PositionKlinePayload> => {
    const payload = await fetchJson<PositionKlinePayload>(
      `/api/position_kline?symbol=${encodeURIComponent(symbol)}&timeframe=${timeframe}`
    );
    return payload && Array.isArray(payload.candles) ? payload : { candles: [] };
  },
};
