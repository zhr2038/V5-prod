import type {
  DashboardData,
  RiskGuardData,
  MarketStateData,
  DecisionAuditData,
  HealthData,
  MLTrainingData,
  ShadowMLData,
  KlineData,
} from './types';

const API_BASE = '';

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

export const api = {
  dashboard: () => fetchJson<DashboardData>('/api/dashboard'),
  riskGuard: () => fetchJson<RiskGuardData>('/api/auto_risk_guard'),
  marketState: () => fetchJson<MarketStateData>('/api/market_state'),
  decisionAudit: () => fetchJson<DecisionAuditData>('/api/decision_audit'),
  health: () => fetchJson<HealthData>('/api/health'),
  mlTraining: () => fetchJson<MLTrainingData>('/api/ml_training'),
  shadowMl: () => fetchJson<ShadowMLData>('/api/shadow_ml_overlay'),
  positionKline: (symbol: string, timeframe: string) =>
    fetchJson<KlineData[]>(`/api/position_kline?symbol=${encodeURIComponent(symbol)}&timeframe=${timeframe}`),
};
