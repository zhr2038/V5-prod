import { lazy, startTransition, useEffect, useState, useCallback } from 'react';
import { LiquidBg } from './components/LiquidBg';
import { TopCommandBar } from './components/TopCommandBar';
import { StatusRibbon } from './components/StatusRibbon';
import { MainTradingGrid } from './components/MainTradingGrid';
import { api, dedupeTradeEntries } from './api';
import { useInterval } from './hooks/useInterval';
import type {
  DashboardData,
  RiskGuardData,
  MarketStateData,
  DecisionAuditData,
  HealthData,
  ApiTelemetrySeriesData,
  Trade,
  QuantLabStatusData,
  QuantLabPermissionData,
  QuantLabCostEstimateData,
} from './types';

const ExecutionInsightsPanel = lazy(() =>
  import('./components/ExecutionInsightsPanel').then((module) => ({ default: module.ExecutionInsightsPanel }))
);
function DeferredPanelFallback() {
  return (
    <div className="material-surface material-reading reading-frame p-5" aria-hidden="true">
      <div className="flex flex-col gap-3">
        <div className="h-3 w-32 rounded-full bg-white/[0.10]" />
        <div className="grid grid-cols-2 gap-3">
          <div className="h-12 rounded-2xl bg-white/[0.055]" />
          <div className="h-12 rounded-2xl bg-white/[0.045]" />
        </div>
      </div>
    </div>
  );
}

type IdleWindow = Window & {
  requestIdleCallback?: (callback: IdleRequestCallback, options?: IdleRequestOptions) => number;
  cancelIdleCallback?: (handle: number) => void;
};

const UI_CACHE_TTL_MS = 45 * 1000;
const UI_CACHE_KEYS = {
  dashboard: 'v5.dashboard.primary',
  riskGuard: 'v5.dashboard.riskGuard',
  health: 'v5.dashboard.health',
  quantLabStatus: 'v5.dashboard.quantLabStatus',
  quantLabPermission: 'v5.dashboard.quantLabPermission',
  quantLabCost: 'v5.dashboard.quantLabCost',
} as const;

function isTouchWebKit() {
  return Boolean(
    window.matchMedia('(hover: none) and (pointer: coarse)').matches &&
      globalThis.CSS?.supports?.('-webkit-touch-callout', 'none')
  );
}

function readUiCache<T>(key: string): T | null {
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { savedAt?: number; value?: T };
    if (!parsed?.savedAt || Date.now() - parsed.savedAt > UI_CACHE_TTL_MS) {
      window.localStorage.removeItem(key);
      return null;
    }
    return parsed.value ?? null;
  } catch {
    return null;
  }
}

function writeUiCache<T>(key: string, value: T | null | undefined) {
  if (!value) return;
  try {
    window.localStorage.setItem(key, JSON.stringify({ savedAt: Date.now(), value }));
  } catch {
    // Storage can be unavailable in private modes; polling still remains authoritative.
  }
}

function deferredPayloadLooksSparse(payload?: Partial<DashboardData> | null) {
  if (!payload) return true;
  const hasTimerList = Object.prototype.hasOwnProperty.call(payload, 'timers') && Array.isArray(payload.timers?.timers);
  const hasScoreList = Object.prototype.hasOwnProperty.call(payload, 'alphaScores') && Array.isArray(payload.alphaScores);
  const hasTradeList = Object.prototype.hasOwnProperty.call(payload, 'trades') && Array.isArray(payload.trades);
  const hasTelemetryField = Object.prototype.hasOwnProperty.call(payload, 'apiTelemetry');
  const hasSlippageField = Object.prototype.hasOwnProperty.call(payload, 'slippageInsights');
  const timerCount = Array.isArray(payload.timers?.timers) ? payload.timers.timers.length : 0;
  const scoreCount = Array.isArray(payload.alphaScores) ? payload.alphaScores.length : 0;
  const telemetryKeys = payload.apiTelemetry && typeof payload.apiTelemetry === 'object'
    ? Object.keys(payload.apiTelemetry).length
    : 0;
  const slippageKeys = payload.slippageInsights && typeof payload.slippageInsights === 'object'
    ? Object.keys(payload.slippageInsights).length
    : 0;

  return (
    !hasTimerList &&
    !hasScoreList &&
    !hasTradeList &&
    !hasTelemetryField &&
    !hasSlippageField &&
    timerCount === 0 &&
    scoreCount === 0 &&
    telemetryKeys === 0 &&
    slippageKeys === 0
  );
}

function pickAuthoritativeList<T>(incoming: T[] | undefined, current: T[] | undefined): T[] {
  if (Array.isArray(incoming)) return incoming;
  if (Array.isArray(current)) return current;
  return [];
}

function pickTimersWithFallback(
  incoming: DashboardData['timers'] | undefined,
  current: DashboardData['timers'] | undefined
): DashboardData['timers'] {
  const incomingTimers = Array.isArray(incoming?.timers) ? incoming.timers : [];
  const currentTimers = Array.isArray(current?.timers) ? current.timers : [];
  if (incoming && Array.isArray(incoming.timers)) return incoming;
  if (incomingTimers.length > 0 && incoming) return incoming;
  if (currentTimers.length > 0 && current) return current;
  if (incoming) return incoming;
  if (current) return current;
  return { timers: [] };
}

function pickObjectWithFallback<T extends object | null | undefined>(incoming: T, current: T) {
  const incomingKeys = incoming && typeof incoming === 'object' ? Object.keys(incoming).length : 0;
  const currentKeys = current && typeof current === 'object' ? Object.keys(current).length : 0;
  if (incomingKeys > 0) return incoming;
  if (currentKeys > 0) return current;
  return incoming || current;
}

function mergeDeferredDashboard(prev: DashboardData | null, deferred: Partial<DashboardData>) {
  if (!prev) return deferred as DashboardData;
  if (deferredPayloadLooksSparse(deferred)) {
    return deferred.systemStatus ? { ...prev, systemStatus: deferred.systemStatus } : prev;
  }
  return {
    ...prev,
    ...deferred,
    alphaScores: pickAuthoritativeList(deferred.alphaScores, prev.alphaScores),
    trades: dedupeTradeEntries(pickAuthoritativeList(deferred.trades, prev.trades)),
    timers: pickTimersWithFallback(deferred.timers, prev.timers),
    apiTelemetry: pickObjectWithFallback(deferred.apiTelemetry, prev.apiTelemetry),
    slippageInsights: pickObjectWithFallback(deferred.slippageInsights, prev.slippageInsights),
  };
}

function quantLabSymbol(symbol?: string) {
  const text = String(symbol || '').trim().toUpperCase();
  if (!text) return '';
  return text.replace('/', '-').replace('_', '-');
}

function tradeTimeValue(trade: Trade) {
  const raw = String(trade.timestamp || '').trim();
  if (!raw) return 0;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function dashboardFocusForQuantLab(dashboard?: DashboardData | null) {
  const firstPosition = dashboard?.positions?.[0];
  if (firstPosition?.symbol) {
    return { symbol: firstPosition.symbol, notional_usdt: Number(firstPosition.value || 0) || 0 };
  }
  const latestTrade = dedupeTradeEntries(dashboard?.trades).sort((a, b) => tradeTimeValue(b) - tradeTimeValue(a))[0];
  if (latestTrade?.symbol) {
    return { symbol: latestTrade.symbol, notional_usdt: Number(latestTrade.value || 0) || 0 };
  }
  return null;
}

function App() {
  const [dashboard, setDashboard] = useState<DashboardData | null>(() => readUiCache(UI_CACHE_KEYS.dashboard));
  const [riskGuard, setRiskGuard] = useState<RiskGuardData | null>(() => readUiCache(UI_CACHE_KEYS.riskGuard));
  const [marketState, setMarketState] = useState<MarketStateData | null>(() => dashboard?.marketState || null);
  const [decisionAudit, setDecisionAudit] = useState<DecisionAuditData | null>(null);
  const [health, setHealth] = useState<HealthData | null>(() => readUiCache(UI_CACHE_KEYS.health));
  const [quantLabStatus, setQuantLabStatus] = useState<QuantLabStatusData | null>(() => readUiCache(UI_CACHE_KEYS.quantLabStatus));
  const [quantLabPermission, setQuantLabPermission] = useState<QuantLabPermissionData | null>(() => readUiCache(UI_CACHE_KEYS.quantLabPermission));
  const [quantLabCost, setQuantLabCost] = useState<QuantLabCostEstimateData | null>(null);
  const [apiTelemetrySeries, setApiTelemetrySeries] = useState<ApiTelemetrySeriesData | null>(null);
  const [updateTime, setUpdateTime] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [showDeferredPanels, setShowDeferredPanels] = useState(false);
  const [focusSymbol, setFocusSymbol] = useState('BNB-USDT');

  const loadQuantLab = useCallback(async (focus?: { symbol?: string; notional_usdt?: number } | null) => {
    const symbol = quantLabSymbol(focus?.symbol);
    const notional = Number(focus?.notional_usdt || 0) || 0;
    const [status, permission, cost] = await Promise.all([
      api.quantLabStatus(),
      api.quantLabLivePermission('v5', '5.0.0'),
      symbol
        ? api.quantLabCostEstimate({
            symbol,
            regime: 'normal',
            notional_usdt: notional,
            quantile: 'p75',
          })
        : Promise.resolve(null),
    ]);
    startTransition(() => {
      if (status) {
        setQuantLabStatus(status);
        writeUiCache(UI_CACHE_KEYS.quantLabStatus, status);
      }
      if (permission) {
        setQuantLabPermission(permission);
        writeUiCache(UI_CACHE_KEYS.quantLabPermission, permission);
      }
      if (symbol) {
        const nextCost =
          cost ||
          ({
            available: false,
            status: 'degraded',
            reason: 'dashboard_fetch_failed',
            symbol,
            regime: 'normal',
            cost_freshness_status: 'unavailable',
          } as QuantLabCostEstimateData);
        setQuantLabCost(nextCost);
        window.localStorage.removeItem(UI_CACHE_KEYS.quantLabCost);
      }
    });
  }, []);

  const loadApiTelemetrySeries = useCallback(async () => {
    const telemetrySeries = await api.apiTelemetrySeries(24, 5);
    if (telemetrySeries) {
      startTransition(() => {
        setApiTelemetrySeries(telemetrySeries);
      });
    }
  }, []);

  const loadPrimary = useCallback(async () => {
    if (document.hidden) return;
    setLoading(true);
    const [d, r, liveTrades] = await Promise.all([
      api.dashboard(),
      api.riskGuard(),
      api.trades(),
    ]);
    if (d) {
      const authoritativeTrades = Array.isArray(liveTrades?.trades) ? liveTrades.trades : d.trades;
      const nextDashboardBase = {
        ...d,
        trades: dedupeTradeEntries(authoritativeTrades),
      } as DashboardData;
      setDashboard((prev) => {
        const merged = prev ? { ...prev, ...nextDashboardBase } : nextDashboardBase;
        writeUiCache(UI_CACHE_KEYS.dashboard, merged);
        return merged;
      });
      setMarketState(d.marketState || null);
      void loadQuantLab(dashboardFocusForQuantLab(nextDashboardBase));
    }
    if (r) {
      setRiskGuard(r);
      writeUiCache(UI_CACHE_KEYS.riskGuard, r);
    }
    setUpdateTime(new Date().toLocaleTimeString('zh-CN', { hour12: false }));
    setLoading(false);
  }, [loadQuantLab]);

  const loadSecondary = useCallback(async () => {
    void loadApiTelemetrySeries();
    const [deferred, dec, h] = await Promise.all([
      api.dashboardDeferred(),
      api.decisionAudit(),
      api.health(),
    ]);
    startTransition(() => {
      if (deferred) {
        setDashboard((prev) => {
          const nextDashboard = mergeDeferredDashboard(prev, deferred);
          writeUiCache(UI_CACHE_KEYS.dashboard, nextDashboard);
          void loadQuantLab(dashboardFocusForQuantLab(nextDashboard));
          return nextDashboard;
        });
      }
      if (dec) setDecisionAudit(dec);
      if (h) {
        setHealth(h);
        writeUiCache(UI_CACHE_KEYS.health, h);
      }
    });
  }, [loadApiTelemetrySeries, loadQuantLab]);

  useEffect(() => {
    let timeoutId: number | null = null;
    const primaryTimeoutId = globalThis.setTimeout(() => {
      void loadPrimary();
    }, 0);
    let idleId: number | null = null;
    const idleWindow = window as IdleWindow;
    const deferSlowPath = isTouchWebKit();

    const runDeferred = () => {
      void loadSecondary();
      startTransition(() => {
        setShowDeferredPanels(true);
      });
    };

    if (idleWindow.requestIdleCallback) {
      idleId = idleWindow.requestIdleCallback(() => runDeferred(), { timeout: deferSlowPath ? 2600 : 1200 });
    } else {
      timeoutId = globalThis.setTimeout(runDeferred, deferSlowPath ? 1800 : 400);
    }

    return () => {
      globalThis.clearTimeout(primaryTimeoutId);
      if (idleId !== null && idleWindow.cancelIdleCallback) {
        idleWindow.cancelIdleCallback(idleId);
      }
      if (timeoutId !== null) {
        globalThis.clearTimeout(timeoutId);
      }
    };
  }, [loadPrimary, loadSecondary]);

  useInterval(() => {
    loadPrimary();
  }, 30000);

  useInterval(() => {
    loadSecondary();
  }, 60000);

  useInterval(() => {
    loadApiTelemetrySeries();
  }, 30000);

  useInterval(() => {
    loadQuantLab(dashboardFocusForQuantLab(dashboard));
  }, 30000);

  const displayMarketState = marketState || dashboard?.marketState || null;

  return (
    <div className="dashboard-shell relative min-h-[100dvh] min-h-[100svh] min-h-screen">
      <LiquidBg />

      <div className="dashboard-frame relative z-10">
        <TopCommandBar
          systemStatus={dashboard?.systemStatus || null}
          health={health}
          updateTime={updateTime}
          loading={loading}
          onRefresh={() => {
            void loadPrimary();
            void loadSecondary();
            void loadQuantLab(dashboardFocusForQuantLab(dashboard));
            void loadApiTelemetrySeries();
          }}
          onSymbolSearch={setFocusSymbol}
        />

        <StatusRibbon
          account={dashboard?.account || null}
          marketState={displayMarketState}
          riskGuard={riskGuard}
          quantLabStatus={quantLabStatus}
          quantLabPermission={quantLabPermission}
          systemStatus={dashboard?.systemStatus || null}
        />

        <div className="dashboard-workspace">
          <MainTradingGrid
            positions={dashboard?.positions || []}
            trades={dashboard?.trades || []}
            focusSymbol={focusSymbol}
            account={dashboard?.account || null}
            marketState={displayMarketState}
            slippageInsights={dashboard?.slippageInsights || null}
            timers={dashboard?.timers || null}
            decisionAudit={decisionAudit}
            apiTelemetry={dashboard?.apiTelemetry || null}
            apiTelemetrySeries={apiTelemetrySeries}
            quantLabCost={quantLabCost}
            showDeferredPanels={showDeferredPanels}
            fallback={<DeferredPanelFallback />}
            ExecutionInsightsPanel={ExecutionInsightsPanel}
          />
        </div>

        {loading && (
          <div className="fixed bottom-4 right-5 z-50 flex items-center gap-2 text-[11px] uppercase tracking-[0.12em] text-[var(--text-dim)]">
            <span className="h-1.5 w-1.5 rounded-full bg-[var(--accent)]/80" />
            <span>刷新中</span>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
