import { startTransition, useEffect, useRef, useState, useCallback } from 'react';
import { CommandCenter } from './components/CommandCenter';
import type { CommandCenterData } from './commandTypes';
import { api, mergePrimaryDashboard, secondaryRefreshState, summarizeTradeOrders } from './api';
import { useInterval } from './hooks/useInterval';
import type {
  DashboardData,
  RiskGuardData,
  MarketStateData,
  DecisionAuditData,
  HealthData,
  EquityPoint,
  Trade,
  QuantLabStatusData,
  QuantLabPermissionData,
  QuantLabCostEstimateData,
} from './types';

type IdleWindow = Window & {
  requestIdleCallback?: (callback: IdleRequestCallback, options?: IdleRequestOptions) => number;
  cancelIdleCallback?: (handle: number) => void;
};

const LEGACY_UI_CACHE_KEYS = [
  'v5.dashboard.primary',
  'v5.dashboard.riskGuard',
  'v5.dashboard.health',
  'v5.dashboard.quantLabStatus',
  'v5.dashboard.quantLabPermission',
  'v5.dashboard.quantLabCost',
] as const;

function isTouchWebKit() {
  return Boolean(
    window.matchMedia('(hover: none) and (pointer: coarse)').matches &&
      globalThis.CSS?.supports?.('-webkit-touch-callout', 'none')
  );
}

function clearLegacyUiCache() {
  try {
    for (const key of LEGACY_UI_CACHE_KEYS) {
      window.localStorage.removeItem(key);
    }
  } catch {
    // Storage can be unavailable in private modes; polling remains authoritative.
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

function pickObjectWithFallback<T extends object | null | undefined>(incoming: T, current: T, incomingPresent = false) {
  if (incomingPresent) return incoming;
  const incomingKeys = incoming && typeof incoming === 'object' ? Object.keys(incoming).length : 0;
  const currentKeys = current && typeof current === 'object' ? Object.keys(current).length : 0;
  if (incomingKeys > 0) return incoming;
  if (currentKeys > 0) return current;
  return incoming || current;
}

function systemStatusEpoch(status?: DashboardData['systemStatus'] | null) {
  const raw = String(status?.lastUpdate || '').trim();
  if (!raw) return null;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function pickFreshSystemStatus(
  incoming: DashboardData['systemStatus'] | undefined,
  current: DashboardData['systemStatus'] | undefined,
  incomingPresent = false
) {
  if (!incomingPresent) return current || incoming;
  if (!incoming) return current;
  if (!current) return incoming;

  const incomingRaw = String(incoming.lastUpdate || '').trim();
  const currentRaw = String(current.lastUpdate || '').trim();
  if (!incomingRaw && currentRaw) return current;
  if (incomingRaw && !currentRaw) return incoming;

  const incomingEpoch = systemStatusEpoch(incoming);
  const currentEpoch = systemStatusEpoch(current);
  if (incomingEpoch === null && currentEpoch !== null) return current;
  if (incomingEpoch !== null && currentEpoch !== null && incomingEpoch < currentEpoch) return current;
  return incoming;
}

function mergeDeferredDashboard(prev: DashboardData | null, deferred: Partial<DashboardData>) {
  if (!prev) return deferred as DashboardData;
  const hasSystemStatus = Object.prototype.hasOwnProperty.call(deferred, 'systemStatus');
  const systemStatus = pickFreshSystemStatus(deferred.systemStatus, prev.systemStatus, hasSystemStatus);
  if (deferredPayloadLooksSparse(deferred)) {
    return systemStatus ? { ...prev, systemStatus } : prev;
  }
  return {
    ...prev,
    ...deferred,
    systemStatus: systemStatus || prev.systemStatus,
    alphaScores: pickAuthoritativeList(deferred.alphaScores, prev.alphaScores),
    trades: summarizeTradeOrders(pickAuthoritativeList(deferred.trades, prev.trades)),
    timers: pickTimersWithFallback(deferred.timers, prev.timers),
    apiTelemetry: pickObjectWithFallback(
      deferred.apiTelemetry,
      prev.apiTelemetry,
      Object.prototype.hasOwnProperty.call(deferred, 'apiTelemetry')
    ),
    slippageInsights: pickObjectWithFallback(
      deferred.slippageInsights,
      prev.slippageInsights,
      Object.prototype.hasOwnProperty.call(deferred, 'slippageInsights')
    ),
  };
}

function quantLabSymbol(symbol?: string) {
  const text = String(symbol || '').trim().toUpperCase();
  if (!text) return '';
  const normalized = text.replace('/', '-').replace('_', '-');
  return normalized.includes('-') ? normalized : `${normalized}-USDT`;
}

function tradeTimeValue(trade: Trade) {
  const raw = String(trade.timestamp || '').trim();
  if (!raw) return 0;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function tradeNotionalUsdt(trade?: Trade | null) {
  if (!trade) return 0;
  const providedValue = Math.max(0, Number(trade.value || 0) || 0);
  if (providedValue > 0) return providedValue;
  const price = Math.max(0, Number(trade.price || 0) || 0);
  const qty = Math.max(0, Number(trade.qty || 0) || 0);
  return price * qty;
}

function positionFocusFromDashboard(dashboard?: DashboardData | null) {
  const positions = Array.isArray(dashboard?.positions) ? dashboard.positions : [];
  let bestSymbol = '';
  let bestNotional = -1;
  for (const position of positions) {
    const symbol = String(position?.symbol || '').trim();
    if (!symbol) continue;
    const notional = Math.max(0, Number(position.value || 0) || 0);
    if (notional <= 0) continue;
    if (notional > bestNotional) {
      bestSymbol = symbol;
      bestNotional = notional;
    }
  }
  if (!bestSymbol) return null;
  return { symbol: bestSymbol, notional_usdt: Math.max(0, bestNotional) };
}

function dashboardFocusForQuantLab(dashboard?: DashboardData | null, preferredSymbol?: string) {
  const normalizedPreferred = quantLabSymbol(preferredSymbol);
  const recentTrades = summarizeTradeOrders(dashboard?.trades)
    .sort((a, b) => tradeTimeValue(b) - tradeTimeValue(a));
  if (normalizedPreferred) {
    const positions = Array.isArray(dashboard?.positions) ? dashboard.positions : [];
    const matchedPosition = positions.find(
      (position) => quantLabSymbol(position?.symbol) === normalizedPreferred
    );
    if (matchedPosition) {
      const positionNotional = Math.max(0, Number(matchedPosition.value || 0) || 0);
      if (positionNotional > 0) {
        return {
          symbol: normalizedPreferred,
          notional_usdt: positionNotional,
        };
      }
    }

    const matchingTrade = recentTrades
      .find((trade) => quantLabSymbol(trade.symbol) === normalizedPreferred);
    const matchingNotional = tradeNotionalUsdt(matchingTrade);
    const fallbackNotional =
      matchingNotional > 0
        ? matchingNotional
        : tradeNotionalUsdt(recentTrades.find((trade) => tradeNotionalUsdt(trade) > 0));
    return {
      symbol: normalizedPreferred,
      notional_usdt: fallbackNotional,
    };
  }

  const positionFocus = positionFocusFromDashboard(dashboard);
  if (positionFocus) return positionFocus;
  const latestTrade = recentTrades[0];
  if (latestTrade?.symbol) {
    return { symbol: latestTrade.symbol, notional_usdt: tradeNotionalUsdt(latestTrade) };
  }
  return null;
}

function App() {
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [riskGuard, setRiskGuard] = useState<RiskGuardData | null>(null);
  const [marketState, setMarketState] = useState<MarketStateData | null>(() => dashboard?.marketState || null);
  const [decisionAudit, setDecisionAudit] = useState<DecisionAuditData | null>(null);
  const [health, setHealth] = useState<HealthData | null>(null);
  const [quantLabStatus, setQuantLabStatus] = useState<QuantLabStatusData | null>(null);
  const [quantLabPermission, setQuantLabPermission] = useState<QuantLabPermissionData | null>(null);
  const [quantLabCost, setQuantLabCost] = useState<QuantLabCostEstimateData | null>(null);

  const [updateTime, setUpdateTime] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [primaryRefreshFailed, setPrimaryRefreshFailed] = useState(false);
  const [primaryReceivedAt, setPrimaryReceivedAt] = useState<number | null>(null);
  const [tradesFailed, setTradesFailed] = useState(false);
  const [tradesReceivedAt, setTradesReceivedAt] = useState<number | null>(null);
  const [secondaryRefresh, setSecondaryRefresh] = useState<{
    failed: boolean; receivedAt: number | null; deferredReceivedAt: number | null;
  }>({ failed: false, receivedAt: null, deferredReceivedAt: null });
  const [command, setCommand] = useState<CommandCenterData | null>(null);
  const [commandFailed, setCommandFailed] = useState(false);
  const [commandReceivedAt, setCommandReceivedAt] = useState<number | null>(null);
  const [equity, setEquity] = useState<EquityPoint[]>([]);
  const [equityLoading, setEquityLoading] = useState(true);
  const [equityFailed, setEquityFailed] = useState(false);
  const commandBusy = useRef(false);
  const equityBusy = useRef(false);
  const primaryBusy = useRef(false);
  const secondaryBusy = useRef(false);

  const manualFocusRef = useRef(false);
  const quantLabRequestIdRef = useRef(0);
  const [focusSymbol, setFocusSymbol] = useState('BNB-USDT');

  const syncPositionFocus = useCallback((nextDashboard: DashboardData) => {
    if (manualFocusRef.current) return;
    const positionFocus = positionFocusFromDashboard(nextDashboard);
    const nextFocusSymbol = quantLabSymbol(positionFocus?.symbol);
    if (!nextFocusSymbol) return;
    startTransition(() => {
      setFocusSymbol((current) => (current === nextFocusSymbol ? current : nextFocusSymbol));
    });
  }, []);

  const loadQuantLab = useCallback(async (focus?: { symbol?: string; notional_usdt?: number } | null) => {
    const requestId = ++quantLabRequestIdRef.current;
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
    if (requestId !== quantLabRequestIdRef.current) return;

    const nextStatus = status || ({
      available: false,
      status: 'degraded',
      mode: 'shadow',
      reason: 'dashboard_fetch_failed',
    } as QuantLabStatusData);
    const nextPermission = permission || ({
      available: false,
      status: 'degraded',
      permission: 'ABORT',
      decision: 'ABORT',
      permission_status: 'UNAVAILABLE',
      enforceable: false,
      reasons: ['dashboard_fetch_failed'],
      live_block_reasons: ['dashboard_fetch_failed'],
    } as QuantLabPermissionData);
    startTransition(() => {
      setQuantLabStatus(nextStatus);
      setQuantLabPermission(nextPermission);
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
      }
    });
  }, []);

  const handleSymbolSearch = useCallback((symbol: string) => {
    manualFocusRef.current = true;
    setFocusSymbol(symbol);
    void loadQuantLab(dashboardFocusForQuantLab(dashboard, symbol));
  }, [dashboard, loadQuantLab]);

  const loadCommand = useCallback(async () => {
    if (document.hidden || commandBusy.current) return;
    commandBusy.current = true;
    try {
      const payload = await api.commandCenter();
      if (payload?.schema_version === 'v5.command_center.v1') {
        setCommand(payload);
        setCommandReceivedAt(Date.now());
        setCommandFailed(false);
      } else setCommandFailed(true);
    } finally { commandBusy.current = false; }
  }, []);

  const loadEquity = useCallback(async () => {
    if (document.hidden || equityBusy.current) return;
    equityBusy.current = true;
    setEquityLoading(true);
    try {
      const payload = await api.equityHistory();
      if (Array.isArray(payload)) { setEquity(payload); setEquityFailed(false); }
      else setEquityFailed(true);
    } finally { equityBusy.current = false; setEquityLoading(false); }
  }, []);

  const loadPrimary = useCallback(async () => {
    if (document.hidden || primaryBusy.current) return;
    primaryBusy.current = true;
    setLoading(true);
    try {
      const d = await api.dashboard();
      if (d) {
        setDashboard((prev) => mergePrimaryDashboard(prev, d));
        syncPositionFocus(d);
        setMarketState(d.marketState || null);
        const receivedAt = Date.now();
        setPrimaryReceivedAt(receivedAt);
        setUpdateTime(new Date(receivedAt).toLocaleTimeString('zh-CN', { hour12: false, timeZone: 'Asia/Shanghai' }));
        setPrimaryRefreshFailed(false);
      } else {
        setPrimaryRefreshFailed(true);
      }
      setLoading(false);

      // Auxiliary calls have independent freshness and must not turn missing trades into an empty history.
      const [r, liveTrades] = await Promise.all([api.riskGuard(), api.trades()]);
      setRiskGuard(r || null);
      const observedTrades = Array.isArray(liveTrades?.trades) ? liveTrades.trades : null;
      setTradesFailed(observedTrades === null);
      if (observedTrades !== null) {
        const normalizedTrades = summarizeTradeOrders(observedTrades);
        setTradesReceivedAt(Date.now());
        setDashboard((prev) => prev ? { ...prev, trades: normalizedTrades } : prev);
      }
      if (d) {
        const authoritativeDashboard = observedTrades === null ? d : { ...d, trades: observedTrades };
        // Use only a successful trade observation when deriving an execution notional.
        const positionFocus = positionFocusFromDashboard(authoritativeDashboard);
        const nextFocusSymbol = manualFocusRef.current
          ? focusSymbol
          : quantLabSymbol(positionFocus?.symbol) || focusSymbol;
        void loadQuantLab(dashboardFocusForQuantLab(authoritativeDashboard, nextFocusSymbol));
      }
    } finally {
      primaryBusy.current = false;
      setLoading(false);
    }
  }, [focusSymbol, loadQuantLab, syncPositionFocus]);

  const loadSecondary = useCallback(async () => {
    if (secondaryBusy.current) return;
    secondaryBusy.current = true;
    try {
      const [deferred, dec, h] = await Promise.all([
        api.dashboardDeferred(), api.decisionAudit(), api.health(),
      ]);
      const receivedAt = Date.now();
      startTransition(() => {
        setSecondaryRefresh((prev) => secondaryRefreshState(prev, deferred, dec, h, receivedAt));
        if (deferred) setDashboard((prev) => mergeDeferredDashboard(prev, deferred));
        if (dec) setDecisionAudit(dec);
        if (h) setHealth(h);
      });
    } finally { secondaryBusy.current = false; }
  }, []);

  useEffect(() => {
    clearLegacyUiCache();
    let timeoutId: number | null = null;
    const primaryTimeoutId = globalThis.setTimeout(() => {
      void loadPrimary();
      void loadCommand();
      void loadEquity();
    }, 0);
    let idleId: number | null = null;
    const idleWindow = window as IdleWindow;
    const deferSlowPath = isTouchWebKit();

    const runDeferred = () => {
      void loadSecondary();
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
  }, [loadPrimary, loadSecondary, loadCommand, loadEquity]);

  useInterval(() => {
    loadPrimary();
  }, 30000);

  useInterval(() => {
    loadSecondary();
    void loadEquity();
  }, 60000);

  useInterval(() => { void loadCommand(); }, 30000);

  const displayMarketState = marketState || dashboard?.marketState || null;

  return <CommandCenter
    dashboard={dashboard} command={command} commandFailed={commandFailed} commandReceivedAt={commandReceivedAt}
    equity={equity} equityLoading={equityLoading} equityFailed={equityFailed}
    riskGuard={riskGuard} decisionAudit={decisionAudit} marketState={displayMarketState}
    health={health} quantLabStatus={quantLabStatus} quantLabPermission={quantLabPermission} quantLabCost={quantLabCost}
    focusSymbol={focusSymbol} loading={loading} refreshFailed={primaryRefreshFailed} updateTime={updateTime}
    primaryReceivedAt={primaryReceivedAt} tradesFailed={tradesFailed} tradesReceivedAt={tradesReceivedAt}
    secondaryFailed={secondaryRefresh.failed} secondaryReceivedAt={secondaryRefresh.receivedAt}
    deferredReceivedAt={secondaryRefresh.deferredReceivedAt}
    onSymbolSearch={handleSymbolSearch}
    onRefresh={() => { void loadPrimary(); void loadSecondary(); void loadCommand(); void loadEquity(); }}
  />;
}

export default App;
