import { Suspense, lazy, startTransition, useEffect, useEffectEvent, useState, useCallback } from 'react';
import { motion } from 'framer-motion';
import { LiquidBg } from './components/LiquidBg';
import { Hero } from './components/Hero';
import { MetricsGrid } from './components/MetricsGrid';
import { MLBand } from './components/MLBand';
import { PositionsPanel } from './components/PositionsPanel';
import { MarketRadar } from './components/MarketRadar';
import { SignalsPanel } from './components/SignalsPanel';
import { Sidebar } from './components/Sidebar';
import { api } from './api';
import { useInterval } from './hooks/useInterval';
import type { DashboardData, RiskGuardData, MarketStateData, DecisionAuditData, HealthData, ShadowMLData } from './types';

const ExecutionInsightsPanel = lazy(() =>
  import('./components/ExecutionInsightsPanel').then((module) => ({ default: module.ExecutionInsightsPanel }))
);
const ShadowMLPanel = lazy(() =>
  import('./components/ShadowMLPanel').then((module) => ({ default: module.ShadowMLPanel }))
);

function DeferredPanelFallback() {
  return <div className="material-surface material-reading reading-frame h-40" />;
}

type IdleWindow = Window & {
  requestIdleCallback?: (callback: IdleRequestCallback, options?: IdleRequestOptions) => number;
  cancelIdleCallback?: (handle: number) => void;
};

function deferredPayloadLooksSparse(payload?: Partial<DashboardData> | null) {
  if (!payload) return true;
  const timerCount = Array.isArray(payload.timers?.timers) ? payload.timers.timers.length : 0;
  const scoreCount = Array.isArray(payload.alphaScores) ? payload.alphaScores.length : 0;
  const tradeCount = Array.isArray(payload.trades) ? payload.trades.length : 0;
  const telemetryKeys = payload.apiTelemetry && typeof payload.apiTelemetry === 'object'
    ? Object.keys(payload.apiTelemetry).length
    : 0;
  const slippageKeys = payload.slippageInsights && typeof payload.slippageInsights === 'object'
    ? Object.keys(payload.slippageInsights).length
    : 0;

  return timerCount === 0 && scoreCount === 0 && tradeCount === 0 && telemetryKeys === 0 && slippageKeys === 0;
}

function pickListWithFallback<T>(incoming: T[] | undefined, current: T[] | undefined): T[] {
  if (Array.isArray(incoming) && incoming.length > 0) return incoming;
  if (Array.isArray(current) && current.length > 0) return current;
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
    alphaScores: pickListWithFallback(deferred.alphaScores, prev.alphaScores),
    trades: pickListWithFallback(deferred.trades, prev.trades),
    timers: pickTimersWithFallback(deferred.timers, prev.timers),
    apiTelemetry: pickObjectWithFallback(deferred.apiTelemetry, prev.apiTelemetry),
    slippageInsights: pickObjectWithFallback(deferred.slippageInsights, prev.slippageInsights),
  };
}

function App() {
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [riskGuard, setRiskGuard] = useState<RiskGuardData | null>(null);
  const [marketState, setMarketState] = useState<MarketStateData | null>(null);
  const [decisionAudit, setDecisionAudit] = useState<DecisionAuditData | null>(null);
  const [health, setHealth] = useState<HealthData | null>(null);
  const [shadowML, setShadowML] = useState<ShadowMLData | null>(null);
  const [updateTime, setUpdateTime] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [showDeferredPanels, setShowDeferredPanels] = useState(false);
  const [secondaryReady, setSecondaryReady] = useState(false);

  const loadPrimary = useCallback(async () => {
    if (document.hidden) return;
    setLoading(true);
    const [d, r] = await Promise.all([
      api.dashboard(),
      api.riskGuard(),
    ]);
    if (d) {
      setDashboard((prev) => (prev ? { ...prev, ...d } : d));
      setMarketState(d.marketState || null);
    }
    if (r) {
      setRiskGuard(r);
    }
    setUpdateTime(new Date().toLocaleTimeString('zh-CN', { hour12: false }));
    setLoading(false);
  }, []);

  const loadSecondary = useCallback(async () => {
    const [deferred, dec, h] = await Promise.all([
      api.dashboardDeferred(),
      api.decisionAudit(),
      api.health(),
    ]);
    startTransition(() => {
      if (deferred) {
        setDashboard((prev) => mergeDeferredDashboard(prev, deferred));
        setSecondaryReady(true);
      }
      if (dec) setDecisionAudit(dec);
      if (h) setHealth(h);
    });
  }, []);

  const loadDeferred = useCallback(async () => {
    const s = await api.shadowMl();
    startTransition(() => {
      if (s) setShadowML(s);
    });
  }, []);

  const loadInitialData = useEffectEvent(() => {
    void loadPrimary();
  });

  useEffect(() => {
    loadInitialData();
    let timeoutId: number | null = null;
    let idleId: number | null = null;
    const idleWindow = window as IdleWindow;

    const runDeferred = () => {
      void loadSecondary();
      startTransition(() => {
        setShowDeferredPanels(true);
      });
    };

    if (idleWindow.requestIdleCallback) {
      idleId = idleWindow.requestIdleCallback(() => runDeferred(), { timeout: 1200 });
    } else {
      timeoutId = globalThis.setTimeout(runDeferred, 400);
    }

    return () => {
      if (idleId !== null && idleWindow.cancelIdleCallback) {
        idleWindow.cancelIdleCallback(idleId);
      }
      if (timeoutId !== null) {
        globalThis.clearTimeout(timeoutId);
      }
    };
  }, [loadSecondary]);

  useEffect(() => {
    if (!showDeferredPanels) return;
    void loadDeferred();
  }, [showDeferredPanels, loadDeferred]);

  useInterval(() => {
    loadPrimary();
  }, 30000);

  useInterval(() => {
    loadSecondary();
  }, 60000);

  useInterval(() => {
    if (!showDeferredPanels) return;
    loadDeferred();
  }, showDeferredPanels ? 120000 : null);

  const focusSymbol = dashboard?.positions?.[0]?.symbol?.replace('-USDT', '') || '';

  return (
    <main className="mobile-page-shell">
      <LiquidBg />

      <div className="page-content relative z-10">
        <Hero
          marketState={marketState}
          riskGuard={riskGuard}
          systemStatus={dashboard?.systemStatus || null}
          updateTime={updateTime}
        />

        <MetricsGrid
          account={dashboard?.account || null}
          systemStatus={dashboard?.systemStatus || null}
          focusSymbol={focusSymbol}
        />

        <MLBand mlTraining={dashboard?.mlTraining || null} />

        <div className="dashboard-section">
          <div className="max-w-[1780px] mx-auto grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div className="lg:col-span-2 flex flex-col gap-4">
              <PositionsPanel
                positions={dashboard?.positions || []}
                trades={dashboard?.trades || []}
                account={dashboard?.account || null}
              />
              <MarketRadar marketState={marketState} />
              <SignalsPanel decisionAudit={decisionAudit} />
              {showDeferredPanels ? (
                secondaryReady ? (
                  <Suspense fallback={<DeferredPanelFallback />}>
                    <ExecutionInsightsPanel slippageInsights={dashboard?.slippageInsights || null} />
                  </Suspense>
                ) : (
                  <DeferredPanelFallback />
                )
              ) : null}
            </div>
            <div className="lg:col-span-1">
              <Sidebar
                timers={dashboard?.timers || null}
                alphaScores={dashboard?.alphaScores || []}
                trades={dashboard?.trades || []}
                health={health}
                decisionAudit={decisionAudit}
                apiTelemetry={dashboard?.apiTelemetry || null}
                deferredReady={secondaryReady}
              />
            </div>
          </div>
        </div>

        <div className="dashboard-section mt-4">
          <div className="max-w-[1780px] mx-auto">
            {showDeferredPanels ? (
              <Suspense fallback={<DeferredPanelFallback />}>
                <ShadowMLPanel shadowML={shadowML} />
              </Suspense>
            ) : null}
          </div>
        </div>

        {loading && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="sticky bottom-4 z-50 ml-auto mt-4 flex w-fit items-center gap-2 text-[11px] uppercase tracking-[0.12em] text-[var(--text-dim)]"
          >
            <span className="h-1.5 w-1.5 rounded-full bg-[var(--accent)]/80" />
            <span>刷新中</span>
          </motion.div>
        )}
      </div>
    </main>
  );
}

export default App;
