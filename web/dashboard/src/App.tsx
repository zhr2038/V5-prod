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

  const loadPrimary = useCallback(async () => {
    if (document.hidden) return;
    setLoading(true);
    const [d, r] = await Promise.all([
      api.dashboard(),
      api.riskGuard(),
    ]);
    if (d) {
      setDashboard(d);
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
        setDashboard((prev) => (prev ? { ...prev, ...deferred } : (deferred as DashboardData)));
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
    void loadSecondary();
  });

  useEffect(() => {
    loadInitialData();
    let timeoutId: number | null = null;
    let idleId: number | null = null;
    const idleWindow = window as IdleWindow;

    const runDeferred = () => {
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
  }, []);

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
    <div className="relative min-h-[100dvh] min-h-[100svh] min-h-screen">
      <LiquidBg />

      <div className="relative z-10 pb-10">
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

        <div className="px-6">
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
                <Suspense fallback={<DeferredPanelFallback />}>
                  <ExecutionInsightsPanel slippageInsights={dashboard?.slippageInsights || null} />
                </Suspense>
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
              />
            </div>
          </div>
        </div>

        <div className="px-6 mt-4">
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
            className="fixed bottom-4 right-4 z-50 text-xs px-3 py-1.5 material-surface material-clear clear-chip tone-pearl"
          >
            刷新中...
          </motion.div>
        )}
      </div>
    </div>
  );
}

export default App;
