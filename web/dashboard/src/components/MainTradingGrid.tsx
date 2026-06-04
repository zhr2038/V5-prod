import { Suspense } from 'react';
import type { ComponentType, ReactNode } from 'react';
import { PositionsPanel } from './PositionsPanel';
import { MarketRadar } from './MarketRadar';
import type { AccountData, MarketStateData, Position, SlippageInsightsData, Trade } from '../types';

interface MainTradingGridProps {
  positions: Position[];
  trades: Trade[];
  account?: AccountData | null;
  marketState?: MarketStateData | null;
  slippageInsights?: SlippageInsightsData | null;
  showDeferredPanels: boolean;
  secondaryReady: boolean;
  fallback: ReactNode;
  ExecutionInsightsPanel: ComponentType<{ slippageInsights?: SlippageInsightsData | null }>;
}

export function MainTradingGrid({
  positions,
  trades,
  account,
  marketState,
  slippageInsights,
  showDeferredPanels,
  secondaryReady,
  fallback,
  ExecutionInsightsPanel,
}: MainTradingGridProps) {
  return (
    <main className="main-trading-grid">
      <div className="trading-primary-stack">
        <PositionsPanel positions={positions} trades={trades} account={account || null} />
        <div className="analytics-row">
          <MarketRadar marketState={marketState || null} />
          {showDeferredPanels ? (
            secondaryReady ? (
              <Suspense fallback={fallback}>
                <ExecutionInsightsPanel slippageInsights={slippageInsights || null} />
              </Suspense>
            ) : (
              fallback
            )
          ) : null}
        </div>
      </div>
    </main>
  );
}
