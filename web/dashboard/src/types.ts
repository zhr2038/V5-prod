export type UnknownRecord = Record<string, unknown>;
export type MetricValue = string | number | boolean | null | undefined;
export type MetricRecord = Record<string, MetricValue>;

export interface AccountData {
  totalEquity: number;
  cash: number;
  positionsValue: number;
  initialCapital: number;
  totalPnl: number;
  realizedPnl: number;
  totalPnlPercent: number;
  todayPnl: number;
  todayPnlPercent: number;
  sharpeRatio: number;
  maxDrawdown: number;
  winRate: number;
  totalTrades: number;
}

export interface Position {
  symbol: string;
  qty: number;
  avgPrice: number;
  currentPrice: number;
  value: number;
  pnl: number;
  pnlPercent: number;
}

export interface Trade {
  id: string;
  timestamp: string;
  symbol: string;
  side: 'buy' | 'sell' | string;
  type: string;
  price: number;
  qty: number;
  value: number;
  fee: number;
}

export interface AlphaScore {
  symbol: string;
  score: number;
  f1_mom_5d: number;
  f2_mom_20d: number;
  f3_vol_adj: number;
  f4_volume: number;
  f5_rsi: number;
  weight: number;
}

export interface SystemStatus {
  isRunning: boolean;
  mode: 'live' | 'dry_run' | 'paper' | string;
  lastUpdate: string;
  killSwitch: boolean;
  errors: string[];
}

export interface TimerData {
  name: string;
  desc?: string;
  icon?: string;
  enabled?: boolean;
  active?: boolean;
  status?: string;
  active_state?: string;
  unit_file_state?: string;
  last_trigger?: string;
  next_trigger?: string;
  next_run?: string;
  time_left?: string;
  countdown_seconds?: number;
  interval_minutes?: number;
  error?: string;
}

export interface ApiTelemetryErrorData {
  method?: string;
  endpoint?: string;
  statusClass?: string;
  httpStatus?: number | null;
  okxCode?: string | null;
  message?: string;
}

export interface ApiTelemetryData {
  status: string;
  lookbackHours: number;
  totalRequests: number;
  successRate?: number | null;
  errorCount: number;
  rateLimitedCount: number;
  p50LatencyMs?: number | null;
  p95LatencyMs?: number | null;
  lastRequestAt?: string;
  lastErrorAt?: string;
  latestError?: ApiTelemetryErrorData | null;
  note?: string;
}

export interface ApiTelemetrySeriesSample {
  timestamp: string;
  ts_ms?: number;
  request_count: number;
  error_count: number;
  rate_limited_count: number;
  p50_latency_ms?: number | null;
  p95_latency_ms?: number | null;
}

export interface ApiTelemetrySeriesData {
  status: string;
  lookbackHours: number;
  bucketMinutes: number;
  samples: ApiTelemetrySeriesSample[];
  note?: string;
}

export interface SlippageHistogramBin {
  label: string;
  startBps?: number | null;
  endBps?: number | null;
  count: number;
}

export interface SlippageInsightsData {
  status: string;
  lookbackDays: number;
  sampleCount: number;
  actualAvgBps?: number | null;
  actualP50Bps?: number | null;
  actualP90Bps?: number | null;
  actualP95Bps?: number | null;
  actualMinBps?: number | null;
  actualMaxBps?: number | null;
  baselineBps?: number | null;
  baselineLabel?: string;
  baselineMode?: string;
  baselineSourceDay?: string | null;
  bins: SlippageHistogramBin[];
  lastFillAt?: string;
  note?: string;
}

export interface DashboardData {
  account: AccountData;
  positions: Position[];
  trades: Trade[];
  alphaScores: AlphaScore[];
  marketState: MarketStateData;
  systemStatus: SystemStatus;
  equityCurve: EquityPoint[];
  timers: { timers: TimerData[] };
  costCalibration: unknown;
  icDiagnostics: unknown;
  mlTraining: MLTrainingData;
  reflectionReports: unknown;
  apiTelemetry?: ApiTelemetryData | null;
  slippageInsights?: SlippageInsightsData | null;
}

export interface EquityPoint {
  timestamp: string;
  equity: number;
}

export interface MarketVote {
  state?: string;
  confidence?: number;
  weight?: number;
  probs?: Record<string, number>;
  summary?: string;
  summary_short?: string;
  raw?: unknown;
}

export interface MarketStateData {
  state?: string;
  method?: string;
  position_multiplier?: number;
  votes?: {
    hmm?: MarketVote;
    funding?: MarketVote;
    rss?: MarketVote;
  };
  signal_health?: {
    funding?: unknown;
    rss?: unknown;
  };
  alerts?: string[];
  history_24h?: MarketHistoryPoint[];
}

export interface MarketHistoryPoint {
  label: string;
  ts_ms: number;
  final: {
    state: string;
    confidence: number;
    score: number;
  };
  votes?: UnknownRecord;
}

export interface RiskGuardData {
  current_level: string;
  config: UnknownRecord;
  history: unknown[];
  metrics: {
    dd_pct?: number;
    last_dd_pct?: number;
    conversion_rate?: number;
    last_conversion_rate?: number;
    [k: string]: MetricValue;
  };
  reason: string;
  last_update: string;
}

export interface StrategySignal {
  strategy?: string;
  type?: string;
  allocation?: number;
  total_signals?: number;
  buy_signals?: number;
  sell_signals?: number;
  signals?: unknown[];
}

export interface DecisionAuditData {
  run_id?: string;
  strategy_signals?: StrategySignal[];
  counts?: {
    selected?: number;
    orders_rebalance?: number;
    orders_exit?: number;
    [k: string]: MetricValue;
  };
  strategy_signal_source?: string;
  ml_signal_overview?: UnknownRecord;
  execution_summary?: MetricRecord;
  rejected_summary?: MetricRecord;
  orders?: UnknownRecord[];
}

export interface HealthCheckItem {
  name: string;
  status: 'healthy' | 'warning' | 'critical';
  detail: string;
}

export interface HealthData {
  status: 'healthy' | 'warning' | 'critical';
  checks: HealthCheckItem[];
  timestamp: string;
  last_update: string;
  warning_count: number;
  critical_count: number;
}

export interface QuantLabProxyMeta {
  source?: string;
  upstream_path?: string;
  upstream_status_code?: number | null;
  latency_ms?: number | null;
  cache_hit?: boolean;
  sampled_at?: string;
}

export interface QuantLabRequestMetricsData extends UnknownRecord {
  available?: boolean;
  reason?: string;
  lookback_minutes?: number | null;
  mode?: string;
  total?: number | null;
  success_count?: number | null;
  error_count?: number | null;
  fallback_count?: number | null;
  success_rate?: number | null;
  p50_latency_ms?: number | null;
  p95_latency_ms?: number | null;
  max_latency_ms?: number | null;
  avg_latency_ms?: number | null;
  latest_endpoint?: string;
  latest_status_code?: number | null;
  latest_latency_ms?: number | null;
  latest_ts_utc?: string;
}

export interface QuantLabStatusData extends UnknownRecord {
  available?: boolean;
  status?: string;
  service?: string;
  mode?: string;
  proxy?: QuantLabProxyMeta;
  request_metrics?: QuantLabRequestMetricsData | null;
  request_status?: string;
  request_detail?: string;
  data?: UnknownRecord;
}

export interface QuantLabPermissionData extends UnknownRecord {
  available?: boolean;
  strategy?: string;
  version?: string;
  permission?: string;
  decision?: string;
  status?: string;
  permission_status?: string;
  allowed_modes?: unknown[];
  max_gross_exposure?: number | null;
  max_gross_exposure_usdt?: number | null;
  max_single_order_usdt?: number | null;
  max_drawdown_pct?: number | null;
  as_of_ts?: string | null;
  expires_at?: string | null;
  freshness_sec?: number | null;
  ttl_remaining_sec?: number | null;
  reasons?: unknown[];
  risk_reason_codes?: unknown[];
  proxy?: QuantLabProxyMeta;
  data?: UnknownRecord;
}

export interface QuantLabCostEstimateData extends UnknownRecord {
  available?: boolean;
  symbol?: string;
  regime?: string;
  notional_usdt?: number | null;
  quantile?: string;
  fee_bps?: number | null;
  slippage_bps?: number | null;
  spread_bps?: number | null;
  total_cost_bps?: number | null;
  cost_bps?: number | null;
  selected_total_cost_bps?: number | null;
  one_way_all_in_cost_bps?: number | null;
  roundtrip_all_in_cost_bps?: number | null;
  fallback_level?: string | null;
  source?: string | null;
  cost_source?: string | null;
  cost_quality?: string | null;
  cost_trusted_for_paper?: boolean | null;
  cost_trusted_for_live?: boolean | null;
  sample_count?: number | null;
  cost_model_version?: string | null;
  as_of_ts?: string | null;
  proxy?: QuantLabProxyMeta;
  data?: UnknownRecord;
}

export interface QuantLabGateDecisionData extends UnknownRecord {
  available?: boolean;
  alpha_id?: string;
  decision?: string;
  permission?: string;
  recommended_mode?: string;
  status?: string;
  reason?: string;
  reasons?: unknown[];
  proxy?: QuantLabProxyMeta;
  data?: UnknownRecord;
}

export interface MLTrainingData {
  status?: string;
  phase?: string;
  stages?: MLStages;
  progress_percent?: number;
  labeled_samples?: number;
  samples_needed?: number;
  last_training_ts?: string;
  last_promotion_ts?: string;
  promotion_fail_reasons?: string[];
  last_runtime_ts?: string;
  last_ic?: number;
  runtime_prediction_count?: number;
  runtime_reason?: string;
  configured_enabled?: boolean;
  live_overlay_status?: string;
  ml_live_overlay_status?: string;
  research_only?: boolean;
  message?: string;
  [k: string]: unknown;
}

export interface MLStages {
  sampling?: boolean;
  trained?: boolean;
  promoted?: boolean;
  liveActive?: boolean;
}

export interface ShadowMLData {
  available?: boolean;
  error?: string;
  impact_status?: string;
  ml_signal_overview?: {
    impact_status?: string;
    coverage_count?: number;
    active_symbols?: number;
    last_step?: {
      delta_bps?: number;
      promoted_symbols?: ShadowMLSymbol[];
      suppressed_symbols?: ShadowMLSymbol[];
    };
    rolling_24h?: {
      topn_delta_mean_bps?: number;
      points?: number;
    };
    [k: string]: unknown;
  };
  [k: string]: unknown;
}

export interface ShadowMLSymbol {
  symbol: string;
  base_rank?: number;
  final_rank?: number;
  rank_delta?: number;
  return_bps?: number;
}

export interface LiveFollowupBundle {
  name: string;
  size_bytes: number;
  mtime_utc: string;
  sha256?: string;
  sha256_available?: boolean;
  download_url: string;
  sha256_download_url?: string;
  source_dir?: string;
}

export interface LiveFollowupBundlesData {
  ok: boolean;
  bundles: LiveFollowupBundle[];
  count: number;
  limit?: number;
  searched_dirs?: string[];
  last_update?: string;
  error?: string;
}

export interface LiveFollowupBundleGenerateResult {
  ok: boolean;
  return_code?: number;
  elapsed_seconds?: number;
  bundle_path?: string;
  sha256_path?: string;
  sha256?: string;
  size_bytes?: number;
  high_issues?: number;
  medium_issues?: number;
  file_count?: number;
  stdout_tail?: string;
  stderr_tail?: string;
  bundles?: LiveFollowupBundle[];
  error?: string;
}

export interface KlineData {
  timestamp?: number;
  ts?: number;
  time?: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface PositionKlineSummary {
  bars?: number;
  open?: number;
  close?: number;
  high?: number;
  low?: number;
  volume?: number;
  change_pct?: number;
  last_time?: string;
}

export interface PositionKlinePayload {
  symbol?: string;
  timeframe?: string;
  source?: string;
  candles?: KlineData[];
  summary?: PositionKlineSummary;
}
