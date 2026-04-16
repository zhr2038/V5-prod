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
  active?: boolean;
  status?: string;
  last_trigger?: string;
  next_trigger?: string;
  interval_minutes?: number;
  error?: string;
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
  costCalibration: any;
  icDiagnostics: any;
  mlTraining: MLTrainingData;
  reflectionReports: any;
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
  raw?: any;
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
    funding?: any;
    rss?: any;
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
  votes?: any;
}

export interface RiskGuardData {
  current_level: string;
  config: Record<string, any>;
  history: any[];
  metrics: {
    dd_pct?: number;
    last_dd_pct?: number;
    conversion_rate?: number;
    last_conversion_rate?: number;
    [k: string]: any;
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
  signals?: any[];
}

export interface DecisionAuditData {
  run_id?: string;
  strategy_signals?: StrategySignal[];
  counts?: {
    selected?: number;
    orders_rebalance?: number;
    orders_exit?: number;
    [k: string]: any;
  };
  strategy_signal_source?: string;
  ml_signal_overview?: any;
  execution_summary?: any;
  rejected_summary?: any;
  orders?: any[];
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

export interface MLTrainingData {
  status?: string;
  phase?: string;
  stages?: MLStages;
  progress_percent?: number;
  labeled_samples?: number;
  samples_needed?: number;
  last_training_ts?: string;
  promotion_fail_reasons?: string[];
  last_runtime_ts?: string;
  last_ic?: number;
  runtime_prediction_count?: number;
  runtime_reason?: string;
  [k: string]: any;
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
    [k: string]: any;
  };
  [k: string]: any;
}

export interface ShadowMLSymbol {
  symbol: string;
  base_rank?: number;
  final_rank?: number;
  rank_delta?: number;
  return_bps?: number;
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
