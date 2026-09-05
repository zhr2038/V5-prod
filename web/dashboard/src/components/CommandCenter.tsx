import { lazy, Suspense, useEffect, useState } from 'react';
import { Activity, ArrowDownLeft, ArrowRight, ArrowUpRight, BarChart3, Check, ChevronDown,
  CircleDot, Clock3, ExternalLink, FlaskConical, Layers3, LayoutDashboard, RefreshCw,
  ShieldCheck, Terminal, Wallet, X } from 'lucide-react';
import type { CommandCenterData, CommandHealth } from '../commandTypes';
import type { DashboardData, DecisionAuditData, EquityPoint, HealthData, MarketStateData,
  QuantLabCostEstimateData, QuantLabPermissionData, QuantLabStatusData, RiskGuardData, TimerData } from '../types';
import { age, beijingEpoch, countQualifier, dateTime, epoch, finite, number, observationState, ratio, reasonLabel, regimeLabels, riskLabels,
  signed, statusLabels } from '../lib/commandFormat';
import './command-center.css';

const PositionsPanel = lazy(() => import('./PositionsPanel').then(m => ({ default: m.PositionsPanel })));
const BundleExportPanel = lazy(() => import('./BundleExportPanel').then(m => ({ default: m.BundleExportPanel })));
const EquityHistoryChart = lazy(() => import('./EquityHistoryChart'));

interface Props {
  dashboard: DashboardData | null;
  command: CommandCenterData | null;
  commandFailed: boolean;
  commandReceivedAt: number | null;
  primaryReceivedAt: number | null;
  tradesFailed: boolean;
  tradesReceivedAt: number | null;
  secondaryFailed: boolean;
  secondaryReceivedAt: number | null;
  deferredReceivedAt: number | null;
  equity: EquityPoint[];
  equityLoading: boolean;
  equityFailed: boolean;
  riskGuard: RiskGuardData | null;
  decisionAudit: DecisionAuditData | null;
  marketState: MarketStateData | null;
  health: HealthData | null;
  quantLabStatus: QuantLabStatusData | null;
  quantLabPermission: QuantLabPermissionData | null;
  quantLabCost: QuantLabCostEstimateData | null;
  focusSymbol: string;
  loading: boolean;
  refreshFailed: boolean;
  updateTime: string;
  onRefresh: () => void;
  onSymbolSearch: (symbol: string) => void;
}

const navigation = [
  { id: 'overview', label: '交易总览', icon: LayoutDashboard, index: '01' },
  { id: 'opportunities', label: '行情与机会', icon: BarChart3, index: '02' },
  { id: 'positions', label: '持仓与成交', icon: Wallet, index: '03' },
  { id: 'participation', label: '策略验证', icon: FlaskConical, index: '04' },
  { id: 'operations', label: '系统运行', icon: Activity, index: '05' },
];
const currencies: Record<string, string> = { BTC: '₿', ETH: 'Ξ', SOL: 'S', BNB: 'B' };
const timerNames: Record<string, string> = {
  'v5-prod.user.timer': '实盘决策', 'v5-event-driven.timer': '事件检查',
  'v5-auto-risk-eval.timer': '风控评估', 'v5-reconcile.timer': '账户对账',
  'v5-ledger.timer': '账本同步', 'v5-sentiment-collect.timer': '情绪采集',
  'v5-quant-lab-selfcheck.timer': '研究数据检查',
};
function baseSymbol(symbol: string) { return symbol.replace(/[-/]?USDT$/, ''); }
function Status({ children, tone = 'muted' }: { children: React.ReactNode; tone?: string }) {
  return <span className={`cc-status cc-${tone}`}><i />{children}</span>;
}
function SectionTitle({ index, title, aside, detail }: { index: string; title: string; aside?: React.ReactNode; detail?: string }) {
  return <div className="cc-section-title"><div><span className="cc-section-index">{index}</span><h2>{title}</h2>{detail && <p>{detail}</p>}</div>{aside}</div>;
}
function LoadingNote({ text = '正在读取真实数据…' }: { text?: string }) { return <div className="cc-empty cc-loading-note"><Activity size={22} /><span>{text}</span></div>; }
function healthTone(item?: CommandHealth): string {
  if (!item) return 'muted';
  if (['observed', 'healthy', 'ok', 'fresh'].includes(item.status) && item.ok !== false) return 'good';
  return ['failed', 'future', 'critical', 'invalid'].includes(item.status) ? 'bad' : 'amber';
}
function countdown(timer: TimerData | undefined, now: number, received: number | null) {
  if (!timer) return '—';
  if (!timer.active) return '已暂停';
  // This API's documented timezone-free next_run is server CST, never browser local time.
  const label = timer.next_run || timer.next_trigger;
  let ts = epoch(label);
  if (ts === null && label && /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(label)) ts = Date.parse(label.replace(' ', 'T') + '+08:00');
  const seconds = ts !== null ? (ts - now) / 1000 : finite(timer.countdown_seconds) !== null && received !== null
    ? Number(timer.countdown_seconds) - (now - received) / 1000 : null;
  if (seconds === null) return '时间待确认';
  if (seconds < 0) return '等待周期更新';
  const n = Math.ceil(seconds);
  return `${String(Math.floor(n / 3600)).padStart(2, '0')}:${String(Math.floor(n % 3600 / 60)).padStart(2, '0')}:${String(n % 60).padStart(2, '0')}`;
}

export function CommandCenter(props: Props) {
  const { dashboard, command, commandFailed, equity, equityLoading, equityFailed, riskGuard,
    decisionAudit, marketState, health, quantLabStatus, quantLabPermission, quantLabCost,
    loading, refreshFailed, onRefresh, onSymbolSearch } = props;
  const [now, setNow] = useState(() => Date.now());
  const [activeSection, setActiveSection] = useState('overview');
  const [chartSymbol, setChartSymbol] = useState<string | null>(null);
  const [tradeFilter, setTradeFilter] = useState('all');
  const [showAllTrades, setShowAllTrades] = useState(false);
  const [showEvidence, setShowEvidence] = useState(false);
  useEffect(() => {
    const tick = window.setInterval(() => setNow(Date.now()), 1000);
    const observer = new IntersectionObserver(entries => {
      const visible = entries.filter(e => e.isIntersecting).sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
      if (visible[0]) setActiveSection(visible[0].target.id);
    }, { rootMargin: '-70px 0px -55% 0px', threshold: 0 });
    navigation.forEach(item => { const el = document.getElementById(item.id); if (el) observer.observe(el); });
    return () => { window.clearInterval(tick); observer.disconnect(); };
  }, []);

  const account = dashboard?.account;
  const positions = dashboard?.positions;
  const knownPositions = Boolean(dashboard && dashboard.positionsObserved !== false && Array.isArray(positions));
  const hasPositions = knownPositions && positions!.length > 0;
  const totalEquity = finite(account?.totalEquity);
  const cash = finite(account?.cash);
  const value = finite(account?.positionsValue);
  const cashShare = totalEquity !== null && totalEquity > 0 && cash !== null ? Math.max(0, Math.min(1, cash / totalEquity)) : null;
  const exposure = totalEquity !== null && totalEquity > 0 && value !== null ? value / totalEquity : null;
  const riskLevel = command?.health.risk.level || riskGuard?.current_level || '';
  const drawdown = finite(command?.health.risk.dd_pct ?? riskGuard?.metrics.dd_pct ?? riskGuard?.metrics.last_dd_pct ?? account?.maxDrawdown);
  const run = command?.latest_decision;
  const windowData = command?.window_72h;
  const runTs = epoch(run?.window_end_ts);
  const runState = observationState(run?.status, runTs, now, 90 * 60000);
  const runStale = runState !== 'observed';
  const snapshotTs = epoch(command?.generated_at);
  const commandStale = commandFailed || observationState(undefined, snapshotTs, now, 90000) !== 'observed';
  const status = dashboard?.systemStatus;
  const stopped = status?.killSwitch === true || command?.health.kill_switch.enabled === true;
  const primaryState = observationState(undefined, beijingEpoch(status?.lastUpdate), now, 2 * 60000);
  const primaryBad = refreshFailed || Boolean(status?.errors?.length) || (dashboard !== null && primaryState !== 'observed');
  const secondaryStale = props.secondaryFailed || observationState(undefined, props.secondaryReceivedAt, now, 2 * 60000) !== 'observed';
  const serviceHealthState = observationState(health?.status, epoch(health?.timestamp), now, 2 * 60000);
  const title = stopped ? '入场已暂停' : primaryBad ? '账户状态需确认' : !knownPositions ? '正在确认账户状态' : hasPositions ? `${positions!.length} 个持仓，持续跟踪` : '当前空仓，等待可执行机会';
  const candidateReasons = command?.candidates.flatMap(c => [...(c.selection_reasons || []), ...(c.router_reasons || [])]) || [];
  const immediateReason = candidateReasons.find(r => !r.startsWith('btc_leadership')) || candidateReasons[0];
  const selectedThisRun = finite(decisionAudit?.counts?.selected);
  const explanation = runStale || commandStale ? '决策数据需要更新，请以最后观测时间为准。'
    : stopped ? '停止开关已生效；查看系统运行区的实际阻断状态。'
    : immediateReason ? `本轮记录：${reasonLabel(immediateReason)}`
    : command && selectedThisRun === 0 ? '本轮没有形成目标仓位，继续等待下一次决策。'
    : hasPositions ? '查看持仓成本、浮动盈亏与最新决策。'
    : '候选、风控、执行各环节的真实结果见下方。';
  const timers = dashboard?.timers?.timers || [];
  const nextTimer = timers.find(t => t.name === 'v5-prod.user.timer');
  const latestTrades = [...(dashboard?.trades || [])].sort((a, b) => (epoch(b.timestamp) || Date.parse(b.timestamp) || 0) - (epoch(a.timestamp) || Date.parse(a.timestamp) || 0));
  const latestFill = decisionAudit?.recent_fill_summary?.latest_fill;
  const lastFillTs = latestFill?.created_ts ?? latestTrades[0]?.timestamp;
  const filteredTrades = latestTrades.filter(t => tradeFilter === 'all' || t.side === tradeFilter);
  const participation = command?.participation;
  const paperTs = epoch(participation?.observed_at);
  const paperStatus = observationState(participation?.status, paperTs, now, 90 * 60000);
  const paperObserved = paperStatus === 'observed';
  const chartCandidate = command?.candidates.find(candidate => candidate.symbol === chartSymbol);
  const regime = run?.regime || marketState?.state || '';
  const marketLabel = regimeLabels[regime] || regimeLabels[regime === 'TRENDING' ? 'Trending' : regime === 'SIDEWAYS' ? 'Sideways' : regime] || regime || '待确认';
  const counts = [
    { name: '候选出现', count: windowData?.selected_candidates, note: '同一币种跨周期分别计数' },
    { name: '生成调仓订单', count: windowData?.generated_orders, note: '程序产生的订单意图' },
    { name: '真实成交订单', count: windowData?.actual_filled_orders, note: '按交易所订单号去重' },
  ];
  const mainBlockers = (command?.blockers || []).slice(0, 5);
  const maxBlockCount = Math.max(1, ...mainBlockers.map(b => b.count));
  const uncertainty = command?.warnings || [];

  return <div className="cc-shell">
    <a className="cc-skip" href="#overview">跳转至交易总览</a>
    <aside className="cc-sidebar">
      <a className="cc-brand" href="#overview" aria-label="V5 交易控制台"><span className="cc-brand-mark">V<span>5</span><i /></span><span>交易控制台<small>TRADING WORKSPACE</small></span></a>
      <span className="cc-nav-caption">工作空间</span>
      <nav aria-label="主导航">{navigation.map(item => <a key={item.id} href={`#${item.id}`} className={activeSection === item.id ? 'is-active' : ''} onClick={() => setActiveSection(item.id)} aria-current={activeSection === item.id ? 'location' : undefined}><item.icon size={17} /><span>{item.label}</span><small>{item.index}</small></a>)}</nav>
      <div className="cc-sidebar-bottom"><div className="cc-connection"><span className="cc-connection-dot" /><span>OKX <b>SPOT</b></span></div><p>生产环境 · qyun</p><button onClick={() => { setShowEvidence(true); document.getElementById('operations')?.scrollIntoView({ behavior: 'smooth' }); }}><Layers3 size={15} /> 运行证据与导出 <ArrowUpRight size={14} /></button><small>所有时间均为北京时间</small></div>
    </aside>

    <div className="cc-main">
      <header className="cc-topbar"><div className="cc-breadcrumb"><span>V5</span><i>/</i><span>{navigation.find(n => n.id === activeSection)?.label || '交易总览'}</span></div><div className="cc-topbar-right"><Status tone={primaryBad ? 'amber' : status?.isRunning ? 'good' : 'muted'}>{primaryBad ? '刷新异常' : status?.isRunning ? '服务运行中' : '服务待确认'}</Status><span className="cc-refresh-time">{props.updateTime ? `${props.updateTime} 更新` : '连接数据源'}</span><button className="cc-icon-button" onClick={onRefresh} disabled={loading} title="立即刷新数据" aria-label="立即刷新数据"><RefreshCw size={16} className={loading ? 'cc-spinning' : ''} /></button></div></header>

      <main className="cc-content">
        <section id="overview" className="cc-overview cc-enter">
          <div className="cc-page-heading"><div><div className="cc-eyebrow">ACCOUNT & EXECUTION</div><h1>交易总览<span className="cc-live-label">{status?.mode === 'live' ? '实盘' : status?.mode === 'paper' ? '模拟盘' : status?.mode === 'dry_run' ? '试运行' : '模式待确认'}</span></h1></div><span className="cc-today">{new Date(now).toLocaleDateString('zh-CN', { timeZone: 'Asia/Shanghai', year: 'numeric', month: 'long', day: 'numeric', weekday: 'short' })}</span></div>
          {(primaryBad || commandStale || runStale) && <div className="cc-alert" role="status"><Activity size={16} /><span>{primaryBad ? '账户快照过期、刷新失败或返回异常。保留的数值不代表当前资金状态。' : '决策数据待确认、过期或时间异常，以下为最后一次可见数据。'}</span><button onClick={onRefresh}>重试 <ArrowRight size={13} /></button></div>}
          <div className="cc-current-state"><div className="cc-state-heading"><div className={`cc-state-symbol ${stopped ? 'cc-bad' : ''}`}><CircleDot size={24} /></div><div><div className="cc-current-title">{title}</div><p>{explanation}</p></div></div><div className="cc-next-run"><span><Clock3 size={13} /> 下一次实盘决策</span><strong>{countdown(nextTimer, now, props.deferredReceivedAt)}</strong><small>{secondaryStale ? '调度信息需刷新' : nextTimer?.active ? '每小时自动运行' : nextTimer ? '定时器未激活' : '正在读取调度信息'}</small></div></div>
          <div className="cc-metric-row">
            <div className="cc-metric cc-metric-primary"><span>账户总权益 <small>USDT</small></span><strong>{number(totalEquity)}<i>USDT</i></strong><p>资金快照 {dateTime(status?.lastUpdate)}</p></div>
            <div className="cc-metric"><span>有效持仓市值</span><strong>{number(value)}<i>USDT</i></strong><p>{knownPositions ? `${positions!.length} 个持仓` : '持仓待确认'}<em>·</em>仓位占比 {ratio(exposure)}</p></div>
            <div className="cc-metric"><span>历史峰值回撤</span><strong className={drawdown !== null && drawdown > 0 ? 'cc-negative' : ''}>{ratio(drawdown, 2)}</strong><p>{riskLabels[riskLevel] || '风险档位待确认'}<em>·</em><code>{riskLevel || '—'}</code></p></div>
            <div className="cc-metric"><span>近 72 小时真实成交</span><strong>{number(windowData?.actual_filled_orders.value, 0)}<i>笔订单</i></strong><p>{countQualifier(windowData?.actual_filled_orders.status) && <span className="cc-amber">{countQualifier(windowData?.actual_filled_orders.status)} · </span>}最后成交 {lastFillTs ? age(lastFillTs, now) : '待确认'}</p></div>
          </div>

          <div className="cc-overview-grid"><div className="cc-equity-region"><Suspense fallback={<LoadingNote text="正在载入权益图…" />}><EquityHistoryChart points={equity} currentEquity={totalEquity} loading={equityLoading} failed={equityFailed} /></Suspense></div><aside className="cc-capital"><div className="cc-panel-heading"><h2>资金分布</h2><Wallet size={16} /></div><div className="cc-cash-head"><span>现金占比</span><strong>{ratio(cashShare, 2)}</strong></div><div className="cc-capital-bar" aria-label={`现金占比 ${ratio(cashShare, 2)}`}><span style={{ width: `${(cashShare ?? 0) * 100}%` }} /></div><dl className="cc-key-values"><div><dt><i className="cc-swatch" />现金</dt><dd>{number(cash)} <small>USDT</small></dd></div><div><dt><i className="cc-swatch muted" />有效持仓</dt><dd>{number(value)} <small>USDT</small></dd></div><div><dt>其他余额 / 差额</dt><dd>{totalEquity !== null && cash !== null && value !== null ? number(totalEquity - cash - value) : '—'} <small>USDT</small></dd></div></dl><div className="cc-capital-note"><ShieldCheck size={17} /><p>账户权益来自资金快照。<br />权益变化可能包含出入金，不能直接视为交易净收益。</p></div></aside></div>
        </section>

        <section id="opportunities" className="cc-section">
          <SectionTitle index="02" title="行情与机会" detail="看清每个候选，以及交易停在哪一步。" aside={<Status tone={runStale ? 'amber' : 'muted'}>{runStale ? `决策${statusLabels[runState] || runState}` : marketLabel}</Status>} />
          <div className="cc-run-meta"><span><CircleDot size={12} /> 最近决策 <code>{run?.run_id || decisionAudit?.run_id || '—'}</code></span><span>信号截止 {dateTime(run?.window_end_ts)}<em>·</em>{run?.window_end_ts ? age(run.window_end_ts, now) : '时间待确认'}</span></div>
          <div className="cc-table-scroll"><table className="cc-table cc-market-table"><thead><tr><th>交易标的</th><th>决策参考价 <small>USDT</small></th><th>Alpha 排序分</th><th>目标仓位</th><th>本轮决策依据</th><th><span className="cc-visually-hidden">查看行情</span></th></tr></thead><tbody>{(command?.candidates || []).map(candidate => { const symbol = baseSymbol(candidate.symbol); const reasons = [...new Set([...(candidate.selection_reasons || []), ...(candidate.router_reasons || [])])]; return <tr key={candidate.symbol}><td><button className="cc-symbol-button" onClick={() => { setChartSymbol(candidate.symbol); onSymbolSearch(candidate.symbol); }}><span className={`cc-coin cc-coin-${symbol.toLowerCase()}`}>{currencies[symbol] || symbol[0]}</span><span><b>{symbol}</b><small>{candidate.symbol} · 现货</small></span></button></td><td className="cc-numeric">{number(candidate.reference_price, 2)}</td><td className="cc-numeric">{number(candidate.alpha_score, 4)}</td><td className="cc-numeric">{ratio(candidate.target_weight)}</td><td><span className={`cc-reason ${reasons.length ? 'cc-amber' : ''}`} title={reasons.join('\n')}>{reasons.length ? reasonLabel(reasons[0]) : candidate.target_weight && candidate.target_weight > 0 ? '已形成目标，执行结果见成交' : candidate.target_weight === null ? '目标仓位不可观测' : '未形成目标仓位'}</span>{reasons.length > 1 && <small className="cc-more-reasons">另有 {reasons.length - 1} 项依据</small>}</td><td><button className="cc-icon-button" aria-label={`查看${symbol}行情`} onClick={() => { setChartSymbol(candidate.symbol); onSymbolSearch(candidate.symbol); }}><ArrowUpRight size={16} /></button></td></tr>; })}</tbody></table></div>
          {!command?.candidates.length && <LoadingNote text={command ? '本轮没有可见的候选数据' : '正在读取候选与决策…'} />}
          <div className="cc-table-note">参考价属于上方决策时点；排序分用于比较候选，不代表预期收益。点击币种查看市场 K 线。</div>
          {chartSymbol && <div className="cc-market-detail"><div className="cc-detail-title"><h3>{baseSymbol(chartSymbol)} 市场详情</h3><button className="cc-icon-button" onClick={() => setChartSymbol(null)} aria-label="收起市场详情"><X size={17} /></button></div>{chartCandidate && <div className="cc-candidate-detail"><dl><div><dt>决策参考价</dt><dd>{number(chartCandidate.reference_price)} USDT</dd></div><div><dt>Alpha 排序分</dt><dd>{number(chartCandidate.alpha_score, 4)}</dd></div><div><dt>目标仓位</dt><dd>{ratio(chartCandidate.target_weight)}</dd></div><div><dt>报价时点</dt><dd>{dateTime(chartCandidate.price_as_of)}</dd></div></dl><p>{[...new Set([...(chartCandidate.selection_reasons || []), ...(chartCandidate.router_reasons || [])])].map(reason => <span key={reason} title={reason}>{reasonLabel(reason)}</span>)}</p></div>}<Suspense fallback={<LoadingNote />}><PositionsPanel positions={positions} trades={dashboard?.trades} account={account} focusSymbol={chartSymbol} /></Suspense></div>}
          <div className="cc-decision-grid"><div className="cc-funnel"><div className="cc-panel-heading"><h3>72 小时参与情况</h3><small>{windowData ? `${windowData.observed_runs} / ${windowData.expected_runs} 轮可见${windowData.coverage_status !== 'complete' ? ' · ' + (statusLabels[windowData.coverage_status] || windowData.coverage_status) : ''}` : '读取中'}</small></div><div className="cc-funnel-counts">{counts.map((item, i) => <div key={item.name}><span>{item.name}</span><strong>{number(item.count?.value, 0)}</strong>{countQualifier(item.count?.status) && <span className="cc-count-quality">{countQualifier(item.count?.status)}</span>}<small>{item.note}</small>{i < 2 && <ArrowRight className="cc-funnel-arrow" size={17} />}</div>)}</div><p className="cc-table-note">各环节统计单位不同，不能直接相除作为成交率。统计区间 {dateTime(windowData?.start_ts)} — {dateTime(windowData?.end_ts)}。</p></div><div className="cc-blockers"><div className="cc-panel-heading"><h3>主要拦截原因</h3><small>72 小时 · 路由事件</small></div>{mainBlockers.map(blocker => <div className="cc-blocker" key={blocker.reason}><div><span title={blocker.reason}>{reasonLabel(blocker.reason)}</span><b>{blocker.count}<small>次</small></b></div><div className="cc-blocker-track"><i style={{ width: `${blocker.count / maxBlockCount * 100}%` }} /></div></div>)}{!mainBlockers.length && <p className="cc-muted">{command ? '统计区间内未见拦截记录' : '拦截记录读取中'}</p>}</div></div>
        </section>

        <section id="positions" className="cc-section"><SectionTitle index="03" title="持仓与成交" detail="以真实账户与交易所成交记录为准。" aside={<span className="cc-small-badge">REAL CAPITAL</span>} />
          <div className="cc-position-region">{primaryBad && hasPositions && <p className="cc-table-note cc-amber">上次可见持仓 · 当前状态待确认</p>}{!knownPositions ? <LoadingNote text="正在确认真实持仓…" /> : !hasPositions ? <div className="cc-flat-state"><div className="cc-flat-icon"><Layers3 size={27} /></div><div><h3>{primaryBad ? '上次快照未见有效持仓' : '当前没有有效持仓'}</h3><p>{primaryBad ? '当前持仓待确认；以下余额来自最后可见快照。' : '资金主要以现金持有。未达可执行金额的零钱不计入有效仓位。'}</p></div><div className="cc-flat-cash"><span>现金余额</span><b>{number(cash)} <small>USDT</small></b></div></div> : <div className="cc-table-scroll"><table className="cc-table"><thead><tr><th>币种</th><th>数量</th><th>平均成本</th><th>参考现价</th><th>市值 USDT</th><th>浮动盈亏 USDT</th><th>建仓时间</th></tr></thead><tbody>{positions!.map(p => <tr key={p.symbol}><td><b>{p.symbol}</b></td><td className="cc-numeric">{number(p.qty, 6)}</td><td className="cc-numeric">{number(p.avgPrice)}</td><td className="cc-numeric">{number(p.currentPrice)}</td><td className="cc-numeric">{number(p.value)}</td><td className={`cc-numeric ${p.pnl < 0 ? 'cc-negative' : 'cc-positive'}`}>{signed(p.pnl)}</td><td>{dateTime(p.entryTime)}</td></tr>)}</tbody></table></div>}</div>
          <div className="cc-trade-heading"><h3>最近成交</h3><div className="cc-segmented" aria-label="成交方向筛选">{[['all', '全部'], ['buy', '买入'], ['sell', '卖出']].map(([key, label]) => <button key={key} aria-pressed={tradeFilter === key} onClick={() => setTradeFilter(key)}>{label}</button>)}</div></div>
          <div className="cc-table-scroll"><table className="cc-table cc-trade-table"><thead><tr><th>成交时间 <small>北京</small></th><th>币种</th><th>方向</th><th>成交均价</th><th>成交数量</th><th>金额 <small>USDT</small></th><th>手续费 <small>USDT</small></th></tr></thead><tbody>{filteredTrades.slice(0, showAllTrades ? 50 : 5).map((trade, i) => <tr key={`${trade.id}-${i}`}><td className="cc-trade-time">{dateTime(trade.timestamp)}</td><td><b>{baseSymbol(trade.symbol)}</b></td><td><span className={`cc-trade-side ${trade.side === 'buy' ? 'buy' : 'sell'}`}>{trade.side === 'buy' ? <ArrowDownLeft size={13} /> : <ArrowUpRight size={13} />}{trade.side === 'buy' ? '买入' : trade.side === 'sell' ? '卖出' : trade.side}</span></td><td className="cc-numeric">{number(trade.price)}</td><td className="cc-numeric">{number(trade.qty, 6)}</td><td className="cc-numeric">{number(trade.value)}</td><td className="cc-numeric cc-muted">{number(trade.fee, 4)}</td></tr>)}</tbody></table></div>
          {props.tradesFailed && <p className="cc-table-note cc-amber">成交刷新失败，保留上次可见记录。最近读取 {dateTime(props.tradesReceivedAt)}。</p>}
          {!filteredTrades.length && <p className="cc-empty-text">{props.tradesReceivedAt === null ? '成交记录尚未确认' : '暂无该方向的可见成交记录'}</p>}
          <div className="cc-trade-footer"><span>同一订单的分笔成交合并显示；买入和卖出分别记录。</span>{filteredTrades.length > 5 && <button onClick={() => setShowAllTrades(!showAllTrades)}>{showAllTrades ? '收起' : `查看全部 ${Math.min(50, filteredTrades.length)} 条`}<ChevronDown size={13} className={showAllTrades ? 'is-up' : ''} /></button>}</div>
        </section>

        <section id="participation" className="cc-section"><SectionTitle index="04" title="新策略 · 前瞻验证" detail="使用后续真实报价独立记账，验证新的入场与持有逻辑。" aside={<span className="cc-small-badge cc-paper-badge"><FlaskConical size={13} /> 模拟资金</span>} />
          <div className="cc-paper-grid"><div className="cc-paper-summary"><div className="cc-paper-top"><Status tone={paperStatus === 'observed' ? 'good' : paperStatus === 'disabled' ? 'muted' : 'amber'}>{statusLabels[paperStatus || 'unknown'] || paperStatus}</Status><span>不影响实盘订单</span></div><h3>{participation?.latest_decision?.reason ? reasonLabel(participation.latest_decision.reason) : participation?.enabled === false ? '策略观察尚未启用' : paperStatus === 'missing' ? '等待首个有效小时周期' : '等待前瞻决策数据'}</h3><p>先判断绝对趋势，再筛选可执行预算，最后按 Alpha 排序选币。模拟组合与实盘资金分开核算。</p><div className="cc-paper-metrics"><div><span>模拟权益 <small>USDT</small></span><b>{number(participation?.equity_usdt)}</b></div><div><span>已实现净收益 <small>USDT</small></span><b className={(participation?.net_realized_pnl_usdt ?? 0) < 0 ? 'cc-negative' : ''}>{signed(participation?.net_realized_pnl_usdt)}</b></div><div><span>完成闭环</span><b>{number(participation?.closed_trade_count, 0)}<small>笔</small></b></div></div><div className="cc-paper-timestamp">最后观测 {dateTime(participation?.observed_at)}<em>·</em>{statusLabels[participation?.valuation_status || ''] || participation?.valuation_status || '估值待确认'}</div></div><div className="cc-validation-path"><h3>验证进度</h3><ol><li className={participation?.enabled ? 'complete' : ''}><span>{participation?.enabled ? <Check size={13} /> : '1'}</span><div><b>前瞻观察配置</b><p>{participation?.enabled === true ? '已启用 · 独立虚拟组合' : participation?.enabled === false ? '未启用' : '配置待确认'}</p></div></li><li className={paperObserved ? 'complete' : 'current'}><span>{paperObserved ? <Check size={13} /> : '2'}</span><div><b>真实周期观测</b><p>{paperObserved ? `${number(participation?.entry_count, 0)} 次模拟入场 · ${age(participation?.observed_at, now)}` : paperStatus === 'missing' ? '等待正常小时任务写入' : `观测待确认 · ${statusLabels[paperStatus] || paperStatus}`}</p></div></li><li className="pending"><span>3</span><div><b>收益质量评估</b><p>观察净收益、成本与回撤；闭环数量本身不代表通过。</p></div></li><li className="pending"><span>4</span><div><b>实盘切换</b><p>尚未授权，不会自动扩大实盘风险。</p></div></li></ol></div></div>
          {participation?.latest_decision?.candidate_reasons && <details className="cc-disclosure"><summary>查看新策略逐币决策依据<ChevronDown size={14} /></summary><div className="cc-paper-reasons">{Object.entries(participation.latest_decision.candidate_reasons).map(([symbol, reason]) => <div key={symbol}><b>{symbol}</b><span>{reasonLabel(reason)}</span><code>{reason}</code></div>)}</div></details>}
        </section>

        <section id="operations" className="cc-section"><SectionTitle index="05" title="系统运行" detail="区分程序健康、风险状态与数据是否足够新鲜。" aside={<span className="cc-small-badge"><Terminal size={13} /> 只读监控</span>} />
          <div className="cc-health-strip">{([
            ['kill_switch', '停止开关', command?.health.kill_switch], ['reconcile', '账户对账', command?.health.reconcile],
            ['ledger', '成交账本', command?.health.ledger], ['risk', '风控评估', command?.health.risk],
          ] as [string, string, CommandHealth | undefined][]).map(([key, label, item]) => <div key={key}><div><span>{label}</span><Status tone={key === 'kill_switch' && item?.enabled ? 'bad' : healthTone(item)}>{key === 'kill_switch' ? item?.enabled === true ? '已触发' : item?.enabled === false ? '未触发' : '待确认' : key === 'risk' ? riskLabels[item?.level || ''] || statusLabels[item?.status || 'unknown'] : statusLabels[item?.status || 'unknown'] || item?.status}</Status></div><small>{item?.observed_at ? `${key === 'kill_switch' ? '最后变更 ' : ''}${dateTime(item.observed_at)} · ${age(item.observed_at, now)}` : '观测时间不可用'}</small></div>)}</div>
          <div className="cc-ops-grid"><div><div className="cc-panel-heading"><h3>自动任务</h3><small className={secondaryStale ? 'cc-amber' : ''}>{secondaryStale ? '上次可见调度 · 待刷新' : '以服务器定时器为准'}</small></div><div className="cc-timer-list">{timers.map(timer => <div key={timer.name}><div><span className={`cc-task-dot ${timer.active ? 'active' : ''}`} /><b>{timerNames[timer.name] || timer.desc || timer.name}</b><small>{timer.active ? '已激活' : '未激活'}</small></div><time>{timer.next_run ? dateTime(timer.next_run) : timer.next_trigger || '下次运行待确认'}</time></div>)}</div>{!timers.length && <p className="cc-muted">调度信息读取中</p>}</div><div className="cc-data-inspector"><div className="cc-panel-heading"><h3>数据与执行质量</h3><Status tone={!secondaryStale && serviceHealthState === 'observed' ? 'good' : health ? 'amber' : 'muted'}>{secondaryStale ? '检查数据需刷新' : serviceHealthState === 'observed' ? '服务检查通过' : statusLabels[serviceHealthState] || serviceHealthState}</Status></div>{secondaryStale && <p className="cc-table-note cc-amber">以下为最后可见的遥测，尚未确认当前状态。</p>}<dl className="cc-key-values"><div><dt>OKX 请求成功率</dt><dd>{dashboard?.apiTelemetry?.successRate == null ? '—' : ratio(dashboard.apiTelemetry.successRate, 1)}</dd></div><div><dt>OKX P95 延迟</dt><dd>{number(dashboard?.apiTelemetry?.p95LatencyMs, 0)} <small>ms</small></dd></div><div><dt>实际滑点样本</dt><dd>{number(dashboard?.slippageInsights?.sampleCount, 0)} <small>笔</small></dd></div><div><dt>研究服务模式</dt><dd>{command?.quant_lab?.mode === 'advisory' ? '仅供参考' : command?.quant_lab?.mode === 'enforced' ? '参与拦截' : command?.quant_lab?.mode || quantLabStatus?.mode || '待确认'}</dd></div><div><dt>研究权限</dt><dd>{command?.quant_lab?.permission || quantLabPermission?.permission || quantLabPermission?.decision || '待确认'}</dd></div><div><dt>研究权限参与实盘拦截</dt><dd>{command?.quant_lab?.permission_gate_enforced === true ? '是' : command?.quant_lab?.permission_gate_enforced === false ? '否 · 仅供参考' : '待确认'}</dd></div><div><dt>成本可用于实盘</dt><dd className="cc-amber">{quantLabCost?.cost_trusted_for_live === true && quantLabCost?.cost_stale === false ? '接口标记可信' : '尚未确认'}</dd></div></dl><p className="cc-table-note">服务正常不等于策略获利，也不代表研究数据具备实盘授权。</p></div></div>
          {!!uncertainty.length && <details className="cc-disclosure"><summary><span>数据说明与缺失项 <b>{uncertainty.length}</b></span><ChevronDown size={14} /></summary><ul className="cc-warning-list">{uncertainty.map((warning, i) => <li key={i}>{warning}</li>)}</ul></details>}
          <button className="cc-evidence-toggle" onClick={() => setShowEvidence(!showEvidence)} aria-expanded={showEvidence}><Layers3 size={16} /><span>运行证据与导出<small>下载原始运行记录，用于复核与分析</small></span><ChevronDown size={16} className={showEvidence ? 'is-up' : ''} /></button>
          {showEvidence && <div className="cc-legacy-detail"><Suspense fallback={<LoadingNote />}><BundleExportPanel /></Suspense></div>}
        </section>
        <footer className="cc-footer"><span><b>V5</b> Trading workspace <em>·</em> qyun 生产环境</span><span>实盘与模拟资金独立展示 <ExternalLink size={11} /></span></footer>
      </main>
    </div>
  </div>;
}
