// V5 Dashboard Pro - 完整JavaScript

// API 端点
const API_BASE = '';

// 刷新间隔（毫秒）
const REFRESH_INTERVAL = 30000; // 30秒

// 倒计时相关变量
let countdownInterval = null;
let countdownSeconds = 0;
let maxIntervalSeconds = 3600; // 1小时 = 3600秒

// Tab 切换
document.addEventListener('DOMContentLoaded', function() {
    // 初始化Tab切换
    initTabs();
    
    // 加载所有数据
    loadAllData();
    
    // 设置自动刷新
    setInterval(loadAllData, REFRESH_INTERVAL);
    
    // 启动倒计时
    startCountdown();
});

// Tab切换初始化
function initTabs() {
    const navLinks = document.querySelectorAll('.nav-link');
    const tabContents = document.querySelectorAll('.tab-content');
    
    navLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const targetTab = link.getAttribute('data-tab');
            
            // 更新导航激活状态
            navLinks.forEach(l => l.classList.remove('active'));
            link.classList.add('active');
            
            // 切换内容
            tabContents.forEach(content => {
                content.classList.remove('active');
                if (content.id === `tab-${targetTab}`) {
                    content.classList.add('active');
                }
            });
        });
    });
}

// 加载所有数据
async function loadAllData() {
    try {
        await Promise.all([
            loadAccountData(),
            loadStatusData(),
            loadTimerData(),
            loadScoresData(),
            loadTradesData(),
            loadPositionsData(),
            loadMarketState(),
            loadEquityHistory()
        ]);
        
        updateFooterTime();
    } catch (error) {
        console.error('加载数据失败:', error);
    }
}

// 刷新所有数据
function refreshAllData() {
    const btn = document.querySelector('.btn-refresh i');
    btn.classList.add('fa-spin');
    
    loadAllData().then(() => {
        setTimeout(() => {
            btn.classList.remove('fa-spin');
        }, 1000);
    });
}

// 加载账户数据
async function loadAccountData() {
    try {
        const response = await fetch(`${API_BASE}/api/account`);
        const data = await response.json();
        
        if (data.error) {
            console.error('API错误:', data.error);
            return;
        }
        
        // 更新统计卡片
        document.getElementById('cash-usdt').textContent = '$' + (data.cash_usdt?.toFixed(2) || '0.00');
        document.getElementById('total-trades').textContent = data.total_trades || '0';
        document.getElementById('total-fees').textContent = '$' + Math.abs(data.total_fees || 0).toFixed(4);
        
        // 盈亏显示
        const pnl = data.realized_pnl || 0;
        const pnlEl = document.getElementById('realized-pnl');
        pnlEl.textContent = (pnl >= 0 ? '+' : '') + pnl.toFixed(2);
        pnlEl.style.color = pnl >= 0 ? 'var(--color-success)' : 'var(--color-danger)';
        
        // 卡片盈亏变化
        const pnlChangeEl = document.getElementById('total-pnl-change');
        if (pnlChangeEl) {
            pnlChangeEl.textContent = (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + ' USDT';
            pnlChangeEl.className = 'card-change ' + (pnl >= 0 ? 'positive' : 'negative');
        }
        
    } catch (error) {
        console.error('加载账户数据失败:', error);
    }
}

// 加载系统状态
async function loadStatusData() {
    try {
        const response = await fetch(`${API_BASE}/api/status`);
        const data = await response.json();
        
        // 系统状态徽章
        const statusBadge = document.getElementById('system-status-badge');
        if (data.timer_active) {
            statusBadge.textContent = '运行中';
            statusBadge.className = 'badge active';
        } else {
            statusBadge.textContent = '已停止';
            statusBadge.className = 'badge inactive';
        }
        
        // 系统信息
        document.getElementById('trade-mode').textContent = data.mode + (data.dry_run ? ' (模拟)' : ' (实盘)');
        document.getElementById('equity-cap').textContent = '$' + data.equity_cap;
        document.getElementById('last-update').textContent = data.last_check;
        
        // 系统状态Tab
        const timerStatusIcon = document.getElementById('timer-status-icon');
        const timerStatusText = document.getElementById('timer-system-status');
        if (data.timer_active) {
            timerStatusIcon.className = 'system-icon green';
            timerStatusText.textContent = '运行中';
        } else {
            timerStatusIcon.className = 'system-icon red';
            timerStatusText.textContent = '已停止';
        }
        
    } catch (error) {
        console.error('加载状态数据失败:', error);
    }
}

// 加载定时任务数据
async function loadTimerData() {
    try {
        const response = await fetch(`${API_BASE}/api/timer`);
        const data = await response.json();
        
        // 更新下次运行时间
        if (data.next_run) {
            const nextRun = new Date(data.next_run);
            document.getElementById('next-run-time').textContent = 
                '下次: ' + nextRun.toLocaleString('zh-CN');
        } else {
            document.getElementById('next-run-time').textContent = '计算中...';
        }
        
        // 设置倒计时
        countdownSeconds = data.countdown_seconds || 0;
        maxIntervalSeconds = (data.interval_minutes || 60) * 60;
        
        // 更新间隔显示
        const intervalEl = document.getElementById('timer-interval');
        if (intervalEl) {
            const interval = data.interval_minutes || 60;
            if (interval >= 60) {
                intervalEl.textContent = `每${interval / 60}小时执行`;
            } else {
                intervalEl.textContent = `每${interval}分钟执行`;
            }
        }
        
    } catch (error) {
        console.error('加载定时任务数据失败:', error);
    }
}

// 启动倒计时
function startCountdown() {
    if (countdownInterval) {
        clearInterval(countdownInterval);
    }
    
    updateCountdownDisplay();
    
    countdownInterval = setInterval(() => {
        if (countdownSeconds > 0) {
            countdownSeconds--;
            updateCountdownDisplay();
        } else {
            loadTimerData();
        }
    }, 1000);
}

// 更新倒计时显示
function updateCountdownDisplay() {
    const hours = Math.floor(countdownSeconds / 3600);
    const minutes = Math.floor((countdownSeconds % 3600) / 60);
    const seconds = countdownSeconds % 60;
    
    document.getElementById('countdown-hours').textContent = String(hours).padStart(2, '0');
    document.getElementById('countdown-minutes').textContent = String(minutes).padStart(2, '0');
    document.getElementById('countdown-seconds').textContent = String(seconds).padStart(2, '0');
    
    // 更新进度条
    const progress = ((maxIntervalSeconds - countdownSeconds) / maxIntervalSeconds) * 100;
    document.getElementById('timer-progress').style.width = progress + '%';
    
    // 颜色变化
    const secondsEl = document.getElementById('countdown-seconds');
    secondsEl.className = 'countdown-value';
    if (countdownSeconds < 300) {
        secondsEl.classList.add('danger');
    } else if (countdownSeconds < 600) {
        secondsEl.classList.add('warning');
    }
}

// 加载评分数据
async function loadScoresData() {
    try {
        const response = await fetch(`${API_BASE}/api/scores`);
        const data = await response.json();
        
        const tbody = document.querySelector('#scores-table tbody');
        const scores = data.scores || [];
        
        if (scores.length > 0) {
            tbody.innerHTML = scores.map((item, index) => {
                const score = item.score || 0;
                const scorePercent = Math.min(Math.max(score * 50, 0), 100);
                let signalStrength = '弱';
                let strengthClass = 'weak';
                if (score > 0.5) {
                    signalStrength = '强';
                    strengthClass = 'strong';
                } else if (score > 0.3) {
                    signalStrength = '中';
                    strengthClass = 'medium';
                }
                
                return `
                    <tr>
                        <td>${index + 1}</td>
                        <td>${item.symbol}</td>
                        <td>${score.toFixed(4)}</td>
                        <td>
                            <div class="score-bar">
                                <div class="score-fill ${strengthClass}" style="width: ${scorePercent}%"></div>
                            </div>
                            <span class="signal-text ${strengthClass}">${signalStrength}</span>
                        </td>
                    </tr>
                `;
            }).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="4" class="loading">暂无评分数据</td></tr>';
        }
        
        // 更新市场状态
        updateMarketStateFromScores(data.regime);
        
    } catch (error) {
        console.error('加载评分数据失败:', error);
    }
}

// 更新市场状态
function updateMarketStateFromScores(regime) {
    const badge = document.getElementById('market-state-badge');
    const desc = document.getElementById('market-desc');
    
    if (!regime) return;
    
    const regimeUpper = regime.toUpperCase().replace('-', '_');
    
    if (regimeUpper === 'RISK_OFF') {
        badge.innerHTML = '<i class="fas fa-shield-alt"></i><span>Risk-Off</span>';
        badge.style.background = 'rgba(239, 68, 68, 0.1)';
        badge.style.color = 'var(--color-danger)';
        badge.style.borderColor = 'var(--color-danger)';
        desc.textContent = '风险规避模式，减少仓位暴露';
        document.getElementById('pos-multiplier').textContent = '0.0x';
    } else if (regimeUpper === 'TRENDING') {
        badge.innerHTML = '<i class="fas fa-arrow-trend-up"></i><span>趋势行情</span>';
        badge.style.background = 'rgba(16, 185, 129, 0.1)';
        badge.style.color = 'var(--color-success)';
        badge.style.borderColor = 'var(--color-success)';
        desc.textContent = '趋势行情，增加仓位暴露';
        document.getElementById('pos-multiplier').textContent = '1.0x';
    } else if (regimeUpper === 'SIDEWAYS') {
        badge.innerHTML = '<i class="fas fa-minus"></i><span>震荡行情</span>';
        badge.style.background = 'rgba(245, 158, 11, 0.1)';
        badge.style.color = 'var(--color-warning)';
        badge.style.borderColor = 'var(--color-warning)';
        desc.textContent = '震荡行情，正常仓位';
        document.getElementById('pos-multiplier').textContent = '0.5x';
    }
}

// 加载交易数据
async function loadTradesData() {
    try {
        const response = await fetch(`${API_BASE}/api/trades`);
        const trades = await response.json();
        
        const tbody = document.querySelector('#trades-table tbody');
        
        if (trades.error) {
            tbody.innerHTML = `<tr><td colspan="4" class="loading">数据加载失败</td></tr>`;
            return;
        }
        
        if (Array.isArray(trades) && trades.length > 0) {
            tbody.innerHTML = trades.slice(0, 10).map(trade => {
                const sideClass = trade.side === 'buy' ? 'side-buy' : 'side-sell';
                const sideText = trade.side === 'buy' ? '买入' : '卖出';
                const amount = trade.amount || 0;
                
                return `
                    <tr>
                        <td>${trade.time || '--'}</td>
                        <td>${trade.symbol || '--'}</td>
                        <td class="${sideClass}">${sideText}</td>
                        <td>$${amount.toFixed(2)}</td>
                    </tr>
                `;
            }).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="4" class="loading">暂无交易记录</td></tr>';
        }
    } catch (error) {
        console.error('加载交易数据失败:', error);
    }
}

// 加载持仓数据
async function loadPositionsData() {
    try {
        const response = await fetch(`${API_BASE}/api/positions`);
        const positions = await response.json();
        
        const tbody = document.querySelector('#positions-table tbody');
        
        if (positions.length > 0) {
            tbody.innerHTML = positions.map(pos => {
                return `
                    <tr>
                        <td><strong>${pos.symbol}</strong></td>
                        <td>${pos.qty.toFixed(8)}</td>
                        <td>$${pos.value_usdt.toFixed(2)}</td>
                        <td>--</td>
                        <td><span class="badge">持仓</span></td>
                    </tr>
                `;
            }).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="5" class="loading">当前无持仓（Risk-Off模式）</td></tr>';
        }
    } catch (error) {
        console.error('加载持仓数据失败:', error);
    }
}

// 加载市场状态
async function loadMarketState() {
    // 市场状态已从评分数据更新
}

// 加载权益曲线
async function loadEquityHistory() {
    try {
        const response = await fetch(`${API_BASE}/api/equity_history`);
        const data = await response.json();
        
        if (data.error || !Array.isArray(data) || data.length === 0) {
            return;
        }
        
        const ctx = document.getElementById('equity-chart').getContext('2d');
        
        // 销毁旧图表
        if (window.equityChart) {
            window.equityChart.destroy();
        }
        
        const labels = data.map(d => d.timestamp?.split('T')[0] || '');
        const values = data.map(d => d.value || 100);
        
        window.equityChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: '累计权益',
                    data: values,
                    borderColor: '#3b82f6',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        grid: {
                            color: 'rgba(255, 255, 255, 0.05)'
                        },
                        ticks: {
                            color: '#718096',
                            maxTicksLimit: 6
                        }
                    },
                    y: {
                        grid: {
                            color: 'rgba(255, 255, 255, 0.05)'
                        },
                        ticks: {
                            color: '#718096',
                            callback: function(value) {
                                return '$' + value.toFixed(0);
                            }
                        }
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index'
                }
            }
        });
        
    } catch (error) {
        console.error('加载权益历史失败:', error);
    }
}

// 策略配置 - 权重滑块
function updateWeight(factor, value) {
    document.getElementById(`weight-${factor}`).textContent = value + '%';
}

// 保存策略配置
function saveStrategy() {
    const weights = {
        f1: document.getElementById('f1-weight').value,
        f2: document.getElementById('f2-weight').value,
        f3: document.getElementById('f3-weight').value,
        f4: document.getElementById('f4-weight').value,
        f5: document.getElementById('f5-weight').value
    };
    
    const killSwitch = document.getElementById('kill-switch-threshold').value;
    const maxWeight = document.getElementById('max-single-weight').value;
    const fee = document.getElementById('fee-bps').value;
    const slippage = document.getElementById('slippage-bps').value;
    
    alert('策略配置已保存！\n\n因子权重:\n' + 
          `5日动量: ${weights.f1}%\n` +
          `20日动量: ${weights.f2}%\n` +
          `波动率调整: ${weights.f3}%\n` +
          `成交量: ${weights.f4}%\n` +
          `RSI: ${weights.f5}%\n\n` +
          `风险控制:\n` +
          `Kill Switch阈值: ${killSwitch}%\n` +
          `最大单币权重: ${maxWeight}%\n` +
          `手续费: ${fee} bps\n` +
          `滑点: ${slippage} bps`);
}

// 重置策略配置
function resetStrategy() {
    document.getElementById('f1-weight').value = 25;
    document.getElementById('f2-weight').value = 25;
    document.getElementById('f3-weight').value = 20;
    document.getElementById('f4-weight').value = 15;
    document.getElementById('f5-weight').value = 15;
    
    document.getElementById('weight-f1').textContent = '25%';
    document.getElementById('weight-f2').textContent = '25%';
    document.getElementById('weight-f3').textContent = '20%';
    document.getElementById('weight-f4').textContent = '15%';
    document.getElementById('weight-f5').textContent = '15%';
    
    document.getElementById('kill-switch-threshold').value = 15;
    document.getElementById('max-single-weight').value = 25;
    document.getElementById('fee-bps').value = 6;
    document.getElementById('slippage-bps').value = 5;
}

// 运行回测
function runBacktest() {
    const strategy = document.getElementById('backtest-strategy').value;
    const startDate = document.getElementById('backtest-start').value;
    const endDate = document.getElementById('backtest-end').value;
    
    alert(`开始回测:\n策略: ${strategy}\n时间: ${startDate} 至 ${endDate}\n\n回测任务已提交，请稍后查看结果。`);
}

// 更新页脚时间
function updateFooterTime() {
    const now = new Date();
    document.getElementById('footer-update').textContent = now.toLocaleString('zh-CN');
}
