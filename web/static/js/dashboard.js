// V5 Dashboard JavaScript

// API 端点
const API_BASE = '';

// 刷新间隔（毫秒）
const REFRESH_INTERVAL = 30000; // 30秒

// 初始化
document.addEventListener('DOMContentLoaded', function() {
    loadAllData();
    setInterval(loadAllData, REFRESH_INTERVAL);
});

// 加载所有数据
async function loadAllData() {
    await Promise.all([
        loadAccountData(),
        loadStatusData(),
        loadScoresData(),
        loadTradesData(),
        loadEquityHistory()
    ]);
    
    document.getElementById('last-update').textContent = 
        '最后更新: ' + new Date().toLocaleString('zh-CN');
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
        
        document.getElementById('cash-usdt').textContent = data.cash_usdt?.toFixed(2) || '--';
        document.getElementById('total-trades').textContent = data.total_trades || '0';
        document.getElementById('total-fees').textContent = (data.total_fees?.toFixed(4) || '0') + ' USDT';
        
        const pnlElement = document.getElementById('realized-pnl');
        const pnl = data.realized_pnl || 0;
        pnlElement.textContent = (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + ' USDT';
        pnlElement.className = 'card-value ' + (pnl >= 0 ? 'positive' : 'negative');
    } catch (error) {
        console.error('加载账户数据失败:', error);
        // 显示错误状态
        document.getElementById('cash-usdt').textContent = 'Error';
        document.getElementById('total-trades').textContent = '--';
        document.getElementById('total-fees').textContent = '--';
        document.getElementById('realized-pnl').textContent = '--';
    }
}

// 加载状态数据
async function loadStatusData() {
    try {
        const response = await fetch(`${API_BASE}/api/status`);
        const data = await response.json();
        
        // 定时器状态
        const timerBadge = document.getElementById('timer-status');
        timerBadge.textContent = data.timer_active ? '运行中' : '已停止';
        timerBadge.className = 'badge ' + (data.timer_active ? 'active' : 'inactive');
        
        // 系统状态
        const systemBadge = document.getElementById('system-status');
        systemBadge.textContent = data.timer_active ? '正常' : '异常';
        systemBadge.className = 'badge ' + (data.timer_active ? 'active' : 'inactive');
        
        // 交易模式
        const modeText = data.mode + (data.dry_run ? ' (模拟)' : ' (实盘)');
        document.getElementById('trade-mode').textContent = modeText;
        
        // 资金上限
        document.getElementById('equity-cap').textContent = data.equity_cap + ' USDT';
    } catch (error) {
        console.error('加载状态数据失败:', error);
    }
}

// 加载评分数据
async function loadScoresData() {
    try {
        const response = await fetch(`${API_BASE}/api/scores`);
        const data = await response.json();
        
        // 市场状态
        const regimeBadge = document.getElementById('market-regime');
        const regime = data.regime || 'Unknown';
        regimeBadge.textContent = regime;
        
        if (regime === 'Risk-Off' || regime === 'Risk_Off') {
            regimeBadge.className = 'badge warning';
        } else if (regime === 'Trending') {
            regimeBadge.className = 'badge active';
        } else if (regime === 'Sideways') {
            regimeBadge.className = 'badge';
        } else {
            regimeBadge.className = 'badge';
        }
        
        // 评分表格
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
    } catch (error) {
        console.error('加载评分数据失败:', error);
        const tbody = document.querySelector('#scores-table tbody');
        tbody.innerHTML = '<tr><td colspan="4" class="loading">加载失败</td></tr>';
    }
}

// 加载交易数据
async function loadTradesData() {
    try {
        const response = await fetch(`${API_BASE}/api/trades`);
        const trades = await response.json();
        
        const tbody = document.querySelector('#trades-table tbody');
        
        if (trades.error) {
            tbody.innerHTML = `<tr><td colspan="5" class="loading">数据加载失败: ${trades.error}</td></tr>`;
            return;
        }
        
        if (Array.isArray(trades) && trades.length > 0) {
            tbody.innerHTML = trades.slice(0, 20).map(trade => {
                const sideClass = trade.side === 'buy' ? 'side-buy' : 'side-sell';
                const sideText = trade.side === 'buy' ? '买入' : '卖出';
                const amount = trade.amount || 0;
                const fee = Math.abs(trade.fee || 0);
                
                return `
                    <tr>
                        <td>${trade.time || '--'}</td>
                        <td>${trade.symbol || '--'}</td>
                        <td class="${sideClass}">${sideText}</td>
                        <td>$${amount.toFixed(2)}</td>
                        <td>$${fee.toFixed(6)}</td>
                    </tr>
                `;
            }).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="5" class="loading">暂无交易记录</td></tr>';
        }
    } catch (error) {
        console.error('加载交易数据失败:', error);
        const tbody = document.querySelector('#trades-table tbody');
        tbody.innerHTML = '<tr><td colspan="5" class="loading">加载失败，请刷新重试</td></tr>';
    }
}

// 权益曲线图表
let equityChart = null;

async function loadEquityHistory() {
    try {
        const response = await fetch(`${API_BASE}/api/equity_history`);
        const data = await response.json();
        
        if (data.error) {
            console.error('权益历史API错误:', data.error);
            return;
        }
        
        if (!Array.isArray(data) || data.length === 0) {
            // 显示空状态
            const ctx = document.getElementById('equity-chart').getContext('2d');
            if (equityChart) {
                equityChart.destroy();
            }
            
            equityChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: []
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        title: {
                            display: true,
                            text: '暂无历史数据',
                            color: '#8892b0'
                        }
                    }
                }
            });
            return;
        }
        
        const labels = data.map(d => d.date);
        const values = data.map(d => d.net_flow);
        
        const ctx = document.getElementById('equity-chart').getContext('2d');
        
        if (equityChart) {
            equityChart.destroy();
        }
        
        // 计算累计盈亏
        let cumulative = 0;
        const cumulativeValues = values.map(v => {
            cumulative += v;
            return cumulative;
        });
        
        equityChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: '累计盈亏',
                    data: cumulativeValues,
                    borderColor: '#00d4ff',
                    backgroundColor: 'rgba(0, 212, 255, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        labels: { color: '#e0e0e0' }
                    }
                },
                scales: {
                    x: {
                        ticks: { color: '#8892b0' },
                        grid: { color: '#1e2444' }
                    },
                    y: {
                        ticks: { 
                            color: '#8892b0',
                            callback: function(value) {
                                return '$' + value.toFixed(2);
                            }
                        },
                        grid: { color: '#1e2444' }
                    }
                }
            }
        });
    } catch (error) {
        console.error('加载权益历史失败:', error);
    }
}
