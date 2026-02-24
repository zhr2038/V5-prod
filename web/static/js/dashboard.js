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
        
        document.getElementById('cash-usdt').textContent = data.cash_usdt.toFixed(2);
        document.getElementById('total-trades').textContent = data.total_trades;
        document.getElementById('total-fees').textContent = data.total_fees.toFixed(4);
        
        const pnlElement = document.getElementById('realized-pnl');
        pnlElement.textContent = (data.realized_pnl >= 0 ? '+' : '') + data.realized_pnl.toFixed(2);
        pnlElement.className = 'card-value ' + (data.realized_pnl >= 0 ? 'positive' : 'negative');
    } catch (error) {
        console.error('加载账户数据失败:', error);
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
        regimeBadge.textContent = data.regime || 'Unknown';
        
        if (data.regime === 'Risk-Off') {
            regimeBadge.className = 'badge warning';
        } else if (data.regime === 'Trending') {
            regimeBadge.className = 'badge active';
        } else {
            regimeBadge.className = 'badge';
        }
        
        // 评分表格
        const tbody = document.querySelector('#scores-table tbody');
        if (data.scores && data.scores.length > 0) {
            tbody.innerHTML = data.scores.map((item, index) => {
                const scorePercent = Math.min(Math.max(item.score * 50, 0), 100);
                let signalStrength = '弱';
                if (item.score > 0.5) signalStrength = '强';
                else if (item.score > 0.3) signalStrength = '中';
                
                return `
                    <tr>
                        <td>${index + 1}</td>
                        <td>${item.symbol}</td>
                        <td>${item.score.toFixed(4)}</td>
                        <td>
                            <div class="score-bar">
                                <div class="score-fill" style="width: ${scorePercent}%"></div>
                            </div>
                            <span style="font-size: 11px; color: #8892b0;">${signalStrength}</span>
                        </td>
                    </tr>
                `;
            }).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="4" class="loading">暂无数据</td></tr>';
        }
    } catch (error) {
        console.error('加载评分数据失败:', error);
    }
}

// 加载交易数据
async function loadTradesData() {
    try {
        const response = await fetch(`${API_BASE}/api/trades`);
        const trades = await response.json();
        
        const tbody = document.querySelector('#trades-table tbody');
        if (trades.length > 0) {
            tbody.innerHTML = trades.slice(0, 20).map(trade => `
                <tr>
                    <td>${trade.time}</td>
                    <td>${trade.symbol}</td>
                    <td class="side-${trade.side}">${trade.side === 'buy' ? '买入' : '卖出'}</td>
                    <td>$${trade.amount.toFixed(2)}</td>
                    <td>$${Math.abs(trade.fee).toFixed(6)}</td>
                </tr>
            `).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="5" class="loading">暂无交易记录</td></tr>';
        }
    } catch (error) {
        console.error('加载交易数据失败:', error);
    }
}

// 权益曲线图表
let equityChart = null;

async function loadEquityHistory() {
    try {
        const response = await fetch(`${API_BASE}/api/equity_history`);
        const data = await response.json();
        
        if (data.length === 0) return;
        
        const labels = data.map(d => d.date);
        const values = data.map(d => d.net_flow);
        
        const ctx = document.getElementById('equity-chart').getContext('2d');
        
        if (equityChart) {
            equityChart.destroy();
        }
        
        equityChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: '日净流入',
                    data: values,
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
                        ticks: { color: '#8892b0' },
                        grid: { color: '#1e2444' }
                    }
                }
            }
        });
    } catch (error) {
        console.error('加载权益历史失败:', error);
    }
}
