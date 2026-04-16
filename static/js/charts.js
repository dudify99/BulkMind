function setChartSymbol(s, el) {
  chartSymbol = s;
  el.parentElement.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('chart-title').textContent = s + ' Price';
  fetchCandles();
}

function setChartExchange(ex, el) {
  chartExchange = ex;
  el.parentElement.querySelectorAll('button').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  fetchCandles();
}

function initCharts() {
  if (typeof LightweightCharts === 'undefined' || window._noCharts) {
    document.getElementById('price-chart').innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text2);font-size:.8rem">Charts require internet (TradingView CDN)</div>';
    document.getElementById('pnl-chart').innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text2);font-size:.8rem">Charts require internet (TradingView CDN)</div>';
    return;
  }
  if (chartsInitialized) { fetchCandles(); fetchPnlHistory(); return; }
  chartsInitialized = true;

  const chartColors = {
    background: '#12121a',
    textColor: '#888',
    gridColor: '#1e1e2e',
  };

  // Price chart
  const priceContainer = document.getElementById('price-chart');
  priceChart = LightweightCharts.createChart(priceContainer, {
    width: priceContainer.clientWidth,
    height: 300,
    layout: { background: { type: 'solid', color: chartColors.background }, textColor: chartColors.textColor },
    grid: { vertLines: { color: chartColors.gridColor }, horzLines: { color: chartColors.gridColor } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: chartColors.gridColor },
    timeScale: { borderColor: chartColors.gridColor, timeVisible: true, secondsVisible: false },
  });

  candleSeries = priceChart.addCandlestickSeries({
    upColor: '#00ff88', downColor: '#ff4444',
    borderUpColor: '#00ff88', borderDownColor: '#ff4444',
    wickUpColor: '#00ff88', wickDownColor: '#ff4444',
  });

  volumeSeries = priceChart.addHistogramSeries({
    color: '#26a69a44',
    priceFormat: { type: 'volume' },
    priceScaleId: '',
  });
  volumeSeries.priceScale().applyOptions({
    scaleMargins: { top: 0.8, bottom: 0 },
  });

  // PnL chart
  const pnlContainer = document.getElementById('pnl-chart');
  pnlChart = LightweightCharts.createChart(pnlContainer, {
    width: pnlContainer.clientWidth,
    height: 300,
    layout: { background: { type: 'solid', color: chartColors.background }, textColor: chartColors.textColor },
    grid: { vertLines: { color: chartColors.gridColor }, horzLines: { color: chartColors.gridColor } },
    rightPriceScale: { borderColor: chartColors.gridColor },
    timeScale: { borderColor: chartColors.gridColor, timeVisible: true },
  });

  pnlSeries = pnlChart.addAreaSeries({
    topColor: 'rgba(0,255,136,0.3)',
    bottomColor: 'rgba(0,255,136,0.02)',
    lineColor: '#00ff88',
    lineWidth: 2,
  });

  // Responsive
  const resizeCharts = () => {
    priceChart.applyOptions({ width: priceContainer.clientWidth });
    pnlChart.applyOptions({ width: pnlContainer.clientWidth });
  };
  window.addEventListener('resize', resizeCharts);

  fetchCandles();
  fetchPnlHistory();
}

async function fetchCandles() {
  if (!priceChart) return;
  try {
    const url = API + '/api/hb/candles?symbol=' + chartSymbol + '&exchange=' + chartExchange + '&interval=15m&limit=200';
    const res = await fetch(url);
    if (!res.ok) throw new Error();
    const data = await res.json();
    if (data.length) {
      candleSeries.setData(data);
      volumeSeries.setData(data.map(c => ({
        time: c.time,
        value: c.volume,
        color: c.close >= c.open ? 'rgba(0,255,136,0.2)' : 'rgba(255,68,68,0.2)',
      })));
      priceChart.timeScale().fitContent();
    }
  } catch (e) {
    // Load demo candles
    loadDemoCandles();
  }
}

function loadDemoCandles() {
  const base = chartSymbol === 'BTC-USD' ? 69000 : chartSymbol === 'ETH-USD' ? 3500 : 185;
  const now = Math.floor(Date.now() / 1000);
  const candles = [];
  let price = base;
  for (let i = 200; i >= 0; i--) {
    const t = now - i * 900;
    const change = (Math.random() - 0.48) * base * 0.003;
    const o = price;
    const c = price + change;
    const h = Math.max(o, c) + Math.random() * base * 0.001;
    const l = Math.min(o, c) - Math.random() * base * 0.001;
    const v = Math.random() * 100 + 10;
    candles.push({ time: t, open: +o.toFixed(2), high: +h.toFixed(2), low: +l.toFixed(2), close: +c.toFixed(2), volume: +v.toFixed(2) });
    price = c;
  }
  candleSeries.setData(candles);
  volumeSeries.setData(candles.map(c => ({
    time: c.time, value: c.volume,
    color: c.close >= c.open ? 'rgba(0,255,136,0.2)' : 'rgba(255,68,68,0.2)',
  })));
  priceChart.timeScale().fitContent();
}

async function fetchPnlHistory() {
  if (!pnlChart || !wallet) { loadDemoPnl(); return; }
  try {
    const res = await fetch(API + '/api/hb/pnl-history/' + wallet);
    if (!res.ok) throw new Error();
    const data = await res.json();
    if (data.equity_curve && data.equity_curve.length) {
      pnlSeries.setData(data.equity_curve);
      // Color based on final PnL
      const finalPnl = data.total_pnl || 0;
      pnlSeries.applyOptions({
        topColor: finalPnl >= 0 ? 'rgba(0,255,136,0.3)' : 'rgba(255,68,68,0.3)',
        bottomColor: finalPnl >= 0 ? 'rgba(0,255,136,0.02)' : 'rgba(255,68,68,0.02)',
        lineColor: finalPnl >= 0 ? '#00ff88' : '#ff4444',
      });
      pnlChart.timeScale().fitContent();
    } else {
      loadDemoPnl();
    }
  } catch (e) {
    loadDemoPnl();
  }
}

function loadDemoPnl() {
  const now = Math.floor(Date.now() / 1000);
  const data = [];
  let pnl = 0;
  for (let i = 50; i >= 0; i--) {
    const t = now - i * 3600;
    pnl += (Math.random() - 0.45) * 50;
    data.push({ time: t, value: +pnl.toFixed(2) });
  }
  pnlSeries.setData(data);
  const finalPnl = data[data.length - 1].value;
  pnlSeries.applyOptions({
    topColor: finalPnl >= 0 ? 'rgba(0,255,136,0.3)' : 'rgba(255,68,68,0.3)',
    bottomColor: finalPnl >= 0 ? 'rgba(0,255,136,0.02)' : 'rgba(255,68,68,0.02)',
    lineColor: finalPnl >= 0 ? '#00ff88' : '#ff4444',
  });
  pnlChart.timeScale().fitContent();
}

// Update price chart with live trade data from WebSocket
function updateChartWithTrade(trade) {
  if (!candleSeries || trade.symbol !== chartSymbol) return;
  const ex = trade.exchange || 'bulk';
  if (ex !== chartExchange) return;
  // Update the last candle's close price
  const now = Math.floor(Date.now() / 1000);
  const candleTime = now - (now % 900); // Round to 15m candle
  candleSeries.update({
    time: candleTime,
    open: trade.price,
    high: trade.price,
    low: trade.price,
    close: trade.price,
  });
}

// --- Live PnL Updates ---
function handlePnlUpdate(updates) {
  if (!Array.isArray(updates)) return;
  for (const u of updates) {
    const el = document.getElementById('pos-' + u.trade_id);
    if (!el) continue;
    const pnlEl = el.querySelector('.pos-pnl');
    if (pnlEl) {
      const pnl = u.pnl_usd || 0;
      pnlEl.textContent = '$' + pnl.toFixed(2);
      pnlEl.className = 'pos-pnl ' + (pnl >= 0 ? 'green' : 'red');
    }
  }
}

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
  initGlobe();
  fetchMarket();
  fetchAchievements();
  fetchLeaderboard('daily');
  marketInterval = setInterval(fetchMarket, 5000);
  connectWebSocket();
});
