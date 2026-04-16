function setOfSymbol(s, el) { ofSymbol = s; activatePill(el); if(ofInitialized) fetchOrderFlow(); }

function initOrderFlow() {
  if (typeof LightweightCharts === 'undefined' || window._noCharts) {
    document.getElementById('cvd-chart').innerHTML = '<div style="padding:20px;text-align:center;color:var(--text2);font-size:.8rem">Charts require internet</div>';
    fetchOrderFlow(); return;
  }
  if (!ofInitialized) {
    ofInitialized = true;
    const opts = (el) => ({
      width: el.clientWidth, height: 250,
      layout: { background: { type: 'solid', color: '#12121a' }, textColor: '#888' },
      grid: { vertLines: { color: '#1e1e2e' }, horzLines: { color: '#1e1e2e' } },
      timeScale: { borderColor: '#1e1e2e', timeVisible: true },
      rightPriceScale: { borderColor: '#1e1e2e' },
    });

    // CVD chart
    const cvdEl = document.getElementById('cvd-chart');
    cvdChart = LightweightCharts.createChart(cvdEl, opts(cvdEl));
    cvdSeries = cvdChart.addLineSeries({ color: '#00ff88', lineWidth: 2 });

    // Delta chart
    const deltaEl = document.getElementById('delta-chart');
    deltaChart = LightweightCharts.createChart(deltaEl, opts(deltaEl));
    deltaBuySeries = deltaChart.addHistogramSeries({ color: '#00ff8866' });
    deltaSellSeries = deltaChart.addHistogramSeries({ color: '#ff444466' });

    // Bubbles chart
    const bubblesEl = document.getElementById('bubbles-chart');
    bubblesChart = LightweightCharts.createChart(bubblesEl, opts(bubblesEl));
    bubbleSeries = bubblesChart.addLineSeries({ color: '#7c4dff', lineWidth: 1, lastValueVisible: false, priceLineVisible: false });

    window.addEventListener('resize', () => {
      cvdChart.applyOptions({ width: cvdEl.clientWidth });
      deltaChart.applyOptions({ width: deltaEl.clientWidth });
      bubblesChart.applyOptions({ width: bubblesEl.clientWidth });
    });
  }
  fetchOrderFlow();
}

async function fetchOrderFlow() {
  // CVD
  try {
    const res = await fetch(API + '/api/hb/orderflow/cvd?symbol=' + ofSymbol);
    if (res.ok) { const d = await res.json(); if(d.length) { cvdSeries.setData(d); cvdChart.timeScale().fitContent(); } }
  } catch(e) { cvdSeries.setData(generateDemoCvd()); cvdChart.timeScale().fitContent(); }

  // Delta
  try {
    const res = await fetch(API + '/api/hb/orderflow/delta?symbol=' + ofSymbol);
    if (res.ok) {
      const d = await res.json();
      deltaBuySeries.setData(d.map(c => ({ time: c.time, value: c.buy_vol, color: '#00ff8866' })));
      deltaSellSeries.setData(d.map(c => ({ time: c.time, value: -c.sell_vol, color: '#ff444466' })));
      deltaChart.timeScale().fitContent();
    }
  } catch(e) {}

  // Bubbles
  try {
    const res = await fetch(API + '/api/hb/orderflow/bubbles?symbol=' + ofSymbol);
    if (res.ok) {
      const d = await res.json();
      const markers = d.map(t => ({
        time: t.time, position: t.side === 'buy' ? 'belowBar' : 'aboveBar',
        color: t.side === 'buy' ? '#00ff88' : '#ff4444',
        shape: 'circle',
        text: '$' + (t.value_usd/1000).toFixed(1) + 'k',
      }));
      bubbleSeries.setData(d.map(t => ({ time: t.time, value: t.price })));
      bubbleSeries.setMarkers(markers);
      bubblesChart.timeScale().fitContent();
    }
  } catch(e) {}

  // Footprint
  try {
    const res = await fetch(API + '/api/hb/orderflow/footprint?symbol=' + ofSymbol);
    if (res.ok) {
      const d = await res.json();
      renderFootprint(d);
    }
  } catch(e) { renderFootprint(generateDemoFootprint()); }
}

function generateDemoCvd() {
  const now = Math.floor(Date.now()/1000); const data = []; let cvd = 0;
  for(let i=200;i>=0;i--) { cvd += (Math.random()-0.48)*2; data.push({time:now-i*60, value:+cvd.toFixed(4)}); }
  return data;
}

function generateDemoFootprint() {
  const base = ofSymbol==='BTC-USD'?69000:ofSymbol==='ETH-USD'?3500:185;
  const step = ofSymbol==='BTC-USD'?50:ofSymbol==='ETH-USD'?5:0.5;
  const levels = {};
  for(let i=-5;i<=5;i++) {
    const p = base + i*step;
    levels[p] = { buy_vol: +(Math.random()*5).toFixed(3), sell_vol: +(Math.random()*5).toFixed(3) };
  }
  return { time: Math.floor(Date.now()/1000), levels };
}

function renderFootprint(data) {
  const el = document.getElementById('footprint-data');
  if (!data || !data.levels) { el.innerHTML = '<div class="empty">No footprint data</div>'; return; }
  const levels = Object.entries(data.levels).sort((a,b) => parseFloat(b[0]) - parseFloat(a[0]));
  const maxVol = Math.max(...levels.map(([_,v]) => Math.max(v.buy_vol, v.sell_vol)), 0.01);
  el.innerHTML = levels.map(([price, v]) => {
    const bw = Math.max(2, (v.buy_vol/maxVol)*120);
    const sw = Math.max(2, (v.sell_vol/maxVol)*120);
    const delta = v.buy_vol - v.sell_vol;
    const dColor = delta >= 0 ? 'var(--accent)' : 'var(--red)';
    return `<div class="bar-row">
      <span class="bar-price">$${parseFloat(price).toLocaleString()}</span>
      <span class="bar-fill buy" style="width:${bw}px"></span>
      <span class="bar-vol">${v.buy_vol.toFixed(3)}</span>
      <span style="color:var(--text2);margin:0 4px">|</span>
      <span class="bar-fill sell" style="width:${sw}px"></span>
      <span class="bar-vol">${v.sell_vol.toFixed(3)}</span>
      <span style="color:${dColor};font-weight:700;min-width:60px;text-align:right">${delta>=0?'+':''}${delta.toFixed(3)}</span>
    </div>`;
  }).join('');
}

// --- Liquidity ---
let liqSymbol = 'BTC-USD';
let depthChart = null, bidDepthSeries = null, askDepthSeries = null;
let liqInitialized = false;

function setLiqSymbol(s, el) { liqSymbol = s; activatePill(el); if(liqInitialized) fetchLiquidity(); }

function initLiquidity() {
  if (typeof LightweightCharts === 'undefined' || window._noCharts) {
    fetchLiquidity(); return;
  }
  if (!liqInitialized) {
    liqInitialized = true;
    const depthEl = document.getElementById('depth-chart');
    depthChart = LightweightCharts.createChart(depthEl, {
      width: depthEl.clientWidth, height: 250,
      layout: { background: { type: 'solid', color: '#12121a' }, textColor: '#888' },
      grid: { vertLines: { color: '#1e1e2e' }, horzLines: { color: '#1e1e2e' } },
      rightPriceScale: { borderColor: '#1e1e2e' },
    });
    bidDepthSeries = depthChart.addAreaSeries({
      topColor: 'rgba(0,255,136,0.3)', bottomColor: 'rgba(0,255,136,0.02)',
      lineColor: '#00ff88', lineWidth: 2,
    });
    askDepthSeries = depthChart.addAreaSeries({
      topColor: 'rgba(255,68,68,0.3)', bottomColor: 'rgba(255,68,68,0.02)',
      lineColor: '#ff4444', lineWidth: 2,
    });
    window.addEventListener('resize', () => depthChart.applyOptions({ width: depthEl.clientWidth }));
  }
  fetchLiquidity();
}

async function fetchLiquidity() {
  // Depth
  try {
    const res = await fetch(API + '/api/hb/liquidity/depth?symbol=' + liqSymbol);
    if (res.ok) {
      const d = await res.json();
      if (d.bids && d.bids.length) {
        // Use price as a "time" axis (lightweight-charts trick for non-time data)
        bidDepthSeries.setData(d.bids.map((b,i) => ({ time: i, value: b.cumulative })));
        askDepthSeries.setData(d.asks.map((a,i) => ({ time: d.bids.length + i, value: a.cumulative })));
        depthChart.timeScale().fitContent();
      }
    }
  } catch(e) {}

  // Heatmap
  try {
    const res = await fetch(API + '/api/hb/liquidity/heatmap?symbol=' + liqSymbol);
    if (res.ok) { const d = await res.json(); renderHeatmap(d); }
    else { renderDemoHeatmap(); }
  } catch(e) { renderDemoHeatmap(); }
}

function renderHeatmap(data) {
  const canvas = document.getElementById('heatmap-canvas');
  const ctx = canvas.getContext('2d');
  const w = canvas.parentElement.clientWidth;
  const h = 250;
  canvas.width = w; canvas.height = h;
  ctx.fillStyle = '#12121a'; ctx.fillRect(0,0,w,h);

  if (!data || !data.length) { renderDemoHeatmap(); return; }

  // Find price range
  let minP = Infinity, maxP = -Infinity, maxSize = 0;
  data.forEach(snap => snap.levels.forEach(l => {
    minP = Math.min(minP, l.price); maxP = Math.max(maxP, l.price);
    maxSize = Math.max(maxSize, l.size);
  }));
  if (maxP === minP) return;

  const colW = Math.max(2, w / data.length);
  data.forEach((snap, i) => {
    snap.levels.forEach(l => {
      const y = h - ((l.price - minP) / (maxP - minP)) * h;
      const intensity = Math.min(1, l.size / (maxSize * 0.5));
      const color = l.side === 'bid'
        ? `rgba(0,255,136,${intensity * 0.8})`
        : `rgba(255,68,68,${intensity * 0.8})`;
      ctx.fillStyle = color;
      ctx.fillRect(i * colW, y - 1, colW, 3);
    });
  });
}

function renderDemoHeatmap() {
  const canvas = document.getElementById('heatmap-canvas');
  const ctx = canvas.getContext('2d');
  const w = canvas.parentElement.clientWidth;
  const h = 250;
  canvas.width = w; canvas.height = h;
  ctx.fillStyle = '#12121a'; ctx.fillRect(0,0,w,h);
  // Generate demo heatmap
  const mid = h/2;
  for(let x=0;x<w;x+=3) {
    for(let y=0;y<h;y+=3) {
      const dist = Math.abs(y - mid);
      const intensity = Math.max(0, 1 - dist/(h*0.4)) * Math.random();
      if(intensity > 0.1) {
        ctx.fillStyle = y < mid
          ? `rgba(255,68,68,${intensity*0.6})`
          : `rgba(0,255,136,${intensity*0.6})`;
        ctx.fillRect(x, y, 3, 3);
      }
    }
  }
  ctx.fillStyle='#888'; ctx.font='11px monospace';
  ctx.fillText('Demo heatmap — connect to see live data', 10, h-10);
}

// --- Derivatives ---
let derivSymbol = 'BTC-USD';
let oiChart = null, oiSeries = null;
let derivInitialized = false;

function setDerivSymbol(s, el) { derivSymbol = s; activatePill(el); if(derivInitialized) fetchDerivatives(); }

function initDerivatives() {
  if (typeof LightweightCharts === 'undefined' || window._noCharts) {
    fetchDerivatives(); return;
  }
  if (!derivInitialized) {
    derivInitialized = true;
    const oiEl = document.getElementById('oi-chart');
    oiChart = LightweightCharts.createChart(oiEl, {
      width: oiEl.clientWidth, height: 250,
      layout: { background: { type: 'solid', color: '#12121a' }, textColor: '#888' },
      grid: { vertLines: { color: '#1e1e2e' }, horzLines: { color: '#1e1e2e' } },
      timeScale: { borderColor: '#1e1e2e', timeVisible: true },
      rightPriceScale: { borderColor: '#1e1e2e' },
    });
    oiSeries = oiChart.addAreaSeries({
      topColor: 'rgba(124,77,255,0.3)', bottomColor: 'rgba(124,77,255,0.02)',
      lineColor: '#7c4dff', lineWidth: 2,
    });
    window.addEventListener('resize', () => oiChart.applyOptions({ width: oiEl.clientWidth }));
  }
  fetchDerivatives();
}

async function fetchDerivatives() {
  // OI
  try {
    const res = await fetch(API + '/api/hb/derivatives/oi?symbol=' + derivSymbol);
    if (res.ok) { const d = await res.json(); if(d.length) { oiSeries.setData(d); oiChart.timeScale().fitContent(); } }
  } catch(e) {}

  // Funding
  try {
    const res = await fetch(API + '/api/hb/derivatives/funding?symbol=' + derivSymbol);
    if (res.ok) {
      const d = await res.json();
      renderFunding(d);
    }
  } catch(e) { renderFunding([]); }

  // Liq map
  try {
    const res = await fetch(API + '/api/hb/derivatives/liqmap?symbol=' + derivSymbol);
    if (res.ok) { const d = await res.json(); renderLiqMap(d); }
  } catch(e) { renderLiqMap({ clusters: [], estimated_levels: [], current_price: 0 }); }
}

function renderFunding(data) {
  const el = document.getElementById('funding-data');
  if (!data || !data.length) {
    el.innerHTML = `
      <div class="funding-bar"><span class="funding-label">Bulk</span>
        <div class="funding-gauge"><div class="funding-gauge-fill" style="width:50%;background:var(--accent)"></div></div>
        <span style="font-family:monospace;font-size:.8rem">0.0100%</span></div>
      <div class="funding-bar"><span class="funding-label">Hyperliquid</span>
        <div class="funding-gauge"><div class="funding-gauge-fill" style="width:45%;background:var(--purple)"></div></div>
        <span style="font-family:monospace;font-size:.8rem">0.0085%</span></div>`;
    return;
  }
  const latest = data[data.length - 1];
  const bRate = ((latest.bulk || 0) * 100).toFixed(4);
  const hRate = ((latest.hl || 0) * 100).toFixed(4);
  const bWidth = Math.min(100, Math.abs(latest.bulk || 0) * 10000);
  const hWidth = Math.min(100, Math.abs(latest.hl || 0) * 10000);
  el.innerHTML = `
    <div class="funding-bar"><span class="funding-label">Bulk</span>
      <div class="funding-gauge"><div class="funding-gauge-fill" style="width:${bWidth}%;background:${parseFloat(bRate)>=0?'var(--accent)':'var(--red)'}"></div></div>
      <span style="font-family:monospace;font-size:.8rem">${bRate}%</span></div>
    <div class="funding-bar"><span class="funding-label">Hyperliquid</span>
      <div class="funding-gauge"><div class="funding-gauge-fill" style="width:${hWidth}%;background:${parseFloat(hRate)>=0?'var(--purple)':'var(--red)'}"></div></div>
      <span style="font-family:monospace;font-size:.8rem">${hRate}%</span></div>`;
}

function renderLiqMap(data) {
  const el = document.getElementById('liqmap-data');
  const price = data.current_price || 0;
  let html = '';
  if (data.estimated_levels && data.estimated_levels.length) {
    html += '<div style="padding:8px 12px;font-size:.75rem;color:var(--text2);font-weight:700">ESTIMATED LIQUIDATION LEVELS</div>';
    data.estimated_levels.forEach(l => {
      html += `<div class="liq-level long">
        <span>${l.leverage}x Long Liq</span>
        <span style="font-family:monospace">$${l.long_liq_price.toLocaleString()} (${l.distance_pct}% below)</span>
      </div>`;
      html += `<div class="liq-level short">
        <span>${l.leverage}x Short Liq</span>
        <span style="font-family:monospace">$${l.short_liq_price.toLocaleString()} (${l.distance_pct}% above)</span>
      </div>`;
    });
  }
  if (data.clusters && data.clusters.length) {
    html += '<div style="padding:8px 12px;font-size:.75rem;color:var(--text2);font-weight:700;margin-top:12px">ACTUAL LIQUIDATION CLUSTERS</div>';
    const maxVal = Math.max(...data.clusters.map(c => c.value_usd));
    data.clusters.forEach(c => {
      const pct = (c.value_usd / maxVal * 100).toFixed(0);
      const side = c.price < price ? 'long' : 'short';
      html += `<div class="liq-level ${side}">
        <span>$${c.price.toLocaleString()}</span>
        <span style="font-family:monospace">$${c.value_usd.toLocaleString()} liquidated</span>
      </div>`;
    });
  }
  if (!html) html = '<div class="empty">No liquidation data yet</div>';
  el.innerHTML = html;
}

// --- Profile ---
let profSymbol = 'BTC-USD';
let profInitialized = false;

function setProfSymbol(s, el) { profSymbol = s; activatePill(el); if(profInitialized) fetchProfile(); }

function initProfile() {
  profInitialized = true;
  fetchProfile();
}

async function fetchProfile() {
  // Volume Profile
  try {
    const res = await fetch(API + '/api/hb/profile/volume?symbol=' + profSymbol);
    if (res.ok) { const d = await res.json(); renderVolumeProfile(d); }
    else { renderVolumeProfile(generateDemoVP()); }
  } catch(e) { renderVolumeProfile(generateDemoVP()); }

  // TPO
  try {
    const res = await fetch(API + '/api/hb/profile/tpo?symbol=' + profSymbol);
    if (res.ok) { const d = await res.json(); renderTPO(d); }
    else { renderTPO(generateDemoTPO()); }
  } catch(e) { renderTPO(generateDemoTPO()); }
}

function generateDemoVP() {
  const base = profSymbol==='BTC-USD'?69000:profSymbol==='ETH-USD'?3500:185;
  const step = profSymbol==='BTC-USD'?50:profSymbol==='ETH-USD'?5:0.5;
  const data = [];
  for(let i=-10;i<=10;i++) {
    const vol = Math.random()*10 * (1 - Math.abs(i)/12);
    data.push({ price: base+i*step, buy_vol:+(vol*0.55).toFixed(3), sell_vol:+(vol*0.45).toFixed(3), total:+vol.toFixed(3), is_poc: i===0 });
  }
  return data;
}

function generateDemoTPO() {
  const base = profSymbol==='BTC-USD'?69000:profSymbol==='ETH-USD'?3500:185;
  const step = profSymbol==='BTC-USD'?50:profSymbol==='ETH-USD'?5:0.5;
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop';
  const data = [];
  for(let i=-8;i<=8;i++) {
    const n = Math.max(1, Math.floor(Math.random()*12 * (1 - Math.abs(i)/10)));
    data.push({ price: base+i*step, periods: n, letters: letters.slice(0, n) });
  }
  return data;
}

function renderVolumeProfile(data) {
  const el = document.getElementById('vprofile-data');
  if (!data || !data.length) { el.innerHTML = '<div class="empty">No volume profile data</div>'; return; }
  const maxVol = Math.max(...data.map(d => d.total), 0.01);
  el.innerHTML = data.sort((a,b) => b.price - a.price).map(d => {
    const bw = Math.max(2, (d.buy_vol/maxVol)*140);
    const sw = Math.max(2, (d.sell_vol/maxVol)*140);
    const cls = d.is_poc ? 'bar-row poc-row' : 'bar-row';
    return `<div class="${cls}">
      <span class="bar-price">${d.is_poc?'► ':''}$${d.price.toLocaleString()}</span>
      <span class="bar-fill buy" style="width:${bw}px"></span>
      <span class="bar-vol" style="color:var(--accent)">${d.buy_vol.toFixed(3)}</span>
      <span class="bar-fill sell" style="width:${sw}px"></span>
      <span class="bar-vol" style="color:var(--red)">${d.sell_vol.toFixed(3)}</span>
      <span class="bar-vol">${d.total.toFixed(3)}</span>
    </div>`;
  }).join('');
}

function renderTPO(data) {
  const el = document.getElementById('tpo-data');
  if (!data || !data.length) { el.innerHTML = '<div class="empty">No TPO data</div>'; return; }
  el.innerHTML = data.sort((a,b) => b.price - a.price).map(d => {
    return `<div class="tpo-row">
      <span class="bar-price">$${d.price.toLocaleString()}</span>
      <span class="tpo-letters">${d.letters}</span>
      <span style="color:var(--text2);font-size:.65rem;margin-left:auto">${d.periods} periods</span>
    </div>`;
  }).join('');
}

// --- Charts (TradingView Lightweight Charts) ---
let priceChart = null, pnlChart = null;
let candleSeries = null, volumeSeries = null, pnlSeries = null, pnlMarkers = [];
let chartSymbol = 'BTC-USD', chartExchange = 'bulk';
let chartsInitialized = false;
