// --- Market ---
async function fetchMarket() {
  try {
    const res = await fetch(API + '/api/hb/market?symbol=' + symbol);
    if (!res.ok) throw new Error('Market fetch failed');
    market = await res.json();
    renderMarket();
  } catch (e) {
    renderDemoMarket();
  }
}

function renderMarket() {
  const p = market.price || 0;
  document.getElementById('price-main').textContent = '$' + p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  document.getElementById('price-bulk').textContent = '$' + (market.bulk_price || p).toLocaleString('en-US', { minimumFractionDigits: 2 });
  document.getElementById('price-hl').textContent = '$' + (market.hl_price || p).toLocaleString('en-US', { minimumFractionDigits: 2 });
}

function renderDemoMarket() {
  const prices = { 'BTC-USD': 69425.30, 'ETH-USD': 3521.18, 'SOL-USD': 187.42 };
  const base = prices[symbol] || 0;
  const jitter = base * (Math.random() * 0.002 - 0.001);
  const p = base + jitter;
  document.getElementById('price-main').textContent = '$' + p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  document.getElementById('price-bulk').textContent = '$' + (p - Math.random() * 2).toFixed(2);
  document.getElementById('price-hl').textContent = '$' + (p + Math.random() * 2).toFixed(2);
}

// --- Trade ---
async function placeTrade(side) {
  if (!wallet) { showToast('Connect wallet first', 'error'); return; }
  const size = parseFloat(document.getElementById('size-input').value);
  if (!size || size <= 0) { showToast('Enter a valid size', 'error'); return; }
  try {
    const res = await fetch(API + '/api/hb/trade', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet, symbol, side, size, exchange })
    });
    if (!res.ok) throw new Error('Trade failed');
    const data = await res.json();
    showToast(side.toUpperCase() + ' ' + size + ' ' + symbol + ' placed!', 'success');
    fetchOpenTrades();
  } catch (e) {
    showToast('Trade submitted (demo)', 'info');
    addDemoPosition(side, size);
  }
}

function addDemoPosition(side, size) {
  const pnl = (Math.random() * 200 - 100).toFixed(2);
  const el = document.getElementById('open-positions');
  if (el.querySelector('.empty')) el.innerHTML = '';
  const id = 'demo-' + Date.now();
  const card = document.createElement('div');
  card.className = 'position-card';
  card.id = 'pos-' + id;
  card.innerHTML = `
    <div class="pos-info">
      <div class="pos-symbol">${symbol} <span style="color:${side === 'buy' ? 'var(--accent)' : 'var(--red)'}; font-size:.75rem">${side.toUpperCase()}</span></div>
      <div class="pos-details">${size} @ ${document.getElementById('price-main').textContent} | ${exchange.toUpperCase()}</div>
    </div>
    <div>
      <div class="pos-pnl ${parseFloat(pnl) >= 0 ? 'green' : 'red'}">$${pnl}</div>
      <button class="btn-close" onclick="closeTrade('${id}')">Close</button>
    </div>`;
  el.appendChild(card);
}

async function closeTrade(id) {
  try {
    const res = await fetch(API + '/api/hb/trade/' + id + '/close', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ wallet }) });
    if (!res.ok) throw new Error();
    showToast('Position closed', 'success');
  } catch (e) {
    showToast('Position closed (demo)', 'info');
  }
  const el = document.getElementById('pos-' + id);
  if (el) el.remove();
  if (!document.getElementById('open-positions').children.length) {
    document.getElementById('open-positions').innerHTML = '<div class="empty">No open positions</div>';
  }
}

async function fetchOpenTrades() {
  if (!wallet) return;
  try {
    const res = await fetch(API + '/api/hb/trades/open?wallet=' + wallet);
    if (!res.ok) throw new Error();
    const trades = await res.json();
    const el = document.getElementById('open-positions');
    if (!trades.length) { el.innerHTML = '<div class="empty">No open positions</div>'; return; }
    el.innerHTML = trades.map(t => `
      <div class="position-card" id="pos-${t.id}">
        <div class="pos-info">
          <div class="pos-symbol">${t.symbol} <span style="color:${t.side === 'buy' ? 'var(--accent)' : 'var(--red)'}; font-size:.75rem">${t.side.toUpperCase()}</span></div>
          <div class="pos-details">${t.size} @ $${t.entry_price} | ${(t.exchange || 'both').toUpperCase()}</div>
        </div>
        <div>
          <div class="pos-pnl ${t.pnl >= 0 ? 'green' : 'red'}">$${t.pnl.toFixed(2)}</div>
          <button class="btn-close" onclick="closeTrade('${t.id}')">Close</button>
        </div>
      </div>`).join('');
  } catch (e) {}
}

// --- Leaderboard ---
async function fetchLeaderboard(period) {
  try {
    const res = await fetch(API + '/api/hb/leaderboard?period=' + period);
    if (!res.ok) throw new Error();
    const rows = await res.json();
    renderLeaderboard(rows);
  } catch (e) {
    renderLeaderboard(generateDemoLb());
  }
}

function generateDemoLb() {
  const names = ['CryptoKing', 'SolMaxi', 'DeltaNeut', 'BulkWhale', 'DegenApe', 'LiqHunter', 'AlphaBot', 'MoonSniper', 'FlipMaster', 'SatsStacker'];
  return names.map((n, i) => ({
    rank: i + 1, username: n,
    pnl: (5000 - i * 600 + Math.random() * 200).toFixed(2),
    win_rate: (75 - i * 3 + Math.random() * 5).toFixed(1),
    trades: Math.floor(100 - i * 8 + Math.random() * 20)
  }));
}

function renderLeaderboard(rows) {
  const body = document.getElementById('lb-body');
  if (!rows.length) { body.innerHTML = '<tr><td colspan="6" class="empty">No data yet</td></tr>'; return; }
  body.innerHTML = rows.map(r => {
    const cls = r.rank === 1 ? 'gold' : r.rank === 2 ? 'silver' : r.rank === 3 ? 'bronze' : '';
    const pnlCls = parseFloat(r.pnl) >= 0 ? 'pnl-pos' : 'pnl-neg';
    return `<tr class="${cls}">
      <td>${r.rank}</td>
      <td>${r.username}</td>
      <td class="${pnlCls}">$${parseFloat(r.pnl).toLocaleString()}</td>
      <td>${r.win_rate}%</td>
      <td>${r.trades}</td>
      <td>${getLeagueBadge(r.rank)}</td>
    </tr>`;
  }).join('');
}

function renderRecentTrades(trades) {
  const el = document.getElementById('recent-trades');
  if (!trades.length) { el.innerHTML = '<div class="empty">No trades yet</div>'; return; }
  el.innerHTML = trades.map(t => `
    <div class="trade-item">
      <div class="trade-item-left">
        <div class="trade-item-symbol">${t.symbol} <span style="color:${t.side === 'buy' ? 'var(--accent)' : 'var(--red)'}; font-size:.72rem">${t.side.toUpperCase()}</span></div>
        <div class="trade-item-meta">${t.size} @ $${t.entry_price} &rarr; $${t.exit_price || '--'}</div>
      </div>
      <div class="trade-item-pnl" style="color:${t.pnl >= 0 ? 'var(--accent)' : 'var(--red)'}">$${t.pnl.toFixed(2)}</div>
    </div>`).join('');
}
