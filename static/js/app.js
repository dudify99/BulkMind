const API = '';
let wallet = null, user = null, symbol = 'BTC-USD', exchange = 'both', market = {};
let lbPeriod = 'daily';
let marketInterval = null;

// ── Privy config ─────────────────────────────────────────────────────────────
// Get your free App ID at https://dashboard.privy.io
// Set PRIVY_APP_ID in config.py or override window.PRIVY_APP_ID before this script.
const PRIVY_APP_ID = window.PRIVY_APP_ID || null;
let _privy = null;  // Privy client instance (null until SDK loads)

// Load Privy SDK asynchronously via ESM; expose on _privy when ready.
(async () => {
  if (!PRIVY_APP_ID) {
    console.info('[Privy] No App ID configured — wallet connect will use demo mode.');
    return;
  }
  try {
    const { PrivyClient } = await import(
      'https://cdn.jsdelivr.net/npm/@privy-io/js-sdk-core@latest/+esm'
    );
    _privy = new PrivyClient({
      appId: PRIVY_APP_ID,
      config: {
        loginMethods: ['email', 'google', 'twitter', 'discord', 'wallet'],
        embeddedWallets: { createOnLogin: 'users-without-wallets' },
      },
    });
    console.info('[Privy] SDK ready, appId:', PRIVY_APP_ID);
    // Hide the "no App ID" notice
    const n = document.getElementById('privy-setup-notice');
    if (n) n.style.display = 'none';
  } catch (e) {
    console.warn('[Privy] SDK failed to load, falling back to demo mode:', e);
  }
})();

const ACHIEVEMENTS_DEF = [
  { id: 'first_blood', emoji: '\u{1FA78}', name: 'First Blood', desc: 'Complete your first trade' },
  { id: 'sniper', emoji: '\u{1F3AF}', name: 'Sniper', desc: '5 winning trades in a row' },
  { id: 'diamond_hands', emoji: '\u{1F48E}', name: 'Diamond Hands', desc: 'Hold a position 24h+' },
  { id: 'on_fire', emoji: '\u{1F525}', name: 'On Fire', desc: '10 trades in one day' },
  { id: 'whale_alert', emoji: '\u{1F40B}', name: 'Whale Alert', desc: 'Single trade > $10k PnL' },
  { id: 'lightning', emoji: '\u26A1', name: 'Lightning', desc: 'Close trade within 60 seconds' },
  { id: 'both_barrels', emoji: '\u{1F3B0}', name: 'Both Barrels', desc: 'Trade on Bulk + HL simultaneously' },
  { id: 'top_10', emoji: '\u{1F451}', name: 'Top 10', desc: 'Reach top 10 on leaderboard' }
];

const LEAGUE_BADGES = ['\u{1F949}', '\u{1F948}', '\u{1F947}', '\u{1F48E}', '\u{1F40B}'];

function getLeagueBadge(rank) {
  if (rank <= 3) return LEAGUE_BADGES[2];
  if (rank <= 10) return LEAGUE_BADGES[3];
  if (rank <= 25) return LEAGUE_BADGES[1];
  if (rank <= 50) return LEAGUE_BADGES[0];
  return '';
}

// --- Toast ---
function showToast(msg, type = 'info') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show ' + type;
  setTimeout(() => t.className = 'toast', 2500);
}

// --- Tabs ---
function switchTab(name) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  const tabs = ['trade', 'flipit', 'sniper', 'rush', 'br', 'charts', 'orderflow', 'liquidity', 'deriv', 'mprofile', 'leaderboard', 'portfolio'];
  const idx = tabs.indexOf(name);
  document.querySelectorAll('.tab-btn')[idx].classList.add('active');
  if (name === 'flipit') initFlip();
  if (name === 'sniper') initSniper();
  if (name === 'rush') initRush();
  if (name === 'br') initBR();
  if (name === 'charts') initCharts();
  if (name === 'orderflow') initOrderFlow();
  if (name === 'liquidity') initLiquidity();
  if (name === 'deriv') initDerivatives();
  if (name === 'mprofile') initProfile();
  if (name === 'leaderboard') fetchLeaderboard(lbPeriod);
  if (name === 'portfolio') fetchPortfolio();
  if (name === 'achievements') fetchAchievements();
}

// --- Pills ---
function activatePill(el) {
  el.parentElement.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
}

function setSymbol(s, el) { symbol = s; activatePill(el); fetchMarket(); }
function setExchange(e, el) { exchange = e; activatePill(el); }
function setSize(v) { document.getElementById('size-input').value = v; }
function setLbPeriod(p, el) { lbPeriod = p; activatePill(el); fetchLeaderboard(p); }

// ── Wallet / Privy ───────────────────────────────────────────────────────────

async function connectWallet() {
  if (wallet) {
    // Disconnect
    if (_privy) { try { await _privy.logout(); } catch(_) {} }
    wallet = null; user = null;
    const btn = document.getElementById('btn-wallet');
    btn.textContent = 'Connect Wallet';
    btn.classList.remove('connected');
    return;
  }
  // Show modal — reset to email step
  _privyReset();
  document.getElementById('wallet-modal').classList.add('show');
  // If no App ID, show setup notice and keep forms usable in demo mode
  if (!PRIVY_APP_ID) {
    const n = document.getElementById('privy-setup-notice');
    if (n) n.style.display = '';
  }
  setTimeout(() => {
    const inp = document.getElementById('privy-email');
    if (inp) inp.focus();
  }, 100);
}

function closeModal() {
  document.getElementById('wallet-modal').classList.remove('show');
  _privyReset();
}

function _privyReset() {
  const show = (id) => { const el = document.getElementById(id); if (el) el.style.display = ''; };
  const hide = (id) => { const el = document.getElementById(id); if (el) el.style.display = 'none'; };
  show('privy-step-email');
  hide('privy-step-otp');
  show('privy-step-social');
  const emailEl = document.getElementById('privy-email');
  const otpEl   = document.getElementById('privy-otp');
  const errEmail = document.getElementById('privy-email-err');
  const errOtp   = document.getElementById('privy-otp-err');
  if (emailEl) emailEl.value = '';
  if (otpEl)   otpEl.value   = '';
  if (errEmail) errEmail.textContent = '';
  if (errOtp)   errOtp.textContent   = '';
  _setBtnLoading('privy-email-btn', false);
  _setBtnLoading('privy-otp-btn', false);
}

function _setBtnLoading(id, loading, label) {
  const btn = document.getElementById(id);
  if (!btn) return;
  btn.disabled = loading;
  if (loading) {
    btn.innerHTML = '<span class="privy-spinner"></span>Verifying…';
  } else {
    btn.innerHTML = label || (id === 'privy-otp-btn' ? 'Verify Code' : 'Continue with Email');
  }
}

// ── Email OTP flow ────────────────────────────────────────────────────────────

async function privySendCode() {
  const email = (document.getElementById('privy-email')?.value || '').trim();
  if (!email || !/\S+@\S+\.\S+/.test(email)) {
    document.getElementById('privy-email-err').textContent = 'Enter a valid email address.';
    return;
  }
  document.getElementById('privy-email-err').textContent = '';
  _setBtnLoading('privy-email-btn', true);

  if (_privy) {
    try {
      await _privy.auth.email.sendCode(email);
    } catch (e) {
      document.getElementById('privy-email-err').textContent = e?.message || 'Failed to send code.';
      _setBtnLoading('privy-email-btn', false);
      return;
    }
  }
  // Show OTP step
  document.getElementById('privy-step-email').style.display = 'none';
  document.getElementById('privy-step-otp').style.display = '';
  const lbl = document.getElementById('privy-email-label');
  if (lbl) lbl.textContent = email;
  _setBtnLoading('privy-email-btn', false);
  setTimeout(() => document.getElementById('privy-otp')?.focus(), 80);
}

async function privyVerifyOtp() {
  const email = (document.getElementById('privy-email')?.value || '').trim();
  const code  = (document.getElementById('privy-otp')?.value || '').trim();
  if (!code || code.length < 6) {
    document.getElementById('privy-otp-err').textContent = 'Enter the 6-digit code.';
    return;
  }
  document.getElementById('privy-otp-err').textContent = '';
  _setBtnLoading('privy-otp-btn', true);

  if (_privy) {
    try {
      const { user: u } = await _privy.auth.email.loginWithCode(email, code);
      _onPrivyLogin(u);
      return;
    } catch (e) {
      document.getElementById('privy-otp-err').textContent = e?.message || 'Invalid code.';
      _setBtnLoading('privy-otp-btn', false);
      return;
    }
  }
  // Demo fallback — no Privy SDK
  _onDemoConnect(email);
}

function privyBackToEmail() {
  document.getElementById('privy-step-otp').style.display  = 'none';
  document.getElementById('privy-step-email').style.display = '';
  document.getElementById('privy-otp-err').textContent = '';
}

// ── OAuth (Google / Twitter / Discord) ───────────────────────────────────────

async function privyOAuth(provider) {
  if (_privy) {
    try {
      const { user: u } = await _privy.auth.oauth.loginWithOAuth(provider);
      _onPrivyLogin(u);
    } catch (e) {
      showToast(e?.message || `${provider} login failed`, 'error');
    }
    return;
  }
  // Demo fallback
  _onDemoConnect(provider + '_demo_' + Math.random().toString(36).slice(2, 7));
}

// ── External wallets ──────────────────────────────────────────────────────────

async function privyMetaMask() {
  if (_privy) {
    try {
      const { user: u } = await _privy.auth.wallets.loginWithMetaMask();
      _onPrivyLogin(u);
    } catch (e) {
      showToast(e?.message || 'MetaMask connect failed', 'error');
    }
    return;
  }
  // Browser MetaMask fallback (no Privy)
  if (window.ethereum) {
    try {
      const accounts = await window.ethereum.request({ method: 'eth_requestAccounts' });
      if (accounts[0]) _onDemoConnect(accounts[0]);
    } catch (e) { showToast('MetaMask rejected', 'error'); }
    return;
  }
  _onDemoConnect('0x' + Math.random().toString(16).slice(2, 42));
}

async function privyPhantom() {
  if (_privy) {
    try {
      const { user: u } = await _privy.auth.wallets.loginWithPhantom();
      _onPrivyLogin(u);
    } catch (e) {
      showToast(e?.message || 'Phantom connect failed', 'error');
    }
    return;
  }
  // Browser Phantom fallback
  if (window.solana?.isPhantom) {
    try {
      const resp = await window.solana.connect();
      _onDemoConnect(resp.publicKey.toString());
    } catch (e) { showToast('Phantom rejected', 'error'); }
    return;
  }
  _onDemoConnect('Ph' + Math.random().toString(36).slice(2, 14).toUpperCase());
}

// ── Shared login handler ──────────────────────────────────────────────────────

function _onPrivyLogin(privyUser) {
  // Prefer Solana wallet (app is Solana-native), then Ethereum, then user ID
  const solWallet = privyUser.linkedAccounts?.find(a => a.type === 'solana_wallet');
  const ethWallet = privyUser.linkedAccounts?.find(a => a.type === 'ethereum_wallet' || a.type === 'wallet');
  const addr = solWallet?.address || ethWallet?.address || privyUser.id;
  _finishConnect(addr, privyUser.email?.address || null);
}

function _onDemoConnect(addr) {
  _finishConnect(addr, null);
}

async function _finishConnect(addr, email) {
  wallet = addr;
  closeModal();
  const btn = document.getElementById('btn-wallet');
  btn.textContent = addr.slice(0, 4) + '…' + addr.slice(-4);
  btn.classList.add('connected');
  showToast('Wallet connected!', 'success');

  // Register with backend
  try {
    const username = email ? email.split('@')[0] : addr.slice(0, 8);
    const res = await fetch(API + '/api/hb/register', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet: addr, username }),
    });
    if (res.ok) user = await res.json();
  } catch (_) {}

  fetchOpenTrades();
  fetchPortfolio();
}

// --- Portfolio ---
// ── Faucet ────────────────────────────────────────────────────────────────────

async function requestFaucet() {
  if (!wallet) { showToast('Connect wallet first', 'error'); return; }
  const btn = document.getElementById('faucet-btn');
  btn.disabled = true;
  btn.textContent = 'Requesting…';
  try {
    const res = await fetch(API + '/api/hb/faucet', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet }),
    });
    const data = await res.json();
    if (res.ok) {
      showToast('Testnet USDC credited!', 'success');
    } else {
      showToast(data.error || 'Faucet failed', 'error');
    }
  } catch (e) {
    showToast('Testnet USDC credited (demo)', 'info');
  }
  btn.disabled = false;
  btn.textContent = 'Get Testnet USDC';
}

// --- Portfolio ---

async function fetchPortfolio() {
  if (!wallet) return;
  try {
    const res = await fetch(API + '/api/hb/user/' + wallet);
    if (!res.ok) throw new Error();
    const d = await res.json();
    document.getElementById('s-pnl').textContent = '$' + (d.total_pnl || 0).toFixed(2);
    document.getElementById('s-pnl').style.color = (d.total_pnl || 0) >= 0 ? 'var(--accent)' : 'var(--red)';
    document.getElementById('s-winrate').textContent = (d.win_rate || 0).toFixed(1) + '%';
    document.getElementById('s-trades').textContent = d.total_trades || 0;
    document.getElementById('s-streak').textContent = d.streak || 0;
    document.getElementById('s-xp').textContent = d.xp || 0;
    document.getElementById('s-level').textContent = d.level || 1;
    renderRecentTrades(d.recent_trades || []);
  } catch (e) {}
}

// --- Achievements ---
async function fetchAchievements() {
  let earned = [];
  if (wallet) {
    try {
      const res = await fetch(API + '/api/hb/achievements/' + wallet);
      if (res.ok) earned = await res.json();
    } catch (e) {}
  }
  const earnedIds = new Set(earned.map(a => a.id || a));
  const grid = document.getElementById('ach-grid');
  grid.innerHTML = ACHIEVEMENTS_DEF.map(a => `
    <div class="ach-card ${earnedIds.has(a.id) ? 'earned' : 'locked'}">
      <div class="ach-emoji">${a.emoji}</div>
      <div class="ach-name">${a.name}</div>
      <div class="ach-desc">${a.desc}</div>
    </div>`).join('');
}

// --- WebSocket Live Feed ---
let ws = null;
let wsRetryCount = 0;
const MAX_FEED_ITEMS = 30;

function connectWebSocket() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(proto + '//' + location.host + '/ws');

  ws.onopen = () => {
    wsRetryCount = 0;
    const badge = document.getElementById('ws-badge');
    badge.textContent = 'LIVE';
    badge.className = 'ws-status connected';
    console.log('[WS] Connected');
    const feed = document.getElementById('live-feed');
    if (feed.querySelector('.empty')) feed.innerHTML = '';
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'trade') {
        const trade = typeof msg.content === 'string' ? JSON.parse(msg.content) : msg.content;
        handleLiveTrade(trade);
      } else if (msg.type === 'pnl_update') {
        const updates = typeof msg.content === 'string' ? JSON.parse(msg.content) : msg.content;
        handlePnlUpdate(updates);
      } else if (msg.type === 'alert') {
        showToast(msg.content.slice(0, 80), 'error');
      }
    } catch (e) {}
  };

  ws.onclose = () => {
    const badge = document.getElementById('ws-badge');
    badge.textContent = 'OFFLINE';
    badge.className = 'ws-status disconnected';
    // Reconnect with backoff
    const delay = Math.min(2000 * Math.pow(2, wsRetryCount), 30000);
    wsRetryCount++;
    console.log('[WS] Reconnecting in ' + delay + 'ms...');
    setTimeout(connectWebSocket, delay);
  };

  ws.onerror = () => {};
}

function handleLiveTrade(trade) {
  // 1. Add dot on globe
  if (typeof addTradeToGlobe === 'function') {
    addTradeToGlobe(trade.side === 'buy' || trade.side === 'BUY');
  }

  // 1b. Update chart with live trade
  if (typeof updateChartWithTrade === 'function') {
    updateChartWithTrade(trade);
  }

  // 2. Update price display if symbol matches
  if (trade.symbol === symbol) {
    const p = trade.price;
    document.getElementById('price-main').textContent = '$' + p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  // 3. Add to live feed panel
  const feed = document.getElementById('live-feed');
  if (feed.querySelector('.empty')) feed.innerHTML = '';

  const side = (trade.side || '').toLowerCase();
  const ts = trade.ts ? new Date(trade.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '';
  const ex = trade.exchange || 'bulk';
  const exLabel = ex === 'hyperliquid' ? 'HL' : 'BLK';
  const item = document.createElement('div');
  item.className = 'feed-item';
  item.innerHTML = `
    <span class="feed-exchange ${ex}">${exLabel}</span>
    <span class="feed-side ${side}">${side.toUpperCase()}</span>
    <span class="feed-symbol">${trade.symbol || '--'}</span>
    <span class="feed-price">$${(trade.price || 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}</span>
    <span class="feed-size">${(trade.size || 0).toFixed(4)}</span>
    <span class="feed-time">${ts}</span>`;
  feed.prepend(item);

  // Trim old entries
  while (feed.children.length > MAX_FEED_ITEMS) {
    feed.removeChild(feed.lastChild);
  }
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
