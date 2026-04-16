function setRushSymbol(s, el) { rushSymbol = s; activatePill(el); }
function initRush() {}

async function rushStart() {
  if (!wallet) { showToast('Connect wallet first', 'error'); return; }
  const bet = parseFloat(document.getElementById('rush-bet-input').value) || 5;
  try {
    const res = await fetch(API + '/api/hb/rush/start', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet, symbol: rushSymbol, bet_amount: bet, exchange: 'bulk' })
    });
    if (!res.ok) throw new Error();
    const data = await res.json();
    rushGameId = data.game_id;
    showToast('Alpha Rush started!', 'success');
    rushRender(data);
    rushStartPolling(data.game_id);
  } catch (e) {
    rushGameId = 'demo';
    showToast('Alpha Rush started (demo)', 'info');
    rushRenderDemo();
  }
}

function rushStartPolling(gameId) {
  if (rushPollInterval) clearInterval(rushPollInterval);
  rushPollInterval = setInterval(async () => {
    try {
      const res = await fetch(API + '/api/hb/rush/' + gameId);
      if (!res.ok) return;
      rushRender(await res.json());
    } catch (e) {}
  }, 1500);
}

async function rushExecute() {
  if (!rushGameId) return;
  try {
    const res = await fetch(API + '/api/hb/rush/' + rushGameId + '/execute', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet })
    });
    if (res.ok) { rushRender(await res.json()); showToast('Signal executed!', 'success'); }
  } catch (e) { showToast('Executed (demo)', 'info'); }
}

async function rushSkip() {
  if (!rushGameId) return;
  try {
    const res = await fetch(API + '/api/hb/rush/' + rushGameId + '/skip', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet })
    });
    if (res.ok) rushRender(await res.json());
  } catch (e) {}
}

function rushRender(data) {
  const area = document.getElementById('rush-signal-area');
  const pnlEl = document.getElementById('rush-pnl');
  const metaEl = document.getElementById('rush-meta');
  const timerEl = document.getElementById('rush-timer');

  // PnL + meta
  const pnl = data.total_pnl || 0;
  pnlEl.textContent = (pnl >= 0 ? '+' : '') + '$' + pnl.toFixed(2);
  pnlEl.style.color = pnl >= 0 ? 'var(--accent)' : 'var(--red)';
  metaEl.textContent = 'Round ' + (data.current_round || 0) + '/5 | AI: ' +
    (data.ai_accuracy || 0) + '/' + (data.rounds_played || 0) + ' correct';

  // Progress dots
  const dots = document.querySelectorAll('#rush-progress .rush-dot');
  (data.rounds || []).forEach((r, i) => {
    if (i < dots.length) {
      if (r.decision === 'skip') dots[i].className = 'rush-dot skipped';
      else if (r.won) dots[i].className = 'rush-dot won';
      else if (r.decision === 'execute') dots[i].className = 'rush-dot lost';
      else dots[i].className = 'rush-dot active';
    }
  });

  // Timer
  if (data.status === 'live' && data.round_time_left > 0) {
    timerEl.style.display = 'block';
    timerEl.textContent = Math.ceil(data.round_time_left);
    timerEl.className = data.round_time_left < 10 ? 'flip-timer urgent' : 'flip-timer';
  } else {
    timerEl.style.display = 'none';
  }

  // Signal card
  if (data.status === 'live' && data.current_signal && !data.current_decision) {
    const sig = data.current_signal;
    const dir = (sig.direction || 'BUY').toLowerCase();
    const confClass = sig.confidence >= 75 ? 'high' : sig.confidence >= 55 ? 'mid' : 'low';
    area.innerHTML = `
      <div class="rush-signal-card ${dir}">
        <div class="rush-signal-header">
          <span class="rush-signal-strategy">${sig.emoji || ''} ${sig.strategy_name || sig.strategy}</span>
          <span class="rush-signal-confidence ${confClass}">${sig.confidence}%</span>
        </div>
        <div class="rush-signal-direction ${dir}">${sig.direction || 'BUY'}</div>
        <div class="rush-levels">
          <div class="rush-level"><div class="rush-level-label">Entry</div><div class="rush-level-value">$${(sig.entry||0).toLocaleString()}</div></div>
          <div class="rush-level"><div class="rush-level-label">Target</div><div class="rush-level-value" style="color:var(--accent)">$${(sig.target||0).toLocaleString()}</div></div>
          <div class="rush-level"><div class="rush-level-label">Stop</div><div class="rush-level-value" style="color:var(--red)">$${(sig.stop||0).toLocaleString()}</div></div>
          <div class="rush-level"><div class="rush-level-label">R:R</div><div class="rush-level-value">${sig.rr_ratio || '—'}</div></div>
        </div>
        <div class="rush-reason">${sig.reason || ''}</div>
        <div class="rush-actions">
          <button class="rush-btn-execute" onclick="rushExecute()">EXECUTE</button>
          <button class="rush-btn-skip" onclick="rushSkip()">SKIP</button>
        </div>
      </div>`;
  } else if (data.status === 'live' && data.current_decision) {
    area.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text2)">Waiting for round to settle...</div>';
  } else if (data.status === 'finished') {
    clearInterval(rushPollInterval);
    const won = (data.total_pnl || 0) > 0;
    area.innerHTML = `
      <div style="text-align:center;padding:30px">
        <div style="font-size:2.5rem;margin-bottom:8px">${won ? '🏆' : '💀'}</div>
        <div style="font-size:1.4rem;font-weight:800;color:${won ? 'var(--accent)' : 'var(--red)'}">${won ? 'ALPHA EARNED' : 'GAME OVER'}</div>
        <div style="font-size:1.8rem;font-weight:800;font-family:monospace;margin:8px 0;color:${won ? 'var(--accent)' : 'var(--red)'}">
          ${(data.total_pnl||0) >= 0 ? '+' : ''}$${(data.total_pnl||0).toFixed(2)}
        </div>
        <div style="font-size:.8rem;color:var(--text2)">
          ${data.rounds_won||0}/5 rounds won | AI accuracy: ${data.ai_accuracy||0}/${data.rounds_played||0}
        </div>
        <button class="btn-sniper fire" style="margin-top:20px;max-width:200px" onclick="rushGameId=null;rushReset()">PLAY AGAIN</button>
      </div>`;
  }

  // Round history
  const rounds = data.rounds || [];
  if (rounds.length) {
    document.getElementById('rush-rounds-title').style.display = 'block';
    document.getElementById('rush-rounds').innerHTML = rounds.map(r => {
      const pnlColor = r.pnl_usd > 0 ? 'var(--accent)' : r.pnl_usd < 0 ? 'var(--red)' : 'var(--text2)';
      const icon = r.decision === 'skip' ? '⏭️' : r.won ? '✅' : r.pnl_usd < 0 ? '❌' : '⏳';
      return `<div class="rush-round-result">
        <span class="rush-round-emoji">${icon}</span>
        <span class="rush-round-info">R${r.round_num} ${r.emoji||''} ${r.strategy||''} ${r.direction||''}</span>
        <span class="rush-round-pnl" style="color:${pnlColor}">${r.decision==='skip'?'SKIP':
          (r.pnl_usd>=0?'+':'')+'\$'+r.pnl_usd.toFixed(2)}</span>
      </div>`;
    }).join('');
  }
}

function rushReset() {
  document.getElementById('rush-signal-area').innerHTML = `
    <div style="text-align:center;padding:40px 20px">
      <div style="font-size:2rem;margin-bottom:10px">🎯</div>
      <div style="font-size:1rem;font-weight:700;margin-bottom:6px">Alpha Rush</div>
      <div style="font-size:.8rem;color:var(--text2);margin-bottom:20px">
        5 rounds × 1 minute. AI gives you a sniper signal each round.<br>Execute or skip. Your PnL is real.
      </div>
      <div class="size-row" style="justify-content:center">
        <input type="number" class="size-input" id="rush-bet-input" placeholder="Stake ($)" value="5" step="1" min="1" style="max-width:120px;text-align:center">
      </div>
      <button class="btn-sniper fire" style="margin-top:14px;max-width:300px" onclick="rushStart()">START ALPHA RUSH — $5</button>
    </div>`;
  document.getElementById('rush-pnl').textContent = '$0.00';
  document.getElementById('rush-pnl').style.color = 'var(--text)';
  document.getElementById('rush-meta').textContent = 'Round 0/5 | AI: 0/0 correct';
  document.querySelectorAll('#rush-progress .rush-dot').forEach(d => d.className = 'rush-dot');
  document.getElementById('rush-rounds-title').style.display = 'none';
  document.getElementById('rush-rounds').innerHTML = '';
}

function rushRenderDemo() {
  rushRender({
    game_id: 'demo', status: 'live', current_round: 1, total_rounds: 5,
    round_time_left: 52, total_pnl: 0, rounds_won: 0, rounds_played: 0, ai_accuracy: 0,
    current_signal: {
      strategy: 'breakout', strategy_name: 'Scalp Breakout', emoji: '💥',
      direction: 'BUY', confidence: 82, entry: 69450, target: 69520, stop: 69410,
      rr_ratio: 1.75, reason: 'BTC broke above 1m Donchian high with 1.8x avg volume. EMA5 > EMA20 confirms.'
    },
    current_decision: '', rounds: [],
  });
}

// --- Battle Royale ---
let brSymbol = 'BTC-USD';
let brGameId = null;
let brPollInterval = null;

function setBrSymbol(s, el) { brSymbol = s; activatePill(el); }

function initBR() { brFetchActive(); }

async function brCreate() {
  if (!wallet) { showToast('Connect wallet first', 'error'); return; }
  try {
    const res = await fetch(API + '/api/hb/br/create', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol: brSymbol, direction: 'long', entry_fee: 10 })
    });
    if (!res.ok) throw new Error();
    const data = await res.json();
    brGameId = data.game_id;
    showToast('Battle #' + data.game_id + ' created!', 'success');
    brFetchActive();
  } catch (e) {
    brGameId = 'demo';
    showToast('Battle created (demo)', 'info');
    brShowDemo();
  }
}

async function brJoin() {
  if (!wallet) { showToast('Connect wallet first', 'error'); return; }
  if (!brGameId) {
    await brCreate();
    return;
  }
  try {
    const res = await fetch(API + '/api/hb/br/' + brGameId + '/join', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet })
    });
    if (!res.ok) throw new Error();
    showToast('Joined battle!', 'success');
    brStartPolling(brGameId);
  } catch (e) {
    showToast('Joined (demo)', 'info');
    brShowDemo();
  }
}

function brStartPolling(gameId) {
  if (brPollInterval) clearInterval(brPollInterval);
  brPollInterval = setInterval(async () => {
    try {
      const res = await fetch(API + '/api/hb/br/' + gameId);
      if (!res.ok) return;
      const data = await res.json();
      brRenderState(data);
      if (data.status === 'settled') {
        clearInterval(brPollInterval);
        showToast('Battle over!', 'info');
      }
    } catch (e) {}
  }, 1500);
}

function brRenderState(data) {
  // Alive count
  document.getElementById('br-alive').textContent = data.alive_count || 0;
  document.getElementById('br-pot').textContent = 'Pot: $' + (data.pot_usd || 0).toFixed(2);
  document.getElementById('br-sl').textContent = 'SL: -' + (data.current_sl_pct || 2).toFixed(2) + '%';

  // Timer
  const elapsed = data.elapsed_sec || 0;
  const min = Math.floor(elapsed / 60);
  const sec = Math.floor(elapsed % 60);
  document.getElementById('br-time').textContent = min + ':' + sec.toString().padStart(2, '0');

  // Shrinking circles (visual — smaller as SL tightens)
  const slRatio = (data.current_sl_pct || 2) / 2;  // Ratio of current to initial
  const outerSize = 90 * slRatio;
  document.getElementById('br-circle-outer').style.width = outerSize + '%';
  document.getElementById('br-circle-outer').style.height = outerSize + '%';

  // Elimination feed
  const elimFeed = document.getElementById('br-elim-feed');
  const elims = data.eliminations || [];
  if (elims.length) {
    elimFeed.innerHTML = elims.slice(-10).reverse().map(e => {
      const survival = e.survival_sec ? e.survival_sec.toFixed(0) + 's' : '';
      return '<div class="br-elim-item">' +
        '<span class="elim-skull">\u{1F480}</span>' +
        '<span class="elim-name">' + e.username + '</span>' +
        '<span class="elim-rank">#' + e.rank + '</span>' +
        '<span class="elim-time">' + survival + '</span>' +
        '</div>';
    }).join('');
  }

  // Player list
  const players = data.players || [];
  const plEl = document.getElementById('br-players');
  plEl.innerHTML = players.map(p => {
    let cls = 'br-player';
    if (p.status === 'eliminated') cls += ' eliminated';
    if (p.status === 'winner') cls += ' winner';
    const payout = p.payout_usd > 0 ? '+$' + p.payout_usd.toFixed(2) : '';
    return '<div class="' + cls + '">' +
      '<span>' + p.username + '</span>' +
      '<span style="color:var(--text2)">' + p.status + '</span>' +
      '<span style="font-family:monospace;color:var(--accent)">' + payout + '</span>' +
      '</div>';
  }).join('');
}

function brShowDemo() {
  brRenderState({
    alive_count: 47, pot_usd: 500, current_sl_pct: 1.5,
    elapsed_sec: 180, shrink_count: 6,
    eliminations: [
      { username: 'DegenApe', rank: 50, survival_sec: 30 },
      { username: 'PaperHands', rank: 49, survival_sec: 45 },
      { username: 'LiqHunter', rank: 48, survival_sec: 62 },
    ],
    players: [
      { username: 'You', status: 'alive', payout_usd: 0 },
      { username: 'CryptoKing', status: 'alive', payout_usd: 0 },
      { username: 'SolMaxi', status: 'alive', payout_usd: 0 },
      { username: 'DegenApe', status: 'eliminated', payout_usd: 0 },
    ],
  });
}

async function brFetchActive() {
  try {
    const res = await fetch(API + '/api/hb/br/active');
    if (!res.ok) throw new Error();
    const games = await res.json();
    const el = document.getElementById('br-active-games');
    if (!games || !games.length) {
      el.innerHTML = '<div class="empty">No active battles \u2014 create one!</div>';
      return;
    }
    el.innerHTML = games.map(g => {
      return '<div class="sniper-round-card" onclick="brGameId=' + g.game_id + ';brStartPolling(' + g.game_id + ')">' +
        '<div class="round-header">' +
        '<span class="round-symbol">' + g.symbol + ' #' + g.game_id + '</span>' +
        '<span class="round-status ' + g.status + '">' + g.status.toUpperCase() + '</span></div>' +
        '<div class="round-meta">' +
        '<span>' + g.alive_count + '/' + g.player_count + ' alive</span>' +
        '<span>Pot: $' + (g.pot_usd || 0).toFixed(0) + '</span>' +
        '<span>SL: -' + (g.current_sl_pct || 2).toFixed(1) + '%</span>' +
        '</div></div>';
    }).join('');
  } catch (e) {}
}

// --- Flip It Game ---
let flipSymbol = 'BTC-USD';
let flipDirection = null;
let flipGameId = null;
let flipPollInterval = null;
let flipInitialized = false;
function setFlipSymbol(s, el) { flipSymbol = s; activatePill(el); flipUpdatePrice(); }

function initFlip() {
  flipInitialized = true;
  flipUpdatePrice();
  flipFetchHistory();
  flipFetchStreak();
}

function flipUpdatePrice() {
  const prices = { 'BTC-USD': 69425, 'ETH-USD': 3521, 'SOL-USD': 187 };
  const p = market.price || market.bulk_price || prices[flipSymbol] || 0;
  document.getElementById('flip-live-price').textContent = '$' + p.toLocaleString('en-US', { minimumFractionDigits: 2 });
}

function setFlipDirection(dir) {
  flipDirection = dir;
  document.querySelectorAll('.flip-dir-btn').forEach(b => b.classList.remove('selected'));
  document.querySelector('.flip-dir-btn.' + dir).classList.add('selected');
  const face = document.getElementById('flip-coin-face');
  face.textContent = dir === 'up' ? '\u2191' : '\u2193';
  face.className = 'flip-coin-face flip-coin-front ' + dir + '-selected';
}

async function flipGo() {
  if (!wallet) { showToast('Connect wallet first', 'error'); return; }
  if (!flipDirection) { showToast('Pick UP or DOWN', 'error'); return; }
  const bet = parseFloat(document.getElementById('flip-bet-input').value) || 5;

  // Disable button
  const btn = document.getElementById('btn-flip-go');
  btn.disabled = true;
  btn.textContent = 'FLIPPING...';
  document.getElementById('flip-result').style.display = 'none';

  // Spin coin animation
  document.getElementById('flip-coin-inner').classList.add('spinning');
  setTimeout(() => document.getElementById('flip-coin-inner').classList.remove('spinning'), 800);

  try {
    const res = await fetch(API + '/api/hb/flip/start', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet, symbol: flipSymbol, direction: flipDirection, bet_amount: bet, exchange: 'bulk' })
    });
    if (!res.ok) throw new Error();
    const data = await res.json();
    flipGameId = data.game_id;
    showToast(flipDirection.toUpperCase() + ' $' + bet + ' on ' + flipSymbol, 'success');

    // Show timer and start polling
    document.getElementById('flip-timer').style.display = 'block';
    document.getElementById('flip-dir-btns').style.display = 'none';
    flipStartPolling(data.game_id);
  } catch (e) {
    // Demo mode — simulate 60s countdown then random result
    showToast('Flip started (demo)', 'info');
    flipGameId = 'demo';
    document.getElementById('flip-timer').style.display = 'block';
    document.getElementById('flip-dir-btns').style.display = 'none';
    flipDemoCountdown(bet);
  }
}

function flipStartPolling(gameId) {
  if (flipPollInterval) clearInterval(flipPollInterval);
  flipPollInterval = setInterval(async () => {
    try {
      const res = await fetch(API + '/api/hb/flip/' + gameId);
      if (!res.ok) return;
      const data = await res.json();

      // Update timer
      const remaining = data.time_remaining || 0;
      const timer = document.getElementById('flip-timer');
      timer.textContent = Math.ceil(remaining);
      timer.className = remaining < 10 ? 'flip-timer urgent' : 'flip-timer';

      // If settled
      if (data.status === 'won' || data.status === 'lost') {
        clearInterval(flipPollInterval);
        flipShowResult(data);
      }
    } catch (e) {}
  }, 1000);
}

function flipDemoCountdown(bet) {
  let remaining = 60;
  const interval = setInterval(() => {
    remaining--;
    const timer = document.getElementById('flip-timer');
    timer.textContent = remaining;
    timer.className = remaining < 10 ? 'flip-timer urgent' : 'flip-timer';

    if (remaining <= 0) {
      clearInterval(interval);
      const won = Math.random() > 0.45;
      flipShowResult({
        status: won ? 'won' : 'lost',
        won: won,
        direction: flipDirection,
        payout_usd: won ? +(bet * 1.8).toFixed(2) : 0,
        pnl_usd: won ? +(bet * 0.8).toFixed(2) : -bet,
        price_change_pct: (Math.random() * 0.4 - 0.2).toFixed(3),
        streak: won ? 1 : 0,
        payout_multiplier: 1.8,
        next_streak_mult: won ? 1.8 : 1.8,
      });
    }
  }, 1000);
}

function flipShowResult(data) {
  const won = data.won || data.status === 'won';
  const face = document.getElementById('flip-coin-face');
  const coin = document.getElementById('flip-coin-inner');

  // Spin and reveal
  coin.classList.add('spinning');
  setTimeout(() => {
    coin.classList.remove('spinning');
    face.textContent = won ? '\u2705' : '\u274C';
    face.className = 'flip-coin-face flip-coin-front ' + (won ? 'won' : 'lost');
  }, 400);

  // Show result
  const result = document.getElementById('flip-result');
  result.style.display = 'block';
  result.className = 'flip-result ' + (won ? 'win' : 'lose');
  document.getElementById('flip-result-text').textContent = won ? 'YOU WON!' : 'YOU LOST';
  document.getElementById('flip-result-text').style.color = won ? 'var(--accent)' : 'var(--red)';
  const pnl = data.pnl_usd || 0;
  document.getElementById('flip-result-pnl').textContent = (pnl >= 0 ? '+' : '') + '$' + pnl.toFixed(2) +
    (won ? ' (' + (data.payout_multiplier || 1.8) + 'x)' : '');
  document.getElementById('flip-result-pnl').style.color = pnl >= 0 ? 'var(--accent)' : 'var(--red)';

  // Update streak
  document.getElementById('flip-streak').textContent = data.streak || 0;
  document.getElementById('flip-mult').textContent = (data.next_streak_mult || 1.8) + 'x payout';

  // Reset UI after 2 seconds
  setTimeout(() => {
    document.getElementById('flip-timer').style.display = 'none';
    document.getElementById('flip-dir-btns').style.display = 'flex';
    document.getElementById('btn-flip-go').disabled = false;
    document.getElementById('btn-flip-go').textContent = 'FLIP IT';
    flipDirection = null;
    document.querySelectorAll('.flip-dir-btn').forEach(b => b.classList.remove('selected'));
    face.textContent = '?';
    face.className = 'flip-coin-face flip-coin-front';
    flipFetchHistory();
  }, 3000);
}

async function flipFetchStreak() {
  if (!wallet) return;
  try {
    const res = await fetch(API + '/api/hb/flip/stats/' + wallet);
    if (!res.ok) return;
    const data = await res.json();
    document.getElementById('flip-streak').textContent = data.current_streak || 0;
    const mult = data.current_streak >= 10 ? 5.0 : data.current_streak >= 7 ? 3.0 :
      data.current_streak >= 5 ? 2.5 : data.current_streak >= 3 ? 2.0 : 1.8;
    document.getElementById('flip-mult').textContent = mult + 'x payout';
  } catch (e) {}
}

async function flipFetchHistory() {
  const el = document.getElementById('flip-history');
  if (!wallet) {
    // Demo history
    el.innerHTML = [
      { direction: 'up', symbol: 'BTC-USD', won: 1, pnl_usd: 4.0, streak: 3 },
      { direction: 'down', symbol: 'ETH-USD', won: 0, pnl_usd: -5.0, streak: 0 },
      { direction: 'up', symbol: 'SOL-USD', won: 1, pnl_usd: 9.0, streak: 2 },
      { direction: 'up', symbol: 'BTC-USD', won: 1, pnl_usd: 4.0, streak: 1 },
    ].map(flipRenderHistoryItem).join('');
    return;
  }
  try {
    const res = await fetch(API + '/api/hb/flip/history/' + wallet);
    if (!res.ok) throw new Error();
    const data = await res.json();
    if (!data.length) { el.innerHTML = '<div class="empty">No flips yet</div>'; return; }
    el.innerHTML = data.slice(0, 10).map(flipRenderHistoryItem).join('');
  } catch (e) {}
}

function flipRenderHistoryItem(g) {
  const won = g.won || g.status === 'won';
  const pnl = g.pnl_usd || 0;
  const arrow = g.direction === 'up' ? '\u2191' : '\u2193';
  const dirColor = g.direction === 'up' ? 'var(--accent)' : 'var(--red)';
  return '<div class="flip-history-item">' +
    '<span class="flip-history-dir" style="color:' + dirColor + '">' + arrow + ' ' + (g.direction || '').toUpperCase() + '</span>' +
    '<span>' + (g.symbol || 'BTC-USD') + '</span>' +
    '<span style="color:' + (won ? 'var(--accent)' : 'var(--red)') + ';font-weight:700">' + (won ? 'WIN' : 'LOSS') + '</span>' +
    '<span style="font-family:monospace;color:' + (pnl >= 0 ? 'var(--accent)' : 'var(--red)') + '">' +
    (pnl >= 0 ? '+' : '') + '$' + pnl.toFixed(2) + '</span>' +
    '<span style="color:var(--text2)">' + (g.streak || 0) + ' streak</span>' +
    '</div>';
}

// --- Sniper Game ---
let sniperSymbol = 'BTC-USD';
let sniperRoundId = null;
let sniperPollInterval = null;
let sniperInitialized = false;
function setSniperSymbol(s, el) {
  sniperSymbol = s;
  activatePill(el);
  sniperUpdateLivePrice();
}

function initSniper() {
  if (!sniperInitialized) {
    sniperInitialized = true;
  }
  sniperUpdateLivePrice();
  sniperFetchActive();
  sniperFetchLeaderboard();
}

function sniperUpdateLivePrice() {
  // Use current market data to show live price in scope
  const prices = { 'BTC-USD': 69425, 'ETH-USD': 3521, 'SOL-USD': 187 };
  const base = market.price || market.bulk_price || prices[sniperSymbol] || 0;
  const el = document.getElementById('sniper-live');
  el.textContent = 'LIVE: $' + base.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  // Auto-fill input if empty
  const input = document.getElementById('sniper-price-input');
  if (!input.value) {
    input.value = base.toFixed(2);
    sniperUpdateTarget();
  }
}

function sniperUpdateTarget() {
  const val = parseFloat(document.getElementById('sniper-price-input').value) || 0;
  document.getElementById('sniper-target-price').textContent = '$' + val.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

// Update target display as user types
document.addEventListener('DOMContentLoaded', () => {
  const inp = document.getElementById('sniper-price-input');
  if (inp) inp.addEventListener('input', sniperUpdateTarget);
});

function sniperAdjust(delta) {
  const inp = document.getElementById('sniper-price-input');
  const val = parseFloat(inp.value) || 0;
  inp.value = (val + delta).toFixed(2);
  sniperUpdateTarget();
}

function sniperSetCurrent() {
  const prices = { 'BTC-USD': 69425, 'ETH-USD': 3521, 'SOL-USD': 187 };
  const base = market.price || market.bulk_price || prices[sniperSymbol] || 0;
  document.getElementById('sniper-price-input').value = base.toFixed(2);
  sniperUpdateTarget();
}

async function sniperCreateRound() {
  if (!wallet) { showToast('Connect wallet first', 'error'); return; }
  try {
    const res = await fetch(API + '/api/hb/sniper/create', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol: sniperSymbol, entry_fee: 5, duration_sec: 300 })
    });
    if (!res.ok) throw new Error();
    const data = await res.json();
    sniperRoundId = data.round_id;
    showToast('Round #' + data.round_id + ' created!', 'success');
    sniperFetchActive();
    sniperStartPolling(data.round_id);
  } catch (e) {
    // Demo mode
    sniperRoundId = 'demo-' + Date.now();
    showToast('Round created (demo)', 'info');
    sniperShowDemoRound();
  }
}

async function sniperFire() {
  if (!wallet) { showToast('Connect wallet first', 'error'); return; }
  const price = parseFloat(document.getElementById('sniper-price-input').value);
  if (!price || price <= 0) { showToast('Enter a price prediction', 'error'); return; }

  if (!sniperRoundId) {
    showToast('Join or create a round first', 'error'); return;
  }

  try {
    const res = await fetch(API + '/api/hb/sniper/' + sniperRoundId + '/predict', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet: wallet, price: price })
    });
    if (!res.ok) { const d = await res.json(); throw new Error(d.error || 'Failed'); }
    const data = await res.json();
    showToast('Prediction locked: $' + price.toLocaleString(), 'success');
    document.getElementById('btn-sniper-fire').disabled = true;
    document.getElementById('btn-sniper-fire').textContent = 'Prediction Submitted';
    sniperFetchActive();
  } catch (e) {
    showToast('Prediction submitted (demo): $' + price.toLocaleString(), 'info');
    document.getElementById('btn-sniper-fire').disabled = true;
    document.getElementById('btn-sniper-fire').textContent = 'Prediction Submitted';
  }
}

function sniperStartPolling(roundId) {
  if (sniperPollInterval) clearInterval(sniperPollInterval);
  sniperPollInterval = setInterval(() => sniperPollRound(roundId), 2000);
}

async function sniperPollRound(roundId) {
  try {
    const res = await fetch(API + '/api/hb/sniper/' + roundId);
    if (!res.ok) return;
    const data = await res.json();

    // Update timer
    const timer = document.getElementById('sniper-timer');
    const remaining = data.time_remaining_sec || 0;
    const min = Math.floor(remaining / 60);
    const sec = Math.floor(remaining % 60);
    timer.textContent = min + ':' + sec.toString().padStart(2, '0');
    timer.className = remaining < 30 ? 'sniper-timer urgent' : 'sniper-timer';

    // Update player count
    document.getElementById('sniper-players').textContent = (data.player_count || 0) + ' snipers';

    // If settled, show results
    if (data.status === 'settled') {
      clearInterval(sniperPollInterval);
      sniperShowResults(data);
    }
  } catch (e) {}
}

function sniperShowResults(data) {
  document.getElementById('sniper-results-title').style.display = 'block';
  const el = document.getElementById('sniper-results');
  const preds = data.predictions || [];

  let html = '<div style="text-align:center;padding:12px;font-size:.85rem;color:var(--text2);margin-bottom:10px">';
  html += 'Actual Price: <span style="color:var(--accent);font-weight:800;font-size:1.1rem;font-family:monospace">$' +
    (data.actual_price || 0).toLocaleString('en-US', {minimumFractionDigits: 2}) + '</span>';
  html += ' | Prize Pool: <span style="color:var(--accent);font-weight:700">$' + (data.prize_pool_usd || 0).toFixed(2) + '</span>';
  html += '</div>';

  preds.forEach((p, i) => {
    const isWinner = p.payout_usd > 0;
    const tierClass = 'tier-' + (p.accuracy_tier || 'miss');
    const tierEmoji = { perfect: '\\u2B50', sniper: '\\uD83C\\uDFAF', sharp: '\\uD83D\\uDD25', close: '\\uD83D\\uDC4C', miss: '\\u274C' };
    const emoji = tierEmoji[p.accuracy_tier] || '\\u274C';

    html += '<div class="sniper-result ' + (isWinner ? 'winner' : '') + '">';
    html += '<div class="result-rank">#' + p.rank + '</div>';
    html += '<div class="result-info">';
    html += '<div class="result-name">' + (p.username || 'Player') + '</div>';
    html += '<div class="result-pred">Predicted: $' + (p.predicted_price || 0).toLocaleString('en-US', {minimumFractionDigits: 2}) +
      ' | $' + (p.distance_usd || 0).toFixed(2) + ' off</div>';
    html += '</div>';
    html += '<div class="result-accuracy">';
    if (isWinner) html += '<div class="result-payout">+$' + p.payout_usd.toFixed(2) + '</div>';
    html += '<div class="result-tier ' + tierClass + '">' + (p.accuracy_tier || 'miss').toUpperCase() + '</div>';
    html += '</div></div>';
  });

  el.innerHTML = html;

  // Reset fire button
  document.getElementById('btn-sniper-fire').disabled = false;
  document.getElementById('btn-sniper-fire').textContent = 'Submit Prediction \u2014 $5 Entry';
  sniperRoundId = null;
}

async function sniperFetchActive() {
  try {
    const res = await fetch(API + '/api/hb/sniper/active');
    if (!res.ok) throw new Error();
    const rounds = await res.json();
    renderSniperRounds(rounds);
  } catch (e) {
    sniperShowDemoRound();
  }
}

function renderSniperRounds(rounds) {
  const el = document.getElementById('sniper-active-rounds');
  if (!rounds || !rounds.length) {
    el.innerHTML = '<div class="empty">No active rounds \u2014 create one!</div>';
    return;
  }
  el.innerHTML = rounds.map(r => {
    const remaining = r.time_remaining_sec || 0;
    const min = Math.floor(remaining / 60);
    const sec = Math.floor(remaining % 60);
    return '<div class="sniper-round-card" onclick="sniperJoinRound(' + r.round_id + ')">' +
      '<div class="round-header">' +
      '<span class="round-symbol">' + r.symbol + ' #' + r.round_id + '</span>' +
      '<span class="round-status ' + r.status + '">' + r.status.toUpperCase() + '</span>' +
      '</div>' +
      '<div class="round-meta">' +
      '<span>' + r.player_count + ' players</span>' +
      '<span>Pot: $' + (r.pot_usd || 0).toFixed(2) + '</span>' +
      '<span>' + min + ':' + sec.toString().padStart(2, '0') + ' left</span>' +
      '</div></div>';
  }).join('');
}

function sniperJoinRound(roundId) {
  sniperRoundId = roundId;
  sniperStartPolling(roundId);
  showToast('Joined round #' + roundId, 'info');
  document.getElementById('sniper-price-input').focus();
}

function sniperShowDemoRound() {
  const el = document.getElementById('sniper-active-rounds');
  el.innerHTML = '<div class="sniper-round-card">' +
    '<div class="round-header">' +
    '<span class="round-symbol">' + sniperSymbol + ' (Demo)</span>' +
    '<span class="round-status open">OPEN</span>' +
    '</div>' +
    '<div class="round-meta">' +
    '<span>3 players</span><span>Pot: $15.00</span><span>4:32 left</span>' +
    '</div></div>';
}

async function sniperFetchLeaderboard() {
  try {
    const res = await fetch(API + '/api/hb/sniper/leaderboard');
    if (!res.ok) throw new Error();
    const data = await res.json();
    renderSniperLb(data);
  } catch (e) {
    renderSniperLb(generateDemoSniperLb());
  }
}

function generateDemoSniperLb() {
  return [
    { username: 'SharpShooter', total_winnings: 342.50, first_places: 12, rounds_played: 45, avg_accuracy: 0.038 },
    { username: 'PriceWhisper', total_winnings: 218.00, first_places: 8, rounds_played: 38, avg_accuracy: 0.054 },
    { username: 'BullseyeBob', total_winnings: 156.75, first_places: 5, rounds_played: 30, avg_accuracy: 0.071 },
    { username: 'SolSniper', total_winnings: 89.25, first_places: 3, rounds_played: 22, avg_accuracy: 0.092 },
    { username: 'ETHEagle', total_winnings: 67.00, first_places: 2, rounds_played: 18, avg_accuracy: 0.105 },
  ];
}

function renderSniperLb(data) {
  const el = document.getElementById('sniper-lb');
  if (!data || !data.length) { el.innerHTML = '<div class="empty">No sniper data yet</div>'; return; }
  el.innerHTML = data.map((d, i) => {
    const rank = i + 1;
    const medal = rank === 1 ? '\\uD83E\\uDD47' : rank === 2 ? '\\uD83E\\uDD48' : rank === 3 ? '\\uD83E\\uDD49' : '#' + rank;
    return '<div class="sniper-result ' + (rank <= 3 ? 'winner' : '') + '">' +
      '<div class="result-rank">' + medal + '</div>' +
      '<div class="result-info">' +
      '<div class="result-name">' + d.username + '</div>' +
      '<div class="result-pred">' + (d.rounds_played || 0) + ' rounds | ' +
        (d.first_places || 0) + ' wins | avg ' + ((d.avg_accuracy || 0)).toFixed(3) + '%</div>' +
      '</div>' +
      '<div class="result-payout">$' + (d.total_winnings || 0).toFixed(2) + '</div>' +
      '</div>';
  }).join('');
}

// --- Order Flow ---
let ofSymbol = 'BTC-USD';
let cvdChart = null, cvdSeries = null;
let deltaChart = null, deltaBuySeries = null, deltaSellSeries = null;
let bubblesChart = null, bubbleSeries = null;
let ofInitialized = false;
