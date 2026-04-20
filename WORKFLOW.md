# BulkMind — Agentic Development Workflow

## Team Structure

Each agent specializes in a domain. Claude Code sessions pick up issues by label.

### Agent Roles

| Agent | Label | Owns | Skills |
|---|---|---|---|
| **Architect** | `agent:architect` | System design, API contracts, data models | Python, async, WebSocket, DB schema |
| **Frontend** | `agent:frontend` | UI components, interactions, responsive | HTML, CSS, JS, Three.js, LightweightCharts |
| **Backend** | `agent:backend` | API routes, game engines, data pipelines | Python, aiohttp, SQLite, WebSocket |
| **Design System** | `agent:design` | Colors, typography, spacing, component library | CSS variables, animations, accessibility |
| **Data Viz** | `agent:dataviz` | Charts, analytics, heatmaps, profiles | Canvas, SVG, LightweightCharts, D3 concepts |
| **Integrations** | `agent:integrations` | Exchange APIs, Privy, Telegram, LLM calls | aiohttp, REST, WebSocket, auth flows |
| **QA** | `agent:qa` | Testing, validation, edge cases, performance | Python, browser testing, load testing |

---

## Sprint Plan (8 Sprints)

### Sprint 1: Design System Foundation
> Build the visual language. Every component after this uses these tokens.

- [ ] Color palette (dark theme, accent colors, semantic colors)
- [ ] Typography scale (font stack, sizes, weights)
- [ ] Spacing system (4px grid)
- [ ] Component library: buttons, inputs, cards, modals, badges, pills, tabs
- [ ] Animation tokens (transitions, easing, durations)
- [ ] Responsive breakpoints

### Sprint 2: Core Layout & Navigation
> The shell that everything lives inside.

- [ ] App shell (sidebar + main content + header)
- [ ] Navigation system (tabs, routing, active states)
- [ ] Responsive layout (desktop → tablet → mobile)
- [ ] WebSocket connection manager (auto-reconnect, status indicator)
- [ ] Toast notification system
- [ ] Loading states and skeletons

### Sprint 3: Trading Interface
> The primary screen — matches Hyperliquid's trading terminal.

- [ ] Price chart (OHLCV candles, drawing tools, timeframe selector)
- [ ] Order panel (market/limit, size, leverage slider)
- [ ] Position table (open trades, unrealized PnL, close button)
- [ ] Live price ticker (bid/ask/spread from both exchanges)
- [ ] Trade history feed (real-time WebSocket)
- [ ] Exchange toggle (Bulk / Hyperliquid / Both)
- [ ] Faucet button (testnet mode)

### Sprint 4: Game Interfaces (5 Games)
> Gamified trading — each game is its own screen with unique UX.

- [ ] Moon or Doom: multiplier display, cashout button, crash animation, add-to-position
- [ ] Flip It: 60s countdown ring, UP/DOWN buttons, streak counter, payout display
- [ ] Sniper: price prediction input, player list, accuracy tier badges, results modal
- [ ] Battle Royale: lobby + player grid, SL tightening bar, elimination feed, survival timer
- [ ] Alpha Rush: 5-round card flow, signal display (direction/confidence/R:R), execute/skip
- [ ] Game lobby system (create, join, spectate)
- [ ] Results modals (PnL, rank, achievements earned)

### Sprint 5: Analytics Dashboard
> Professional-grade analytics — 11 chart types across 4 categories.

- [ ] Order Flow tab: CVD line, volume delta bars, footprint grid, large trade bubbles
- [ ] Liquidity tab: orderbook heatmap, depth chart
- [ ] Derivatives tab: OI line, funding comparison (Bulk vs HL), liquidation map
- [ ] Market Profile tab: volume profile + POC, TPO letter blocks
- [ ] Symbol + timeframe selectors (shared across all charts)
- [ ] Real-time updates via WebSocket

### Sprint 6: User System & Social
> Identity, progression, and competition.

- [ ] Wallet connect modal (Privy: email, Google, Twitter, Discord, MetaMask, Phantom)
- [ ] User profile page (stats, equity curve, trade history)
- [ ] Achievement system (8 badges with unlock animations)
- [ ] Leaderboard (7 tabs × 4 time periods = 28 views)
- [ ] Portfolio overview (total PnL, win rate, current streak, XP/level)
- [ ] PnL history chart (equity curve)

### Sprint 7: Agent Monitor & Admin
> Operations dashboard for autonomous trading bots.

- [ ] Agent status grid (7 bots, live heartbeat indicators)
- [ ] Per-agent detail: scan count, signal count, trade count, errors, restarts
- [ ] Status badges: running (green), error (red), restarting (yellow), stopped (gray)
- [ ] Stale detection (no heartbeat in 5 min)
- [ ] Error log viewer (last 50 issues by agent)
- [ ] Agent trade feed (recent trades per bot)
- [ ] System health overview (exchange latency, DB stats, uptime)

### Sprint 8: Polish & Performance
> Hyperliquid-level quality bar.

- [ ] Animations: page transitions, number countups, chart loading
- [ ] Responsive: every screen works on mobile
- [ ] Performance: lazy load charts, virtualize long lists, debounce inputs
- [ ] Keyboard shortcuts: Cmd+T (trade), Cmd+L (leaderboard), Escape (close modal)
- [ ] Error handling: graceful fallbacks, retry logic, offline state
- [ ] SEO + meta tags (Open Graph, Twitter cards)
- [ ] PWA support (installable, offline shell)

---

## Workflow Rules

### Issue Lifecycle
```
BACKLOG → IN PROGRESS → REVIEW → DONE
```

### Labels
```
sprint:1 through sprint:8     — which sprint
agent:frontend                — who builds it
agent:backend
agent:design
agent:dataviz
agent:integrations
agent:qa
priority:critical             — blocks other work
priority:high                 — needed this sprint
priority:medium               — nice to have this sprint
priority:low                  — backlog
status:blocked                — waiting on dependency
```

### Issue Template
```markdown
## What
One sentence describing the deliverable.

## Acceptance Criteria
- [ ] Specific, testable requirement 1
- [ ] Specific, testable requirement 2
- [ ] Works on mobile (if UI)
- [ ] API endpoint tested with curl

## Dependencies
- Requires: #issue_number
- Blocks: #issue_number

## Agent
agent:frontend | agent:backend | agent:design | agent:dataviz

## Files
Expected files to create/modify.
```

### Definition of Done
1. Code compiles (`python -c "from main import *"`)
2. Feature works in browser (if UI)
3. API returns expected response (if backend)
4. No regressions in existing features
5. Committed to branch with descriptive message
6. Issue closed with summary comment

---

## Progress Tracking

### Daily Check
```bash
# See all open issues by sprint
gh issue list --label "sprint:1" --state open

# See what's in progress
gh issue list --label "status:in-progress"

# See blocked items
gh issue list --label "status:blocked"
```

### Sprint Completion
Track completion by counting closed vs open issues per sprint label.

### Burndown
Each sprint targets ~5-8 issues. A Claude Code session can typically close 2-3 issues per sitting. Estimated timeline:

| Sprint | Issues | Sessions | Calendar |
|---|---|---|---|
| Sprint 1: Design System | 6 | 2-3 | Week 1 |
| Sprint 2: Core Layout | 6 | 2-3 | Week 1-2 |
| Sprint 3: Trading | 7 | 3-4 | Week 2-3 |
| Sprint 4: Games | 7 | 4-5 | Week 3-4 |
| Sprint 5: Analytics | 6 | 3-4 | Week 4-5 |
| Sprint 6: User System | 6 | 2-3 | Week 5-6 |
| Sprint 7: Agent Monitor | 7 | 2-3 | Week 6 |
| Sprint 8: Polish | 7 | 3-4 | Week 7-8 |

**Total: ~52 issues, ~24 sessions, ~8 weeks to Hyperliquid-level UI**

---

## Reference: Hyperliquid Design Patterns

What makes HL's UI "professional":
- **Dark theme only** — #0d1117 base, high-contrast text
- **Monospace numbers** — all prices/sizes in monospace font
- **Minimal chrome** — no borders, subtle separators, lots of whitespace
- **Information density** — shows a lot of data without feeling cluttered
- **Green/Red semantic** — profit/buy = green, loss/sell = red, neutral = gray
- **Instant feedback** — every click has visual response <100ms
- **No loading spinners** — skeleton states or instant optimistic updates
- **Keyboard-first** — power users can trade without touching the mouse
