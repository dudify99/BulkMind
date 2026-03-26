# BulkMind 🧠

**Self-evolving trading intelligence for Bulk perpetuals exchange.**

Built on: [bulk-keychain](https://github.com/Bulk-trade/bulk-keychain) + [EvoSkill](https://github.com/sentient-agi/EvoSkill)

---

## What It Does

### 🔍 BulkWatch (Pillar 1)
Real-time monitoring of Bulk exchange health:
- Latency tracking per endpoint (p50/p95/p99)
- Downtime detection + alerting
- Order book depth + spread monitoring
- Funding rate anomaly detection
- Issue logging with severity levels
- Hourly reports via Telegram

### 🤖 BulkAlpha (Pillar 2) — BreakoutBot
Technical analysis trading agent:
- Donchian Channel breakout detection
- Volume confirmation filter
- ATR-based SL/TP sizing
- Trend filter (EMA50)
- Atomic bracket orders via bulk-keychain `signGroup`
- Paper trading mode (safe to run immediately)
- Self-improvement via EvoSkill (learns from losing trades)

---

## Setup

### 1. Install dependencies
```bash
pip install aiohttp
pip install bulk-keychain      # For live trading
```

### 2. Environment variables
```bash
export BULK_PRIVATE_KEY="your-base58-private-key"    # Only for live trading
export TELEGRAM_BOT_TOKEN="your-bot-token"            # Optional
export TELEGRAM_CHAT_ID="your-chat-id"                # Optional
export ANTHROPIC_API_KEY="your-api-key"               # For EvoSkill
```

### 3. Run
```bash
# Paper trading mode (safe default)
python main.py

# Live trading — set BREAKOUT_PAPER_MODE=False in config.py
```

---

## Project Structure

```
bulkmind/
├── main.py                  # Orchestrator — runs everything
├── config.py                # All settings in one place
├── db.py                    # SQLite database layer
├── reporter.py              # Telegram alerts
├── executor.py              # bulk-keychain wrapper
├── bulk_watch.py            # BulkWatch monitoring suite
├── ta.py                    # Technical analysis library
├── breakout_bot.py          # BreakoutBot trading agent
├── evoskill_integration.py  # EvoSkill self-improvement loop
└── data/
    ├── bulkmind.db          # SQLite database
    └── failures.json        # EvoSkill training data
```

---

## How EvoSkill Integration Works

```
BreakoutBot runs → logs losing trades → exports failure trajectories
       ↓
EvoSkill Proposer analyzes failures → suggests new skill files
       ↓
Generator writes improved skills to .claude/skills/
       ↓
Evaluator scores new skills on held-out failures
       ↓
Frontier keeps best-performing skill combinations
       ↓
BreakoutBot uses evolved skills → fewer losses
```

---

## Coming Next

- [ ] FundingArb agent
- [ ] HLCopier (Hyperliquid wallet mirroring)
- [ ] NewsTrader (LLM-powered event detection)
- [ ] MacroTrader (economic calendar)
- [ ] WarTrader (geopolitical event classifier)
- [ ] Public dashboard (FastAPI + React)
- [ ] CT auto-posting agent
