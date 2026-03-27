# BulkMind

Self-evolving trading intelligence for Bulk perpetuals exchange (Solana).
Three pillars: **BulkWatch** (exchange health) + **BulkSOL** (staking analytics) + **BulkAlpha** (autonomous trading agents).
Stack: Python 3.12+, asyncio, aiohttp, SQLite, bulk-keychain, EvoSkill.

---

## Architecture

```
main.py               → Orchestrator (asyncio.gather all loops)
config.py             → Single source of truth for ALL settings
db.py                 → SQLite layer (latency, trades, issues, candles, snapshots)
reporter.py           → Telegram alerts (console fallback)
executor.py           → bulk-keychain wrapper (paper + live modes)
bulk_watch.py         → BulkWatch: latency, downtime, orderbook, funding
bulk_stream.py        → BulkStream: WebSocket trade feed, wallet discovery, liquidations
bulk_profile.py       → BulkProfile: wallet profiling via POST /account
bulk_sol.py           → BulkSOL: staking analytics, DeFi deployments, validator earnings
dashboard.py          → FastAPI dashboard + REST API + WebSocket
ta.py                 → Technical analysis (ATR, EMA, Donchian, breakouts)
breakout_bot.py       → BreakoutBot agent (Donchian + volume + ATR SL/TP)
evoskill_integration.py → EvoSkill loop (learns from losing trades)
```

See @README.md for full setup and context.

---

## Key Conventions

**Paper mode is default.** `BREAKOUT_PAPER_MODE = True` in `config.py`.  
Never flip to live without explicit confirmation.

**All settings live in `config.py`.** Don't hardcode values in agent files.

**All DB operations go through `db.py` functions.** Don't write raw SQL elsewhere.

**Every order goes through `BulkExecutor`.** Never call bulk-keychain directly from agents.

**Log issues with `log_issue(severity, category, title, details)`.** Severity: CRITICAL / HIGH / MEDIUM / LOW.

**Async everywhere.** All I/O must be `async`/`await`. No blocking calls in the main loop.

---

## Running the Project

```bash
# Install
pip install aiohttp

# Set env vars
export ANTHROPIC_API_KEY="..."
export BULK_PRIVATE_KEY="..."        # Only for live trading
export TELEGRAM_BOT_TOKEN="..."      # Optional
export TELEGRAM_CHAT_ID="..."        # Optional

# Run
python main.py
```

---

## Adding a New Trading Agent

1. Create `{agent_name}_bot.py` following `breakout_bot.py` structure
2. Implement `async def run(self)` loop
3. Use `log_trade()` / `close_trade()` from `db.py` for all trade tracking
4. Add agent to `asyncio.gather()` in `main.py`
5. Export failure trajectories via `export_failure_trajectories()` for EvoSkill
6. Register as EvoSkill task in `evoskill_integration.py`

Planned agents (in priority order): FundingArb → HLCopier → NewsTrader → MacroTrader → WarTrader

---

## BulkWatch Issue Categories

Use these exact strings for the `category` param in `log_issue()`:

`LATENCY` | `DOWNTIME` | `SLIPPAGE` | `LIQUIDITY` | `FUNDING` | `ORDER_REJECT` | `API_ERROR` | `CODE` | `LIQUIDATION` | `AGENT_ERROR` | `SYSTEM`

---

## bulk-keychain API (Python)

```python
from bulk_keychain import Keypair, Signer
keypair = Keypair.from_base58(key)
signer  = Signer(keypair)

signer.sign(order)                    # single order
signer.sign_all([o1, o2, o3])         # parallel, each own tx
signer.sign_group([entry, sl, tp])    # atomic bracket order
```

Full docs: https://github.com/Bulk-trade/bulk-keychain

---

## EvoSkill Loop

EvoSkill reads failure trajectories from `data/failures.json`.  
Each trajectory format:
```json
{
  "question": "Should I have taken this BUY breakout on BTC-USD?",
  "ground_truth": "NO",
  "agent_answer": "YES",
  "context": { "entry": ..., "pnl_pct": ..., "signal": {...} }
}
```

Skills live in `.claude/skills/`. EvoSkill auto-discovers and rewrites them.  
Full docs: https://github.com/sentient-agi/EvoSkill

---

## What NOT to Do

- Don't add code style rules here — use a linter
- Don't inline Bulk API docs — reference the repo link above
- Don't add new config values outside `config.py`
- Don't use `time.sleep()` — always `await asyncio.sleep()`
- Don't commit private keys or `.env` files
- Don't flip `BREAKOUT_PAPER_MODE = False` without explicit approval

---

## When compacting, always preserve:
- Open trade IDs and their entry/SL/TP levels
- Current BulkWatch downtime state (`is_down`, `down_since`)
- List of modified files and any failing tests
