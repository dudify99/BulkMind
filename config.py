"""
BulkMind Configuration
Centralized config for BulkWatch + BulkAlpha
"""

import os
from dataclasses import dataclass, field
from typing import Optional

# ── API Endpoints ──────────────────────────────────────────────
BULK_API_BASE = "https://exchange-api.bulk.trade/api/v1"
BULK_WS_URL   = "wss://exchange-ws1.bulk.trade"

# ── Credentials (set via env vars, never hardcode) ─────────────
BULK_PRIVATE_KEY   = os.getenv("BULK_PRIVATE_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")

# ── Discord ──────────────────────────────────────────────────
DISCORD_WEBHOOK_URL  = os.getenv("DISCORD_WEBHOOK_URL", "")

# ── Dashboard ────────────────────────────────────────────────
DASHBOARD_HOST       = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT       = int(os.getenv("DASHBOARD_PORT", "8080"))

# ── BulkWatch Settings ─────────────────────────────────────────
WATCH_PING_INTERVAL_SEC     = 30       # heartbeat check
WATCH_LATENCY_THRESHOLD_MS  = 500      # alert if above this
WATCH_DOWNTIME_ALERT_SEC    = 60       # alert if down for this long
WATCH_LOG_DIR               = "logs/watch"
WATCH_REPORT_INTERVAL_MIN   = 60       # generate report every N mins

# ── BreakoutBot Settings ───────────────────────────────────────
BREAKOUT_SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD"]
BREAKOUT_TIMEFRAME_MIN  = 15           # candle size
BREAKOUT_LOOKBACK       = 20           # bars to compute range
BREAKOUT_VOLUME_MULT    = 1.5          # volume must be 1.5x avg
BREAKOUT_ATR_MULT       = 1.0          # SL = entry ± 1x ATR
BREAKOUT_TP_RATIO       = 2.0          # TP = 2x risk (1:2 RR)
BREAKOUT_MAX_POSITION_USD = 100        # max position size (paper mode)
BREAKOUT_PAPER_MODE     = True         # set False for live trading

# ── EvoSkill Settings ──────────────────────────────────────────
EVOSKILL_MAX_ITERATIONS = 20
EVOSKILL_FRONTIER_SIZE  = 3
EVOSKILL_TRAIN_RATIO    = 0.7
EVOSKILL_VAL_RATIO      = 0.3

# ── Wallet Discovery & Trade Stream ───────────────────────────
WATCH_WS_RECONNECT_SEC       = 5       # WebSocket reconnect delay
WALLET_PROFILE_INTERVAL_SEC  = 10      # delay between account queries
WALLET_PROFILE_BATCH_SIZE    = 20      # max wallets to profile per cycle
WALLET_DISCOVERY_MAX_QUEUE   = 500     # max pending wallets
WALLET_REFRESH_HOURS         = 6       # re-profile wallet after N hours
LIQUIDATION_ALERT_THRESHOLD_USD = 10000  # alert if liquidation above this
WATCHED_SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD"]

# ── BulkSOL Analytics ────────────────────────────────────────
BULKSOL_LIVE_CHECK_SEC   = 300        # 5 min — fetch live stats for dashboard
BULKSOL_SNAPSHOT_SEC     = 6 * 3600   # 6 hours — persist snapshot to DB for charts
BULKSOL_SUPPLY_ALERT_PCT = 5          # alert if supply changes more than 5%

# ── Storage ────────────────────────────────────────────────────
DB_PATH = "data/bulkmind.db"

@dataclass
class SymbolConfig:
    symbol: str
    min_size: float = 0.001
    tick_size: float = 0.1
    max_leverage: int = 10

SYMBOL_CONFIGS = {
    "BTC-USD": SymbolConfig("BTC-USD", min_size=0.001, tick_size=0.1, max_leverage=20),
    "ETH-USD": SymbolConfig("ETH-USD", min_size=0.01,  tick_size=0.01, max_leverage=20),
    "SOL-USD": SymbolConfig("SOL-USD", min_size=0.1,   tick_size=0.001, max_leverage=20),
}
