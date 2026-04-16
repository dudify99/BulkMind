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
HL_API_BASE   = "https://api.hyperliquid.xyz"

# ── Credentials (set via env vars, never hardcode) ─────────────
BULK_PRIVATE_KEY   = os.getenv("BULK_PRIVATE_KEY", "")
HL_PRIVATE_KEY     = os.getenv("HL_PRIVATE_KEY", "")       # Hyperliquid (EVM key)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

# ── Privy Wallet Auth ─────────────────────────────────────────
# Sign up free at https://dashboard.privy.io → Create App → copy App ID
PRIVY_APP_ID = os.getenv("PRIVY_APP_ID", "")   # e.g. "cm1abc2def3ghi"

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

# ── NewsTrader Settings ───────────────────────────────────────
NEWS_EXCHANGES          = ["bulk", "hyperliquid"]  # trade on both exchanges
NEWS_SYMBOLS            = ["BTC-USD", "ETH-USD", "SOL-USD"]
NEWS_POLL_INTERVAL_SEC  = 60           # check news sources every 60s
NEWS_MIN_IMPACT_SCORE   = 7            # only trade impact score >= 7/10
NEWS_ATR_MULT           = 1.5          # wider SL for news volatility (1.5x ATR)
NEWS_TP_RATIO           = 2.0          # TP = 2x risk (1:2 RR)
NEWS_MAX_POSITION_USD   = 100          # max position size (paper mode)
NEWS_MAX_HOLD_MIN       = 30           # force-close news trade after 30 min
NEWS_MAX_AGE_MIN        = 30           # ignore articles older than 30 min
NEWS_PAPER_MODE         = True         # paper trading default
NEWS_LLM_MODEL          = "claude-haiku-4-5-20251001"  # fast + cheap for news analysis
CRYPTOPANIC_API_KEY     = os.getenv("CRYPTOPANIC_API_KEY", "")  # optional, free tier works without

# ── Social/CT News Sources ───────────────────────────────────
# LunarCrush — free API key from lunarcrush.com/developers (primary CT source)
LUNARCRUSH_API_KEY      = os.getenv("LUNARCRUSH_API_KEY", "")
LUNARCRUSH_TOPICS       = ["bitcoin", "ethereum", "solana", "crypto"]  # topics to monitor
LUNARCRUSH_BASE_URL     = "https://lunarcrush.com/api4"

# SocialData.tools — pay-as-you-go ($0.0002/tweet), optional power-up for full tweet search
SOCIALDATA_API_KEY      = os.getenv("SOCIALDATA_API_KEY", "")
SOCIALDATA_QUERIES      = [
    "(bitcoin OR btc) (hack OR exploit OR etf OR ban OR approved OR crash)",
    "(ethereum OR eth) (upgrade OR hack OR exploit OR sec)",
    "(solana OR sol) (outage OR hack OR exploit OR upgrade)",
    "(crypto) (ban OR regulation OR collapse OR bankrupt)",
]

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

# ── Hyperliquid Settings ─────────────────────────────────────
# Symbol mapping: BulkMind uses "BTC-USD" internally, Hyperliquid uses "BTC"
HL_SYMBOL_MAP = {
    "BTC-USD": "BTC",
    "ETH-USD": "ETH",
    "SOL-USD": "SOL",
}
HL_SYMBOL_MAP_REVERSE = {v: k for k, v in HL_SYMBOL_MAP.items()}

# Hyperliquid asset indices (for order placement)
HL_ASSET_IDS = {
    "BTC": 0,
    "ETH": 1,
    "SOL": 5,
}

# ── FundingArb Settings ───────────────────────────────────────
FUNDING_SYMBOLS         = ["BTC-USD", "ETH-USD", "SOL-USD"]
FUNDING_CHECK_SEC       = 300          # check funding every 5 min
FUNDING_MIN_DIFF_BPS    = 5            # min diff in bps to open arb (0.05%)
FUNDING_POSITION_USD    = 100          # position size per leg
FUNDING_MAX_POSITIONS   = 3            # max simultaneous arb positions
FUNDING_CLOSE_DIFF_BPS  = 1            # close when diff narrows to 1 bps
FUNDING_PAPER_MODE      = True

# ── HLCopier Settings ────────────────────────────────────────
COPIER_WALLETS = [                     # whale wallets to monitor on HL
    # Add HL wallet addresses here (public addresses only)
]
COPIER_SYMBOLS          = ["BTC-USD", "ETH-USD", "SOL-USD"]
COPIER_CHECK_SEC        = 15           # poll wallet positions every 15s
COPIER_POSITION_USD     = 50           # copy size (scaled down from whale)
COPIER_MAX_POSITIONS    = 5            # max simultaneous copies
COPIER_MIN_SIZE_USD     = 10000        # only copy whale trades > $10k
COPIER_PAPER_MODE       = True

# ── MacroTrader Settings ─────────────────────────────────────
MACRO_SYMBOLS           = ["BTC-USD", "ETH-USD"]
MACRO_CHECK_SEC         = 300          # check calendar every 5 min
MACRO_POSITION_USD      = 100          # position size for macro trades
MACRO_PRE_EVENT_MIN     = 15           # enter position 15 min before event
MACRO_POST_EVENT_MIN    = 60           # exit within 60 min after event
MACRO_PAPER_MODE        = True
MACRO_LLM_MODEL         = "claude-haiku-4-5-20251001"

# ── WarTrader Settings ───────────────────────────────────────
WAR_SYMBOLS             = ["BTC-USD", "ETH-USD", "SOL-USD"]
WAR_CHECK_SEC           = 120          # check geopolitical news every 2 min
WAR_POSITION_USD        = 100          # position size for war trades
WAR_MIN_SEVERITY        = 7            # only trade severity >= 7/10
WAR_MAX_HOLD_MIN        = 60           # force-close after 60 min
WAR_PAPER_MODE          = True
WAR_LLM_MODEL           = "claude-haiku-4-5-20251001"
WAR_KEYWORDS            = [
    "war", "invasion", "sanctions", "military", "nuclear", "missile",
    "conflict", "ceasefire", "embargo", "blockade", "coup",
    "tariff", "trade war", "retaliation",
]

HL_PAPER_MODE = True                   # paper trading default
