"""
BulkMind Database Layer
Stores: latency logs, trade history, issues, agent performance
Thread-safe connection pool with WAL mode for async concurrency.
"""

import sqlite3
import json
import threading
from queue import Queue, Empty
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
from config import DB_PATH

# ── Connection Pool ──────────────────────────────────────────────
# Reuses connections instead of opening/closing per call.
# WAL mode allows concurrent reads while a write is in progress.

_pool: Queue = Queue(maxsize=8)
_pool_lock = threading.Lock()


def _create_conn() -> sqlite3.Connection:
    """Create a new SQLite connection with optimal settings."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def get_conn() -> sqlite3.Connection:
    """Get a connection from the pool, creating one if needed."""
    try:
        conn = _pool.get_nowait()
        # Verify connection is still alive
        try:
            conn.execute("SELECT 1")
        except Exception:
            conn = _create_conn()
        return conn
    except Empty:
        return _create_conn()


def release_conn(conn: sqlite3.Connection):
    """Return a connection to the pool instead of closing it."""
    try:
        _pool.put_nowait(conn)
    except Exception:
        # Pool is full, close the connection
        try:
            release_conn(conn)
        except Exception:
            pass


def init_db():
    conn = get_conn()
    c = conn.cursor()

    # ── BulkWatch Tables ──────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS latency_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            endpoint    TEXT NOT NULL,
            latency_ms  REAL NOT NULL,
            status_code INTEGER,
            error       TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS downtime_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts    TEXT NOT NULL,
            end_ts      TEXT,
            duration_sec REAL,
            reason      TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS execution_quality (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            order_type      TEXT NOT NULL,
            expected_price  REAL NOT NULL,
            filled_price    REAL,
            slippage_bps    REAL,          -- basis points
            latency_ms      REAL,
            order_id        TEXT,
            status          TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS funding_rates (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        TEXT NOT NULL,
            symbol    TEXT NOT NULL,
            rate      REAL NOT NULL,
            next_ts   TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS issues (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            severity    TEXT NOT NULL,   -- CRITICAL, HIGH, MEDIUM, LOW
            category    TEXT NOT NULL,   -- LATENCY, DOWNTIME, SLIPPAGE, CODE, LIQUIDATION
            title       TEXT NOT NULL,
            details     TEXT,
            resolved    INTEGER DEFAULT 0,
            resolved_ts TEXT
        )
    """)

    # ── BulkAlpha Tables ──────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              TEXT NOT NULL,
            agent           TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            side            TEXT NOT NULL,   -- BUY / SELL
            entry_price     REAL NOT NULL,
            size            REAL NOT NULL,
            sl_price        REAL,
            tp_price        REAL,
            exit_price      REAL,
            exit_ts         TEXT,
            pnl_usd         REAL,
            pnl_pct         REAL,
            status          TEXT DEFAULT 'OPEN',  -- OPEN, WIN, LOSS, CANCELLED
            paper           INTEGER DEFAULT 1,     -- 1=paper, 0=live
            order_id        TEXT,
            signal_data     TEXT                   -- JSON of signal that triggered trade
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        TEXT NOT NULL,
            symbol    TEXT NOT NULL,
            timeframe INTEGER NOT NULL,   -- minutes
            open      REAL NOT NULL,
            high      REAL NOT NULL,
            low       REAL NOT NULL,
            close     REAL NOT NULL,
            volume    REAL NOT NULL,
            UNIQUE(ts, symbol, timeframe)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS traders (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            wallet      TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            side        TEXT NOT NULL,
            pnl_usd     REAL NOT NULL,
            pnl_pct     REAL NOT NULL,
            volume_usd  REAL NOT NULL,
            trades_count INTEGER DEFAULT 1
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS trade_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            wallet      TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            side        TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price  REAL,
            size        REAL NOT NULL,
            pnl_usd     REAL,
            pnl_pct     REAL,
            status      TEXT DEFAULT 'OPEN',
            fee_usd     REAL DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS wallet_balances (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet      TEXT NOT NULL UNIQUE,
            balance_usd REAL NOT NULL DEFAULT 10000,
            equity_usd  REAL NOT NULL DEFAULT 10000,
            unrealized_pnl REAL DEFAULT 0,
            margin_used REAL DEFAULT 0,
            updated_at  TEXT NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS agent_performance (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            agent       TEXT NOT NULL,
            total_trades INTEGER DEFAULT 0,
            wins        INTEGER DEFAULT 0,
            losses      INTEGER DEFAULT 0,
            total_pnl   REAL DEFAULT 0,
            win_rate    REAL DEFAULT 0,
            avg_rr      REAL DEFAULT 0,
            skill_version TEXT
        )
    """)

    # ── Trade Stream / Wallet Discovery Tables ─────────────

    c.execute("""
        CREATE TABLE IF NOT EXISTS observed_trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            side        TEXT NOT NULL,
            price       REAL NOT NULL,
            size        REAL NOT NULL,
            maker       TEXT,
            taker       TEXT,
            reason      TEXT DEFAULT 'normal',
            raw_data    TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS liquidations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            side        TEXT NOT NULL,
            price       REAL NOT NULL,
            size        REAL NOT NULL,
            value_usd   REAL NOT NULL,
            wallet      TEXT,
            raw_data    TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS wallet_discovery (
            wallet      TEXT PRIMARY KEY,
            first_seen  TEXT NOT NULL,
            last_profiled TEXT,
            trade_count INTEGER DEFAULT 1,
            status      TEXT DEFAULT 'pending'
        )
    """)

    # ── NewsTrader Tables ─────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS news_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            source      TEXT NOT NULL,
            article_id  TEXT NOT NULL,
            title       TEXT NOT NULL,
            sentiment   TEXT,          -- BUY / SELL / NEUTRAL
            impact      INTEGER,       -- 1-10
            symbols     TEXT,          -- JSON list e.g. ["BTC-USD"]
            traded      INTEGER DEFAULT 0,
            trade_id    INTEGER,
            UNIQUE(source, article_id)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_news_events_ts ON news_events(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_news_events_traded ON news_events(traded)")

    # ── FundingArb Tables ────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS funding_arb_positions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            long_exchange  TEXT NOT NULL,
            short_exchange TEXT NOT NULL,
            entry_diff_bps REAL NOT NULL,
            position_usd   REAL NOT NULL,
            status      TEXT DEFAULT 'open',
            exit_diff_bps  REAL,
            pnl_usd     REAL DEFAULT 0,
            closed_at   TEXT,
            long_trade_id  INTEGER,
            short_trade_id INTEGER
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_arb_status ON funding_arb_positions(status)")

    # ── HLCopier Tables ──────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS copier_positions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            whale_wallet TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            side        TEXT NOT NULL,
            whale_size  REAL NOT NULL,
            copy_size   REAL NOT NULL,
            entry_price REAL NOT NULL,
            status      TEXT DEFAULT 'open',
            exit_price  REAL,
            pnl_usd     REAL DEFAULT 0,
            closed_at   TEXT,
            trade_id    INTEGER
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_copier_status ON copier_positions(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_copier_whale ON copier_positions(whale_wallet)")

    # ── MacroTrader / WarTrader Tables ───────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS macro_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            event_name  TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            severity    INTEGER DEFAULT 5,
            direction   TEXT,
            symbols     TEXT,
            traded      INTEGER DEFAULT 0,
            trade_id    INTEGER,
            source      TEXT DEFAULT 'calendar'
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_macro_ts ON macro_events(ts)")

    # ── HyperBulk Tables ─────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS hb_users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet      TEXT NOT NULL UNIQUE,
            username    TEXT UNIQUE,
            avatar_url  TEXT,
            xp          INTEGER DEFAULT 0,
            level       INTEGER DEFAULT 1,
            league      TEXT DEFAULT 'bronze',
            created_at  TEXT NOT NULL,
            last_active TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS hb_trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            exchange    TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            side        TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price  REAL,
            size        REAL NOT NULL,
            pnl_usd     REAL,
            pnl_pct     REAL,
            status      TEXT DEFAULT 'OPEN',
            order_id    TEXT,
            opened_at   TEXT NOT NULL,
            closed_at   TEXT,
            FOREIGN KEY (user_id) REFERENCES hb_users(id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS hb_achievements (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            achievement TEXT NOT NULL,
            earned_at   TEXT NOT NULL,
            UNIQUE(user_id, achievement)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS hb_daily_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER NOT NULL,
            date            TEXT NOT NULL,
            trades          INTEGER DEFAULT 0,
            wins            INTEGER DEFAULT 0,
            losses          INTEGER DEFAULT 0,
            pnl_usd         REAL DEFAULT 0,
            best_trade_pnl  REAL DEFAULT 0,
            streak          INTEGER DEFAULT 0,
            UNIQUE(user_id, date)
        )
    """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_hb_trades_user ON hb_trades(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hb_trades_exchange ON hb_trades(exchange)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hb_trades_status ON hb_trades(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hb_daily_date ON hb_daily_stats(date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hb_users_league ON hb_users(league)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hb_users_xp ON hb_users(xp DESC)")

    # ── Moon or Doom Game Tables ─────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS hb_games (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            symbol      TEXT NOT NULL,
            exchange    TEXT NOT NULL DEFAULT 'bulk',
            bet_amount  REAL NOT NULL,
            leverage    REAL NOT NULL DEFAULT 50,
            entry_price REAL,
            exit_price  REAL,
            size        REAL,
            multiplier  REAL DEFAULT 1.0,
            high_water  REAL DEFAULT 1.0,
            pnl_usd     REAL DEFAULT 0,
            status      TEXT DEFAULT 'waiting',
            order_id    TEXT,
            started_at  TEXT,
            ended_at    TEXT,
            created_at  TEXT NOT NULL
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_hb_games_user ON hb_games(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hb_games_status ON hb_games(status)")

    # ── Sniper Game Tables ───────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS sniper_rounds (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol       TEXT NOT NULL,
            entry_fee    REAL NOT NULL DEFAULT 5.0,
            duration_sec INTEGER NOT NULL DEFAULT 300,
            rake_pct     REAL NOT NULL DEFAULT 10.0,
            status       TEXT DEFAULT 'open',
            player_count INTEGER DEFAULT 0,
            pot_usd      REAL DEFAULT 0,
            prize_pool   REAL DEFAULT 0,
            rake_usd     REAL DEFAULT 0,
            actual_price REAL,
            created_at   TEXT NOT NULL,
            locks_at     TEXT NOT NULL,
            settles_at   TEXT NOT NULL,
            settled_at   TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sniper_predictions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id        INTEGER NOT NULL,
            user_id         INTEGER NOT NULL,
            predicted_price REAL NOT NULL,
            distance_usd    REAL,
            accuracy_pct    REAL,
            accuracy_tier   TEXT,
            rank            INTEGER,
            payout_usd      REAL DEFAULT 0,
            submitted_at    TEXT NOT NULL,
            UNIQUE(round_id, user_id)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_sniper_rounds_status ON sniper_rounds(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sniper_preds_round ON sniper_predictions(round_id)")

    # ── Flip It Game Tables ──────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS flip_games (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            symbol       TEXT NOT NULL,
            exchange     TEXT NOT NULL DEFAULT 'bulk',
            direction    TEXT NOT NULL,
            bet_amount   REAL NOT NULL,
            entry_price  REAL,
            exit_price   REAL,
            size         REAL,
            price_change_pct REAL,
            won          INTEGER DEFAULT 0,
            streak       INTEGER DEFAULT 0,
            payout_mult  REAL DEFAULT 1.8,
            payout_usd   REAL DEFAULT 0,
            pnl_usd      REAL DEFAULT 0,
            status       TEXT DEFAULT 'pending',
            order_id     TEXT,
            started_at   TEXT,
            ended_at     TEXT,
            created_at   TEXT NOT NULL
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_flip_user ON flip_games(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_flip_status ON flip_games(status)")

    # ── Battle Royale Tables ─────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS br_games (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol       TEXT NOT NULL,
            direction    TEXT NOT NULL DEFAULT 'long',
            entry_fee    REAL NOT NULL DEFAULT 10,
            rake_pct     REAL NOT NULL DEFAULT 10,
            player_count INTEGER DEFAULT 0,
            alive_count  INTEGER DEFAULT 0,
            pot_usd      REAL DEFAULT 0,
            prize_pool   REAL DEFAULT 0,
            rake_usd     REAL DEFAULT 0,
            entry_price  REAL,
            status       TEXT DEFAULT 'lobby',
            created_at   TEXT NOT NULL,
            started_at   TEXT,
            ended_at     TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS br_players (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id     INTEGER NOT NULL,
            user_id     INTEGER NOT NULL,
            status      TEXT DEFAULT 'alive',
            entry_price REAL,
            elim_price  REAL,
            rank        INTEGER,
            payout_usd  REAL DEFAULT 0,
            survival_sec REAL DEFAULT 0,
            joined_at   TEXT NOT NULL,
            UNIQUE(game_id, user_id)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_br_games_status ON br_games(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_br_players_game ON br_players(game_id)")

    # ── Indexes ──────────────────────────────────────────────
    c.execute("CREATE INDEX IF NOT EXISTS idx_observed_trades_ts ON observed_trades(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_observed_trades_maker ON observed_trades(maker)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_observed_trades_taker ON observed_trades(taker)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_liquidations_ts ON liquidations(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_wallet_discovery_status ON wallet_discovery(status, last_profiled)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_traders_wallet ON traders(wallet)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_traders_ts ON traders(ts)")

    conn.commit()
    release_conn(conn)
    print("✅ Database initialized")


# ── Helper Functions ──────────────────────────────────────────

def log_latency(endpoint: str, latency_ms: float, status_code: int = None, error: str = None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO latency_log (ts, endpoint, latency_ms, status_code, error) VALUES (?,?,?,?,?)",
        (datetime.utcnow().isoformat(), endpoint, latency_ms, status_code, error)
    )
    conn.commit()
    release_conn(conn)


def log_issue(severity: str, category: str, title: str, details: str = None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO issues (ts, severity, category, title, details) VALUES (?,?,?,?,?)",
        (datetime.utcnow().isoformat(), severity, category, title, details)
    )
    conn.commit()
    release_conn(conn)
    print(f"🚨 ISSUE [{severity}] {title}")


def log_trade(agent: str, symbol: str, side: str, entry_price: float,
              size: float, sl: float, tp: float, signal_data: dict,
              paper: bool = True, order_id: str = None) -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO trades 
           (ts, agent, symbol, side, entry_price, size, sl_price, tp_price, 
            paper, order_id, signal_data)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (datetime.utcnow().isoformat(), agent, symbol, side, entry_price,
         size, sl, tp, int(paper), order_id, json.dumps(signal_data))
    )
    trade_id = cur.lastrowid
    conn.commit()
    release_conn(conn)
    return trade_id


def close_trade(trade_id: int, exit_price: float, status: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
    if not row:
        release_conn(conn)
        return

    side = row["side"]
    entry = row["entry_price"]
    size  = row["size"]

    if side == "BUY":
        pnl_pct = (exit_price - entry) / entry * 100
    else:
        pnl_pct = (entry - exit_price) / entry * 100

    pnl_usd = pnl_pct / 100 * entry * size

    conn.execute(
        """UPDATE trades SET exit_price=?, exit_ts=?, pnl_usd=?, pnl_pct=?, status=?
           WHERE id=?""",
        (exit_price, datetime.utcnow().isoformat(), pnl_usd, pnl_pct, status, trade_id)
    )
    conn.commit()
    release_conn(conn)
    return pnl_usd


def save_candle(symbol: str, timeframe: int, ts: str,
                o: float, h: float, l: float, c: float, v: float):
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO candles 
           (ts, symbol, timeframe, open, high, low, close, volume)
           VALUES (?,?,?,?,?,?,?,?)""",
        (ts, symbol, timeframe, o, h, l, c, v)
    )
    conn.commit()
    release_conn(conn)


def get_candles(symbol: str, timeframe: int, limit: int = 100) -> list:
    conn = get_conn()
    rows = conn.execute(
        """SELECT * FROM candles WHERE symbol=? AND timeframe=?
           ORDER BY ts DESC LIMIT ?""",
        (symbol, timeframe, limit)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in reversed(rows)]


def get_open_trades(agent: str = None) -> list:
    conn = get_conn()
    if agent:
        rows = conn.execute(
            "SELECT * FROM trades WHERE status='OPEN' AND agent=?", (agent,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM trades WHERE status='OPEN'"
        ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


def get_agent_stats(agent: str) -> dict:
    conn = get_conn()
    row = conn.execute(
        """SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END) as losses,
            SUM(COALESCE(pnl_usd, 0)) as total_pnl,
            AVG(CASE WHEN pnl_pct IS NOT NULL THEN pnl_pct END) as avg_pnl_pct
           FROM trades WHERE agent=? AND status != 'OPEN'""",
        (agent,)
    ).fetchone()
    release_conn(conn)
    return dict(row) if row else {}


def get_top_traders(hours: int = 24, limit: int = 50) -> dict:
    conn = get_conn()
    profitable = conn.execute(
        """SELECT wallet, SUM(pnl_usd) as total_pnl, SUM(volume_usd) as total_volume,
                  SUM(trades_count) as total_trades,
                  ROUND(AVG(pnl_pct), 2) as avg_pnl_pct
           FROM traders
           WHERE ts > datetime('now', ?)
           GROUP BY wallet HAVING total_pnl > 0
           ORDER BY total_pnl DESC LIMIT ?""",
        (f"-{hours} hours", limit)
    ).fetchall()

    losers = conn.execute(
        """SELECT wallet, SUM(pnl_usd) as total_pnl, SUM(volume_usd) as total_volume,
                  SUM(trades_count) as total_trades,
                  ROUND(AVG(pnl_pct), 2) as avg_pnl_pct
           FROM traders
           WHERE ts > datetime('now', ?)
           GROUP BY wallet HAVING total_pnl <= 0
           ORDER BY total_pnl ASC LIMIT ?""",
        (f"-{hours} hours", limit)
    ).fetchall()

    release_conn(conn)
    return {
        "profitable": [dict(r) for r in profitable],
        "losers": [dict(r) for r in losers],
        "profitable_count": len(profitable),
        "losers_count": len(losers),
    }


def search_wallets(query: str, limit: int = 20) -> list:
    conn = get_conn()
    rows = conn.execute(
        """SELECT wallet, SUM(pnl_usd) as total_pnl, SUM(volume_usd) as total_volume,
                  SUM(trades_count) as total_trades
           FROM traders WHERE wallet LIKE ?
           GROUP BY wallet ORDER BY total_pnl DESC LIMIT ?""",
        (f"%{query}%", limit)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


def get_wallet_profile(wallet: str) -> dict:
    conn = get_conn()

    # Aggregate stats from traders table
    stats = conn.execute(
        """SELECT SUM(pnl_usd) as total_pnl, SUM(volume_usd) as total_volume,
                  SUM(trades_count) as total_trades,
                  SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as winning_sessions,
                  SUM(CASE WHEN pnl_usd <= 0 THEN 1 ELSE 0 END) as losing_sessions,
                  ROUND(AVG(pnl_pct), 2) as avg_pnl_pct,
                  MAX(pnl_usd) as best_trade,
                  MIN(pnl_usd) as worst_trade
           FROM traders WHERE wallet=?""",
        (wallet,)
    ).fetchone()

    # Balance
    balance = conn.execute(
        "SELECT * FROM wallet_balances WHERE wallet=?", (wallet,)
    ).fetchone()

    # Trade history
    history = conn.execute(
        """SELECT * FROM trade_history WHERE wallet=?
           ORDER BY ts DESC LIMIT 50""",
        (wallet,)
    ).fetchall()

    # Open trades
    open_trades = conn.execute(
        """SELECT * FROM trade_history WHERE wallet=? AND status='OPEN'
           ORDER BY ts DESC""",
        (wallet,)
    ).fetchall()

    release_conn(conn)

    return {
        "wallet": wallet,
        "stats": dict(stats) if stats else {},
        "balance": dict(balance) if balance else {},
        "history": [dict(r) for r in history],
        "open_trades": [dict(r) for r in open_trades],
    }


def get_leaderboard(tab: str = "top_traders", period: str = "24h", limit: int = 100) -> list:
    """Query traders by different criteria for leaderboard tabs.
    tab: top_traders | most_liquidated | most_active | hall_of_shame
    period: 24h | 7d | 30d | all
    """
    conn = get_conn()
    period_map = {"24h": 24, "7d": 168, "30d": 720, "all": 999999}
    hours = period_map.get(period, 24)

    time_filter = ""
    params = []
    if period != "all":
        time_filter = "WHERE t.ts > datetime('now', ?)"
        params.append(f"-{hours} hours")

    if tab == "top_traders":
        query = f"""
            SELECT t.wallet,
                   SUM(t.pnl_usd) as total_pnl,
                   SUM(t.volume_usd) as total_volume,
                   SUM(t.trades_count) as total_trades,
                   ROUND(AVG(t.pnl_pct), 2) as avg_pnl_pct,
                   COALESCE(wb.balance_usd, 0) as balance_usd
            FROM traders t
            LEFT JOIN wallet_balances wb ON t.wallet = wb.wallet
            {time_filter}
            GROUP BY t.wallet
            HAVING total_pnl > 0
            ORDER BY total_pnl DESC LIMIT ?
        """
        params.append(limit)
    elif tab == "most_liquidated":
        query = f"""
            SELECT t.wallet,
                   SUM(t.pnl_usd) as total_pnl,
                   SUM(t.volume_usd) as total_volume,
                   SUM(t.trades_count) as total_trades,
                   ROUND(AVG(t.pnl_pct), 2) as avg_pnl_pct,
                   COALESCE(wb.balance_usd, 0) as balance_usd
            FROM traders t
            LEFT JOIN wallet_balances wb ON t.wallet = wb.wallet
            {time_filter}
            GROUP BY t.wallet
            HAVING total_pnl < -1000
            ORDER BY total_pnl ASC LIMIT ?
        """
        params.append(limit)
    elif tab == "most_active":
        query = f"""
            SELECT t.wallet,
                   SUM(t.pnl_usd) as total_pnl,
                   SUM(t.volume_usd) as total_volume,
                   SUM(t.trades_count) as total_trades,
                   ROUND(AVG(t.pnl_pct), 2) as avg_pnl_pct,
                   COALESCE(wb.balance_usd, 0) as balance_usd
            FROM traders t
            LEFT JOIN wallet_balances wb ON t.wallet = wb.wallet
            {time_filter}
            GROUP BY t.wallet
            ORDER BY total_trades DESC LIMIT ?
        """
        params.append(limit)
    elif tab == "hall_of_shame":
        query = f"""
            SELECT t.wallet,
                   SUM(t.pnl_usd) as total_pnl,
                   SUM(t.volume_usd) as total_volume,
                   SUM(t.trades_count) as total_trades,
                   ROUND(AVG(t.pnl_pct), 2) as avg_pnl_pct,
                   COALESCE(wb.balance_usd, 0) as balance_usd
            FROM traders t
            LEFT JOIN wallet_balances wb ON t.wallet = wb.wallet
            {time_filter}
            GROUP BY t.wallet
            HAVING total_pnl < 0
            ORDER BY total_pnl ASC LIMIT ?
        """
        params.append(limit)
    else:
        release_conn(conn)
        return []

    rows = conn.execute(query, params).fetchall()
    release_conn(conn)

    results = []
    for i, r in enumerate(rows):
        d = dict(r)
        total_trades = d.get("total_trades") or 0
        pnl = d.get("total_pnl") or 0
        avg_pct = d.get("avg_pnl_pct") or 0
        # Estimate win rate from avg_pnl_pct direction
        if avg_pct > 0:
            win_rate = min(round(50 + avg_pct * 0.5, 1), 95)
        elif avg_pct < 0:
            win_rate = max(round(50 + avg_pct * 0.5, 1), 5)
        else:
            win_rate = 50.0
        d["rank"] = i + 1
        d["win_rate"] = win_rate
        results.append(d)

    return results


def get_analytics() -> dict:
    """Aggregate exchange-wide stats."""
    conn = get_conn()

    total_trades = conn.execute(
        "SELECT COALESCE(SUM(trades_count), 0) as c FROM traders"
    ).fetchone()["c"]

    total_volume = conn.execute(
        "SELECT COALESCE(SUM(volume_usd), 0) as v FROM traders"
    ).fetchone()["v"]

    unique_traders = conn.execute(
        "SELECT COUNT(DISTINCT wallet) as c FROM traders"
    ).fetchone()["c"]

    # Open interest approximation from open trade_history positions
    oi = conn.execute(
        "SELECT COALESCE(SUM(entry_price * size), 0) as oi FROM trade_history WHERE status='OPEN'"
    ).fetchone()["oi"]

    # 24h stats
    trades_24h = conn.execute(
        "SELECT COALESCE(SUM(trades_count), 0) as c FROM traders WHERE ts > datetime('now', '-24 hours')"
    ).fetchone()["c"]

    volume_24h = conn.execute(
        "SELECT COALESCE(SUM(volume_usd), 0) as v FROM traders WHERE ts > datetime('now', '-24 hours')"
    ).fetchone()["v"]

    traders_24h = conn.execute(
        "SELECT COUNT(DISTINCT wallet) as c FROM traders WHERE ts > datetime('now', '-24 hours')"
    ).fetchone()["c"]

    # Top symbols by volume
    top_symbols = conn.execute(
        """SELECT symbol, SUM(volume_usd) as vol, SUM(trades_count) as trades,
                  COUNT(DISTINCT wallet) as traders
           FROM traders
           GROUP BY symbol ORDER BY vol DESC LIMIT 10"""
    ).fetchall()

    release_conn(conn)

    return {
        "total_trades": total_trades,
        "total_volume": round(total_volume, 2),
        "open_interest": round(oi, 2),
        "unique_traders": unique_traders,
        "trades_24h": trades_24h,
        "volume_24h": round(volume_24h, 2),
        "traders_24h": traders_24h,
        "top_symbols": [dict(r) for r in top_symbols],
    }


def get_whales(min_balance: float = 50000) -> list:
    """Find whale wallets - large balance or large single trade."""
    conn = get_conn()

    rows = conn.execute(
        """SELECT wb.wallet,
                  wb.balance_usd,
                  wb.equity_usd,
                  wb.unrealized_pnl,
                  wb.updated_at,
                  COALESCE(MAX(ABS(th.entry_price * th.size)), 0) as largest_trade,
                  COALESCE(SUM(t.volume_usd), 0) as total_volume,
                  COALESCE(SUM(t.pnl_usd), 0) as total_pnl,
                  MAX(t.ts) as last_active
           FROM wallet_balances wb
           LEFT JOIN traders t ON wb.wallet = t.wallet
           LEFT JOIN trade_history th ON wb.wallet = th.wallet
           WHERE wb.balance_usd >= ? OR th.entry_price * th.size >= 10000
           GROUP BY wb.wallet
           ORDER BY wb.balance_usd DESC""",
        (min_balance,)
    ).fetchall()

    release_conn(conn)
    return [dict(r) for r in rows]


# ── Trade Stream / Wallet Discovery Helpers ──────────────────

def log_observed_trade(symbol: str, side: str, price: float, size: float,
                       maker: str = None, taker: str = None,
                       reason: str = "normal", raw_data: str = None):
    conn = get_conn()
    conn.execute(
        """INSERT INTO observed_trades (ts, symbol, side, price, size, maker, taker, reason, raw_data)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (datetime.utcnow().isoformat(), symbol, side, price, size,
         maker, taker, reason, raw_data)
    )
    conn.commit()
    release_conn(conn)


def log_liquidation(symbol: str, side: str, price: float, size: float,
                    value_usd: float, wallet: str = None, raw_data: str = None):
    conn = get_conn()
    conn.execute(
        """INSERT INTO liquidations (ts, symbol, side, price, size, value_usd, wallet, raw_data)
           VALUES (?,?,?,?,?,?,?,?)""",
        (datetime.utcnow().isoformat(), symbol, side, price, size,
         value_usd, wallet, raw_data)
    )
    conn.commit()
    release_conn(conn)


def upsert_discovered_wallet(wallet: str):
    conn = get_conn()
    conn.execute(
        """INSERT INTO wallet_discovery (wallet, first_seen, trade_count)
           VALUES (?, ?, 1)
           ON CONFLICT(wallet) DO UPDATE SET trade_count = trade_count + 1""",
        (wallet, datetime.utcnow().isoformat())
    )
    conn.commit()
    release_conn(conn)


def get_pending_wallets(limit: int = 20) -> list:
    conn = get_conn()
    rows = conn.execute(
        """SELECT wallet FROM wallet_discovery
           WHERE status = 'pending'
              OR (status = 'profiled' AND last_profiled < datetime('now', '-6 hours'))
           ORDER BY trade_count DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()
    release_conn(conn)
    return [r["wallet"] for r in rows]


def mark_wallet_profiled(wallet: str):
    conn = get_conn()
    conn.execute(
        """UPDATE wallet_discovery SET status='profiled', last_profiled=?
           WHERE wallet=?""",
        (datetime.utcnow().isoformat(), wallet)
    )
    conn.commit()
    release_conn(conn)


def upsert_wallet_balance(wallet: str, balance_usd: float, equity_usd: float,
                           unrealized_pnl: float = 0, margin_used: float = 0):
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO wallet_balances
           (wallet, balance_usd, equity_usd, unrealized_pnl, margin_used, updated_at)
           VALUES (?,?,?,?,?,?)""",
        (wallet, balance_usd, equity_usd, unrealized_pnl, margin_used,
         datetime.utcnow().isoformat())
    )
    conn.commit()
    release_conn(conn)


def upsert_trader_record(wallet: str, symbol: str, side: str,
                          pnl_usd: float, pnl_pct: float,
                          volume_usd: float, trades_count: int):
    conn = get_conn()
    conn.execute(
        """INSERT INTO traders (ts, wallet, symbol, side, pnl_usd, pnl_pct, volume_usd, trades_count)
           VALUES (?,?,?,?,?,?,?,?)""",
        (datetime.utcnow().isoformat(), wallet, symbol, side,
         round(pnl_usd, 2), round(pnl_pct, 2), round(volume_usd, 2), trades_count)
    )
    conn.commit()
    release_conn(conn)


def get_liquidation_stats(hours: int = 24) -> dict:
    conn = get_conn()
    longs = conn.execute(
        """SELECT COUNT(*) as count, COALESCE(SUM(value_usd), 0) as total_usd
           FROM liquidations WHERE side='LONG' AND ts > datetime('now', ?)""",
        (f"-{hours} hours",)
    ).fetchone()
    shorts = conn.execute(
        """SELECT COUNT(*) as count, COALESCE(SUM(value_usd), 0) as total_usd
           FROM liquidations WHERE side='SHORT' AND ts > datetime('now', ?)""",
        (f"-{hours} hours",)
    ).fetchone()
    release_conn(conn)
    return {
        "longs_liquidated": longs["count"],
        "longs_value_usd": round(longs["total_usd"], 2),
        "shorts_liquidated": shorts["count"],
        "shorts_value_usd": round(shorts["total_usd"], 2),
        "total_count": longs["count"] + shorts["count"],
        "total_value_usd": round(longs["total_usd"] + shorts["total_usd"], 2),
    }


def get_recent_liquidations(limit: int = 50) -> list:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM liquidations ORDER BY ts DESC LIMIT ?", (limit,)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


def get_exchange_summary() -> dict:
    conn = get_conn()
    total_observed = conn.execute(
        "SELECT COUNT(*) as c FROM observed_trades"
    ).fetchone()["c"]
    observed_24h = conn.execute(
        "SELECT COUNT(*) as c FROM observed_trades WHERE ts > datetime('now', '-24 hours')"
    ).fetchone()["c"]
    unique_wallets = conn.execute(
        "SELECT COUNT(*) as c FROM wallet_discovery"
    ).fetchone()["c"]
    profiled_wallets = conn.execute(
        "SELECT COUNT(*) as c FROM wallet_discovery WHERE status='profiled'"
    ).fetchone()["c"]
    active_24h = conn.execute(
        """SELECT COUNT(DISTINCT wallet) as c FROM (
            SELECT maker as wallet FROM observed_trades WHERE ts > datetime('now', '-24 hours') AND maker IS NOT NULL
            UNION
            SELECT taker as wallet FROM observed_trades WHERE ts > datetime('now', '-24 hours') AND taker IS NOT NULL
        )"""
    ).fetchone()["c"]
    open_positions = conn.execute(
        "SELECT COUNT(*) as c FROM trade_history WHERE status='OPEN'"
    ).fetchone()["c"]
    release_conn(conn)
    return {
        "total_observed_trades": total_observed,
        "observed_trades_24h": observed_24h,
        "unique_wallets_discovered": unique_wallets,
        "wallets_profiled": profiled_wallets,
        "active_wallets_24h": active_24h,
        "open_positions": open_positions,
    }


def get_observed_trades(limit: int = 50, symbol: str = None) -> list:
    conn = get_conn()
    if symbol:
        rows = conn.execute(
            "SELECT * FROM observed_trades WHERE symbol=? ORDER BY ts DESC LIMIT ?",
            (symbol, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM observed_trades ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


def cleanup_old_observed_trades(days: int = 7):
    conn = get_conn()
    conn.execute(
        "DELETE FROM observed_trades WHERE ts < datetime('now', ?)",
        (f"-{days} days",)
    )
    conn.commit()
    release_conn(conn)


# ── HyperBulk Helpers ────────────────────────────────────────

def hb_register_user(wallet: str, username: str = None) -> int:
    """Register a new HyperBulk user. Returns the user id."""
    conn = get_conn()
    now = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO hb_users (wallet, username, created_at, last_active)
           VALUES (?,?,?,?)""",
        (wallet, username, now, now)
    )
    conn.commit()
    row = conn.execute("SELECT id FROM hb_users WHERE wallet=?", (wallet,)).fetchone()
    release_conn(conn)
    return row["id"]


def hb_get_user(wallet: str) -> Optional[dict]:
    """Look up a HyperBulk user by wallet address."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM hb_users WHERE wallet=?", (wallet,)).fetchone()
    release_conn(conn)
    return dict(row) if row else None


def hb_get_user_by_id(user_id: int) -> Optional[dict]:
    """Look up a HyperBulk user by id."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM hb_users WHERE id=?", (user_id,)).fetchone()
    release_conn(conn)
    return dict(row) if row else None


def hb_log_trade(user_id: int, exchange: str, symbol: str, side: str,
                 entry_price: float, size: float, order_id: str = None) -> int:
    """Open a new HyperBulk trade. Returns the trade id."""
    conn = get_conn()
    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        """INSERT INTO hb_trades
           (user_id, exchange, symbol, side, entry_price, size, order_id, opened_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (user_id, exchange, symbol, side, entry_price, size, order_id, now)
    )
    trade_id = cur.lastrowid
    # Update last_active
    conn.execute("UPDATE hb_users SET last_active=? WHERE id=?", (now, user_id))
    conn.commit()
    release_conn(conn)
    return trade_id


def hb_close_trade(trade_id: int, exit_price: float) -> dict:
    """Close a HyperBulk trade, compute PnL, update daily stats and XP."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM hb_trades WHERE id=?", (trade_id,)).fetchone()
    if not row:
        release_conn(conn)
        return {}

    side = row["side"]
    entry = row["entry_price"]
    size = row["size"]
    user_id = row["user_id"]

    if side == "BUY":
        pnl_pct = (exit_price - entry) / entry * 100
    else:
        pnl_pct = (entry - exit_price) / entry * 100

    pnl_usd = pnl_pct / 100 * entry * size
    status = "WIN" if pnl_usd > 0 else "LOSS"
    now = datetime.utcnow().isoformat()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Update the trade
    conn.execute(
        """UPDATE hb_trades SET exit_price=?, pnl_usd=?, pnl_pct=?, status=?, closed_at=?
           WHERE id=?""",
        (exit_price, round(pnl_usd, 2), round(pnl_pct, 2), status, now, trade_id)
    )

    # Upsert daily stats
    conn.execute(
        """INSERT INTO hb_daily_stats (user_id, date, trades, wins, losses, pnl_usd, best_trade_pnl, streak)
           VALUES (?,?,1,?,?,?,?,?)
           ON CONFLICT(user_id, date) DO UPDATE SET
               trades = trades + 1,
               wins = wins + ?,
               losses = losses + ?,
               pnl_usd = pnl_usd + ?,
               best_trade_pnl = MAX(best_trade_pnl, ?),
               streak = CASE WHEN ? = 'WIN' THEN streak + 1 ELSE 0 END""",
        (
            user_id, today,
            1 if status == "WIN" else 0,
            1 if status == "LOSS" else 0,
            round(pnl_usd, 2),
            round(pnl_usd, 2) if pnl_usd > 0 else 0,
            1 if status == "WIN" else 0,
            # ON CONFLICT params
            1 if status == "WIN" else 0,
            1 if status == "LOSS" else 0,
            round(pnl_usd, 2),
            round(pnl_usd, 2),
            status,
        )
    )

    # Award XP: +10 per trade, +25 bonus for a win
    xp_gain = 10 + (25 if status == "WIN" else 0)
    conn.execute(
        "UPDATE hb_users SET xp = xp + ?, last_active=? WHERE id=?",
        (xp_gain, now, user_id)
    )

    conn.commit()
    release_conn(conn)
    return {"pnl_usd": round(pnl_usd, 2), "status": status}


def hb_get_leaderboard(period: str = "daily", limit: int = 50) -> list:
    """Get HyperBulk leaderboard. period: daily | weekly | alltime"""
    conn = get_conn()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    if period == "daily":
        rows = conn.execute(
            """SELECT u.username, u.wallet, u.league, u.xp,
                      ds.pnl_usd, ds.wins, ds.losses, ds.trades
               FROM hb_daily_stats ds
               JOIN hb_users u ON ds.user_id = u.id
               WHERE ds.date = ?
               ORDER BY ds.pnl_usd DESC LIMIT ?""",
            (today, limit)
        ).fetchall()
    elif period == "weekly":
        week_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        rows = conn.execute(
            """SELECT u.username, u.wallet, u.league, u.xp,
                      SUM(ds.pnl_usd) as pnl_usd,
                      SUM(ds.wins) as wins,
                      SUM(ds.losses) as losses,
                      SUM(ds.trades) as trades
               FROM hb_daily_stats ds
               JOIN hb_users u ON ds.user_id = u.id
               WHERE ds.date >= ?
               GROUP BY ds.user_id
               ORDER BY pnl_usd DESC LIMIT ?""",
            (week_ago, limit)
        ).fetchall()
    else:  # alltime
        rows = conn.execute(
            """SELECT u.username, u.wallet, u.league, u.xp,
                      SUM(t.pnl_usd) as pnl_usd,
                      SUM(CASE WHEN t.status='WIN' THEN 1 ELSE 0 END) as wins,
                      SUM(CASE WHEN t.status='LOSS' THEN 1 ELSE 0 END) as losses,
                      COUNT(*) as trades
               FROM hb_trades t
               JOIN hb_users u ON t.user_id = u.id
               WHERE t.status != 'OPEN'
               GROUP BY t.user_id
               ORDER BY pnl_usd DESC LIMIT ?""",
            (limit,)
        ).fetchall()

    release_conn(conn)
    return [dict(r) for r in rows]


def hb_get_user_stats(user_id: int) -> dict:
    """Aggregate stats for a HyperBulk user."""
    conn = get_conn()

    row = conn.execute(
        """SELECT
            COUNT(*) as total_trades,
            SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END) as losses,
            SUM(COALESCE(pnl_usd, 0)) as total_pnl,
            MAX(pnl_usd) as best_trade
           FROM hb_trades WHERE user_id=? AND status != 'OPEN'""",
        (user_id,)
    ).fetchone()

    stats = dict(row) if row else {}
    total = stats.get("total_trades") or 0
    wins = stats.get("wins") or 0
    stats["win_rate"] = round(wins / total * 100, 1) if total > 0 else 0.0

    # Current win streak from most recent trades
    recent = conn.execute(
        """SELECT status FROM hb_trades
           WHERE user_id=? AND status != 'OPEN'
           ORDER BY closed_at DESC""",
        (user_id,)
    ).fetchall()
    streak = 0
    for r in recent:
        if r["status"] == "WIN":
            streak += 1
        else:
            break
    stats["current_streak"] = streak

    # User profile fields
    user = conn.execute(
        "SELECT xp, level, league FROM hb_users WHERE id=?", (user_id,)
    ).fetchone()
    if user:
        stats["xp"] = user["xp"]
        stats["level"] = user["level"]
        stats["league"] = user["league"]

    release_conn(conn)
    return stats


def hb_award_achievement(user_id: int, achievement: str) -> bool:
    """Award an achievement. Returns True if newly awarded, False if already had it."""
    conn = get_conn()
    cur = conn.execute(
        """INSERT OR IGNORE INTO hb_achievements (user_id, achievement, earned_at)
           VALUES (?,?,?)""",
        (user_id, achievement, datetime.utcnow().isoformat())
    )
    conn.commit()
    newly_awarded = cur.rowcount > 0
    release_conn(conn)
    return newly_awarded


def hb_get_achievements(user_id: int) -> list:
    """Get all achievements for a user."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM hb_achievements WHERE user_id=? ORDER BY earned_at DESC",
        (user_id,)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


def hb_get_open_trades(user_id: int = None) -> list:
    """Get open HyperBulk trades. If user_id is None, return all open trades."""
    conn = get_conn()
    if user_id:
        rows = conn.execute(
            "SELECT * FROM hb_trades WHERE user_id=? AND status='OPEN' ORDER BY opened_at DESC",
            (user_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM hb_trades WHERE status='OPEN' ORDER BY opened_at DESC"
        ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


# ── Battle Royale Helpers ─────────────────────────────────────

def br_create_game(symbol: str, direction: str, entry_fee: float,
                   rake_pct: float = 10.0) -> int:
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO br_games (symbol, direction, entry_fee, rake_pct, status, created_at) VALUES (?,?,?,?,?,?)",
        (symbol, direction, entry_fee, rake_pct, "lobby", datetime.utcnow().isoformat())
    )
    conn.commit()
    gid = cur.lastrowid
    release_conn(conn)
    return gid


def br_join_game(game_id: int, user_id: int):
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO br_players (game_id, user_id, status, joined_at) VALUES (?,?,?,?)",
        (game_id, user_id, "alive", datetime.utcnow().isoformat())
    )
    conn.execute(
        "UPDATE br_games SET player_count = (SELECT COUNT(*) FROM br_players WHERE game_id=?) WHERE id=?",
        (game_id, game_id)
    )
    conn.commit()
    release_conn(conn)


def br_settle_game(game_id: int, entry_price: float, pot_usd: float,
                   prize_pool: float, rake_usd: float, players: list):
    conn = get_conn()
    conn.execute(
        """UPDATE br_games SET status='settled', entry_price=?, pot_usd=?,
           prize_pool=?, rake_usd=?, ended_at=? WHERE id=?""",
        (entry_price, pot_usd, prize_pool, rake_usd,
         datetime.utcnow().isoformat(), game_id)
    )
    for p in players:
        conn.execute(
            """UPDATE br_players SET status=?, rank=?, payout_usd=?,
               survival_sec=?, elim_price=? WHERE game_id=? AND user_id=?""",
            (p["status"], p.get("rank", 0), p.get("payout_usd", 0),
             p.get("survival_sec", 0), p.get("elim_price", 0),
             game_id, p["user_id"])
        )
    conn.commit()
    release_conn(conn)


def br_get_leaderboard(limit: int = 50) -> list:
    conn = get_conn()
    rows = conn.execute(
        """SELECT p.user_id, u.username, u.wallet,
                  SUM(p.payout_usd) as total_winnings,
                  COUNT(*) as games_played,
                  SUM(CASE WHEN p.rank=1 THEN 1 ELSE 0 END) as victories,
                  AVG(p.survival_sec) as avg_survival
           FROM br_players p JOIN hb_users u ON p.user_id = u.id
           WHERE p.status IN ('winner','eliminated')
           GROUP BY p.user_id ORDER BY total_winnings DESC LIMIT ?""",
        (limit,)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


# ── Flip It Game Helpers ──────────────────────────────────────

def flip_create(user_id: int, symbol: str, exchange: str,
                direction: str, bet_amount: float, streak: int = 0) -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO flip_games (user_id, symbol, exchange, direction, bet_amount,
           streak, status, created_at) VALUES (?,?,?,?,?,?,?,?)""",
        (user_id, symbol, exchange, direction, bet_amount, streak,
         "pending", datetime.utcnow().isoformat())
    )
    conn.commit()
    game_id = cur.lastrowid
    release_conn(conn)
    return game_id


def flip_start(game_id: int, entry_price: float, size: float,
               order_id: str = ""):
    conn = get_conn()
    conn.execute(
        """UPDATE flip_games SET entry_price=?, size=?, order_id=?,
           status='live', started_at=? WHERE id=?""",
        (entry_price, size, order_id, datetime.utcnow().isoformat(), game_id)
    )
    conn.commit()
    release_conn(conn)


def flip_settle(game_id: int, exit_price: float, won: bool,
                price_change_pct: float, payout_mult: float,
                payout_usd: float, pnl_usd: float, streak: int):
    conn = get_conn()
    conn.execute(
        """UPDATE flip_games SET exit_price=?, won=?, price_change_pct=?,
           payout_mult=?, payout_usd=?, pnl_usd=?, streak=?,
           status=?, ended_at=? WHERE id=?""",
        (exit_price, int(won), price_change_pct, payout_mult,
         payout_usd, pnl_usd, streak,
         "won" if won else "lost", datetime.utcnow().isoformat(), game_id)
    )
    conn.commit()
    release_conn(conn)


def flip_get_streak(user_id: int) -> int:
    """Get current win streak for a user."""
    conn = get_conn()
    rows = conn.execute(
        """SELECT won FROM flip_games WHERE user_id=? AND status IN ('won','lost')
           ORDER BY id DESC LIMIT 20""",
        (user_id,)
    ).fetchall()
    release_conn(conn)
    streak = 0
    for r in rows:
        if r["won"]:
            streak += 1
        else:
            break
    return streak


def flip_get_history(user_id: int, limit: int = 50) -> list:
    conn = get_conn()
    rows = conn.execute(
        """SELECT * FROM flip_games WHERE user_id=? AND status IN ('won','lost')
           ORDER BY id DESC LIMIT ?""",
        (user_id, limit)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


def flip_get_stats(user_id: int) -> dict:
    conn = get_conn()
    row = conn.execute(
        """SELECT COUNT(*) as total,
                  SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
                  SUM(pnl_usd) as total_pnl,
                  MAX(streak) as best_streak,
                  MAX(payout_mult) as best_mult
           FROM flip_games WHERE user_id=? AND status IN ('won','lost')""",
        (user_id,)
    ).fetchone()
    release_conn(conn)
    d = dict(row) if row else {}
    total = d.get("total", 0) or 0
    wins = d.get("wins", 0) or 0
    return {
        "total": total,
        "wins": wins,
        "losses": total - wins,
        "win_rate": round(wins / total * 100, 1) if total else 0,
        "total_pnl": round(d.get("total_pnl", 0) or 0, 2),
        "best_streak": d.get("best_streak", 0) or 0,
        "best_mult": d.get("best_mult", 1.8) or 1.8,
        "current_streak": flip_get_streak(user_id),
    }


def flip_get_leaderboard(limit: int = 50) -> list:
    conn = get_conn()
    rows = conn.execute(
        """SELECT f.user_id, u.username, u.wallet,
                  COUNT(*) as total_flips,
                  SUM(CASE WHEN f.won=1 THEN 1 ELSE 0 END) as wins,
                  SUM(f.pnl_usd) as total_pnl,
                  MAX(f.streak) as best_streak
           FROM flip_games f JOIN hb_users u ON f.user_id = u.id
           WHERE f.status IN ('won','lost')
           GROUP BY f.user_id
           ORDER BY total_pnl DESC LIMIT ?""",
        (limit,)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


# ── Sniper Game Helpers ───────────────────────────────────────

def sniper_save_round(round_id: int, symbol: str, entry_fee: float,
                      duration_sec: int, rake_pct: float,
                      locks_at: str, settles_at: str) -> int:
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO sniper_rounds
           (id, symbol, entry_fee, duration_sec, rake_pct, status, created_at, locks_at, settles_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (round_id, symbol, entry_fee, duration_sec, rake_pct, "open",
         datetime.utcnow().isoformat(), locks_at, settles_at)
    )
    conn.commit()
    release_conn(conn)
    return round_id


def sniper_save_prediction(round_id: int, user_id: int,
                           predicted_price: float) -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT OR IGNORE INTO sniper_predictions
           (round_id, user_id, predicted_price, submitted_at)
           VALUES (?,?,?,?)""",
        (round_id, user_id, predicted_price, datetime.utcnow().isoformat())
    )
    # Update player count on round
    conn.execute(
        "UPDATE sniper_rounds SET player_count = player_count + 1, pot_usd = (player_count + 1) * entry_fee WHERE id=?",
        (round_id,)
    )
    conn.commit()
    row_id = cur.lastrowid or 0
    release_conn(conn)
    return row_id


def sniper_settle_round(round_id: int, actual_price: float,
                        prize_pool: float, rake_usd: float,
                        results: list):
    """Persist settlement results. results = [{user_id, rank, accuracy_pct, distance_usd, payout_usd, accuracy_tier}]"""
    conn = get_conn()
    conn.execute(
        """UPDATE sniper_rounds SET status='settled', actual_price=?, prize_pool=?,
           rake_usd=?, settled_at=? WHERE id=?""",
        (actual_price, prize_pool, rake_usd, datetime.utcnow().isoformat(), round_id)
    )
    for r in results:
        conn.execute(
            """UPDATE sniper_predictions SET rank=?, accuracy_pct=?, distance_usd=?,
               accuracy_tier=?, payout_usd=? WHERE round_id=? AND user_id=?""",
            (r["rank"], r["accuracy_pct"], r["distance_usd"],
             r["accuracy_tier"], r["payout_usd"], round_id, r["user_id"])
        )
    conn.commit()
    release_conn(conn)


def sniper_get_round(round_id: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM sniper_rounds WHERE id=?", (round_id,)).fetchone()
    release_conn(conn)
    return dict(row) if row else None


def sniper_get_leaderboard(limit: int = 50) -> list:
    """Best snipers by total winnings."""
    conn = get_conn()
    rows = conn.execute(
        """SELECT p.user_id, u.username, u.wallet,
                  SUM(p.payout_usd) as total_winnings,
                  COUNT(*) as rounds_played,
                  SUM(CASE WHEN p.rank = 1 THEN 1 ELSE 0 END) as first_places,
                  AVG(p.accuracy_pct) as avg_accuracy
           FROM sniper_predictions p
           JOIN hb_users u ON p.user_id = u.id
           WHERE p.rank IS NOT NULL
           GROUP BY p.user_id
           ORDER BY total_winnings DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


# ── Moon or Doom Game Helpers ─────────────────────────────────

def hb_create_game(user_id: int, symbol: str, exchange: str,
                   bet_amount: float, leverage: float = 50.0) -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO hb_games (user_id, symbol, exchange, bet_amount, leverage, status, created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (user_id, symbol, exchange, bet_amount, leverage, "waiting",
         datetime.utcnow().isoformat())
    )
    conn.commit()
    game_id = cur.lastrowid
    release_conn(conn)
    return game_id


def hb_start_game(game_id: int, entry_price: float, size: float,
                  order_id: str = "") -> bool:
    conn = get_conn()
    conn.execute(
        """UPDATE hb_games SET entry_price=?, size=?, order_id=?, status='live',
           started_at=? WHERE id=?""",
        (entry_price, size, order_id, datetime.utcnow().isoformat(), game_id)
    )
    conn.commit()
    release_conn(conn)
    return True


def hb_end_game(game_id: int, exit_price: float, multiplier: float,
                high_water: float, pnl_usd: float, status: str) -> dict:
    conn = get_conn()
    conn.execute(
        """UPDATE hb_games SET exit_price=?, multiplier=?, high_water=?,
           pnl_usd=?, status=?, ended_at=? WHERE id=?""",
        (exit_price, multiplier, high_water, pnl_usd, status,
         datetime.utcnow().isoformat(), game_id)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM hb_games WHERE id=?", (game_id,)).fetchone()
    release_conn(conn)
    return dict(row) if row else {}


def hb_get_game(game_id: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM hb_games WHERE id=?", (game_id,)).fetchone()
    release_conn(conn)
    return dict(row) if row else None


def hb_get_active_game(user_id: int) -> Optional[dict]:
    """Get user's currently active (live) game, if any."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM hb_games WHERE user_id=? AND status='live' ORDER BY id DESC LIMIT 1",
        (user_id,)
    ).fetchone()
    release_conn(conn)
    return dict(row) if row else None


def hb_get_game_history(user_id: int, limit: int = 50) -> list:
    conn = get_conn()
    rows = conn.execute(
        """SELECT * FROM hb_games WHERE user_id=? AND status IN ('cashed_out','crashed')
           ORDER BY ended_at DESC LIMIT ?""",
        (user_id, limit)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


def hb_get_game_leaderboard(limit: int = 50) -> list:
    """Leaderboard by highest single-game multiplier (cashed out only)."""
    conn = get_conn()
    rows = conn.execute(
        """SELECT g.*, u.username, u.wallet
           FROM hb_games g JOIN hb_users u ON g.user_id = u.id
           WHERE g.status='cashed_out'
           ORDER BY g.multiplier DESC LIMIT ?""",
        (limit,)
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


# ── NewsTrader Helpers ────────────────────────────────────────

def save_news_event(source: str, article_id: str, title: str,
                    sentiment: str = None, impact: int = None,
                    symbols: list = None) -> int:
    """Insert a news event; return its row id. Silently skips duplicates."""
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT OR IGNORE INTO news_events
               (ts, source, article_id, title, sentiment, impact, symbols)
               VALUES (?,?,?,?,?,?,?)""",
            (
                datetime.utcnow().isoformat(),
                source, article_id, title,
                sentiment, impact,
                json.dumps(symbols or []),
            )
        )
        conn.commit()
        row_id = cur.lastrowid or 0
    finally:
        release_conn(conn)
    return row_id


def is_news_seen(source: str, article_id: str) -> bool:
    """Return True if this article has already been saved."""
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM news_events WHERE source=? AND article_id=?",
        (source, article_id)
    ).fetchone()
    release_conn(conn)
    return row is not None


def mark_news_traded(event_id: int, trade_id: int):
    """Mark a news event as traded and record the trade_id."""
    conn = get_conn()
    conn.execute(
        "UPDATE news_events SET traded=1, trade_id=? WHERE id=?",
        (trade_id, event_id)
    )
    conn.commit()
    release_conn(conn)


# ── FundingArb Helpers ────────────────────────────────────────

def save_arb_position(symbol: str, long_exchange: str, short_exchange: str,
                      entry_diff_bps: float, position_usd: float,
                      long_trade_id: int = 0, short_trade_id: int = 0) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO funding_arb_positions
               (ts, symbol, long_exchange, short_exchange, entry_diff_bps,
                position_usd, long_trade_id, short_trade_id)
               VALUES (?,?,?,?,?,?,?,?)""",
            (datetime.utcnow().isoformat(), symbol, long_exchange,
             short_exchange, entry_diff_bps, position_usd,
             long_trade_id, short_trade_id)
        )
        conn.commit()
        return cur.lastrowid or 0
    finally:
        release_conn(conn)


def close_arb_position(arb_id: int, exit_diff_bps: float, pnl_usd: float):
    conn = get_conn()
    conn.execute(
        """UPDATE funding_arb_positions
           SET status='closed', exit_diff_bps=?, pnl_usd=?, closed_at=?
           WHERE id=?""",
        (exit_diff_bps, pnl_usd, datetime.utcnow().isoformat(), arb_id)
    )
    conn.commit()
    release_conn(conn)


def get_open_arb_positions() -> list:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM funding_arb_positions WHERE status='open'"
    ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


# ── HLCopier Helpers ─────────────────────────────────────────

def save_copier_position(whale_wallet: str, symbol: str, side: str,
                         whale_size: float, copy_size: float,
                         entry_price: float, trade_id: int = 0) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO copier_positions
               (ts, whale_wallet, symbol, side, whale_size, copy_size,
                entry_price, trade_id)
               VALUES (?,?,?,?,?,?,?,?)""",
            (datetime.utcnow().isoformat(), whale_wallet, symbol, side,
             whale_size, copy_size, entry_price, trade_id)
        )
        conn.commit()
        return cur.lastrowid or 0
    finally:
        release_conn(conn)


def close_copier_position(cp_id: int, exit_price: float, pnl_usd: float):
    conn = get_conn()
    conn.execute(
        """UPDATE copier_positions
           SET status='closed', exit_price=?, pnl_usd=?, closed_at=?
           WHERE id=?""",
        (exit_price, pnl_usd, datetime.utcnow().isoformat(), cp_id)
    )
    conn.commit()
    release_conn(conn)


def get_open_copier_positions(whale_wallet: str = None) -> list:
    conn = get_conn()
    if whale_wallet:
        rows = conn.execute(
            "SELECT * FROM copier_positions WHERE status='open' AND whale_wallet=?",
            (whale_wallet,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM copier_positions WHERE status='open'"
        ).fetchall()
    release_conn(conn)
    return [dict(r) for r in rows]


# ── MacroTrader / WarTrader Helpers ──────────────────────────

def save_macro_event(event_name: str, event_type: str, severity: int,
                     direction: str = None, symbols: list = None,
                     source: str = "calendar") -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO macro_events
               (ts, event_name, event_type, severity, direction, symbols, source)
               VALUES (?,?,?,?,?,?,?)""",
            (datetime.utcnow().isoformat(), event_name, event_type,
             severity, direction, json.dumps(symbols or []), source)
        )
        conn.commit()
        return cur.lastrowid or 0
    finally:
        release_conn(conn)


def mark_macro_traded(event_id: int, trade_id: int):
    conn = get_conn()
    conn.execute(
        "UPDATE macro_events SET traded=1, trade_id=? WHERE id=?",
        (trade_id, event_id)
    )
    conn.commit()
    release_conn(conn)


if __name__ == "__main__":
    init_db()
