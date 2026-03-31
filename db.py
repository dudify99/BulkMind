"""
BulkMind Database Layer
Stores: latency logs, trade history, issues, agent performance
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from config import DB_PATH


def get_conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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

    # ── Indexes ──────────────────────────────────────────────
    c.execute("CREATE INDEX IF NOT EXISTS idx_observed_trades_ts ON observed_trades(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_observed_trades_maker ON observed_trades(maker)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_observed_trades_taker ON observed_trades(taker)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_liquidations_ts ON liquidations(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_wallet_discovery_status ON wallet_discovery(status, last_profiled)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_traders_wallet ON traders(wallet)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_traders_ts ON traders(ts)")

    conn.commit()
    conn.close()
    print("✅ Database initialized")


# ── Helper Functions ──────────────────────────────────────────

def log_latency(endpoint: str, latency_ms: float, status_code: int = None, error: str = None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO latency_log (ts, endpoint, latency_ms, status_code, error) VALUES (?,?,?,?,?)",
        (datetime.utcnow().isoformat(), endpoint, latency_ms, status_code, error)
    )
    conn.commit()
    conn.close()


def log_issue(severity: str, category: str, title: str, details: str = None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO issues (ts, severity, category, title, details) VALUES (?,?,?,?,?)",
        (datetime.utcnow().isoformat(), severity, category, title, details)
    )
    conn.commit()
    conn.close()
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
    conn.close()
    return trade_id


def close_trade(trade_id: int, exit_price: float, status: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
    if not row:
        conn.close()
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
    conn.close()
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
    conn.close()


def get_candles(symbol: str, timeframe: int, limit: int = 100) -> list:
    conn = get_conn()
    rows = conn.execute(
        """SELECT * FROM candles WHERE symbol=? AND timeframe=?
           ORDER BY ts DESC LIMIT ?""",
        (symbol, timeframe, limit)
    ).fetchall()
    conn.close()
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
    conn.close()
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
    conn.close()
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

    conn.close()
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
    conn.close()
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

    conn.close()

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
        conn.close()
        return []

    rows = conn.execute(query, params).fetchall()
    conn.close()

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

    conn.close()

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

    conn.close()
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
    conn.close()


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
    conn.close()


def upsert_discovered_wallet(wallet: str):
    conn = get_conn()
    conn.execute(
        """INSERT INTO wallet_discovery (wallet, first_seen, trade_count)
           VALUES (?, ?, 1)
           ON CONFLICT(wallet) DO UPDATE SET trade_count = trade_count + 1""",
        (wallet, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()


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
    conn.close()
    return [r["wallet"] for r in rows]


def mark_wallet_profiled(wallet: str):
    conn = get_conn()
    conn.execute(
        """UPDATE wallet_discovery SET status='profiled', last_profiled=?
           WHERE wallet=?""",
        (datetime.utcnow().isoformat(), wallet)
    )
    conn.commit()
    conn.close()


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
    conn.close()


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
    conn.close()


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
    conn.close()
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
    conn.close()
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
    conn.close()
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
    conn.close()
    return [dict(r) for r in rows]


def cleanup_old_observed_trades(days: int = 7):
    conn = get_conn()
    conn.execute(
        "DELETE FROM observed_trades WHERE ts < datetime('now', ?)",
        (f"-{days} days",)
    )
    conn.commit()
    conn.close()


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
        conn.close()
    return row_id


def is_news_seen(source: str, article_id: str) -> bool:
    """Return True if this article has already been saved."""
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM news_events WHERE source=? AND article_id=?",
        (source, article_id)
    ).fetchone()
    conn.close()
    return row is not None


def mark_news_traded(event_id: int, trade_id: int):
    """Mark a news event as traded and record the trade_id."""
    conn = get_conn()
    conn.execute(
        "UPDATE news_events SET traded=1, trade_id=? WHERE id=?",
        (trade_id, event_id)
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
