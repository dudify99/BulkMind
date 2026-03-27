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


if __name__ == "__main__":
    init_db()
