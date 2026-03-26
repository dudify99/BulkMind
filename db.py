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


if __name__ == "__main__":
    init_db()
