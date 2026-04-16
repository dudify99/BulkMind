"""
BulkMind Preview Server
Run with: python preview.py
Opens: http://localhost:8080/hyperbulk

Starts only the Dashboard — no exchange connections, no trading agents.
Seeds the DB with demo data so all UI tabs are populated immediately.
"""

import asyncio
import random
import time
from datetime import datetime, timedelta
from db import init_db, get_conn, release_conn, hb_register_user, hb_log_trade, hb_close_trade
from reporter import Reporter
from dashboard import Dashboard

# ── Config ────────────────────────────────────────────────────────────────────

HOST = "0.0.0.0"
PORT = 8080

DEMO_USERS = [
    ("prev_wallet_btc",  "PreviewWhale"),
    ("prev_wallet_sol",  "PreviewSniper"),
    ("prev_wallet_eth",  "PreviewHunter"),
    ("prev_wallet_moon", "PreviewMoon"),
    ("prev_wallet_rush", "PreviewRush"),
    ("prev_wallet_degen","PreviewDegen"),
    ("prev_wallet_chart","PreviewChart"),
    ("prev_wallet_bk",   "PreviewBreakout"),
]

SYMBOLS   = ["BTC-USD", "ETH-USD", "SOL-USD"]
EXCHANGES = ["bulk", "hyperliquid"]

# Realistic base prices
BASE_PRICES = {"BTC-USD": 94_500.0, "ETH-USD": 3_220.0, "SOL-USD": 148.0}


# ── Demo Data Seeder ──────────────────────────────────────────────────────────

def _seed_demo_data():
    conn = get_conn()

    # Check if already seeded (both users AND trades must exist)
    user_count  = conn.execute("SELECT COUNT(*) FROM hb_users").fetchone()[0]
    trade_count = conn.execute("SELECT COUNT(*) FROM hb_trades").fetchone()[0]
    if user_count >= len(DEMO_USERS) and trade_count >= 30:
        release_conn(conn)
        print("   Demo data already present — skipping seed.")
        return

    release_conn(conn)

    print("   Seeding demo users + trades …")
    rng = random.Random(42)

    user_ids = []
    conn2 = get_conn()
    for wallet, name in DEMO_USERS:
        # Try insert; on any conflict fall back to a suffixed username
        for suffix in ("", "_2", "_3"):
            try:
                conn2.execute(
                    "INSERT OR IGNORE INTO hb_users (wallet, username, created_at, last_active) VALUES (?,?,?,?)",
                    (wallet, name + suffix, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
                )
                conn2.commit()
                break
            except Exception:
                continue
        row2 = conn2.execute("SELECT id FROM hb_users WHERE wallet=?", (wallet,)).fetchone()
        if row2:
            user_ids.append(row2["id"])
    release_conn(conn2)
    if not user_ids:
        print("   Could not seed users — skipping.")
        return

    # Seed 40 closed trades spread across users
    now = datetime.utcnow()
    for i in range(40):
        uid   = rng.choice(user_ids)
        sym   = rng.choice(SYMBOLS)
        ex    = rng.choice(EXCHANGES)
        side  = rng.choice(["BUY", "SELL"])
        base  = BASE_PRICES[sym]
        entry = base * rng.uniform(0.97, 1.03)
        size  = rng.uniform(0.01, 0.5)
        days_ago = rng.uniform(0, 7)

        trade_id = hb_log_trade(uid, ex, sym, side, entry, size)

        # Close with win or loss
        win = rng.random() > 0.42
        if win:
            exit_px = entry * (1 + rng.uniform(0.005, 0.04)) if side == "BUY" else entry * (1 - rng.uniform(0.005, 0.04))
        else:
            exit_px = entry * (1 - rng.uniform(0.003, 0.02)) if side == "BUY" else entry * (1 + rng.uniform(0.003, 0.02))

        hb_close_trade(trade_id, exit_px)

    # Seed a couple of open trades
    for uid in user_ids[:3]:
        sym  = rng.choice(SYMBOLS)
        ex   = rng.choice(EXCHANGES)
        side = rng.choice(["BUY", "SELL"])
        base = BASE_PRICES[sym]
        entry = base * rng.uniform(0.99, 1.01)
        size  = rng.uniform(0.05, 0.3)
        hb_log_trade(uid, ex, sym, side, entry, size)

    # Seed agent trades (for the main leaderboard / stats API)
    conn = get_conn()
    agents = [
        ("BreakoutBot:bulk", "BTC-USD", "BUY",  94200.0, 0.1, 94900.0, "WIN"),
        ("BreakoutBot:bulk", "ETH-USD", "SELL", 3250.0,  0.5, 3180.0,  "WIN"),
        ("NewsTrader:bulk",  "SOL-USD", "BUY",  145.0,   2.0, 152.0,   "WIN"),
        ("MacroTrader:bulk", "BTC-USD", "SELL", 95000.0, 0.2, 94100.0, "WIN"),
        ("WarTrader:bulk",   "BTC-USD", "BUY",  93000.0, 0.15, 91000.0,"LOSS"),
        ("FundingArb:bulk",  "ETH-USD", "BUY",  3200.0,  1.0, 3220.0,  "WIN"),
    ]
    ts = now.isoformat()
    for agent, sym, side, entry, sz, exit_px, status in agents:
        pnl = (exit_px - entry) * sz if side == "BUY" else (entry - exit_px) * sz
        conn.execute(
            """INSERT OR IGNORE INTO trades
               (agent, symbol, side, entry_price, exit_price, size,
                pnl_usd, status, ts, exit_ts, paper)
               VALUES (?,?,?,?,?,?,?,?,?,?,1)""",
            (agent, sym, side, entry, exit_px, sz, round(pnl, 2), status, ts, ts)
        )

    # Seed a latency record so /api/latency returns data
    for endpoint in ["/ticker/BTC-USD", "/orderbook/BTC-USD", "/candles/BTC-USD"]:
        conn.execute(
            """INSERT OR IGNORE INTO latency_log (ts, endpoint, latency_ms, status)
               VALUES (?,?,?,?)""",
            (ts, endpoint, round(random.uniform(20, 80), 1), "ok")
        )

    conn.commit()
    release_conn(conn)
    print(f"   Seeded {len(DEMO_USERS)} users, 40 closed trades, 3 open trades, {len(agents)} agent trades.")


# ── Fake price tick via WebSocket broadcast ───────────────────────────────────

async def _price_ticker_loop(dashboard: Dashboard):
    """Broadcast simulated price ticks every 2 s so the live feed and globe animate."""
    rng = random.Random()
    prices = dict(BASE_PRICES)
    sides = ["BUY", "SELL"]
    symbols = list(prices.keys())
    exs = ["bulk", "hyperliquid"]

    while True:
        await asyncio.sleep(2)
        try:
            sym = rng.choice(symbols)
            ex  = rng.choice(exs)
            # Random walk
            prices[sym] *= (1 + rng.uniform(-0.0005, 0.0005))
            trade_msg = {
                "symbol": sym,
                "side":   rng.choice(sides).lower(),
                "price":  round(prices[sym], 2),
                "size":   round(rng.uniform(0.01, 2.0), 4),
                "exchange": ex,
                "ts":     datetime.utcnow().isoformat() + "Z",
            }
            import json
            await dashboard.reporter._ws_broadcast("trade", json.dumps(trade_msg))
        except Exception:
            pass


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print("=" * 52)
    print("  BulkMind Preview Server")
    print(f"  http://localhost:{PORT}/hyperbulk")
    print("=" * 52)

    init_db()
    _seed_demo_data()

    reporter  = Reporter()           # console-only, no Telegram needed
    dashboard = Dashboard(reporter)  # no executors → view-only (no real trades)

    print(f"\n  Open: http://localhost:{PORT}/hyperbulk\n")

    await asyncio.gather(
        dashboard.run(),
        _price_ticker_loop(dashboard),
    )


if __name__ == "__main__":
    asyncio.run(main())
