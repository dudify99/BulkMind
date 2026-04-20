"""
BulkWatch — Exchange Health Monitoring
Tracks: latency, downtime, orderbook depth/spread, funding rate anomalies
"""

import asyncio
import aiohttp
import time
import statistics
from datetime import datetime
from db import log_latency, log_issue, get_conn, release_conn, cleanup_old_observed_trades
from config import (
    BULK_API_BASE, WATCH_PING_INTERVAL_SEC, WATCH_LATENCY_THRESHOLD_MS,
    WATCH_DOWNTIME_ALERT_SEC, WATCH_LOG_DIR, WATCH_REPORT_INTERVAL_MIN
)
from reporter import Reporter
from agent_monitor import monitor as agent_monitor
from pathlib import Path


class BulkWatch:
    def __init__(self, reporter: Reporter):
        self.reporter     = reporter
        self.is_down      = False
        self.down_since   = None
        self.last_report  = datetime.utcnow()
        self.last_cleanup = datetime.utcnow()
        Path(WATCH_LOG_DIR).mkdir(parents=True, exist_ok=True)

    ENDPOINTS = {
        "ticker":       "/ticker/BTC-USD",
        "stats":        "/stats?period=1d",
        "exchangeInfo": "/exchangeInfo",
    }

    # ── Latency Probe ─────────────────────────────────────────

    async def probe_endpoint(self, session: aiohttp.ClientSession,
                             name: str, path: str) -> dict:
        url = BULK_API_BASE + path
        start = time.perf_counter()
        result = {"endpoint": name, "latency_ms": None,
                  "status": None, "error": None}
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                elapsed = (time.perf_counter() - start) * 1000
                result.update({
                    "latency_ms": round(elapsed, 2),
                    "status": resp.status,
                    "body": await resp.json(content_type=None)
                })
                log_latency(name, elapsed, resp.status)

                if elapsed > WATCH_LATENCY_THRESHOLD_MS:
                    log_issue("HIGH", "LATENCY",
                              f"High latency on {name}: {elapsed:.0f}ms",
                              f"Threshold: {WATCH_LATENCY_THRESHOLD_MS}ms | URL: {url}")
                    await self.reporter.alert(
                        f"⚠️ HIGH LATENCY\n"
                        f"Endpoint: `{name}`\n"
                        f"Latency: `{elapsed:.0f}ms`\n"
                        f"Threshold: `{WATCH_LATENCY_THRESHOLD_MS}ms`"
                    )

        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            result.update({"error": "TIMEOUT", "latency_ms": elapsed})
            log_latency(name, elapsed, error="TIMEOUT")
        except Exception as e:
            result.update({"error": str(e)})
            log_latency(name, -1, error=str(e))

        return result

    # ── Downtime Detection ────────────────────────────────────

    async def heartbeat(self, session: aiohttp.ClientSession):
        result = await self.probe_endpoint(session, "heartbeat", "/ticker/BTC-USD")

        if result["error"] or (result["status"] and result["status"] >= 500):
            if not self.is_down:
                self.is_down    = True
                self.down_since = datetime.utcnow()
                log_issue("CRITICAL", "DOWNTIME",
                          "Bulk API is DOWN",
                          f"First failure: {self.down_since.isoformat()}")
                await self.reporter.alert(
                    "🔴 BULK API IS DOWN\nFirst detected: " +
                    self.down_since.isoformat()
                )
            else:
                duration = (datetime.utcnow() - self.down_since).total_seconds()
                if duration > WATCH_DOWNTIME_ALERT_SEC:
                    await self.reporter.alert(
                        f"🔴 BULK STILL DOWN\nDuration: `{duration:.0f}s`"
                    )
        else:
            if self.is_down:
                duration = (datetime.utcnow() - self.down_since).total_seconds()
                conn = get_conn()
                conn.execute(
                    """INSERT INTO downtime_log (start_ts, end_ts, duration_sec)
                       VALUES (?,?,?)""",
                    (self.down_since.isoformat(),
                     datetime.utcnow().isoformat(), duration)
                )
                conn.commit()
                release_conn(conn)
                await self.reporter.alert(
                    f"🟢 BULK BACK ONLINE\nDowntime: `{duration:.0f}s`"
                )
                self.is_down    = False
                self.down_since = None

    # ── Funding Rate Monitor ──────────────────────────────────

    async def check_funding_rates(self, session: aiohttp.ClientSession):
        result = await self.probe_endpoint(session, "stats", "/stats?period=1d")
        if result.get("body"):
            data = result["body"]
            markets = data.get("markets", [])
            conn = get_conn()
            for market in markets:
                symbol = market.get("symbol", "")
                rate = float(market.get("fundingRate", 0))
                conn.execute(
                    "INSERT INTO funding_rates (ts, symbol, rate) VALUES (?,?,?)",
                    (datetime.utcnow().isoformat(), symbol, rate)
                )
                if abs(rate) > 0.001:
                    log_issue("MEDIUM", "FUNDING",
                              f"Anomalous funding rate on {symbol}: {rate:.4%}",
                              f"Rate: {rate}")
            conn.commit()
            release_conn(conn)

    # ── Orderbook Health ──────────────────────────────────────

    async def stress_test_orderbook(self, session: aiohttp.ClientSession,
                                    symbol: str = "BTC-USD"):
        result = await self.probe_endpoint(
            session, "orderbook",
            f"/l2book?type=l2book&coin={symbol}&nlevels=10"
        )
        if not result.get("body"):
            return

        book = result["body"]
        levels = book.get("levels", [[], []])
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []

        if not bids or not asks:
            log_issue("HIGH", "LIQUIDITY",
                      f"Empty orderbook for {symbol}",
                      "No bids or asks returned")
            return

        best_bid = float(bids[0].get("px", 0))
        best_ask = float(asks[0].get("px", 0))
        if best_bid == 0 or best_ask == 0:
            return
        spread_bps = (best_ask - best_bid) / best_bid * 10000

        if spread_bps > 10:
            log_issue("MEDIUM", "SLIPPAGE",
                      f"Wide spread on {symbol}: {spread_bps:.1f}bps",
                      f"Bid: {best_bid} | Ask: {best_ask}")

        bid_depth = sum(float(b.get("sz", 0)) for b in bids[:5])
        ask_depth = sum(float(a.get("sz", 0)) for a in asks[:5])

        if bid_depth < 1.0 or ask_depth < 1.0:
            log_issue("HIGH", "LIQUIDITY",
                      f"Thin orderbook on {symbol}",
                      f"Bid depth: {bid_depth:.3f} | Ask depth: {ask_depth:.3f}")

    # ── Latency Stats ─────────────────────────────────────────

    def compute_latency_stats(self) -> dict:
        conn = get_conn()
        rows = conn.execute(
            """SELECT latency_ms FROM latency_log
               WHERE ts > datetime('now', '-1 hour')
               AND latency_ms > 0 AND error IS NULL"""
        ).fetchall()
        release_conn(conn)

        vals = [r["latency_ms"] for r in rows]
        if not vals:
            return {}

        vals.sort()
        return {
            "count":  len(vals),
            "min_ms": min(vals),
            "max_ms": max(vals),
            "avg_ms": round(statistics.mean(vals), 2),
            "p50_ms": round(vals[len(vals)//2], 2),
            "p95_ms": round(vals[int(len(vals)*0.95)], 2),
            "p99_ms": round(vals[int(len(vals)*0.99)], 2),
        }

    # ── Hourly Report ─────────────────────────────────────────

    async def maybe_send_report(self):
        now = datetime.utcnow()
        if (now - self.last_report).total_seconds() < WATCH_REPORT_INTERVAL_MIN * 60:
            return
        self.last_report = now

        stats = self.compute_latency_stats()
        conn = get_conn()
        issues = conn.execute(
            """SELECT * FROM issues
               WHERE ts > datetime('now', '-1 hours') AND resolved=0
               ORDER BY severity, ts DESC"""
        ).fetchall()
        release_conn(conn)
        issues = [dict(r) for r in issues]

        critical = [i for i in issues if i["severity"] == "CRITICAL"]
        high     = [i for i in issues if i["severity"] == "HIGH"]

        report = (
            f"📊 *BulkWatch Hourly Report*\n"
            f"`{now.strftime('%Y-%m-%d %H:%M UTC')}`\n\n"
            f"*Latency (last 1h)*\n"
            f"  avg: `{stats.get('avg_ms','N/A')}ms` | "
            f"p95: `{stats.get('p95_ms','N/A')}ms` | "
            f"p99: `{stats.get('p99_ms','N/A')}ms`\n\n"
            f"*Issues*\n"
            f"  🔴 Critical: `{len(critical)}`\n"
            f"  🟠 High: `{len(high)}`\n"
            f"  Total open: `{len(issues)}`\n"
        )

        if critical:
            report += "\n*Critical Issues:*\n"
            for iss in critical[:3]:
                report += f"  • {iss['title']}\n"

        await self.reporter.send(report)

    # ── Main Loop ─────────────────────────────────────────────

    async def run(self):
        print("🔍 BulkWatch started — exchange health monitoring")
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    agent_monitor.heartbeat("BulkWatch")
                    await self.heartbeat(session)
                    await self.check_funding_rates(session)

                    for name, path in self.ENDPOINTS.items():
                        if name != "order_place":
                            await self.probe_endpoint(session, name, path)

                    await self.stress_test_orderbook(session, "BTC-USD")
                    await self.stress_test_orderbook(session, "ETH-USD")
                    await self.maybe_send_report()

                    # Periodic cleanup of old observed trades (every 6h)
                    now = datetime.utcnow()
                    if (now - self.last_cleanup).total_seconds() > 21600:
                        cleanup_old_observed_trades(days=7)
                        self.last_cleanup = now

                except Exception as e:
                    print(f"BulkWatch error: {e}")
                    log_issue("HIGH", "SYSTEM",
                              "BulkWatch internal error", str(e))

                await asyncio.sleep(WATCH_PING_INTERVAL_SEC)
