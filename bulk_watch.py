"""
BulkWatch — Monitoring + Issue Detection Suite
Tracks: latency, downtime, execution quality, funding rates, API anomalies
"""

import asyncio
import aiohttp
import time
import json
import statistics
from datetime import datetime, timedelta
from typing import Optional
from db import log_latency, log_issue, get_conn
from config import (
    BULK_API_BASE, WATCH_PING_INTERVAL_SEC,
    WATCH_LATENCY_THRESHOLD_MS, WATCH_DOWNTIME_ALERT_SEC,
    WATCH_LOG_DIR, WATCH_REPORT_INTERVAL_MIN
)
from reporter import Reporter
from pathlib import Path


class BulkWatch:
    def __init__(self, reporter: Reporter):
        self.reporter     = reporter
        self.is_down      = False
        self.down_since   = None
        self.latency_buf  = []          # rolling 100 samples
        self.last_report  = datetime.utcnow()
        Path(WATCH_LOG_DIR).mkdir(parents=True, exist_ok=True)

    # ── Core Endpoints to Monitor ─────────────────────────────

    ENDPOINTS = {
        "orderbook":   "/l2book?type=l2book&coin=BTC-USD",
        "ticker":      "/ticker/BTC-USD",
        "stats":       "/stats?period=1d",
        "exchangeInfo": "/exchangeInfo",
        "order_place": "/order",          # POST — stress tested separately
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
                    log_issue(
                        severity="HIGH",
                        category="LATENCY",
                        title=f"High latency on {name}: {elapsed:.0f}ms",
                        details=f"Threshold: {WATCH_LATENCY_THRESHOLD_MS}ms | URL: {url}"
                    )
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
                self.is_down   = True
                self.down_since = datetime.utcnow()
                log_issue("CRITICAL", "DOWNTIME",
                          "Bulk API is DOWN",
                          f"First failure: {self.down_since.isoformat()}")
                await self.reporter.alert("🔴 BULK API IS DOWN\nFirst detected: " +
                                          self.down_since.isoformat())
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
                conn.close()
                await self.reporter.alert(
                    f"🟢 BULK BACK ONLINE\nDowntime: `{duration:.0f}s`"
                )
                self.is_down   = False
                self.down_since = None

    # ── Funding Rate Monitor ──────────────────────────────────

    async def check_funding_rates(self, session: aiohttp.ClientSession):
        """Fetch funding rates from GET /stats which includes per-market funding data"""
        result = await self.probe_endpoint(session, "stats", "/stats?period=1d")
        if result.get("body"):
            data = result["body"]
            funding = data.get("funding", {}).get("rates", {})
            markets = data.get("markets", [])
            conn = get_conn()
            for market in markets:
                symbol = market.get("symbol", "")
                rate = float(market.get("fundingRate", 0))
                conn.execute(
                    "INSERT INTO funding_rates (ts, symbol, rate) VALUES (?,?,?)",
                    (datetime.utcnow().isoformat(), symbol, rate)
                )
                # Flag anomalous funding rates
                if abs(rate) > 0.001:  # > 0.1% per 8h is unusual
                    log_issue("MEDIUM", "FUNDING",
                              f"Anomalous funding rate on {symbol}: {rate:.4%}",
                              f"Rate: {rate}")
            conn.commit()
            conn.close()

    # ── Execution Quality Test ────────────────────────────────

    async def stress_test_orderbook(self, session: aiohttp.ClientSession,
                                    symbol: str = "BTC-USD"):
        """Check bid-ask spread and depth via GET /l2book — flag if abnormal"""
        result = await self.probe_endpoint(
            session, "orderbook",
            f"/l2book?type=l2book&coin={symbol}&nlevels=10"
        )
        if not result.get("body"):
            return

        book = result["body"]
        # /l2book returns levels as [bids, asks] array
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

        if spread_bps > 10:  # > 10bps spread is a flag
            log_issue("MEDIUM", "SLIPPAGE",
                      f"Wide spread on {symbol}: {spread_bps:.1f}bps",
                      f"Bid: {best_bid} | Ask: {best_ask}")

        # Depth check — total depth in top 5 levels
        bid_depth = sum(float(b.get("sz", 0)) for b in bids[:5])
        ask_depth = sum(float(a.get("sz", 0)) for a in asks[:5])

        if bid_depth < 1.0 or ask_depth < 1.0:  # < 1 BTC depth = thin
            log_issue("HIGH", "LIQUIDITY",
                      f"Thin orderbook on {symbol}",
                      f"Bid depth: {bid_depth:.3f} | Ask depth: {ask_depth:.3f}")

        return {
            "symbol": symbol,
            "spread_bps": spread_bps,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth
        }

    # ── Latency Stats ─────────────────────────────────────────

    def compute_latency_stats(self) -> dict:
        conn = get_conn()
        rows = conn.execute(
            """SELECT latency_ms FROM latency_log
               WHERE ts > datetime('now', '-1 hour')
               AND latency_ms > 0 AND error IS NULL"""
        ).fetchall()
        conn.close()

        vals = [r["latency_ms"] for r in rows]
        if not vals:
            return {}

        vals.sort()
        return {
            "count":   len(vals),
            "min_ms":  min(vals),
            "max_ms":  max(vals),
            "avg_ms":  round(statistics.mean(vals), 2),
            "p50_ms":  round(vals[len(vals)//2], 2),
            "p95_ms":  round(vals[int(len(vals)*0.95)], 2),
            "p99_ms":  round(vals[int(len(vals)*0.99)], 2),
        }

    # ── Issue Summary ─────────────────────────────────────────

    def get_recent_issues(self, hours: int = 1) -> list:
        conn = get_conn()
        rows = conn.execute(
            """SELECT * FROM issues
               WHERE ts > datetime('now', ?) AND resolved=0
               ORDER BY severity, ts DESC""",
            (f"-{hours} hours",)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Hourly Report ─────────────────────────────────────────

    async def maybe_send_report(self):
        now = datetime.utcnow()
        if (now - self.last_report).total_seconds() < WATCH_REPORT_INTERVAL_MIN * 60:
            return
        self.last_report = now

        stats  = self.compute_latency_stats()
        issues = self.get_recent_issues()

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
        print("🔍 BulkWatch started")
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    await self.heartbeat(session)
                    await self.check_funding_rates(session)

                    # Full probe every 5 minutes
                    for name, path in self.ENDPOINTS.items():
                        if name != "order_place":
                            await self.probe_endpoint(session, name, path)

                    await self.stress_test_orderbook(session, "BTC-USD")
                    await self.stress_test_orderbook(session, "ETH-USD")
                    await self.maybe_send_report()

                except Exception as e:
                    print(f"BulkWatch error: {e}")
                    log_issue("HIGH", "SYSTEM",
                              "BulkWatch internal error", str(e))

                await asyncio.sleep(WATCH_PING_INTERVAL_SEC)
