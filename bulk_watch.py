"""
BulkWatch — Exchange Health + Trader Intelligence Suite
Tracks: latency, downtime, orderbook, funding, live trades, wallets, liquidations
"""

import asyncio
import aiohttp
import time
import json
import statistics
from datetime import datetime, timedelta
from typing import Optional
from db import (
    log_latency, log_issue, get_conn,
    log_observed_trade, log_liquidation,
    upsert_discovered_wallet, get_pending_wallets,
    mark_wallet_profiled, upsert_wallet_balance,
    upsert_trader_record, cleanup_old_observed_trades
)
from config import (
    BULK_API_BASE, BULK_WS_URL,
    WATCH_PING_INTERVAL_SEC, WATCH_LATENCY_THRESHOLD_MS,
    WATCH_DOWNTIME_ALERT_SEC, WATCH_LOG_DIR,
    WATCH_REPORT_INTERVAL_MIN, WATCH_WS_RECONNECT_SEC,
    WALLET_PROFILE_INTERVAL_SEC, WALLET_PROFILE_BATCH_SIZE,
    LIQUIDATION_ALERT_THRESHOLD_USD, WATCHED_SYMBOLS
)
from reporter import Reporter
from pathlib import Path


class BulkWatch:
    def __init__(self, reporter: Reporter, client=None):
        self.reporter     = reporter
        self.client       = client
        self.is_down      = False
        self.down_since   = None
        self.last_report  = datetime.utcnow()
        self.last_cleanup = datetime.utcnow()
        Path(WATCH_LOG_DIR).mkdir(parents=True, exist_ok=True)

    # ── Core Endpoints to Monitor ─────────────────────────────

    ENDPOINTS = {
        "ticker":       "/ticker/BTC-USD",
        "stats":        "/stats?period=1d",
        "exchangeInfo": "/exchangeInfo",
    }

    # ══════════════════════════════════════════════════════════
    # HEALTH MONITORING (existing, updated endpoints)
    # ══════════════════════════════════════════════════════════

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

    async def check_funding_rates(self, session: aiohttp.ClientSession):
        """Fetch funding rates from GET /stats which includes per-market funding data"""
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
            conn.close()

    async def stress_test_orderbook(self, session: aiohttp.ClientSession,
                                    symbol: str = "BTC-USD"):
        """Check bid-ask spread and depth via GET /l2book"""
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

        return {
            "symbol": symbol,
            "spread_bps": spread_bps,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth
        }

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

    # ══════════════════════════════════════════════════════════
    # TRADE STREAM CONSUMER (WebSocket)
    # ══════════════════════════════════════════════════════════

    async def run_trade_stream(self):
        """Connect to Bulk WebSocket, subscribe to trade feeds,
        discover wallets and track liquidations from real trade data."""
        print("📡 Trade stream starting...")
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        BULK_WS_URL,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as ws:
                        print("📡 Trade stream connected")

                        # Subscribe to trades for watched symbols
                        sub_msg = {
                            "method": "subscribe",
                            "subscription": [
                                {"type": "trades", "symbol": sym}
                                for sym in WATCHED_SYMBOLS
                            ]
                        }
                        await ws.send_json(sub_msg)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    await self._process_ws_message(data)
                                except json.JSONDecodeError:
                                    continue
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break

            except Exception as e:
                print(f"📡 Trade stream error: {e}")
                log_issue("HIGH", "SYSTEM",
                          "Trade stream disconnected", str(e))

            print(f"📡 Reconnecting in {WATCH_WS_RECONNECT_SEC}s...")
            await asyncio.sleep(WATCH_WS_RECONNECT_SEC)

    async def _process_ws_message(self, data: dict):
        """Process a WebSocket message — extract trades, detect liquidations."""
        # Handle different message formats from Bulk WS
        trades = []

        if isinstance(data, list):
            trades = data
        elif isinstance(data, dict):
            if "data" in data:
                payload = data["data"]
                trades = payload if isinstance(payload, list) else [payload]
            elif "symbol" in data and "price" in data:
                trades = [data]

        for trade in trades:
            symbol = trade.get("symbol") or trade.get("s", "")
            price = float(trade.get("price") or trade.get("px", 0))
            size = float(trade.get("amount") or trade.get("sz") or trade.get("qty", 0))
            side = trade.get("side") or ("buy" if trade.get("isBuy") else "sell")
            maker = trade.get("maker")
            taker = trade.get("taker")
            reason = trade.get("reason", "normal")
            ts = trade.get("timestamp") or trade.get("t")

            if not symbol or price == 0:
                continue

            value_usd = price * size

            # Store the trade
            log_observed_trade(
                symbol=symbol, side=side, price=price, size=size,
                maker=maker, taker=taker, reason=reason,
                raw_data=json.dumps(trade)
            )

            # Discover wallets
            if maker:
                upsert_discovered_wallet(maker)
            if taker:
                upsert_discovered_wallet(taker)

            # Track liquidations
            if reason in ("liquidation", "adl"):
                # If a long is liquidated, the closing trade is a sell
                liq_side = "LONG" if side in ("sell", "SELL") else "SHORT"
                liq_wallet = taker if side in ("sell", "SELL") else maker

                log_liquidation(
                    symbol=symbol, side=liq_side, price=price,
                    size=size, value_usd=value_usd,
                    wallet=liq_wallet, raw_data=json.dumps(trade)
                )

                if value_usd >= LIQUIDATION_ALERT_THRESHOLD_USD:
                    await self.reporter.alert(
                        f"💀 LIQUIDATION\n"
                        f"Side: `{liq_side}`\n"
                        f"Symbol: `{symbol}`\n"
                        f"Size: `{size}` @ `{price}`\n"
                        f"Value: `${value_usd:,.0f}`\n"
                        f"Wallet: `{(liq_wallet or 'unknown')[:16]}...`"
                    )

    # ══════════════════════════════════════════════════════════
    # WALLET PROFILER (background enrichment)
    # ══════════════════════════════════════════════════════════

    async def run_wallet_profiler(self):
        """Loop: pick discovered wallets, query POST /account,
        populate traders + wallet_balances tables for leaderboard."""
        print("👛 Wallet profiler starting...")
        await asyncio.sleep(30)  # let trade stream discover some wallets first

        while True:
            try:
                wallets = get_pending_wallets(WALLET_PROFILE_BATCH_SIZE)
                if wallets:
                    print(f"👛 Profiling {len(wallets)} wallets...")

                for wallet in wallets:
                    try:
                        await self._profile_wallet(wallet)
                    except Exception as e:
                        print(f"👛 Error profiling {wallet[:12]}...: {e}")
                    await asyncio.sleep(1)  # rate limit between wallets

            except Exception as e:
                print(f"👛 Wallet profiler error: {e}")

            await asyncio.sleep(WALLET_PROFILE_INTERVAL_SEC)

    async def _profile_wallet(self, wallet: str):
        """Query a single wallet's full account and store results."""
        async with aiohttp.ClientSession() as session:
            url = f"{BULK_API_BASE}/account"

            # Get full account
            async with session.post(
                url, json={"type": "fullAccount", "user": wallet},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    mark_wallet_profiled(wallet)
                    return
                data = await resp.json(content_type=None)

            if not data or not isinstance(data, list):
                mark_wallet_profiled(wallet)
                return

            # Extract account data
            for item in data:
                account = item.get("fullAccount")
                if not account:
                    continue

                margin = account.get("margin", {})
                total_balance = float(margin.get("totalBalance", 0))
                available = float(margin.get("availableBalance", 0))
                margin_used = float(margin.get("marginUsed", 0))
                unrealized = float(margin.get("unrealizedPnl", 0))
                realized = float(margin.get("realizedPnl", 0))
                equity = total_balance + unrealized

                # Store balance
                upsert_wallet_balance(
                    wallet=wallet,
                    balance_usd=total_balance,
                    equity_usd=equity,
                    unrealized_pnl=unrealized,
                    margin_used=margin_used
                )

                # Extract positions and compute per-symbol PnL
                positions = account.get("positions", [])
                for pos in positions:
                    symbol = pos.get("symbol", "")
                    size = float(pos.get("size", 0))
                    rpnl = float(pos.get("realizedPnl", 0))
                    notional = float(pos.get("notional", 0))
                    side = "BUY" if size > 0 else "SELL"
                    pnl_pct = (rpnl / notional * 100) if notional else 0

                    if symbol and notional > 0:
                        upsert_trader_record(
                            wallet=wallet,
                            symbol=symbol,
                            side=side,
                            pnl_usd=rpnl,
                            pnl_pct=pnl_pct,
                            volume_usd=abs(notional),
                            trades_count=1
                        )

                break  # only process first fullAccount item

        mark_wallet_profiled(wallet)

    # ══════════════════════════════════════════════════════════
    # MAIN RUN LOOP
    # ══════════════════════════════════════════════════════════

    async def _health_loop(self):
        """Existing health monitoring loop."""
        async with aiohttp.ClientSession() as session:
            while True:
                try:
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
                    print(f"BulkWatch health error: {e}")
                    log_issue("HIGH", "SYSTEM",
                              "BulkWatch internal error", str(e))

                await asyncio.sleep(WATCH_PING_INTERVAL_SEC)

    async def run(self):
        print("🔍 BulkWatch started (health + trades + wallets)")
        await asyncio.gather(
            self._health_loop(),
            self.run_trade_stream(),
            self.run_wallet_profiler(),
        )
