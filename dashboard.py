"""
BulkMind Dashboard — FastAPI + WebSocket real-time dashboard
Serves REST API + live WebSocket feed + static frontend
"""

import asyncio
import json
import aiohttp
from datetime import datetime
from aiohttp import web
from db import (
    get_conn, get_open_trades, get_agent_stats, get_top_traders,
    search_wallets, get_wallet_profile, get_leaderboard, get_analytics,
    get_whales, get_liquidation_stats, get_recent_liquidations,
    get_exchange_summary, get_observed_trades
)
from reporter import Reporter
from config import DASHBOARD_HOST, DASHBOARD_PORT, BREAKOUT_PAPER_MODE, BULK_API_BASE
from pathlib import Path


STATIC_DIR = Path(__file__).parent / "static"


class Dashboard:
    def __init__(self, reporter: Reporter, bulksol=None):
        self.reporter = reporter
        self.bulksol = bulksol
        self.app = web.Application()
        self._setup_routes()

    def _setup_routes(self):
        self.app.router.add_get("/", self._serve_index)
        self.app.router.add_get("/api/status", self._api_status)
        self.app.router.add_get("/api/trades", self._api_trades)
        self.app.router.add_get("/api/trades/open", self._api_open_trades)
        self.app.router.add_get("/api/stats", self._api_stats)
        self.app.router.add_get("/api/issues", self._api_issues)
        self.app.router.add_get("/api/latency", self._api_latency)
        self.app.router.add_get("/api/traders", self._api_traders)
        self.app.router.add_get("/api/explorer/search", self._api_explorer_search)
        self.app.router.add_get("/api/explorer/wallet", self._api_explorer_wallet)
        self.app.router.add_get("/api/leaderboard", self._api_leaderboard)
        self.app.router.add_get("/api/analytics", self._api_analytics)
        self.app.router.add_get("/api/whales", self._api_whales)
        self.app.router.add_get("/api/market", self._api_market)
        self.app.router.add_get("/api/exchange-stats", self._api_exchange_stats)
        self.app.router.add_get("/api/exchange-summary", self._api_exchange_summary)
        self.app.router.add_get("/api/liquidations", self._api_liquidations)
        self.app.router.add_get("/api/liquidations/stats", self._api_liquidation_stats)
        self.app.router.add_get("/api/trades/feed", self._api_trades_feed)
        self.app.router.add_get("/api/account/{pubkey}", self._api_account)
        self.app.router.add_get("/api/account/{pubkey}/fills", self._api_account_fills)
        self.app.router.add_get("/api/account/{pubkey}/positions", self._api_account_positions)
        # BulkSOL staking analytics
        self.app.router.add_get("/api/bulksol", self._api_bulksol)
        self.app.router.add_get("/api/bulksol/history", self._api_bulksol_history)
        self.app.router.add_get("/api/bulksol/deployments", self._api_bulksol_deployments)
        self.app.router.add_get("/api/bulksol/validators", self._api_bulksol_validators)
        self.app.router.add_get("/ws", self._ws_handler)
        if STATIC_DIR.exists():
            self.app.router.add_static("/static/", path=str(STATIC_DIR), name="static")

    # ── WebSocket ──────────────────────────────────────────────

    async def _ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.reporter.register_ws(ws)
        print("🌐 Dashboard WebSocket connected")
        try:
            async for msg in ws:
                pass  # client messages ignored for now
        finally:
            self.reporter.unregister_ws(ws)
            print("🌐 Dashboard WebSocket disconnected")
        return ws

    # ── Pages ──────────────────────────────────────────────────

    async def _serve_index(self, request):
        index = STATIC_DIR / "index.html"
        if index.exists():
            return web.FileResponse(index)
        return web.Response(text="BulkMind Dashboard — static/index.html not found", status=404)

    # ── REST API ───────────────────────────────────────────────

    async def _api_status(self, request):
        conn = get_conn()
        trade_count = conn.execute("SELECT COUNT(*) as c FROM trades").fetchone()["c"]
        open_count = conn.execute("SELECT COUNT(*) as c FROM trades WHERE status='OPEN'").fetchone()["c"]
        issue_count = conn.execute(
            "SELECT COUNT(*) as c FROM issues WHERE resolved=0"
        ).fetchone()["c"]

        # Latest latency
        lat = conn.execute(
            "SELECT AVG(latency_ms) as avg_ms FROM latency_log WHERE ts > datetime('now', '-5 minutes') AND latency_ms > 0 AND error IS NULL"
        ).fetchone()
        conn.close()

        return web.json_response({
            "status": "online",
            "mode": "PAPER" if BREAKOUT_PAPER_MODE else "LIVE",
            "total_trades": trade_count,
            "open_trades": open_count,
            "unresolved_issues": issue_count,
            "avg_latency_ms": round(lat["avg_ms"], 2) if lat["avg_ms"] else None,
            "ts": datetime.utcnow().isoformat(),
        })

    async def _api_trades(self, request):
        limit = int(request.query.get("limit", "50"))
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return web.json_response([dict(r) for r in rows])

    async def _api_open_trades(self, request):
        trades = get_open_trades()
        return web.json_response(trades)

    async def _api_stats(self, request):
        agent = request.query.get("agent", "BreakoutBot")
        stats = get_agent_stats(agent)
        return web.json_response(stats)

    async def _api_issues(self, request):
        hours = int(request.query.get("hours", "24"))
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM issues WHERE ts > datetime('now', ?) ORDER BY ts DESC",
            (f"-{hours} hours",)
        ).fetchall()
        conn.close()
        return web.json_response([dict(r) for r in rows])

    async def _api_explorer_search(self, request):
        q = request.query.get("q", "")
        if len(q) < 3:
            return web.json_response({"results": [], "error": "Query must be at least 3 characters"})
        results = search_wallets(q, limit=20)
        return web.json_response({"results": results})

    async def _api_explorer_wallet(self, request):
        wallet = request.query.get("wallet", "")
        if not wallet:
            return web.json_response({"error": "wallet param required"}, status=400)
        profile = get_wallet_profile(wallet)
        return web.json_response(profile)

    async def _api_traders(self, request):
        hours = int(request.query.get("hours", "24"))
        limit = int(request.query.get("limit", "50"))
        data = get_top_traders(hours, limit)
        return web.json_response(data)

    async def _api_leaderboard(self, request):
        tab = request.query.get("tab", "top_traders")
        period = request.query.get("period", "24h")
        limit = int(request.query.get("limit", "100"))
        data = get_leaderboard(tab, period, limit)
        return web.json_response(data)

    async def _api_analytics(self, request):
        data = get_analytics()
        return web.json_response(data)

    async def _api_whales(self, request):
        min_balance = float(request.query.get("min_balance", "50000"))
        data = get_whales(min_balance)
        return web.json_response(data)

    async def _api_market(self, request):
        """Fetch live market data from Bulk Exchange API."""
        symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]
        tickers = {}
        try:
            async with aiohttp.ClientSession() as session:
                for symbol in symbols:
                    url = f"{BULK_API_BASE}/ticker/{symbol}"
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            t = await resp.json(content_type=None)
                            tickers[symbol] = {
                                "instrument": t.get("symbol", symbol),
                                "price": float(t.get("lastPrice", 0)),
                                "change_pct": float(t.get("priceChangePercent", 0)) * 100,
                                "high_24h": float(t.get("highPrice", 0)),
                                "low_24h": float(t.get("lowPrice", 0)),
                                "volume": float(t.get("volume", 0)),
                                "volume_24h": float(t.get("quoteVolume", 0)),
                                "open_interest": float(t.get("openInterest", 0)),
                                "mark_price": float(t.get("markPrice", 0)),
                                "funding_rate": float(t.get("fundingRate", 0)),
                            }
        except Exception as e:
            return web.json_response({"error": str(e)}, status=502)

        return web.json_response({
            "source": "Bulk Exchange API (live)",
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "tickers": tickers,
        })

    async def _api_exchange_summary(self, request):
        """Real exchange summary from observed trade data."""
        data = get_exchange_summary()
        return web.json_response(data)

    async def _api_liquidations(self, request):
        """Recent liquidation events."""
        limit = int(request.query.get("limit", "50"))
        data = get_recent_liquidations(limit)
        return web.json_response(data)

    async def _api_liquidation_stats(self, request):
        """Aggregated liquidation stats — longs vs shorts."""
        hours = int(request.query.get("hours", "24"))
        data = get_liquidation_stats(hours)
        return web.json_response(data)

    async def _api_trades_feed(self, request):
        """Recent observed trades from the WebSocket feed."""
        limit = int(request.query.get("limit", "50"))
        symbol = request.query.get("symbol", None)
        data = get_observed_trades(limit, symbol)
        return web.json_response(data)

    async def _api_exchange_stats(self, request):
        """Fetch live exchange stats from GET /stats."""
        period = request.query.get("period", "1d")
        symbol = request.query.get("symbol", None)
        try:
            async with aiohttp.ClientSession() as session:
                params = {"period": period}
                if symbol:
                    params["symbol"] = symbol
                url = f"{BULK_API_BASE}/stats"
                async with session.get(url, params=params,
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        return web.json_response(data)
                    return web.json_response({"error": f"API returned {resp.status}"}, status=502)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=502)

    async def _api_account(self, request):
        """Query any wallet's full account via POST /account (unsigned)."""
        pubkey = request.match_info["pubkey"]
        query_type = request.query.get("type", "fullAccount")
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{BULK_API_BASE}/account"
                async with session.post(url, json={"type": query_type, "user": pubkey},
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json(content_type=None)
                    return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=502)

    async def _api_account_fills(self, request):
        """Query wallet fills via POST /account."""
        pubkey = request.match_info["pubkey"]
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{BULK_API_BASE}/account"
                async with session.post(url, json={"type": "fills", "user": pubkey},
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json(content_type=None)
                    return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=502)

    async def _api_account_positions(self, request):
        """Query wallet closed positions via POST /account."""
        pubkey = request.match_info["pubkey"]
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{BULK_API_BASE}/account"
                async with session.post(url, json={"type": "positions", "user": pubkey},
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json(content_type=None)
                    return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=502)

    # ── BulkSOL Staking Analytics ───────────────────────────

    async def _api_bulksol(self, request):
        """Full BulkSOL staking stats: supply, APY, price, validator earnings, DeFi deployments."""
        if not self.bulksol:
            return web.json_response({"error": "BulkSOL module not initialized"}, status=503)
        try:
            async with aiohttp.ClientSession() as session:
                data = await self.bulksol.get_full_stats(session)
            return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=502)

    async def _api_bulksol_history(self, request):
        """BulkSOL historical snapshots for chart data."""
        if not self.bulksol:
            return web.json_response({"error": "BulkSOL module not initialized"}, status=503)
        hours = int(request.query.get("hours", "168"))
        data = self.bulksol.get_snapshots(hours)
        return web.json_response({
            "snapshots": data,
            "metrics": ["supply", "sol_value", "apy_pct", "price_usd",
                         "market_cap_usd", "total_sol_staked",
                         "validator_earnings_24h_usd"],
            "chart_labels": {
                "supply": "BulkSOL Supply",
                "sol_value": "SOL per BulkSOL",
                "apy_pct": "Staking APY %",
                "price_usd": "Price (USD)",
                "market_cap_usd": "Market Cap (USD)",
                "total_sol_staked": "Total SOL Staked",
                "validator_earnings_24h_usd": "Validator Earnings 24h (USD)",
            },
        })

    async def _api_bulksol_deployments(self, request):
        """Where BulkSOL is deployed across DeFi protocols with earnings data."""
        if not self.bulksol:
            return web.json_response({"error": "BulkSOL module not initialized"}, status=503)
        deployments = self.bulksol.get_protocol_deployments()
        return web.json_response({
            "deployments": deployments,
            "summary": {
                "total_protocols": len(deployments),
                "protocols_with_yield": sum(1 for d in deployments if d.get("apy")),
                "largest_deployment": "Exponent Finance (17,943 BulkSOL)",
            },
        })

    async def _api_bulksol_validators(self, request):
        """Validator earnings from Bulk exchange fee share."""
        if not self.bulksol:
            return web.json_response({"error": "BulkSOL module not initialized"}, status=503)
        try:
            async with aiohttp.ClientSession() as session:
                earnings = await self.bulksol.estimate_validator_earnings(session)
            return web.json_response({
                "earnings": earnings,
                "validator_info": {
                    "stake_pool": "3aUmJDNpMHjkxunQEkHTj2chzyryKoH2uQj6YACLD174",
                    "vote_account": "votem3UdGx5xWFbY9EFbyZ1X2pBuswfR5yd2oB3JAaj",
                    "active_validators": 1,
                    "commission": "0%",
                    "fee_share": "12.5% of all taker fees (USDC)",
                    "rewards_fee": "2.5%",
                    "citation": "https://solanacompass.com/stake-pools/3aUmJDNpMHjkxunQEkHTj2chzyryKoH2uQj6YACLD174",
                },
                "yield_stack": [
                    {"source": "SOL Inflation", "est_apy": None, "type": "base",
                     "note": "Variable per epoch, not hardcoded"},
                    {"source": "Jito MEV Tips", "est_apy": None, "type": "bonus",
                     "note": "Variable, included in Sanctum APY"},
                    {"source": "Bulk Fee Share (12.5%)", "est_apy": None, "type": "bonus",
                     "note": "⚠️ Taker fee rate estimated at 6bps",
                     "citation": "https://chainflow.io/bulk-exchange-the-architecture-that-pays-everyone-to-win/"},
                ],
                "staking_page": "https://early.bulk.trade/stake",
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=502)

    async def _api_latency(self, request):
        minutes = int(request.query.get("minutes", "60"))
        conn = get_conn()
        rows = conn.execute(
            """SELECT endpoint,
                      ROUND(AVG(latency_ms),2) as avg_ms,
                      ROUND(MIN(latency_ms),2) as min_ms,
                      ROUND(MAX(latency_ms),2) as max_ms,
                      COUNT(*) as samples
               FROM latency_log
               WHERE ts > datetime('now', ?) AND latency_ms > 0 AND error IS NULL
               GROUP BY endpoint""",
            (f"-{minutes} minutes",)
        ).fetchall()
        conn.close()
        return web.json_response([dict(r) for r in rows])

    # ── Run ────────────────────────────────────────────────────

    async def run(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, DASHBOARD_HOST, DASHBOARD_PORT)
        await site.start()
        print(f"🌐 Dashboard running at http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
        # Keep running forever
        while True:
            await asyncio.sleep(3600)
