"""
BulkMind Dashboard — aiohttp + WebSocket real-time dashboard
Serves REST API + live WebSocket feed + static frontend
"""

import asyncio
import json
import time
import aiohttp
from datetime import datetime
from aiohttp import web
from db import (
    get_conn, release_conn, get_open_trades, get_agent_stats, get_top_traders,
    search_wallets, get_wallet_profile, get_leaderboard, get_analytics,
    get_whales, get_liquidation_stats, get_recent_liquidations,
    get_exchange_summary, get_observed_trades,
    hb_register_user, hb_get_user, hb_get_user_stats, hb_log_trade,
    hb_close_trade, hb_get_leaderboard, hb_get_open_trades,
    hb_get_achievements, hb_award_achievement,
    hb_create_game, hb_start_game, hb_end_game, hb_get_game,
    hb_get_active_game, hb_get_game_history, hb_get_game_leaderboard,
    sniper_save_round, sniper_save_prediction, sniper_settle_round,
    sniper_get_round, sniper_get_leaderboard,
    flip_create, flip_start, flip_settle, flip_get_streak,
    flip_get_history, flip_get_stats, flip_get_leaderboard,
    br_create_game, br_join_game, br_settle_game, br_get_leaderboard,
)
from reporter import Reporter
from config import DASHBOARD_HOST, DASHBOARD_PORT, BREAKOUT_PAPER_MODE, BULK_API_BASE, HL_API_BASE
from validation import (
    validate_int, validate_float, validate_wallet, validate_symbol,
    validate_exchange, validate_side, validate_direction, validate_period,
    validate_interval, validate_username,
)
from collections import defaultdict
from pathlib import Path


STATIC_DIR = Path(__file__).parent / "static"

# Rate limiting: requests per IP per window
RATE_LIMIT_WINDOW = 60       # seconds
RATE_LIMIT_MAX = 120         # max requests per window (2/sec average)
RATE_LIMIT_POST_MAX = 30     # stricter limit for POST (trade/game actions)

_rate_buckets: dict = defaultdict(list)  # ip → [timestamps]


@web.middleware
async def rate_limit_middleware(request, handler):
    """Per-IP sliding window rate limiter."""
    # Skip static files and websocket
    path = request.path
    if path.startswith("/static/") or path == "/ws":
        return await handler(request)

    ip = request.remote or "unknown"
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW

    # Prune old entries
    bucket = _rate_buckets[ip]
    _rate_buckets[ip] = bucket = [t for t in bucket if t > cutoff]

    # Choose limit based on method
    limit = RATE_LIMIT_POST_MAX if request.method == "POST" else RATE_LIMIT_MAX

    if len(bucket) >= limit:
        return web.json_response(
            {"error": "Rate limit exceeded. Try again shortly."},
            status=429,
        )

    bucket.append(now)
    return await handler(request)


class Dashboard:
    def __init__(self, reporter: Reporter, bulksol=None,
                 bulk_executor=None, hl_executor=None):
        self.reporter = reporter
        self.bulksol = bulksol
        self.bulk_executor = bulk_executor
        self.hl_executor = hl_executor
        self.active_games: dict = {}  # game_id → MoonOrDoomEngine
        self.app = web.Application(middlewares=[rate_limit_middleware])
        self._setup_routes()

    def _setup_routes(self):
        self.app.router.add_get("/", self._serve_index)
        self.app.router.add_get("/api/status", self._api_status)
        self.app.router.add_get("/api/agents", self._api_agents)
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
        # ── HyperBulk Routes ──
        self.app.router.add_get("/hyperbulk", self._serve_hyperbulk)
        self.app.router.add_post("/api/hb/register", self._hb_register)
        self.app.router.add_get("/api/hb/user/{wallet}", self._hb_user)
        self.app.router.add_post("/api/hb/trade", self._hb_trade)
        self.app.router.add_post("/api/hb/trade/{trade_id}/close", self._hb_close_trade)
        self.app.router.add_get("/api/hb/leaderboard", self._hb_leaderboard)
        self.app.router.add_get("/api/hb/trades/open", self._hb_open_trades)
        self.app.router.add_get("/api/hb/achievements/{wallet}", self._hb_achievements)
        self.app.router.add_get("/api/hb/market", self._hb_market)
        self.app.router.add_post("/api/hb/faucet", self._hb_faucet)
        self.app.router.add_get("/api/hb/candles", self._hb_candles)
        self.app.router.add_get("/api/hb/pnl-history/{wallet}", self._hb_pnl_history)
        # ── Signal Engine + Alpha Rush Routes ──
        self.app.router.add_get("/api/hb/signals", self._hb_signals)
        self.app.router.add_get("/api/hb/signals/backtest", self._hb_signals_backtest)
        self.app.router.add_post("/api/hb/rush/start", self._rush_start)
        self.app.router.add_get("/api/hb/rush/{game_id}", self._rush_state)
        self.app.router.add_post("/api/hb/rush/{game_id}/execute", self._rush_execute)
        self.app.router.add_post("/api/hb/rush/{game_id}/skip", self._rush_skip)
        self.app.router.add_get("/api/hb/rush/history/{wallet}", self._rush_history)
        # ── Battle Royale Routes ──
        self.app.router.add_post("/api/hb/br/create", self._br_create)
        self.app.router.add_post("/api/hb/br/{game_id}/join", self._br_join)
        self.app.router.add_get("/api/hb/br/{game_id}", self._br_state)
        self.app.router.add_post("/api/hb/br/{game_id}/start", self._br_start)
        self.app.router.add_get("/api/hb/br/active", self._br_active)
        self.app.router.add_get("/api/hb/br/leaderboard", self._br_leaderboard)
        # ── Flip It Game Routes ──
        self.app.router.add_post("/api/hb/flip/start", self._flip_start)
        self.app.router.add_get("/api/hb/flip/{game_id}", self._flip_state)
        self.app.router.add_get("/api/hb/flip/stats/{wallet}", self._flip_stats)
        self.app.router.add_get("/api/hb/flip/history/{wallet}", self._flip_history)
        self.app.router.add_get("/api/hb/flip/leaderboard", self._flip_leaderboard)
        # ── Sniper Game Routes ──
        self.app.router.add_post("/api/hb/sniper/create", self._sniper_create)
        self.app.router.add_post("/api/hb/sniper/{round_id}/predict", self._sniper_predict)
        self.app.router.add_get("/api/hb/sniper/{round_id}", self._sniper_state)
        self.app.router.add_post("/api/hb/sniper/{round_id}/settle", self._sniper_settle)
        self.app.router.add_get("/api/hb/sniper/active", self._sniper_active)
        self.app.router.add_get("/api/hb/sniper/leaderboard", self._sniper_leaderboard)
        # ── Moon or Doom Game Routes ──
        self.app.router.add_post("/api/hb/game/start", self._hb_game_start)
        self.app.router.add_post("/api/hb/game/{game_id}/cashout", self._hb_game_cashout)
        self.app.router.add_post("/api/hb/game/{game_id}/add", self._hb_game_add)
        self.app.router.add_get("/api/hb/game/{game_id}", self._hb_game_state)
        self.app.router.add_get("/api/hb/game/history/{wallet}", self._hb_game_history)
        self.app.router.add_get("/api/hb/game/leaderboard", self._hb_game_leaderboard)
        # ── Market Dice Routes ──
        self.app.router.add_post("/api/hb/dice/roll", self._dice_roll)
        self.app.router.add_get("/api/hb/dice/{roll_id}", self._dice_state)
        self.app.router.add_get("/api/hb/dice/history/{wallet}", self._dice_history)
        self.app.router.add_get("/api/hb/dice/stats/{wallet}", self._dice_stats)
        self.app.router.add_get("/api/hb/dice/leaderboard", self._dice_leaderboard)
        self.app.router.add_get("/api/hb/dice/verify/{roll_id}", self._dice_verify)
        # ── Analytics Routes (MMT-style) ──
        self.app.router.add_get("/api/hb/orderflow/cvd", self._hb_cvd)
        self.app.router.add_get("/api/hb/orderflow/delta", self._hb_volume_delta)
        self.app.router.add_get("/api/hb/orderflow/bubbles", self._hb_large_trades)
        self.app.router.add_get("/api/hb/orderflow/footprint", self._hb_footprint)
        self.app.router.add_get("/api/hb/liquidity/heatmap", self._hb_heatmap)
        self.app.router.add_get("/api/hb/liquidity/depth", self._hb_depth)
        self.app.router.add_get("/api/hb/derivatives/oi", self._hb_oi)
        self.app.router.add_get("/api/hb/derivatives/funding", self._hb_funding)
        self.app.router.add_get("/api/hb/derivatives/liqmap", self._hb_liq_map)
        self.app.router.add_get("/api/hb/profile/volume", self._hb_volume_profile)
        self.app.router.add_get("/api/hb/profile/tpo", self._hb_tpo)
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
        release_conn(conn)

        return web.json_response({
            "status": "online",
            "mode": "PAPER" if BREAKOUT_PAPER_MODE else "LIVE",
            "total_trades": trade_count,
            "open_trades": open_count,
            "unresolved_issues": issue_count,
            "avg_latency_ms": round(lat["avg_ms"], 2) if lat["avg_ms"] else None,
            "ts": datetime.utcnow().isoformat(),
        })

    async def _api_agents(self, request):
        from agent_monitor import monitor
        return web.json_response(monitor.get_all())

    async def _api_trades(self, request):
        limit = validate_int(request.query.get("limit", "50"), default=50, min_val=1, max_val=500)
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        release_conn(conn)
        return web.json_response([dict(r) for r in rows])

    async def _api_open_trades(self, request):
        trades = get_open_trades()
        return web.json_response(trades)

    async def _api_stats(self, request):
        agent = request.query.get("agent", "BreakoutBot")
        stats = get_agent_stats(agent)
        return web.json_response(stats)

    async def _api_issues(self, request):
        hours = validate_int(request.query.get("hours", "24"), default=24, min_val=1, max_val=720)
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM issues WHERE ts > datetime('now', ?) ORDER BY ts DESC",
            (f"-{hours} hours",)
        ).fetchall()
        release_conn(conn)
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
        hours = validate_int(request.query.get("hours", "24"), default=24, min_val=1, max_val=720)
        limit = validate_int(request.query.get("limit", "50"), default=50, min_val=1, max_val=500)
        data = get_top_traders(hours, limit)
        return web.json_response(data)

    async def _api_leaderboard(self, request):
        tab = request.query.get("tab", "top_traders")
        period = validate_period(request.query.get("period", "24h"), default="24h")
        limit = validate_int(request.query.get("limit", "100"), default=100, min_val=1, max_val=500)
        data = get_leaderboard(tab, period, limit)
        return web.json_response(data)

    async def _api_analytics(self, request):
        data = get_analytics()
        return web.json_response(data)

    async def _api_whales(self, request):
        min_balance = validate_float(request.query.get("min_balance", "50000"), default=50000.0, min_val=0.0, max_val=100_000_000.0)
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
        limit = validate_int(request.query.get("limit", "50"), default=50, min_val=1, max_val=500)
        data = get_recent_liquidations(limit)
        return web.json_response(data)

    async def _api_liquidation_stats(self, request):
        """Aggregated liquidation stats — longs vs shorts."""
        hours = validate_int(request.query.get("hours", "24"), default=24, min_val=1, max_val=720)
        data = get_liquidation_stats(hours)
        return web.json_response(data)

    async def _api_trades_feed(self, request):
        """Recent observed trades from the WebSocket feed."""
        limit = validate_int(request.query.get("limit", "50"), default=50, min_val=1, max_val=500)
        symbol = request.query.get("symbol", None)
        if symbol:
            symbol = validate_symbol(symbol)
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
        hours = validate_int(request.query.get("hours", "168"), default=168, min_val=1, max_val=8760)
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

    # ── HyperBulk Handlers ──────────────────────────────────────

    async def _serve_hyperbulk(self, request):
        """Serve the HyperBulk frontend with PRIVY_APP_ID injected."""
        from config import PRIVY_APP_ID
        page = STATIC_DIR / "hyperbulk.html"
        if not page.exists():
            return web.Response(text="HyperBulk — static/hyperbulk.html not found", status=404)
        if PRIVY_APP_ID:
            html = page.read_text()
            inject = f'<script>window.PRIVY_APP_ID="{PRIVY_APP_ID}";</script>'
            html = html.replace("<head>", "<head>" + inject, 1)
            return web.Response(text=html, content_type="text/html")
        return web.FileResponse(page)

    async def _hb_register(self, request):
        """Register a new HyperBulk user."""
        try:
            body = await request.json()
            wallet, err = validate_wallet(body.get("wallet"))
            if err:
                return web.json_response({"error": err}, status=400)
            username, err = validate_username(body.get("username"))
            if err:
                return web.json_response({"error": err}, status=400)
            user = hb_register_user(wallet, username)
            return web.json_response(user)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_user(self, request):
        """Get HyperBulk user profile with stats."""
        try:
            wallet = request.match_info["wallet"]
            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)
            stats = hb_get_user_stats(user["id"])
            return web.json_response({**user, **stats})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_trade(self, request):
        """Open a new HyperBulk trade on one or both exchanges.
        Calls real executors (paper or live) and logs to DB."""
        try:
            body = await request.json()
            wallet, err = validate_wallet(body.get("wallet"))
            if err:
                return web.json_response({"error": err}, status=400)
            exchange = validate_exchange(body.get("exchange", "bulk"))
            symbol = validate_symbol(body.get("symbol", "BTC-USD"))
            side, err = validate_side(body.get("side", "BUY"))
            if err:
                return web.json_response({"error": err}, status=400)
            size = validate_float(body.get("size", 0), min_val=0.0, max_val=1000.0)
            if size <= 0:
                return web.json_response({"error": "positive size is required"}, status=400)

            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found. Register first."}, status=404)

            # Determine which exchanges to execute on
            venues = []
            if exchange in ("bulk", "both"):
                venues.append("bulk")
            if exchange in ("hyperliquid", "both"):
                venues.append("hyperliquid")

            results = []
            for ex in venues:
                # Get fill price from the correct exchange
                fill_price = await self._get_price(symbol, ex)

                # Execute via real executor
                order_id = ""
                executor = self.bulk_executor if ex == "bulk" else self.hl_executor
                if executor:
                    ex_symbol = symbol
                    if ex == "hyperliquid":
                        from config import HL_SYMBOL_MAP
                        ex_symbol = HL_SYMBOL_MAP.get(symbol, symbol)
                    order = await executor.place_order(
                        symbol=ex_symbol, side=side,
                        price=fill_price, size=size,
                        order_type="limit",
                    )
                    if order:
                        order_id = order.get("order_id", "")
                        fill_price = float(order.get("price", fill_price))

                # Log to HyperBulk DB
                trade_id = hb_log_trade(
                    user_id=user["id"],
                    exchange=ex,
                    symbol=symbol,
                    side=side,
                    size=size,
                    entry_price=fill_price,
                    order_id=order_id,
                )

                results.append({
                    "trade_id": trade_id,
                    "exchange": ex,
                    "fill_price": fill_price,
                    "order_id": order_id,
                })

            # Check "both_barrels" achievement
            if len(venues) == 2:
                hb_award_achievement(user["id"], "both_barrels")

            # Check "first_blood" on first trade
            stats = hb_get_user_stats(user["id"])
            if stats.get("total_trades", 0) <= len(venues):
                hb_award_achievement(user["id"], "first_blood")

            # Broadcast trade event for live feed
            for r in results:
                await self.reporter.broadcast_trade({
                    "symbol": symbol,
                    "side": side.lower(),
                    "price": r["fill_price"],
                    "size": size,
                    "value_usd": round(r["fill_price"] * size, 2),
                    "exchange": r["exchange"],
                    "reason": "hyperbulk",
                    "ts": datetime.utcnow().isoformat(),
                })

            return web.json_response({
                "trades": results,
                "exchange": exchange,
                "paper": bool(executor and executor.paper) if executor else True,
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # ── In-memory price cache (updated by analytics loop) ──
    _price_cache: dict = {}        # "bulk:BTC-USD" → 84723.45
    _price_cache_str: dict = {}    # "bulk:BTC-USD" → "84723.45"
    _price_cache_ts: dict = {}     # "bulk:BTC-USD" → timestamp

    @classmethod
    def cache_price(cls, symbol: str, exchange: str, price: float):
        import time as _t
        key = f"{exchange}:{symbol}"
        cls._price_cache[key] = price
        cls._price_cache_str[key] = f"{price}"
        cls._price_cache_ts[key] = _t.time()

    @classmethod
    def get_cached_price(cls, symbol: str, exchange: str) -> tuple:
        """Return (price_float, price_str) from cache. Zero-latency."""
        key = f"{exchange}:{symbol}"
        p = cls._price_cache.get(key, 0.0)
        s = cls._price_cache_str.get(key, "0")
        return p, s

    async def _get_price(self, symbol: str, exchange: str) -> float:
        """Fetch current price — uses cache if fresh (<10s), else API call."""
        import time as _t
        key = f"{exchange}:{symbol}"
        cached_ts = self._price_cache_ts.get(key, 0)
        if _t.time() - cached_ts < 10 and self._price_cache.get(key, 0) > 0:
            return self._price_cache[key]

        try:
            async with aiohttp.ClientSession() as session:
                if exchange == "hyperliquid":
                    url = f"{HL_API_BASE}/info"
                    async with session.post(url, json={"type": "allMids"},
                                            timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            hl_map = {"BTC-USD": "BTC", "ETH-USD": "ETH", "SOL-USD": "SOL"}
                            hl_sym = hl_map.get(symbol, symbol.replace("-USD", ""))
                            if hl_sym in data:
                                price = float(data[hl_sym])
                                self.cache_price(symbol, exchange, price)
                                return price
                else:
                    url = f"{BULK_API_BASE}/ticker/{symbol}"
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            t = await resp.json(content_type=None)
                            price = float(t.get("lastPrice", 0))
                            if price:
                                self.cache_price(symbol, exchange, price)
                            return price
        except Exception:
            pass
        return self._price_cache.get(key, 0.0)

    async def _hb_close_trade(self, request):
        """Close an open HyperBulk trade with proper price from the right exchange."""
        try:
            trade_id = int(request.match_info["trade_id"])

            # Look up the trade to get symbol + exchange
            from db import hb_get_user_by_id
            conn = get_conn()
            trade_row = conn.execute(
                "SELECT * FROM hb_trades WHERE id=? AND status='OPEN'",
                (trade_id,)
            ).fetchone()
            release_conn(conn)

            if not trade_row:
                return web.json_response({"error": "Trade not found or already closed"}, status=404)

            trade_info = dict(trade_row)
            symbol = trade_info["symbol"]
            ex = trade_info.get("exchange", "bulk")

            # Fetch price from the correct exchange
            current_price = await self._get_price(symbol, ex)

            result = hb_close_trade(trade_id, current_price)
            if not result:
                return web.json_response({"error": "Close failed"}, status=500)

            # Award achievements based on updated stats
            user_id = trade_info["user_id"]
            stats = hb_get_user_stats(user_id)
            new_achievements = []

            checks = [
                ("first_blood",    stats.get("total_trades", 0) >= 1),
                ("on_fire",        stats.get("total_trades", 0) >= 10),
                ("sniper",         stats.get("current_streak", 0) >= 5),
                ("lightning",      True),  # TODO: check close time < 60s
                ("whale_alert",    abs(result.get("pnl_usd", 0)) >= 10000),
            ]
            for ach_id, condition in checks:
                if condition and hb_award_achievement(user_id, ach_id):
                    new_achievements.append(ach_id)

            # Check top_10 from leaderboard
            lb = hb_get_leaderboard("alltime", limit=10)
            for entry in lb:
                if entry.get("user_id") == user_id:
                    if hb_award_achievement(user_id, "top_10"):
                        new_achievements.append("top_10")
                    break

            result["new_achievements"] = new_achievements
            return web.json_response(result)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_leaderboard(self, request):
        """HyperBulk leaderboard by period."""
        try:
            period = validate_period(request.query.get("period", "alltime"))
            data = hb_get_leaderboard(period)
            return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_open_trades(self, request):
        """List open HyperBulk trades, optionally filtered by wallet."""
        try:
            wallet = request.query.get("wallet", None)
            user_id = None
            if wallet:
                user = hb_get_user(wallet)
                if not user:
                    return web.json_response({"error": "User not found"}, status=404)
                user_id = user["id"]
            data = hb_get_open_trades(user_id)
            return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_achievements(self, request):
        """Get achievements for a HyperBulk user."""
        try:
            wallet = request.match_info["wallet"]
            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)
            data = hb_get_achievements(user["id"])
            return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_faucet(self, request):
        """Request testnet USDC from the Bulk faucet for a connected wallet."""
        try:
            data = await request.json()
            wallet_addr = validate_wallet(data.get("wallet", ""))
            if not wallet_addr:
                return web.json_response({"error": "Wallet address required"}, status=400)

            if self.bulk_executor:
                result = await self.bulk_executor.faucet()
                if result:
                    return web.json_response({
                        "status": "ok",
                        "message": "Testnet USDC requested for your Bulk account",
                        "wallet": wallet_addr,
                        "result": result,
                    })
                return web.json_response({"error": "Faucet request failed"}, status=500)

            # No executor — return demo response
            return web.json_response({
                "status": "ok",
                "message": "Testnet USDC credited (demo mode)",
                "wallet": wallet_addr,
                "amount": 10000,
                "paper": True,
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_market(self, request):
        """Fetch tickers from BOTH Bulk and Hyperliquid APIs with spread comparison."""
        symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]
        bulk_prices = {}
        hl_prices = {}
        spreads = {}

        try:
            async with aiohttp.ClientSession() as session:
                # Fetch Bulk prices
                for symbol in symbols:
                    try:
                        url = f"{BULK_API_BASE}/ticker/{symbol}"
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                            if resp.status == 200:
                                t = await resp.json(content_type=None)
                                bulk_prices[symbol] = float(t.get("lastPrice", 0))
                    except Exception:
                        bulk_prices[symbol] = None

                # Fetch Hyperliquid mid prices
                try:
                    url = f"{HL_API_BASE}/info"
                    async with session.post(url, json={"type": "allMids"},
                                            timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            # HL uses short names: BTC, ETH, SOL
                            hl_map = {"BTC-USD": "BTC", "ETH-USD": "ETH", "SOL-USD": "SOL"}
                            for symbol in symbols:
                                hl_sym = hl_map.get(symbol)
                                if hl_sym and hl_sym in data:
                                    hl_prices[symbol] = float(data[hl_sym])
                                else:
                                    hl_prices[symbol] = None
                except Exception:
                    for symbol in symbols:
                        hl_prices[symbol] = None

                # Calculate spreads
                for symbol in symbols:
                    bp = bulk_prices.get(symbol)
                    hp = hl_prices.get(symbol)
                    if bp and hp and hp > 0:
                        spreads[symbol] = round(((bp - hp) / hp) * 100, 4)
                    else:
                        spreads[symbol] = None

        except Exception as e:
            return web.json_response({"error": str(e)}, status=502)

        return web.json_response({
            "bulk": bulk_prices,
            "hyperliquid": hl_prices,
            "spread": spreads,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        })

    # ── Signal Engine + Alpha Rush Handlers ─────────────────

    async def _hb_signals(self, request):
        """Get current AI signals for a symbol. <20ms response."""
        from signal_engine import signals
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        limit = validate_int(request.query.get("limit", "5"), default=5, min_val=1, max_val=20)
        return web.json_response(signals.get_signals(symbol, limit))

    async def _hb_signals_backtest(self, request):
        """Get backtest results per strategy."""
        from signal_engine import signals
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        return web.json_response(signals.get_backtest(symbol))

    async def _rush_start(self, request):
        """Start an Alpha Rush game — 5 rounds of AI signals."""
        try:
            from rush_engine import rush
            body = await request.json()
            wallet, err = validate_wallet(body.get("wallet"))
            if err:
                return web.json_response({"error": err}, status=400)
            symbol = validate_symbol(body.get("symbol", "BTC-USD"))
            bet = validate_float(body.get("bet_amount", 5.0), default=5.0, min_val=1.0, max_val=10000.0)
            exchange = validate_exchange(body.get("exchange", "bulk"))

            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)

            game = rush.create_game(user["id"], symbol, exchange, bet)
            rush.start_game(game.game_id)
            return web.json_response(rush.to_dict(game))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _rush_state(self, request):
        """Get Alpha Rush game state. Auto-ticks rounds."""
        try:
            from rush_engine import rush
            game_id = int(request.match_info["game_id"])
            game = rush.games.get(game_id)
            if not game:
                return web.json_response({"error": "Game not found"}, status=404)

            # Tick with current price
            price = await self._get_price(game.symbol, game.exchange)
            if price:
                rush.tick(game_id, price)

            # Auto-advance if between rounds
            if game.status == "between_rounds":
                rush.advance_round(game_id)

            return web.json_response(rush.to_dict(game))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _rush_execute(self, request):
        """Execute the current AI signal (open real position)."""
        try:
            from rush_engine import rush
            game_id = int(request.match_info["game_id"])
            game = rush.games.get(game_id)
            if not game:
                return web.json_response({"error": "Game not found"}, status=404)

            price = await self._get_price(game.symbol, game.exchange)
            if not price:
                return web.json_response({"error": "Could not fetch price"}, status=502)

            # Place real order
            rnd = game.rounds[-1] if game.rounds else None
            if not rnd:
                return web.json_response({"error": "No active round"}, status=400)

            direction = rnd.signal.get("direction", "BUY")
            side = direction
            size = round(game.per_round * 10 / price, 6)
            order_id = ""

            executor = self.bulk_executor if game.exchange == "bulk" else self.hl_executor
            if executor:
                ex_symbol = game.symbol
                if game.exchange == "hyperliquid":
                    from config import HL_SYMBOL_MAP
                    ex_symbol = HL_SYMBOL_MAP.get(game.symbol, game.symbol)
                order = await executor.place_order(
                    symbol=ex_symbol, side=side, price=price,
                    size=size, order_type="market",
                )
                if order:
                    order_id = order.get("order_id", "")
                    price = float(order.get("price", price))

            rush.execute_round(game_id, price, order_id)
            return web.json_response(rush.to_dict(game))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _rush_skip(self, request):
        """Skip the current AI signal."""
        try:
            from rush_engine import rush
            game_id = int(request.match_info["game_id"])
            rush.skip_round(game_id)
            game = rush.games.get(game_id)
            if not game:
                return web.json_response({"error": "Game not found"}, status=404)
            # Auto-advance
            if game.status == "between_rounds":
                rush.advance_round(game_id)
            return web.json_response(rush.to_dict(game))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _rush_history(self, request):
        """Get Alpha Rush history for a user."""
        try:
            from rush_engine import rush
            wallet = request.match_info["wallet"]
            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)
            history = [
                rush.to_dict(g)
                for g in rush.games.values()
                if g.user_id == user["id"] and g.status == "finished"
            ]
            return web.json_response(history[-20:])
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # ── Battle Royale Handlers ─────────────────────────────

    async def _br_create(self, request):
        """Create a Battle Royale lobby."""
        try:
            from br_engine import battle_royale, BRConfig
            body = await request.json()
            symbol = validate_symbol(body.get("symbol", "BTC-USD"))
            direction = validate_direction(body.get("direction", "long"), default="long")
            entry_fee = validate_float(body.get("entry_fee", 10.0), default=10.0, min_val=1.0, max_val=10000.0)

            config = BRConfig(symbol=symbol, direction=direction, entry_fee=entry_fee)
            game = battle_royale.create_game(config)
            db_id = br_create_game(symbol, direction, entry_fee)
            game.game_id = db_id
            battle_royale.games[db_id] = game

            return web.json_response(battle_royale.get_state(db_id))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _br_join(self, request):
        """Join a Battle Royale lobby."""
        try:
            from br_engine import battle_royale
            game_id = validate_int(request.match_info["game_id"], default=0, min_val=1, max_val=999999)
            body = await request.json()
            wallet, err = validate_wallet(body.get("wallet"))
            if err:
                return web.json_response({"error": err}, status=400)

            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)

            error = battle_royale.join_game(
                game_id, user["id"], wallet,
                user.get("username", wallet[:8]),
            )
            if error:
                return web.json_response({"error": error}, status=400)

            br_join_game(game_id, user["id"])

            # Broadcast join event
            await self.reporter._ws_broadcast("br_event", json.dumps({
                "type": "player_joined",
                "game_id": game_id,
                "username": user.get("username", wallet[:8]),
                "player_count": len(battle_royale.games[game_id].players),
            }))

            return web.json_response(battle_royale.get_state(game_id))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _br_start(self, request):
        """Start a Battle Royale game (admin or auto when lobby full/timeout)."""
        try:
            from br_engine import battle_royale
            game_id = int(request.match_info["game_id"])
            game = battle_royale.games.get(game_id)
            if not game:
                return web.json_response({"error": "Game not found"}, status=404)

            entry_price = await self._get_price(game.config.symbol, "bulk")
            if not entry_price:
                return web.json_response({"error": "Could not fetch price"}, status=502)

            battle_royale.start_game(game_id, entry_price)

            await self.reporter._ws_broadcast("br_event", json.dumps({
                "type": "game_started",
                "game_id": game_id,
                "entry_price": entry_price,
                "player_count": len(game.players),
            }))

            return web.json_response(battle_royale.get_state(game_id))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _br_state(self, request):
        """Get Battle Royale game state. Auto-ticks with current price."""
        try:
            from br_engine import battle_royale
            game_id = int(request.match_info["game_id"])
            game = battle_royale.games.get(game_id)
            if not game:
                return web.json_response({"error": "Game not found"}, status=404)

            # Tick with live price
            if game.status == "live":
                price = await self._get_price(game.config.symbol, "bulk")
                if price:
                    prev_alive = len([p for p in game.players if p.status == "alive"])
                    battle_royale.tick(game_id, price)
                    new_alive = len([p for p in game.players if p.status == "alive"])

                    # Broadcast eliminations
                    if new_alive < prev_alive:
                        for elim in game.eliminations[-(prev_alive - new_alive):]:
                            await self.reporter._ws_broadcast("br_event", json.dumps({
                                "type": "elimination",
                                "game_id": game_id,
                                **elim,
                            }))

                    # If settled, persist
                    if game.status == "settled":
                        br_settle_game(
                            game_id, game.entry_price, game.pot_usd,
                            game.prize_pool_usd, game.rake_usd,
                            [{"user_id": p.user_id, "status": p.status,
                              "rank": p.rank, "payout_usd": p.payout_usd,
                              "survival_sec": p.survival_sec,
                              "elim_price": p.eliminated_price}
                             for p in game.players]
                        )

            return web.json_response(battle_royale.get_state(game_id))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _br_active(self, request):
        """List active Battle Royale games."""
        try:
            from br_engine import battle_royale
            return web.json_response(battle_royale.get_active_games())
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _br_leaderboard(self, request):
        """Battle Royale leaderboard."""
        try:
            return web.json_response(br_get_leaderboard())
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # ── Flip It Game Handlers ────────────────────────────────

    async def _flip_start(self, request):
        """Start a Flip It game — pick UP or DOWN, 60 seconds."""
        try:
            from flip_engine import flip
            body = await request.json()
            wallet, err = validate_wallet(body.get("wallet"))
            if err:
                return web.json_response({"error": err}, status=400)
            symbol = validate_symbol(body.get("symbol", "BTC-USD"))
            direction = validate_direction(body.get("direction", "up"), default="up")
            if direction not in ("up", "down"):
                return web.json_response({"error": "direction must be up or down"}, status=400)
            bet_amount = validate_float(body.get("bet_amount", 5.0), default=5.0, min_val=1.0, max_val=10000.0)
            exchange = validate_exchange(body.get("exchange", "bulk"))

            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)

            # Get current streak
            streak = flip_get_streak(user["id"])

            # Create in engine + DB
            game = flip.create_game(
                user["id"], symbol, exchange, direction, bet_amount,
                streak=streak,
            )
            db_id = flip_create(user["id"], symbol, exchange, direction, bet_amount, streak)
            game.game_id = db_id

            # Get entry price
            fill_price = await self._get_price(symbol, exchange)
            if not fill_price:
                return web.json_response({"error": "Could not fetch price"}, status=502)

            # Open position (BUY for UP, SELL for DOWN)
            side = "BUY" if direction == "up" else "SELL"
            size = round(bet_amount * 10 / fill_price, 6)  # 10x notional for visible PnL
            order_id = ""

            executor = self.bulk_executor if exchange == "bulk" else self.hl_executor
            if executor:
                ex_symbol = symbol
                if exchange == "hyperliquid":
                    from config import HL_SYMBOL_MAP
                    ex_symbol = HL_SYMBOL_MAP.get(symbol, symbol)
                order = await executor.place_order(
                    symbol=ex_symbol, side=side, price=fill_price,
                    size=size, order_type="market",
                )
                if order:
                    order_id = order.get("order_id", "")
                    fill_price = float(order.get("price", fill_price))

            # Start
            flip.start_game(db_id, fill_price, size, order_id)
            flip_start(db_id, fill_price, size, order_id)

            # Broadcast
            await self.reporter.broadcast_trade({
                "symbol": symbol, "side": side.lower(),
                "price": fill_price, "size": size,
                "value_usd": round(fill_price * size, 2),
                "exchange": exchange, "reason": "flip",
                "ts": datetime.utcnow().isoformat(),
            })

            return web.json_response(flip.to_dict(game))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _flip_state(self, request):
        """Get live state of a Flip It game. Auto-settles when timer expires."""
        try:
            from flip_engine import flip
            game_id = int(request.match_info["game_id"])
            game = flip.active_games.get(game_id)

            if not game:
                # Check DB for finished game
                from db import get_conn
                conn = get_conn()
                row = conn.execute("SELECT * FROM flip_games WHERE id=?", (game_id,)).fetchone()
                release_conn(conn)
                if row:
                    return web.json_response(dict(row))
                return web.json_response({"error": "Game not found"}, status=404)

            # Tick with current price
            current_price = await self._get_price(game.symbol, game.exchange)
            if current_price:
                flip.tick(game_id, current_price)

            # If just settled, persist
            if game.status in ("won", "lost"):
                flip_settle(
                    game_id, game.exit_price, game.won,
                    game.price_change_pct, game.payout_multiplier,
                    game.payout_usd, game.pnl_usd, game.streak,
                )
                # Close the position
                executor = self.bulk_executor if game.exchange == "bulk" else self.hl_executor
                if executor and game.size > 0:
                    close_side = "SELL" if game.direction == "up" else "BUY"
                    ex_symbol = game.symbol
                    if game.exchange == "hyperliquid":
                        from config import HL_SYMBOL_MAP
                        ex_symbol = HL_SYMBOL_MAP.get(game.symbol, game.symbol)
                    await executor.place_order(
                        symbol=ex_symbol, side=close_side,
                        price=current_price, size=game.size,
                        order_type="market",
                    )

            return web.json_response(flip.to_dict(game))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _flip_stats(self, request):
        """Get Flip It stats for a user."""
        try:
            wallet = request.match_info["wallet"]
            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)
            return web.json_response(flip_get_stats(user["id"]))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _flip_history(self, request):
        """Get Flip It game history for a user."""
        try:
            wallet = request.match_info["wallet"]
            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)
            return web.json_response(flip_get_history(user["id"]))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _flip_leaderboard(self, request):
        """Flip It leaderboard — most profitable flippers."""
        try:
            return web.json_response(flip_get_leaderboard())
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # ── Sniper Game Handlers ────────────────────────────────

    async def _sniper_create(self, request):
        """Create a new Sniper prediction round."""
        try:
            from sniper_engine import sniper, SniperConfig
            body = await request.json()
            symbol = validate_symbol(body.get("symbol", "BTC-USD"))
            entry_fee = validate_float(body.get("entry_fee", 5.0), default=5.0, min_val=1.0, max_val=10000.0)
            duration = validate_int(body.get("duration_sec", 300), default=300, min_val=60, max_val=3600)

            config = SniperConfig(
                symbol=symbol, entry_fee=entry_fee,
                duration_sec=duration,
            )
            rnd = sniper.create_round(config)

            # Persist to DB
            sniper_save_round(
                rnd.round_id, symbol, entry_fee, duration,
                config.rake_pct,
                datetime.utcfromtimestamp(rnd.locks_at).isoformat(),
                datetime.utcfromtimestamp(rnd.settles_at).isoformat(),
            )

            return web.json_response(sniper.get_round_state(rnd.round_id))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _sniper_predict(self, request):
        """Submit a price prediction for a Sniper round."""
        try:
            from sniper_engine import sniper
            round_id = validate_int(request.match_info["round_id"], default=0, min_val=1, max_val=999999)
            body = await request.json()
            wallet, err = validate_wallet(body.get("wallet"))
            if err:
                return web.json_response({"error": err}, status=400)
            predicted_price = validate_float(body.get("price", 0), min_val=0.001, max_val=10_000_000.0)
            if predicted_price <= 0:
                return web.json_response({"error": "positive price required"}, status=400)

            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)

            error = sniper.submit_prediction(
                round_id, user["id"], wallet,
                user.get("username", wallet[:8]),
                predicted_price,
            )
            if error:
                return web.json_response({"error": error}, status=400)

            # Persist prediction
            sniper_save_prediction(round_id, user["id"], predicted_price)

            return web.json_response({
                "status": "submitted",
                "predicted_price": predicted_price,
                **sniper.get_round_state(round_id),
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _sniper_state(self, request):
        """Get current state of a Sniper round."""
        try:
            from sniper_engine import sniper
            round_id = int(request.match_info["round_id"])

            # Check if should auto-lock
            sniper.check_lock(round_id)

            # Check if should auto-settle
            rnd = sniper.rounds.get(round_id)
            if rnd and rnd.status.value == "locked" and time.time() > rnd.settles_at:
                # Fetch actual price and settle
                actual = await self._get_price(rnd.config.symbol, "bulk")
                if actual:
                    settled = sniper.settle(round_id, actual)
                    if settled and settled.status.value == "settled":
                        # Persist settlement
                        results = [
                            {
                                "user_id": p.user_id, "rank": p.rank,
                                "accuracy_pct": p.accuracy_pct,
                                "distance_usd": p.distance_usd,
                                "accuracy_tier": p.accuracy_tier,
                                "payout_usd": p.payout_usd,
                            }
                            for p in settled.predictions
                        ]
                        sniper_settle_round(
                            round_id, actual, settled.prize_pool_usd,
                            settled.rake_usd, results,
                        )

            state = sniper.get_round_state(round_id)
            if not state:
                # Try DB
                db_round = sniper_get_round(round_id)
                if db_round:
                    return web.json_response(db_round)
                return web.json_response({"error": "Round not found"}, status=404)

            return web.json_response(state)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _sniper_settle(self, request):
        """Manually settle a round (admin/debug)."""
        try:
            from sniper_engine import sniper
            round_id = int(request.match_info["round_id"])
            rnd = sniper.rounds.get(round_id)
            if not rnd:
                return web.json_response({"error": "Round not found"}, status=404)

            actual = await self._get_price(rnd.config.symbol, "bulk")
            if not actual:
                return web.json_response({"error": "Could not fetch price"}, status=502)

            settled = sniper.settle(round_id, actual)
            if not settled:
                return web.json_response({"error": "Settlement failed"}, status=500)

            # Persist
            results = [
                {
                    "user_id": p.user_id, "rank": p.rank,
                    "accuracy_pct": p.accuracy_pct,
                    "distance_usd": p.distance_usd,
                    "accuracy_tier": p.accuracy_tier,
                    "payout_usd": p.payout_usd,
                }
                for p in settled.predictions
            ]
            sniper_settle_round(
                round_id, actual, settled.prize_pool_usd,
                settled.rake_usd, results,
            )

            return web.json_response(sniper.get_round_state(round_id))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _sniper_active(self, request):
        """List all active Sniper rounds."""
        try:
            from sniper_engine import sniper
            # Auto-check locks
            for rid in list(sniper.rounds.keys()):
                sniper.check_lock(rid)
            return web.json_response(sniper.get_active_rounds())
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _sniper_leaderboard(self, request):
        """Sniper all-time leaderboard by total winnings."""
        try:
            return web.json_response(sniper_get_leaderboard())
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # ── Moon or Doom Game Handlers ────────────────────────

    async def _hb_game_start(self, request):
        """Start a Moon or Doom game — opens a real 50x leveraged position."""
        try:
            from game_engine import MoonOrDoomEngine, GameConfig
            body = await request.json()
            wallet, err = validate_wallet(body.get("wallet"))
            if err:
                return web.json_response({"error": err}, status=400)
            symbol = validate_symbol(body.get("symbol", "BTC-USD"))
            bet_amount = validate_float(body.get("bet_amount", 10.0), default=10.0, min_val=1.0, max_val=10000.0)
            exchange = validate_exchange(body.get("exchange", "bulk"))
            auto_cashout = validate_float(body.get("auto_cashout", 0), default=0, min_val=0, max_val=100.0)

            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)

            # Check for existing active game
            active = hb_get_active_game(user["id"])
            if active:
                return web.json_response({"error": "Already have an active game", "game_id": active["id"]}, status=409)

            # Create game in DB
            config = GameConfig(
                symbol=symbol, bet_amount=bet_amount, leverage=50.0,
                crash_threshold_pct=0.02, auto_cashout_mult=auto_cashout,
                exchange=exchange,
            )
            game_id = hb_create_game(user["id"], symbol, exchange, bet_amount, 50.0)

            # Get current price
            fill_price = await self._get_price(symbol, exchange)
            if not fill_price:
                return web.json_response({"error": "Could not fetch price"}, status=502)

            # Calculate position size: notional = bet × leverage, size = notional / price
            notional = bet_amount * 50.0
            size = round(notional / fill_price, 6)

            # Place market order via real executor
            order_id = ""
            executor = self.bulk_executor if exchange == "bulk" else self.hl_executor
            if executor:
                ex_symbol = symbol
                if exchange == "hyperliquid":
                    from config import HL_SYMBOL_MAP
                    ex_symbol = HL_SYMBOL_MAP.get(symbol, symbol)
                order = await executor.place_order(
                    symbol=ex_symbol, side="BUY", price=fill_price,
                    size=size, order_type="market",
                )
                if order:
                    order_id = order.get("order_id", "")
                    fill_price = float(order.get("price", fill_price))
                    size = float(order.get("size", size))

            # Start game engine
            engine = MoonOrDoomEngine(config)
            engine.start_game(fill_price, size, order_id)
            self.active_games[game_id] = engine

            # Update DB
            hb_start_game(game_id, fill_price, size, order_id)

            # Award first_blood if first game
            hb_award_achievement(user["id"], "first_blood")

            return web.json_response({
                "game_id": game_id,
                **engine.to_dict(),
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_game_cashout(self, request):
        """Cash out of an active Moon or Doom game."""
        try:
            game_id = int(request.match_info["game_id"])
            engine = self.active_games.get(game_id)
            if not engine:
                return web.json_response({"error": "Game not found or not active"}, status=404)

            # Get current price for final PnL
            current_price = await self._get_price(
                engine.state.config.symbol, engine.state.config.exchange
            )
            state = engine.cash_out(current_price)

            # Close the real position
            executor = self.bulk_executor if state.config.exchange == "bulk" else self.hl_executor
            if executor and state.size > 0:
                ex_symbol = state.config.symbol
                if state.config.exchange == "hyperliquid":
                    from config import HL_SYMBOL_MAP
                    ex_symbol = HL_SYMBOL_MAP.get(state.config.symbol, state.config.symbol)
                await executor.place_order(
                    symbol=ex_symbol, side="SELL", price=current_price,
                    size=state.size, order_type="market",
                )

            # Save to DB
            result = hb_end_game(
                game_id, state.exit_price, state.exit_multiplier,
                state.high_water_mark, state.pnl_usd, "cashed_out"
            )

            # Clean up
            del self.active_games[game_id]

            return web.json_response(engine.to_dict())
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_game_add(self, request):
        """Add to position in an active Moon or Doom game."""
        try:
            game_id = int(request.match_info["game_id"])
            engine = self.active_games.get(game_id)
            if not engine:
                return web.json_response({"error": "Game not found or not active"}, status=404)

            body = await request.json()
            add_amount = validate_float(body.get("amount", engine.state.config.bet_amount), default=engine.state.config.bet_amount, min_val=1.0, max_val=10000.0)

            current_price = await self._get_price(
                engine.state.config.symbol, engine.state.config.exchange
            )
            add_notional = add_amount * 50.0
            add_size = round(add_notional / current_price, 6)

            # Place additional market order
            executor = self.bulk_executor if engine.state.config.exchange == "bulk" else self.hl_executor
            if executor:
                ex_symbol = engine.state.config.symbol
                if engine.state.config.exchange == "hyperliquid":
                    from config import HL_SYMBOL_MAP
                    ex_symbol = HL_SYMBOL_MAP.get(engine.state.config.symbol, engine.state.config.symbol)
                await executor.place_order(
                    symbol=ex_symbol, side="BUY", price=current_price,
                    size=add_size, order_type="market",
                )

            engine.add_to_position(current_price, add_size)
            engine.state.config.bet_amount += add_amount

            return web.json_response(engine.to_dict())
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_game_state(self, request):
        """Get current state of an active game (polled by frontend)."""
        try:
            game_id = int(request.match_info["game_id"])
            engine = self.active_games.get(game_id)
            if not engine:
                # Check DB for finished game
                game = hb_get_game(game_id)
                if game:
                    return web.json_response(game)
                return web.json_response({"error": "Game not found"}, status=404)

            # Update with latest price
            current_price = await self._get_price(
                engine.state.config.symbol, engine.state.config.exchange
            )
            if current_price:
                engine.process_tick(current_price)

            state = engine.to_dict()

            # If game just crashed, close position and persist
            if engine.state.status.value == "crashed":
                executor = self.bulk_executor if engine.state.config.exchange == "bulk" else self.hl_executor
                if executor and engine.state.size > 0:
                    ex_symbol = engine.state.config.symbol
                    if engine.state.config.exchange == "hyperliquid":
                        from config import HL_SYMBOL_MAP
                        ex_symbol = HL_SYMBOL_MAP.get(engine.state.config.symbol, engine.state.config.symbol)
                    await executor.place_order(
                        symbol=ex_symbol, side="SELL", price=current_price,
                        size=engine.state.size, order_type="market",
                    )
                hb_end_game(
                    game_id, engine.state.exit_price, engine.state.exit_multiplier,
                    engine.state.high_water_mark, engine.state.pnl_usd, "crashed"
                )
                del self.active_games[game_id]

            state["price_history"] = engine.state.price_history[-100:]
            return web.json_response(state)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_game_history(self, request):
        """Get game history for a user."""
        try:
            wallet = request.match_info["wallet"]
            user = hb_get_user(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)
            return web.json_response(hb_get_game_history(user["id"]))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_game_leaderboard(self, request):
        """Moon or Doom leaderboard — highest multiplier cash-outs."""
        try:
            return web.json_response(hb_get_game_leaderboard())
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # ── Market Dice Handlers ─────────────────────────────────

    async def _dice_roll(self, request):
        """Roll dice. Instant mode (<70ms) by default. Pass mode=animated for 5s window."""
        from dice_engine import dice
        from db import save_dice_game, settle_dice_game, hb_get_user_by_wallet, hb_update_balance
        try:
            data = await request.json()
            wallet = data.get("wallet", "")
            symbol = validate_symbol(data.get("symbol", "BTC-USD"))
            exchange = data.get("exchange", "bulk")
            game_type = data.get("game_type", "pick")
            bet_amount = float(data.get("bet_amount", 1.0))
            player_pick = int(data.get("player_pick", 1))
            mode = data.get("mode", "instant")  # "instant" or "animated"

            user = hb_get_user_by_wallet(wallet)
            if not user:
                return web.json_response({"error": "User not found"}, status=404)
            if user["balance"] < bet_amount:
                return web.json_response({"error": "Insufficient balance"}, status=400)

            # Debit bet
            hb_update_balance(user["id"], -bet_amount)

            if mode == "instant":
                # ── INSTANT MODE: settle in this request (<70ms total) ──
                price, price_str = self.get_cached_price(symbol, exchange)
                if not price:
                    price = await self._get_price(symbol, exchange)
                    price_str = f"{price}"

                roll = dice.instant_roll(
                    user_id=user["id"], symbol=symbol, exchange=exchange,
                    game_type=game_type, bet_amount=bet_amount,
                    player_pick=player_pick,
                    price=price, price_str=price_str,
                )

                # Persist (async-safe, doesn't block response)
                db_id = save_dice_game(
                    user["id"], symbol, exchange, game_type,
                    bet_amount, player_pick, roll.commitment_hash,
                )
                settle_dice_game(
                    db_id, roll.entry_price_str, roll.settlement_price_str,
                    roll.raw_digits, roll.dice_result, roll.won,
                    roll.payout_multiplier, roll.payout_usd, roll.pnl_usd,
                )
                roll.roll_id = db_id

                # Credit payout
                if roll.won:
                    hb_update_balance(user["id"], roll.payout_usd)

                # Broadcast to spectators
                await self.reporter._ws_broadcast("dice_result", json.dumps({
                    "roll_id": db_id,
                    "dice_result": roll.dice_result,
                    "won": roll.won,
                    "payout_usd": roll.payout_usd,
                    "pnl_usd": roll.pnl_usd,
                    "game_type": game_type,
                    "bet_amount": bet_amount,
                    "player_pick": player_pick,
                }))

                return web.json_response(dice.to_dict(roll))

            else:
                # ── ANIMATED MODE: 5-second settlement window ──
                roll = dice.create_roll(
                    user_id=user["id"], symbol=symbol, exchange=exchange,
                    game_type=game_type, bet_amount=bet_amount,
                    player_pick=player_pick,
                )

                db_id = save_dice_game(
                    user["id"], symbol, exchange, game_type,
                    bet_amount, player_pick, roll.commitment_hash,
                )
                roll.roll_id = db_id
                dice.active_rolls[db_id] = dice.active_rolls.pop(roll.roll_id, roll)
                roll.roll_id = db_id

                price, price_str = self.get_cached_price(symbol, exchange)
                if not price:
                    price = await self._get_price(symbol, exchange)
                    price_str = f"{price}"
                if price:
                    dice.start_roll(db_id, price, price_str)

                return web.json_response(dice.to_dict(roll))

        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _dice_state(self, request):
        """Get roll state. Auto-settles when window expires."""
        from dice_engine import dice
        from db import settle_dice_game, hb_update_balance
        try:
            roll_id = int(request.match_info["roll_id"])
            roll = dice.active_rolls.get(roll_id)
            if not roll:
                from db import get_conn, release_conn
                conn = get_conn()
                row = conn.execute("SELECT * FROM dice_games WHERE id=?", (roll_id,)).fetchone()
                release_conn(conn)
                if row:
                    return web.json_response(dict(row))
                return web.json_response({"error": "Roll not found"}, status=404)

            price = await self._get_price(roll.symbol, roll.exchange)
            if price:
                dice.tick(roll_id, price, f"{price}")

            if roll.status in ("won", "lost"):
                settle_dice_game(
                    roll_id, roll.entry_price_str, roll.settlement_price_str,
                    roll.raw_digits, roll.dice_result, roll.won,
                    roll.payout_multiplier, roll.payout_usd, roll.pnl_usd,
                )
                if roll.won:
                    hb_update_balance(roll.user_id, roll.payout_usd)
                await self.reporter._ws_broadcast("dice_result", json.dumps({
                    "roll_id": roll_id,
                    "dice_result": roll.dice_result,
                    "won": roll.won,
                    "payout_usd": roll.payout_usd,
                    "game_type": roll.game_type,
                }))

            return web.json_response(dice.to_dict(roll))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _dice_history(self, request):
        from db import get_dice_history, hb_get_user_by_wallet
        wallet = request.match_info["wallet"]
        user = hb_get_user_by_wallet(wallet)
        if not user:
            return web.json_response([])
        return web.json_response(get_dice_history(user["id"]))

    async def _dice_stats(self, request):
        from db import get_dice_stats, hb_get_user_by_wallet
        wallet = request.match_info["wallet"]
        user = hb_get_user_by_wallet(wallet)
        if not user:
            return web.json_response({})
        return web.json_response(get_dice_stats(user["id"]))

    async def _dice_leaderboard(self, request):
        from db import get_dice_leaderboard
        return web.json_response(get_dice_leaderboard())

    async def _dice_verify(self, request):
        """Provable fairness — anyone can verify any past roll."""
        from dice_engine import DiceEngine
        try:
            roll_id = int(request.match_info["roll_id"])
            from db import get_conn, release_conn
            conn = get_conn()
            row = conn.execute("SELECT * FROM dice_games WHERE id=?", (roll_id,)).fetchone()
            release_conn(conn)
            if not row:
                return web.json_response({"error": "Roll not found"}, status=404)
            r = dict(row)
            if r["status"] not in ("won", "lost"):
                return web.json_response({"error": "Roll not yet settled"}, status=400)

            result = DiceEngine.verify(
                roll_id=r["id"],
                user_id=r["user_id"],
                created_at=0,
                settlement_price_str=r["settlement_price"],
                expected_result=r["dice_result"],
            )
            result["settlement_price"] = r["settlement_price"]
            result["player_pick"] = r["player_pick"]
            result["game_type"] = r["game_type"]
            return web.json_response(result)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # ── Analytics Handlers (MMT-style) ──────────────────────

    async def _hb_cvd(self, request):
        """Cumulative Volume Delta time series."""
        from analytics import orderflow
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        limit = validate_int(request.query.get("limit", "500"), default=500, min_val=1, max_val=2000)
        return web.json_response(orderflow.get_cvd(symbol, limit))

    async def _hb_volume_delta(self, request):
        """Volume delta per candle (buy/sell volume + delta + counts)."""
        from analytics import orderflow
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        limit = validate_int(request.query.get("limit", "100"), default=100, min_val=1, max_val=1000)
        return web.json_response(orderflow.get_volume_delta(symbol, limit))

    async def _hb_large_trades(self, request):
        """Large trades / volume bubbles (>= $5k)."""
        from analytics import orderflow
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        limit = validate_int(request.query.get("limit", "100"), default=100, min_val=1, max_val=1000)
        return web.json_response(orderflow.get_large_trades(symbol, limit))

    async def _hb_footprint(self, request):
        """Footprint chart data — volume at each price level per candle."""
        from analytics import orderflow
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        candle = request.query.get("candle", None)
        candle_ts = validate_int(candle, default=0, min_val=0, max_val=9999999999) if candle else None
        return web.json_response(orderflow.get_footprint(symbol, candle_ts))

    async def _hb_heatmap(self, request):
        """Orderbook heatmap — bid/ask depth snapshots over time."""
        from analytics import liquidity
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        limit = validate_int(request.query.get("limit", "100"), default=100, min_val=1, max_val=1000)
        return web.json_response(liquidity.get_heatmap(symbol, limit))

    async def _hb_depth(self, request):
        """Orderbook depth chart — cumulative bid/ask levels."""
        from analytics import liquidity
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        return web.json_response(liquidity.get_depth(symbol))

    async def _hb_oi(self, request):
        """Open Interest time series."""
        from analytics import derivatives
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        limit = validate_int(request.query.get("limit", "200"), default=200, min_val=1, max_val=2000)
        return web.json_response(derivatives.get_oi_series(symbol, limit))

    async def _hb_funding(self, request):
        """Funding rate comparison (Bulk vs Hyperliquid) time series."""
        from analytics import derivatives
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        limit = validate_int(request.query.get("limit", "200"), default=200, min_val=1, max_val=2000)
        return web.json_response(derivatives.get_funding_series(symbol, limit))

    async def _hb_liq_map(self, request):
        """Liquidation map — actual clusters + estimated levels."""
        from analytics import derivatives
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        clusters = derivatives.get_liq_map(symbol)
        price = await self._get_price(symbol, "bulk")
        estimated = derivatives.estimate_liq_levels(symbol, price)
        return web.json_response({
            "clusters": clusters,
            "estimated_levels": estimated,
            "current_price": price,
        })

    async def _hb_volume_profile(self, request):
        """Volume Profile — volume at price with POC."""
        from analytics import profile
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        return web.json_response(profile.get_volume_profile(symbol))

    async def _hb_tpo(self, request):
        """TPO (Time Price Opportunity) — market profile letters."""
        from analytics import profile
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        return web.json_response(profile.get_tpo(symbol))

    async def _hb_candles(self, request):
        """Fetch OHLCV candles from Bulk and/or Hyperliquid for charting."""
        symbol = validate_symbol(request.query.get("symbol", "BTC-USD"))
        exchange = validate_exchange(request.query.get("exchange", "bulk"))
        interval = validate_interval(request.query.get("interval", "15m"))
        limit = validate_int(request.query.get("limit", "100"), default=100, min_val=1, max_val=1000)

        try:
            async with aiohttp.ClientSession() as session:
                if exchange == "hyperliquid":
                    # HL candles via POST /info {type: candleSnapshot}
                    from config import HL_SYMBOL_MAP
                    hl_coin = HL_SYMBOL_MAP.get(symbol, symbol.replace("-USD", ""))
                    url = f"{HL_API_BASE}/info"
                    async with session.post(url, json={
                        "type": "candleSnapshot",
                        "coin": hl_coin,
                        "interval": interval,
                        "startTime": 0,
                    }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status != 200:
                            return web.json_response({"error": f"HL returned {resp.status}"}, status=502)
                        raw = await resp.json(content_type=None)
                        if not isinstance(raw, list):
                            return web.json_response({"error": "Invalid HL response"}, status=502)
                        # Format for lightweight-charts: {time, open, high, low, close, volume}
                        candles = []
                        for c in raw[-limit:]:
                            t = c.get("t") or c.get("T", 0)
                            # HL timestamps are in ms
                            ts = int(t) // 1000 if int(t) > 1e12 else int(t)
                            candles.append({
                                "time": ts,
                                "open": float(c.get("o", 0)),
                                "high": float(c.get("h", 0)),
                                "low": float(c.get("l", 0)),
                                "close": float(c.get("c", 0)),
                                "volume": float(c.get("v", 0)),
                            })
                        return web.json_response(candles)
                else:
                    # Bulk candles via GET /klines
                    url = f"{BULK_API_BASE}/klines"
                    params = {"symbol": symbol, "interval": interval}
                    async with session.get(url, params=params,
                                           timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status != 200:
                            return web.json_response({"error": f"Bulk returned {resp.status}"}, status=502)
                        raw = await resp.json(content_type=None)
                        data = raw if isinstance(raw, list) else raw.get("data", [])
                        candles = []
                        for c in data[-limit:]:
                            t = c.get("t") or c.get("timestamp", 0)
                            ts = int(t) // 1000 if isinstance(t, (int, float)) and t > 1e12 else int(t) if isinstance(t, (int, float)) else 0
                            candles.append({
                                "time": ts,
                                "open": float(c.get("o") or c.get("open", 0)),
                                "high": float(c.get("h") or c.get("high", 0)),
                                "low": float(c.get("l") or c.get("low", 0)),
                                "close": float(c.get("c") or c.get("close", 0)),
                                "volume": float(c.get("v") or c.get("volume", 0)),
                            })
                        return web.json_response(candles)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _hb_pnl_history(self, request):
        """Get cumulative PnL history for a user (equity curve data)."""
        wallet = request.match_info["wallet"]
        user = hb_get_user(wallet)
        if not user:
            return web.json_response({"error": "User not found"}, status=404)

        conn = get_conn()
        rows = conn.execute(
            """SELECT closed_at as time, pnl_usd, symbol, side, exchange
               FROM hb_trades
               WHERE user_id=? AND status != 'OPEN' AND closed_at IS NOT NULL
               ORDER BY closed_at ASC""",
            (user["id"],)
        ).fetchall()
        release_conn(conn)

        # Build cumulative PnL series
        cumulative = 0.0
        series = []
        trades = []
        for r in rows:
            d = dict(r)
            cumulative += (d["pnl_usd"] or 0)
            time_str = d["time"] or ""
            # Convert ISO timestamp to unix seconds
            try:
                from datetime import datetime as dt
                ts = int(dt.fromisoformat(time_str).timestamp())
            except Exception:
                ts = 0
            series.append({"time": ts, "value": round(cumulative, 2)})
            trades.append({
                "time": ts,
                "pnl": round(d["pnl_usd"] or 0, 2),
                "symbol": d["symbol"],
                "side": d["side"],
                "exchange": d["exchange"],
            })

        return web.json_response({
            "equity_curve": series,
            "trades": trades,
            "total_pnl": round(cumulative, 2),
        })

    async def _api_latency(self, request):
        minutes = validate_int(request.query.get("minutes", "60"), default=60, min_val=1, max_val=1440)
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
        release_conn(conn)
        return web.json_response([dict(r) for r in rows])

    # ── Run ────────────────────────────────────────────────────

    async def run(self):
        from agent_monitor import monitor
        monitor.inject_broadcast(self.reporter._ws_broadcast)

        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, DASHBOARD_HOST, DASHBOARD_PORT)
        await site.start()
        print(f"🌐 Dashboard running at http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
        # Push agent status to WebSocket clients every 10s
        while True:
            await asyncio.sleep(10)
            await monitor.push_update()
