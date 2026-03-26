"""
BulkMind Dashboard — FastAPI + WebSocket real-time dashboard
Serves REST API + live WebSocket feed + static frontend
"""

import asyncio
import json
from datetime import datetime
from aiohttp import web
from db import get_conn, get_open_trades, get_agent_stats
from reporter import Reporter
from config import DASHBOARD_HOST, DASHBOARD_PORT, BREAKOUT_PAPER_MODE
from pathlib import Path


STATIC_DIR = Path(__file__).parent / "static"


class Dashboard:
    def __init__(self, reporter: Reporter):
        self.reporter = reporter
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
