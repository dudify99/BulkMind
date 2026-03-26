"""
Reporter — Multi-channel alert system
Broadcasts to: Telegram, Discord, Web Dashboard (WebSocket)
Falls back to console if no channels are configured
"""

import asyncio
import aiohttp
import json
from datetime import datetime
from config import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    DISCORD_WEBHOOK_URL,
)


class Reporter:
    def __init__(self):
        self.telegram_enabled = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
        self.discord_enabled = bool(DISCORD_WEBHOOK_URL)
        self.ws_clients: set = set()  # WebSocket connections from dashboard

        channels = []
        if self.telegram_enabled:
            channels.append("Telegram")
        if self.discord_enabled:
            channels.append("Discord")
        channels.append("Dashboard (WebSocket)")

        if not self.telegram_enabled and not self.discord_enabled:
            print("⚠️ No external channels configured — logging to console + dashboard only")
            print("   Set TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID and/or DISCORD_WEBHOOK_URL")
        else:
            print(f"📡 Reporter channels: {', '.join(channels)}")

    # ── Public API ─────────────────────────────────────────────

    async def send(self, message: str):
        """Send a message to all configured channels"""
        print(f"\n📢 {message}\n")
        await asyncio.gather(
            self._telegram_send(message),
            self._discord_send(message),
            self._ws_broadcast("message", message),
            return_exceptions=True,
        )

    async def alert(self, message: str):
        """High-priority alert to all channels"""
        alert_msg = f"🚨 ALERT\n{message}"
        print(f"\n📢 {alert_msg}\n")
        await asyncio.gather(
            self._telegram_send(alert_msg),
            self._discord_send(alert_msg, is_alert=True),
            self._ws_broadcast("alert", alert_msg),
            return_exceptions=True,
        )

    # ── WebSocket (Dashboard) ──────────────────────────────────

    def register_ws(self, ws):
        self.ws_clients.add(ws)

    def unregister_ws(self, ws):
        self.ws_clients.discard(ws)

    async def _ws_broadcast(self, msg_type: str, content: str):
        if not self.ws_clients:
            return
        payload = json.dumps({
            "type": msg_type,
            "content": content,
            "ts": datetime.utcnow().isoformat(),
        })
        stale = set()
        for ws in self.ws_clients:
            try:
                await ws.send_str(payload)
            except Exception:
                stale.add(ws)
        self.ws_clients -= stale

    # ── Telegram ───────────────────────────────────────────────

    async def _telegram_send(self, text: str, retries: int = 3):
        if not self.telegram_enabled:
            return
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       text[:4096],
            "parse_mode": "Markdown"
        }
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.post(url, json=payload,
                                         timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            return
                        data = await resp.json()
                        print(f"Telegram error: {data}")
            except Exception as e:
                print(f"Telegram send failed (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)

    # ── Discord ────────────────────────────────────────────────

    async def _discord_send(self, text: str, is_alert: bool = False, retries: int = 3):
        if not self.discord_enabled:
            return

        # Convert Markdown bold from Telegram format (*text*) to Discord (**text**)
        discord_text = text.replace("*", "**")

        # Discord embeds for alerts, plain content for regular messages
        if is_alert:
            payload = {
                "embeds": [{
                    "title": "🚨 BulkMind Alert",
                    "description": discord_text[:4096],
                    "color": 0xFF0000,
                    "timestamp": datetime.utcnow().isoformat(),
                }]
            }
        else:
            payload = {"content": discord_text[:2000]}

        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.post(
                        DISCORD_WEBHOOK_URL,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status in (200, 204):
                            return
                        data = await resp.text()
                        print(f"Discord error: {data}")
            except Exception as e:
                print(f"Discord send failed (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
