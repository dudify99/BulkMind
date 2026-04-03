"""
HLStream — Hyperliquid Live Trade Stream Consumer
Connects to Hyperliquid WebSocket, consumes trades, broadcasts to dashboard.
Mirrors BulkStream but for Hyperliquid exchange.
"""

import asyncio
import aiohttp
import json
from datetime import datetime
from db import log_issue
from config import WATCHED_SYMBOLS, HL_SYMBOL_MAP
from reporter import Reporter

HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
HL_RECONNECT_SEC = 5
HL_PING_INTERVAL_SEC = 50


class HLStream:
    def __init__(self, reporter: Reporter):
        self.reporter = reporter

    async def run(self):
        """Connect to Hyperliquid WebSocket, subscribe to trade feeds,
        broadcast trades to HyperBulk dashboard in real-time."""
        print("📡 HLStream started — Hyperliquid live trade feed")

        # Map our symbols to HL coin names (BTC-USD → BTC)
        hl_coins = [
            HL_SYMBOL_MAP.get(s, s.replace("-USD", ""))
            for s in WATCHED_SYMBOLS
        ]

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        HL_WS_URL,
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as ws:
                        print(f"📡 HLStream connected to {HL_WS_URL}")

                        # Subscribe to trades for each coin
                        for coin in hl_coins:
                            await ws.send_json({
                                "method": "subscribe",
                                "subscription": {
                                    "type": "trades",
                                    "coin": coin,
                                },
                            })
                        print(f"📡 HLStream subscribed: {hl_coins}")

                        # Start ping task to keep connection alive
                        ping_task = asyncio.create_task(
                            self._ping_loop(ws)
                        )

                        try:
                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    try:
                                        data = json.loads(msg.data)
                                        await self._process_message(data)
                                    except json.JSONDecodeError:
                                        continue
                                elif msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.ERROR,
                                ):
                                    break
                        finally:
                            ping_task.cancel()

            except Exception as e:
                print(f"📡 HLStream error: {e}")
                log_issue("HIGH", "SYSTEM",
                          "HLStream disconnected", str(e))

            print(f"📡 HLStream reconnecting in {HL_RECONNECT_SEC}s...")
            await asyncio.sleep(HL_RECONNECT_SEC)

    async def _ping_loop(self, ws):
        """Send periodic pings to keep the HL WebSocket alive."""
        try:
            while True:
                await asyncio.sleep(HL_PING_INTERVAL_SEC)
                await ws.send_json({"method": "ping"})
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _process_message(self, data: dict):
        """Process a Hyperliquid WebSocket message — extract and broadcast trades."""
        # Skip pong responses
        if data.get("channel") == "pong" or data.get("method") == "pong":
            return

        # Trade messages: {"channel": "trades", "data": [{...}, ...]}
        if data.get("channel") != "trades":
            return

        trades = data.get("data", [])
        if not isinstance(trades, list):
            trades = [trades]

        # Reverse lookup: HL coin → internal symbol
        hl_reverse = {v: k for k, v in HL_SYMBOL_MAP.items()}

        for trade in trades:
            coin = trade.get("coin", "")
            # Map HL side: "B" = buy, "A" = sell (ask)
            raw_side = trade.get("side", "")
            if raw_side in ("B", "Buy", "buy"):
                side = "buy"
            elif raw_side in ("A", "Sell", "sell"):
                side = "sell"
            else:
                side = raw_side.lower() if raw_side else "buy"

            price = float(trade.get("px", 0))
            size = float(trade.get("sz", 0))

            if not coin or price == 0:
                continue

            # Resolve symbol
            symbol = hl_reverse.get(coin, f"{coin}-USD")
            value_usd = round(price * size, 2)

            # Broadcast to all dashboard WebSocket clients
            await self.reporter.broadcast_trade({
                "symbol": symbol,
                "side": side,
                "price": price,
                "size": size,
                "value_usd": value_usd,
                "exchange": "hyperliquid",
                "reason": "normal",
                "ts": datetime.utcnow().isoformat(),
            })
