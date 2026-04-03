"""
BulkStream — Live Trade Stream Consumer
Connects to Bulk WebSocket, discovers wallets, tracks liquidations
"""

import asyncio
import aiohttp
import json
from datetime import datetime
from db import (
    log_issue, log_observed_trade, log_liquidation,
    upsert_discovered_wallet
)
from config import (
    BULK_WS_URL, WATCH_WS_RECONNECT_SEC,
    LIQUIDATION_ALERT_THRESHOLD_USD, WATCHED_SYMBOLS
)
from reporter import Reporter


class BulkStream:
    def __init__(self, reporter: Reporter):
        self.reporter = reporter

    # ── WebSocket Trade Feed ──────────────────────────────────

    async def run(self):
        """Connect to Bulk WebSocket, subscribe to trade feeds,
        discover wallets and track liquidations from real trade data."""
        print("📡 BulkStream started — live trade feed")
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        BULK_WS_URL,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as ws:
                        print("📡 BulkStream connected")

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
                                    await self._process_message(data)
                                except json.JSONDecodeError:
                                    continue
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break

            except Exception as e:
                print(f"📡 BulkStream error: {e}")
                log_issue("HIGH", "SYSTEM",
                          "BulkStream disconnected", str(e))

            print(f"📡 BulkStream reconnecting in {WATCH_WS_RECONNECT_SEC}s...")
            await asyncio.sleep(WATCH_WS_RECONNECT_SEC)

    # ── Message Processing ────────────────────────────────────

    async def _process_message(self, data: dict):
        """Process a WebSocket message — extract trades, detect liquidations."""
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

            if not symbol or price == 0:
                continue

            value_usd = price * size

            # Store the trade
            log_observed_trade(
                symbol=symbol, side=side, price=price, size=size,
                maker=maker, taker=taker, reason=reason,
                raw_data=json.dumps(trade)
            )

            # Broadcast to all WebSocket clients (drives HyperBulk globe + feed)
            await self.reporter.broadcast_trade({
                "symbol": symbol,
                "side": side,
                "price": price,
                "size": size,
                "value_usd": round(value_usd, 2),
                "exchange": "bulk",
                "reason": reason,
                "ts": datetime.utcnow().isoformat(),
            })

            # Discover wallets
            if maker:
                upsert_discovered_wallet(maker)
            if taker:
                upsert_discovered_wallet(taker)

            # Track liquidations
            if reason in ("liquidation", "adl"):
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
