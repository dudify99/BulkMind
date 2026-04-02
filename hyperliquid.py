"""
Hyperliquid Client + Executor
Wraps Hyperliquid REST API for market data and order execution.
Paper mode supported — same interface as BulkExecutor.
"""

import asyncio
import time
import aiohttp
from datetime import datetime
from typing import Optional, Dict, List
from config import HL_API_BASE, HL_PRIVATE_KEY
from db import log_latency, log_issue


class HyperliquidClient:
    """
    Async HTTP client for Hyperliquid API.
    Uses POST /info for all market data queries.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _info_post(self, payload: dict) -> Optional[dict]:
        """POST to /info endpoint — all Hyperliquid market data goes here."""
        url   = HL_API_BASE + "/info"
        start = time.perf_counter()
        try:
            async with self.session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                elapsed = (time.perf_counter() - start) * 1000
                log_latency(f"hl:/info/{payload.get('type','')}", elapsed, resp.status)
                if resp.status == 200:
                    return await resp.json(content_type=None)
                else:
                    text = await resp.text()
                    log_issue("MEDIUM", "API_ERROR",
                              f"HL non-200: {resp.status}", text[:200])
                    return None
        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            log_latency(f"hl:/info/{payload.get('type','')}", elapsed, error="TIMEOUT")
            return None
        except Exception as e:
            log_issue("HIGH", "API_ERROR", "HL info request failed", str(e))
            return None

    # ── Market Data ──────────────────────────────────────────

    async def get_candles(self, symbol: str, interval: str = "15m",
                          limit: int = 20, **kwargs) -> List[dict]:
        """Fetch OHLCV candles via POST /info {type: candleSnapshot}."""
        # Map interval string to Hyperliquid format
        data = await self._info_post({
            "type":     "candleSnapshot",
            "coin":     symbol,
            "interval": interval,
            "startTime": 0,
        })
        if not data or not isinstance(data, list):
            return []

        candles = []
        for c in data[-limit:]:
            candles.append({
                "ts":     c.get("t") or c.get("T"),
                "open":   float(c.get("o", 0)),
                "high":   float(c.get("h", 0)),
                "low":    float(c.get("l", 0)),
                "close":  float(c.get("c", 0)),
                "volume": float(c.get("v", 0)),
            })
        return candles

    async def get_ticker(self, symbol: str) -> Optional[dict]:
        """Get current mid price from allMids endpoint."""
        data = await self._info_post({"type": "allMids"})
        if not data:
            return None

        # allMids returns {"BTC": "12345.6", "ETH": "3456.7", ...}
        if isinstance(data, dict):
            price_str = data.get(symbol)
            if price_str:
                return {"lastPrice": price_str, "price": price_str}
        return None

    async def get_orderbook(self, symbol: str, nlevels: int = 20,
                            **kwargs) -> Optional[dict]:
        """GET l2 book snapshot."""
        return await self._info_post({
            "type":     "l2Book",
            "coin":     symbol,
            "nSigFigs": 5,
        })

    async def get_meta(self) -> Optional[dict]:
        """Get exchange metadata (all perpetual assets and their specs)."""
        return await self._info_post({"type": "meta"})

    async def get_user_state(self, address: str) -> Optional[dict]:
        """Get user's positions, account value, margin."""
        return await self._info_post({
            "type": "clearinghouseState",
            "user": address,
        })


class HyperliquidExecutor:
    """
    Order execution for Hyperliquid.
    Paper mode: simulates orders locally.
    Live mode: signs and submits to /exchange endpoint.
    """

    def __init__(self, client: HyperliquidClient, paper: bool = True):
        self.client = client
        self.paper  = paper
        self._signer = None

        if not paper:
            self._init_signer()

    def _init_signer(self):
        """Initialize Hyperliquid signer for live trading."""
        try:
            from eth_account import Account
            if not HL_PRIVATE_KEY:
                raise ValueError("HL_PRIVATE_KEY env var not set")
            self._account = Account.from_key(HL_PRIVATE_KEY)
            print(f"✅ Hyperliquid signer initialized: {self._account.address}")
        except ImportError:
            log_issue("CRITICAL", "SYSTEM",
                      "eth_account not installed",
                      "Run: pip install eth-account")
            raise

    async def place_order(self, symbol: str, side: str,
                          price: float, size: float,
                          order_type: str = "limit",
                          tif: str = "GTC") -> Optional[dict]:
        """Place a single order on Hyperliquid."""
        if self.paper:
            return self._paper_order(symbol, side, price, size, order_type)

        # Live order submission via POST /exchange
        # Requires EIP-712 signing — full implementation for live mode
        order_payload = {
            "type": "order",
            "orders": [{
                "a": self._coin_to_asset_id(symbol),
                "b": side == "BUY",
                "p": str(price),
                "s": str(size),
                "r": False,
                "t": {"limit": {"tif": "Gtc" if tif == "GTC" else tif}},
            }],
            "grouping": "na",
        }
        return await self._submit_action(order_payload)

    async def place_bracket(self, symbol: str, side: str,
                            entry_price: float, size: float,
                            sl_price: float, tp_price: float) -> Optional[dict]:
        """
        Place a bracket order on Hyperliquid.
        Entry + SL trigger + TP limit.
        """
        if self.paper:
            return self._paper_bracket(symbol, side, entry_price, sl_price, tp_price, size)

        # Hyperliquid supports tp/sl via order grouping
        is_buy = side == "BUY"
        asset_id = self._coin_to_asset_id(symbol)

        order_payload = {
            "type": "order",
            "orders": [
                {
                    "a": asset_id,
                    "b": is_buy,
                    "p": str(entry_price),
                    "s": str(size),
                    "r": False,
                    "t": {"limit": {"tif": "Gtc"}},
                },
            ],
            "grouping": "na",
        }
        result = await self._submit_action(order_payload)
        if not result:
            return None

        # Place SL and TP as separate trigger orders
        sl_payload = {
            "type": "order",
            "orders": [{
                "a": asset_id,
                "b": not is_buy,
                "p": str(sl_price),
                "s": str(size),
                "r": True,
                "t": {"trigger": {
                    "triggerPx": str(sl_price),
                    "isMarket": True,
                    "tpsl": "sl",
                }},
            }],
            "grouping": "na",
        }
        await self._submit_action(sl_payload)

        tp_payload = {
            "type": "order",
            "orders": [{
                "a": asset_id,
                "b": not is_buy,
                "p": str(tp_price),
                "s": str(size),
                "r": True,
                "t": {"trigger": {
                    "triggerPx": str(tp_price),
                    "isMarket": False,
                    "tpsl": "tp",
                }},
            }],
            "grouping": "na",
        }
        await self._submit_action(tp_payload)

        return result

    async def cancel_order(self, symbol: str, order_id: str) -> Optional[dict]:
        if self.paper:
            return {"status": "cancelled", "order_id": order_id}

        payload = {
            "type": "cancel",
            "cancels": [{
                "a": self._coin_to_asset_id(symbol),
                "o": int(order_id),
            }],
        }
        return await self._submit_action(payload)

    async def _submit_action(self, action: dict) -> Optional[dict]:
        """Sign and submit an action to POST /exchange."""
        # EIP-712 signing for Hyperliquid
        # This requires constructing the typed data and signing with eth_account
        try:
            import json as _json
            nonce = int(time.time() * 1000)

            # Hyperliquid exchange endpoint
            url = HL_API_BASE + "/exchange"
            payload = {
                "action":    action,
                "nonce":     nonce,
                "signature": self._sign_action(action, nonce),
                "vaultAddress": None,
            }
            async with self.client.session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json(content_type=None)
                if resp.status == 200:
                    return data
                log_issue("HIGH", "ORDER_REJECT",
                          f"HL order rejected: {resp.status}", str(data)[:200])
                return None
        except Exception as e:
            log_issue("HIGH", "API_ERROR", "HL exchange submit failed", str(e))
            return None

    def _sign_action(self, action: dict, nonce: int) -> dict:
        """EIP-712 sign an action for Hyperliquid."""
        import json as _json
        from eth_account.messages import encode_typed_data

        # Hyperliquid uses a specific EIP-712 domain
        domain = {
            "name":              "Exchange",
            "version":           "1",
            "chainId":           42161,  # Arbitrum
            "verifyingContract": "0x0000000000000000000000000000000000000000",
        }
        msg_types = {
            "Agent": [
                {"name": "source", "type": "string"},
                {"name": "connectionId", "type": "bytes32"},
            ]
        }
        # Build connection ID from action hash
        action_str = _json.dumps(action, sort_keys=True, separators=(",", ":"))
        import hashlib
        connection_id = hashlib.sha256(
            (action_str + str(nonce)).encode()
        ).digest()

        message = {
            "source":       "a",
            "connectionId": connection_id,
        }

        signable = encode_typed_data(domain, msg_types, message)
        signed = self._account.sign_message(signable)
        return {
            "r": hex(signed.r),
            "s": hex(signed.s),
            "v": signed.v,
        }

    def _coin_to_asset_id(self, symbol: str) -> int:
        """Map coin symbol to Hyperliquid asset index.
        This is a simplified mapping — production should fetch from /info meta."""
        from config import HL_ASSET_IDS
        return HL_ASSET_IDS.get(symbol, 0)

    # ── Paper Trading Simulation ─────────────────────────────

    def _paper_order(self, symbol: str, side: str, price: float,
                     size: float, order_type: str) -> dict:
        fake_id = f"HL_PAPER_{int(time.time()*1000)}"
        print(f"📝 [HL PAPER] {side} {size} {symbol} @ {price} ({order_type})")
        return {
            "status":   "filled",
            "order_id": fake_id,
            "price":    price,
            "size":     size,
            "paper":    True,
        }

    def _paper_bracket(self, symbol: str, side: str,
                       entry: float, sl: float, tp: float, size: float) -> dict:
        fake_id = f"HL_PAPER_BRACKET_{int(time.time()*1000)}"
        print(f"📝 [HL PAPER BRACKET] {side} {size} {symbol}")
        print(f"   Entry: {entry} | SL: {sl} | TP: {tp}")
        return {
            "status":   "filled",
            "order_id": fake_id,
            "entry":    entry,
            "sl":       sl,
            "tp":       tp,
            "paper":    True,
            "order_ids": [fake_id + "_entry", fake_id + "_sl", fake_id + "_tp"],
        }
