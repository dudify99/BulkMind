"""
Bulk Executor
Wraps bulk-keychain Python SDK + Bulk REST API
Handles: data fetching, order signing, submission, tracking
"""

import asyncio
import aiohttp
import time
from datetime import datetime
from typing import Optional, Dict, List
from config import BULK_API_BASE, BULK_PRIVATE_KEY, BREAKOUT_PAPER_MODE
from db import log_latency, log_issue


class BulkClient:
    """
    Async HTTP client for Bulk exchange API
    Tracks latency on every call automatically
    """

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get(self, path: str, params: dict = None) -> Optional[dict]:
        url   = BULK_API_BASE + path
        start = time.perf_counter()
        try:
            async with self.session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                elapsed = (time.perf_counter() - start) * 1000
                log_latency(path, elapsed, resp.status)
                if resp.status == 200:
                    return await resp.json(content_type=None)
                else:
                    text = await resp.text()
                    log_issue("MEDIUM", "API_ERROR",
                              f"Non-200 on {path}: {resp.status}", text)
                    return None
        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            log_latency(path, elapsed, error="TIMEOUT")
            log_issue("HIGH", "LATENCY", f"Timeout on {path}", "")
            return None
        except Exception as e:
            log_issue("HIGH", "API_ERROR", f"Error on {path}", str(e))
            return None

    async def post(self, path: str, payload: dict) -> Optional[dict]:
        url   = BULK_API_BASE + path
        start = time.perf_counter()
        try:
            async with self.session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                elapsed = (time.perf_counter() - start) * 1000
                log_latency(path, elapsed, resp.status)
                data = await resp.json(content_type=None)
                if resp.status not in (200, 201):
                    log_issue("HIGH", "ORDER_REJECT",
                              f"Order rejected on {path}: {resp.status}",
                              str(data))
                return data
        except Exception as e:
            log_issue("HIGH", "API_ERROR", f"POST error on {path}", str(e))
            return None

    # ── Market Data ──────────────────────────────────────────

    async def get_candles(self, symbol: str, interval: str = "15m",
                          limit: int = 100) -> List[dict]:
        """Fetch OHLCV candles"""
        data = await self.get(f"/candles/{symbol}",
                              params={"interval": interval, "limit": limit})
        if not data:
            return []

        candles = []
        for c in (data if isinstance(data, list) else data.get("data", [])):
            candles.append({
                "ts":     c.get("t") or c.get("timestamp"),
                "open":   float(c.get("o") or c.get("open", 0)),
                "high":   float(c.get("h") or c.get("high", 0)),
                "low":    float(c.get("l") or c.get("low", 0)),
                "close":  float(c.get("c") or c.get("close", 0)),
                "volume": float(c.get("v") or c.get("volume", 0)),
            })
        return candles

    async def get_ticker(self, symbol: str) -> Optional[dict]:
        return await self.get(f"/ticker/{symbol}")

    async def get_orderbook(self, symbol: str) -> Optional[dict]:
        return await self.get(f"/orderbook/{symbol}")

    async def get_funding_rates(self) -> Optional[dict]:
        return await self.get("/funding-rates")

    async def get_trades(self, symbol: str, limit: int = 50) -> List[dict]:
        data = await self.get(f"/trades/{symbol}", params={"limit": limit})
        return data if isinstance(data, list) else (data or {}).get("data", [])


class BulkExecutor:
    """
    Order execution layer
    In PAPER MODE: simulates orders, logs to DB
    In LIVE MODE: signs with bulk-keychain, submits to API
    """

    def __init__(self, client: BulkClient, paper: bool = True):
        self.client = client
        self.paper  = paper
        self._signer = None

        if not paper:
            self._init_signer()

    def _init_signer(self):
        """Initialize bulk-keychain signer for live trading"""
        try:
            from bulk_keychain import Keypair, Signer
            if not BULK_PRIVATE_KEY:
                raise ValueError("BULK_PRIVATE_KEY env var not set")
            keypair      = Keypair.from_base58(BULK_PRIVATE_KEY)
            self._signer = Signer(keypair)
            self._signer.set_compute_batch_order_ids(True)
            print("✅ bulk-keychain signer initialized")
        except ImportError:
            log_issue("CRITICAL", "SYSTEM",
                      "bulk-keychain not installed",
                      "Run: pip install bulk-keychain")
            raise

    async def place_order(self, symbol: str, side: str,
                          price: float, size: float,
                          order_type: str = "limit",
                          tif: str = "GTC") -> Optional[dict]:
        """Place a single order"""
        if self.paper:
            return self._paper_order(symbol, side, price, size, order_type)

        order = {
            "type":      "order",
            "symbol":    symbol,
            "is_buy":    side == "BUY",
            "price":     price,
            "size":      size,
            "order_type": {"type": order_type, "tif": tif}
        }
        signed = self._signer.sign(order)
        return await self.client.post("/order", {
            "actions":   signed["actions"],
            "nonce":     signed["nonce"],
            "account":   signed["account"],
            "signer":    signed["signer"],
            "signature": signed["signature"],
        })

    async def place_bracket(self, symbol: str, side: str,
                            entry_price: float, size: float,
                            sl_price: float, tp_price: float) -> Optional[dict]:
        """
        Atomic bracket order: entry + SL + TP in ONE transaction
        Uses bulk-keychain signGroup — all or nothing
        """
        is_buy   = side == "BUY"
        sl_side  = not is_buy
        tp_side  = not is_buy

        entry_order = {
            "type":      "order",
            "symbol":    symbol,
            "is_buy":    is_buy,
            "price":     entry_price,
            "size":      size,
            "order_type": {"type": "limit", "tif": "GTC"}
        }
        sl_order = {
            "type":       "order",
            "symbol":     symbol,
            "is_buy":     sl_side,
            "price":      sl_price,
            "size":       size,
            "order_type": {"type": "market", "is_market": True, "trigger_px": sl_price}
        }
        tp_order = {
            "type":       "order",
            "symbol":     symbol,
            "is_buy":     tp_side,
            "price":      tp_price,
            "size":       size,
            "order_type": {"type": "limit", "tif": "GTC"}
        }

        if self.paper:
            return self._paper_bracket(symbol, side, entry_price, sl_price, tp_price, size)

        signed = self._signer.sign_group([entry_order, sl_order, tp_order])
        result = await self.client.post("/order", {
            "actions":   signed["actions"],
            "nonce":     signed["nonce"],
            "account":   signed["account"],
            "signer":    signed["signer"],
            "signature": signed["signature"],
        })
        return result

    async def cancel_order(self, symbol: str, order_id: str) -> Optional[dict]:
        if self.paper:
            return {"status": "cancelled", "order_id": order_id}

        cancel = {"type": "cancel", "symbol": symbol, "order_id": order_id}
        signed = self._signer.sign(cancel)
        return await self.client.post("/order", {
            "actions":   signed["actions"],
            "nonce":     signed["nonce"],
            "account":   signed["account"],
            "signer":    signed["signer"],
            "signature": signed["signature"],
        })

    # ── Paper Trading Simulation ──────────────────────────────

    def _paper_order(self, symbol: str, side: str, price: float,
                     size: float, order_type: str) -> dict:
        fake_id = f"PAPER_{int(time.time()*1000)}"
        print(f"📝 [PAPER] {side} {size} {symbol} @ {price} ({order_type})")
        return {
            "status":   "filled",
            "order_id": fake_id,
            "price":    price,
            "size":     size,
            "paper":    True
        }

    def _paper_bracket(self, symbol: str, side: str,
                        entry: float, sl: float, tp: float, size: float) -> dict:
        fake_id = f"PAPER_BRACKET_{int(time.time()*1000)}"
        print(f"📝 [PAPER BRACKET] {side} {size} {symbol}")
        print(f"   Entry: {entry} | SL: {sl} | TP: {tp}")
        return {
            "status":   "filled",
            "order_id": fake_id,
            "entry":    entry,
            "sl":       sl,
            "tp":       tp,
            "paper":    True,
            "order_ids": [fake_id + "_entry", fake_id + "_sl", fake_id + "_tp"]
        }
