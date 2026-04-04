"""
Bulk Executor
Wraps bulk-keychain Python SDK + Bulk REST API
Handles: data fetching, order signing, submission, tracking

Order action types (matching Bulk Swift SDK):
  "l"    = limit order
  "m"    = market order
  "cx"   = cancel single order
  "cxa"  = cancel all orders
  "faucet" = request testnet funds
"""

import asyncio
import aiohttp
import math
import random
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
    # Endpoints per docs.bulk.trade OpenAPI spec

    async def get_candles(self, symbol: str, interval: str = "15m",
                          limit: int = None, start_time: int = None,
                          end_time: int = None) -> List[dict]:
        """Fetch OHLCV candles via GET /klines.
        Note: limit param kept for backward compat but /klines doesn't support it.
        Use startTime/endTime to control range."""
        params = {"symbol": symbol, "interval": interval}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        data = await self.get("/klines", params=params)
        if not data:
            return []

        raw = data if isinstance(data, list) else data.get("data", [])
        candles = []
        for c in raw:
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
        """GET /ticker/{symbol} — price, volume, OI, funding, mark/oracle"""
        return await self.get(f"/ticker/{symbol}")

    async def get_orderbook(self, symbol: str, nlevels: int = 20,
                            aggregation: float = None) -> Optional[dict]:
        """GET /l2book — order book snapshot"""
        params = {"type": "l2book", "coin": symbol}
        if nlevels:
            params["nlevels"] = nlevels
        if aggregation:
            params["aggregation"] = aggregation
        return await self.get("/l2book", params=params)

    async def get_exchange_info(self) -> Optional[dict]:
        """GET /exchangeInfo — returns list of {symbol, lotSize, tickSize, ...}"""
        return await self.get("/exchangeInfo")

    async def get_metrics(self) -> Optional[dict]:
        """GET /metrics — runtime performance metrics"""
        return await self.get("/metrics")

    async def get_stats(self, period: str = "1d",
                        symbol: str = None) -> Optional[dict]:
        """GET /stats — exchange-wide volume, OI, funding rates"""
        params = {"period": period}
        if symbol:
            params["symbol"] = symbol
        return await self.get("/stats", params=params)

    # ── Account Data (unsigned, public) ────────────────────

    async def get_account(self, user_pubkey: str,
                          query_type: str = "fullAccount") -> Optional[dict]:
        """POST /account — query any wallet's positions, orders, fills, PnL.
        No signature required. query_type: fullAccount|openOrders|fills|positions|fundingHistory|orderHistory"""
        data = await self.post("/account", {
            "type": query_type,
            "user": user_pubkey,
        })
        return data

    async def get_fills(self, user_pubkey: str) -> List[dict]:
        """Get recent fills for a wallet"""
        data = await self.get_account(user_pubkey, "fills")
        if not data or not isinstance(data, list):
            return []
        results = []
        for item in data:
            if "fills" in item:
                fills = item["fills"]
                if isinstance(fills, list):
                    results.extend(fills)
                else:
                    results.append(fills)
        return results

    async def get_positions(self, user_pubkey: str) -> List[dict]:
        """Get closed positions for a wallet"""
        data = await self.get_account(user_pubkey, "positions")
        if not data or not isinstance(data, list):
            return []
        results = []
        for item in data:
            if "positions" in item:
                pos = item["positions"]
                if isinstance(pos, list):
                    results.extend(pos)
                else:
                    results.append(pos)
        return results


class BulkExecutor:
    """
    Order execution layer
    In PAPER MODE: simulates orders, logs to DB
    In LIVE MODE: signs with bulk-keychain, submits to API

    Action type codes (Bulk Swift SDK conventions):
      "l"      = limit order
      "m"      = market order
      "cx"     = cancel single order
      "cxa"    = cancel all orders
      "faucet" = request testnet funds
    """

    def __init__(self, client: BulkClient, paper: bool = True):
        self.client = client
        self.paper  = paper
        self._signer = None
        self._exchange_info: Dict[str, dict] = {}  # symbol -> {lotSize, tickSize, ...}

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
            print("[executor] bulk-keychain signer initialized")
        except ImportError:
            log_issue("CRITICAL", "SYSTEM",
                      "bulk-keychain not installed",
                      "Run: pip install bulk-keychain")
            raise

    # ── Exchange Info & Lot Size ─────────────────────────────

    async def _load_exchange_info(self):
        """Fetch and cache exchange info (lot sizes, tick sizes per symbol)"""
        data = await self.client.get_exchange_info()
        if not data:
            return
        # Handle both list and dict-with-list formats
        instruments = data if isinstance(data, list) else data.get("data", data.get("instruments", []))
        if isinstance(instruments, list):
            for inst in instruments:
                sym = inst.get("symbol") or inst.get("name", "")
                if sym:
                    self._exchange_info[sym] = inst
        print(f"[executor] cached exchange info for {len(self._exchange_info)} symbols")

    def lot_size(self, symbol: str) -> float:
        """Return lot size for a symbol from cached exchange info.
        Falls back to 0.001 if not cached."""
        info = self._exchange_info.get(symbol)
        if info:
            return float(info.get("lotSize", info.get("lot_size", 0.001)))
        return 0.001

    @staticmethod
    def _round_to_lot(size: float, lot_size: float) -> float:
        """Round size DOWN to the nearest lot size increment."""
        if lot_size <= 0:
            return size
        # Use Decimal-safe floor: floor(size / lot_size) * lot_size
        steps = math.floor(size / lot_size)
        return round(steps * lot_size, 10)  # round to avoid float artifacts

    # ── Signing & Submission Helpers ─────────────────────────

    def _sign_and_build_payload(self, action) -> dict:
        """Sign a single action and return the POST /order payload."""
        signed = self._signer.sign(action)
        return {
            "actions":   signed["actions"],
            "nonce":     signed["nonce"],
            "account":   signed["account"],
            "signer":    signed["signer"],
            "signature": signed["signature"],
        }

    def _sign_group_and_build_payload(self, actions: list) -> dict:
        """Sign a group of actions atomically and return the POST /order payload."""
        signed = self._signer.sign_group(actions)
        return {
            "actions":   signed["actions"],
            "nonce":     signed["nonce"],
            "account":   signed["account"],
            "signer":    signed["signer"],
            "signature": signed["signature"],
        }

    # ── Order Methods ────────────────────────────────────────

    async def place_order(self, symbol: str, side: str,
                          price: float, size: float,
                          order_type: str = "limit",
                          tif: str = "GTC") -> Optional[dict]:
        """Place a single order (limit or market).

        Args:
            symbol: e.g. "BTC-USD"
            side: "BUY" or "SELL"
            price: limit price (ignored for market orders)
            size: order size in base asset
            order_type: "limit" or "market"
            tif: time-in-force for limit orders ("GTC", "IOC", "FOK")
        """
        # Round size to lot
        ls = self.lot_size(symbol)
        size = self._round_to_lot(size, ls)
        if size <= 0:
            return None

        if self.paper:
            return self._paper_order(symbol, side, price, size, order_type)

        is_buy = side.upper() == "BUY"

        if order_type == "market":
            action = {
                "type":        "m",
                "symbol":      symbol,
                "is_buy":      is_buy,
                "sz":          size,
                "reduce_only": False,
            }
        else:
            # Limit order
            action = {
                "type":       "l",
                "symbol":     symbol,
                "is_buy":     is_buy,
                "px":         price,
                "sz":         size,
                "order_type": {"tif": tif},
            }

        payload = self._sign_and_build_payload(action)
        return await self.client.post("/order", payload)

    async def place_market_order(self, symbol: str, side: str,
                                 size: float,
                                 reduce_only: bool = False) -> Optional[dict]:
        """Place a market order — the primary order type for the game.

        Uses action type "m" per Swift SDK conventions.

        Args:
            symbol: e.g. "BTC-USD"
            side: "BUY" or "SELL"
            size: order size in base asset
            reduce_only: if True, only reduces existing position
        """
        # Round size to lot
        ls = self.lot_size(symbol)
        size = self._round_to_lot(size, ls)
        if size <= 0:
            return None

        if self.paper:
            return self._paper_market_order(symbol, side, size, reduce_only)

        is_buy = side.upper() == "BUY"
        action = {
            "type":        "m",
            "symbol":      symbol,
            "is_buy":      is_buy,
            "sz":          size,
            "reduce_only": reduce_only,
        }

        payload = self._sign_and_build_payload(action)
        return await self.client.post("/order", payload)

    async def place_bracket(self, symbol: str, side: str,
                            entry_price: float, size: float,
                            sl_price: float, tp_price: float) -> Optional[dict]:
        """
        Atomic bracket order: entry + SL + TP in ONE transaction
        Uses bulk-keychain signGroup — all or nothing
        """
        # Round size to lot
        ls = self.lot_size(symbol)
        size = self._round_to_lot(size, ls)
        if size <= 0:
            return None

        is_buy  = side.upper() == "BUY"
        sl_side = not is_buy
        tp_side = not is_buy

        entry_order = {
            "type":       "l",
            "symbol":     symbol,
            "is_buy":     is_buy,
            "px":         entry_price,
            "sz":         size,
            "order_type": {"tif": "GTC"},
        }
        sl_order = {
            "type":        "m",
            "symbol":      symbol,
            "is_buy":      sl_side,
            "sz":          size,
            "reduce_only": True,
            "trigger_px":  sl_price,
        }
        tp_order = {
            "type":       "l",
            "symbol":     symbol,
            "is_buy":     tp_side,
            "px":         tp_price,
            "sz":         size,
            "order_type": {"tif": "GTC"},
            "reduce_only": True,
        }

        if self.paper:
            return self._paper_bracket(symbol, side, entry_price, sl_price, tp_price, size)

        payload = self._sign_group_and_build_payload([entry_order, sl_order, tp_order])
        return await self.client.post("/order", payload)

    async def cancel_order(self, symbol: str, order_id: str) -> Optional[dict]:
        """Cancel a single order by ID. Uses action type "cx"."""
        if self.paper:
            return {"status": "cancelled", "order_id": order_id}

        action = {"type": "cx", "symbol": symbol, "oid": order_id}
        payload = self._sign_and_build_payload(action)
        return await self.client.post("/order", payload)

    async def cancel_all(self, symbol: str = None) -> Optional[dict]:
        """Cancel all open orders. Optional symbol filter. Uses action type "cxa"."""
        if self.paper:
            return {"status": "cancelled_all", "symbol": symbol}

        action = {"type": "cxa"}
        if symbol:
            action["symbol"] = symbol
        payload = self._sign_and_build_payload(action)
        return await self.client.post("/order", payload)

    async def faucet(self) -> Optional[dict]:
        """Request testnet funds. Uses action type "faucet"."""
        if self.paper:
            print("[executor] [PAPER] faucet request (simulated)")
            return {"status": "ok", "paper": True}

        action = {"type": "faucet"}
        payload = self._sign_and_build_payload(action)
        return await self.client.post("/order", payload)

    async def close_position_with_retry(self, symbol: str, size: float,
                                        side: str = "SELL",
                                        max_attempts: int = 5) -> Optional[dict]:
        """Close a position with exponential backoff retry.

        Follows the TradingService pattern from the Swift SDK.

        Args:
            symbol: e.g. "BTC-USD"
            size: position size to close
            side: "SELL" to close a long, "BUY" to close a short
            max_attempts: max retry attempts (default 5)
        """
        delay = 1.0
        last_error = None
        for attempt in range(max_attempts):
            try:
                result = await self.place_market_order(
                    symbol, side, size, reduce_only=True
                )
                if result is not None:
                    return result
                # None result means API error, treat as retryable
                raise RuntimeError(f"place_market_order returned None for {symbol}")
            except Exception as e:
                last_error = e
                if attempt == max_attempts - 1:
                    log_issue("HIGH", "ORDER_REJECT",
                              f"Failed to close {symbol} after {max_attempts} attempts",
                              str(e))
                    raise
                print(f"[executor] close retry {attempt+1}/{max_attempts} "
                      f"for {symbol}: {e} — retrying in {delay:.1f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 8.0)
        return None  # unreachable but keeps type checker happy

    # ── Paper Trading Simulation ──────────────────────────────

    def _paper_order(self, symbol: str, side: str, price: float,
                     size: float, order_type: str) -> dict:
        fake_id = f"PAPER_{int(time.time()*1000)}"
        print(f"[PAPER] {side} {size} {symbol} @ {price} ({order_type})")
        return {
            "status":   "filled",
            "order_id": fake_id,
            "price":    price,
            "size":     size,
            "paper":    True
        }

    def _paper_market_order(self, symbol: str, side: str,
                            size: float, reduce_only: bool) -> dict:
        """Simulate a market order fill with realistic slippage (0.01-0.05%)."""
        fake_id = f"PAPER_MKT_{int(time.time()*1000)}"
        # Simulate slippage: 0.01% to 0.05% adverse
        slippage_pct = random.uniform(0.0001, 0.0005)
        # We don't have a real price here, so we report slippage_bps for the caller
        # to apply against their reference price
        slippage_bps = round(slippage_pct * 10000, 2)
        ro_tag = " [REDUCE_ONLY]" if reduce_only else ""
        print(f"[PAPER] MARKET {side} {size} {symbol}{ro_tag} "
              f"(simulated slippage: {slippage_bps:.1f} bps)")
        return {
            "status":       "filled",
            "order_id":     fake_id,
            "size":         size,
            "slippage_bps": slippage_bps,
            "reduce_only":  reduce_only,
            "paper":        True,
        }

    def _paper_bracket(self, symbol: str, side: str,
                        entry: float, sl: float, tp: float, size: float) -> dict:
        fake_id = f"PAPER_BRACKET_{int(time.time()*1000)}"
        print(f"[PAPER BRACKET] {side} {size} {symbol}")
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
