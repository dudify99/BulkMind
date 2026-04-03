"""
HyperBulk Analytics Engine
Processes trade stream data into order flow, liquidity, derivatives, and profile metrics.
Feeds the pro trading dashboard (MMT-style visualizations).
"""

import json
import math
from datetime import datetime, timedelta
from collections import defaultdict
from db import get_conn
from config import BULK_API_BASE, HL_API_BASE, HL_SYMBOL_MAP


# ══════════════════════════════════════════════════════════════
#  ORDER FLOW — CVD, Volume Delta, Footprint, Trade Count
# ══════════════════════════════════════════════════════════════

class OrderFlowEngine:
    """In-memory order flow processor fed by WebSocket trade stream."""

    def __init__(self):
        # CVD series: {symbol: [(ts, cvd_value), ...]}
        self.cvd: dict[str, list] = defaultdict(list)
        # Volume delta per candle: {symbol: {candle_ts: {buy_vol, sell_vol, delta}}}
        self.candle_deltas: dict[str, dict] = defaultdict(lambda: defaultdict(lambda: {
            "buy_vol": 0.0, "sell_vol": 0.0, "delta": 0.0,
            "buy_count": 0, "sell_count": 0,
        }))
        # Large trades (volume bubbles): {symbol: [(ts, price, size, side, value_usd)]}
        self.large_trades: dict[str, list] = defaultdict(list)
        # Footprint: {symbol: {candle_ts: {price_level: {buy_vol, sell_vol}}}}
        self.footprint: dict[str, dict] = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
            "buy_vol": 0.0, "sell_vol": 0.0,
        })))
        # Running CVD value per symbol
        self._cvd_running: dict[str, float] = defaultdict(float)

    def process_trade(self, trade: dict):
        """Process a single trade from the WebSocket stream."""
        symbol = trade.get("symbol", "")
        side = trade.get("side", "").lower()
        price = float(trade.get("price", 0))
        size = float(trade.get("size", 0))
        value_usd = float(trade.get("value_usd", 0)) or (price * size)
        ts = trade.get("ts", datetime.utcnow().isoformat())

        if not symbol or price == 0:
            return

        # Parse timestamp to unix seconds
        try:
            if isinstance(ts, str):
                unix_ts = int(datetime.fromisoformat(ts.replace("Z", "")).timestamp())
            else:
                unix_ts = int(ts)
        except Exception:
            unix_ts = int(datetime.utcnow().timestamp())

        # Candle bucket (15 min)
        candle_ts = unix_ts - (unix_ts % 900)

        is_buy = side in ("buy", "b")
        vol_signed = size if is_buy else -size

        # 1. CVD
        self._cvd_running[symbol] += vol_signed
        self.cvd[symbol].append((unix_ts, round(self._cvd_running[symbol], 6)))
        # Trim to last 2000 points
        if len(self.cvd[symbol]) > 2000:
            self.cvd[symbol] = self.cvd[symbol][-2000:]

        # 2. Candle volume delta
        cd = self.candle_deltas[symbol][candle_ts]
        if is_buy:
            cd["buy_vol"] += size
            cd["buy_count"] += 1
        else:
            cd["sell_vol"] += size
            cd["sell_count"] += 1
        cd["delta"] = cd["buy_vol"] - cd["sell_vol"]

        # 3. Volume bubbles (large trades >= $5000)
        if value_usd >= 5000:
            self.large_trades[symbol].append({
                "time": unix_ts,
                "price": price,
                "size": size,
                "side": side,
                "value_usd": round(value_usd, 2),
            })
            if len(self.large_trades[symbol]) > 500:
                self.large_trades[symbol] = self.large_trades[symbol][-500:]

        # 4. Footprint (price bucketed)
        tick = _price_bucket(price, symbol)
        fp = self.footprint[symbol][candle_ts][tick]
        if is_buy:
            fp["buy_vol"] += size
        else:
            fp["sell_vol"] += size

    def get_cvd(self, symbol: str, limit: int = 500) -> list:
        """Return CVD time series: [{time, value}]"""
        data = self.cvd.get(symbol, [])[-limit:]
        return [{"time": t, "value": v} for t, v in data]

    def get_volume_delta(self, symbol: str, limit: int = 100) -> list:
        """Return volume delta per candle: [{time, buy_vol, sell_vol, delta, buy_count, sell_count}]"""
        candles = self.candle_deltas.get(symbol, {})
        sorted_ts = sorted(candles.keys())[-limit:]
        return [{"time": t, **candles[t]} for t in sorted_ts]

    def get_large_trades(self, symbol: str, limit: int = 100) -> list:
        """Return volume bubbles (large trades)."""
        return self.large_trades.get(symbol, [])[-limit:]

    def get_footprint(self, symbol: str, candle_ts: int = None) -> dict:
        """Return footprint data for a candle. If no candle_ts, return latest."""
        fp = self.footprint.get(symbol, {})
        if not fp:
            return {}
        if candle_ts is None:
            candle_ts = max(fp.keys())
        levels = fp.get(candle_ts, {})
        return {
            "time": candle_ts,
            "levels": {str(k): v for k, v in sorted(levels.items())},
        }


# ══════════════════════════════════════════════════════════════
#  LIQUIDITY — Orderbook Heatmap, Depth Chart
# ══════════════════════════════════════════════════════════════

class LiquidityEngine:
    """Tracks orderbook snapshots over time for heatmap visualization."""

    def __init__(self):
        # Heatmap: {symbol: [(ts, bids, asks)]} — snapshots over time
        self.snapshots: dict[str, list] = defaultdict(list)

    def record_snapshot(self, symbol: str, bids: list, asks: list):
        """Record an orderbook snapshot. bids/asks = [(price, size), ...]"""
        ts = int(datetime.utcnow().timestamp())
        self.snapshots[symbol].append({
            "time": ts,
            "bids": [(float(p), float(s)) for p, s in bids[:30]],
            "asks": [(float(p), float(s)) for p, s in asks[:30]],
        })
        # Keep last 200 snapshots (~3h at 1/min)
        if len(self.snapshots[symbol]) > 200:
            self.snapshots[symbol] = self.snapshots[symbol][-200:]

    def get_heatmap(self, symbol: str, limit: int = 100) -> list:
        """Return heatmap data: [{time, levels: [{price, size, side}]}]"""
        snaps = self.snapshots.get(symbol, [])[-limit:]
        result = []
        for snap in snaps:
            levels = []
            for price, size in snap["bids"]:
                levels.append({"price": price, "size": size, "side": "bid"})
            for price, size in snap["asks"]:
                levels.append({"price": price, "size": size, "side": "ask"})
            result.append({"time": snap["time"], "levels": levels})
        return result

    def get_depth(self, symbol: str) -> dict:
        """Return latest depth chart data: {bids: [{price, cumulative}], asks: [{price, cumulative}]}"""
        snaps = self.snapshots.get(symbol, [])
        if not snaps:
            return {"bids": [], "asks": []}
        latest = snaps[-1]

        # Cumulative bids (descending price)
        cum = 0.0
        bids = []
        for price, size in sorted(latest["bids"], key=lambda x: -x[0]):
            cum += size
            bids.append({"price": price, "cumulative": round(cum, 4)})

        # Cumulative asks (ascending price)
        cum = 0.0
        asks = []
        for price, size in sorted(latest["asks"], key=lambda x: x[0]):
            cum += size
            asks.append({"price": price, "cumulative": round(cum, 4)})

        return {"bids": bids, "asks": asks}


# ══════════════════════════════════════════════════════════════
#  DERIVATIVES — Open Interest, Liquidation Map, Funding Rate
# ══════════════════════════════════════════════════════════════

class DerivativesEngine:
    """Tracks OI, funding rates, and builds liquidation cluster maps."""

    def __init__(self):
        # OI time series: {symbol: [(ts, oi_value)]}
        self.oi_series: dict[str, list] = defaultdict(list)
        # Funding rate history: {symbol: [(ts, bulk_rate, hl_rate)]}
        self.funding_series: dict[str, list] = defaultdict(list)
        # Liquidation clusters: {symbol: {price_level: total_value_usd}}
        self.liq_clusters: dict[str, dict] = defaultdict(lambda: defaultdict(float))

    def record_oi(self, symbol: str, oi: float):
        ts = int(datetime.utcnow().timestamp())
        self.oi_series[symbol].append((ts, oi))
        if len(self.oi_series[symbol]) > 500:
            self.oi_series[symbol] = self.oi_series[symbol][-500:]

    def record_funding(self, symbol: str, bulk_rate: float, hl_rate: float):
        ts = int(datetime.utcnow().timestamp())
        self.funding_series[symbol].append((ts, bulk_rate, hl_rate))
        if len(self.funding_series[symbol]) > 500:
            self.funding_series[symbol] = self.funding_series[symbol][-500:]

    def record_liquidation(self, symbol: str, price: float, value_usd: float):
        """Record a liquidation event for the cluster map."""
        bucket = _price_bucket(price, symbol)
        self.liq_clusters[symbol][bucket] += value_usd

    def get_oi_series(self, symbol: str, limit: int = 200) -> list:
        data = self.oi_series.get(symbol, [])[-limit:]
        return [{"time": t, "value": v} for t, v in data]

    def get_funding_series(self, symbol: str, limit: int = 200) -> list:
        data = self.funding_series.get(symbol, [])[-limit:]
        return [{"time": t, "bulk": b, "hl": h} for t, b, h in data]

    def get_liq_map(self, symbol: str) -> list:
        """Return liquidation cluster map: [{price, value_usd}] sorted by price."""
        clusters = self.liq_clusters.get(symbol, {})
        return [
            {"price": float(p), "value_usd": round(v, 2)}
            for p, v in sorted(clusters.items())
            if v > 0
        ]

    def estimate_liq_levels(self, symbol: str, current_price: float,
                            oi: float = 0) -> list:
        """Estimate where liquidation clusters might form based on common leverages."""
        if not current_price:
            return []
        levels = []
        # Common leverage levels: 2x, 3x, 5x, 10x, 20x, 50x, 100x
        for lev in [2, 3, 5, 10, 20, 50, 100]:
            # Long liquidation = entry * (1 - 1/lev)
            long_liq = round(current_price * (1 - 1 / lev), 2)
            # Short liquidation = entry * (1 + 1/lev)
            short_liq = round(current_price * (1 + 1 / lev), 2)
            levels.append({
                "leverage": lev,
                "long_liq_price": long_liq,
                "short_liq_price": short_liq,
                "distance_pct": round(100 / lev, 2),
            })
        return levels


# ══════════════════════════════════════════════════════════════
#  PROFILE — Volume Profile, TPO
# ══════════════════════════════════════════════════════════════

class ProfileEngine:
    """Builds volume profile and TPO (Time Price Opportunity) from trade data."""

    def __init__(self):
        # Volume at price: {symbol: {price_bucket: {buy_vol, sell_vol, total}}}
        self.volume_profile: dict[str, dict] = defaultdict(lambda: defaultdict(lambda: {
            "buy_vol": 0.0, "sell_vol": 0.0, "total": 0.0,
        }))
        # TPO: {symbol: {price_bucket: set of candle_ts}}
        self.tpo: dict[str, dict] = defaultdict(lambda: defaultdict(set))

    def process_trade(self, trade: dict):
        """Add a trade to the volume profile and TPO."""
        symbol = trade.get("symbol", "")
        price = float(trade.get("price", 0))
        size = float(trade.get("size", 0))
        side = trade.get("side", "").lower()

        if not symbol or price == 0:
            return

        bucket = _price_bucket(price, symbol)

        # Volume profile
        vp = self.volume_profile[symbol][bucket]
        if side in ("buy", "b"):
            vp["buy_vol"] += size
        else:
            vp["sell_vol"] += size
        vp["total"] += size

        # TPO — each 30-min period the price visits
        ts = int(datetime.utcnow().timestamp())
        tpo_period = ts - (ts % 1800)  # 30-min buckets
        self.tpo[symbol][bucket].add(tpo_period)

    def get_volume_profile(self, symbol: str) -> list:
        """Return volume profile: [{price, buy_vol, sell_vol, total}] sorted by price."""
        vp = self.volume_profile.get(symbol, {})
        result = [
            {"price": float(p), **v}
            for p, v in sorted(vp.items())
            if v["total"] > 0
        ]
        # Find POC (Point of Control) — price level with highest volume
        if result:
            poc = max(result, key=lambda x: x["total"])
            poc["is_poc"] = True
        return result

    def get_tpo(self, symbol: str) -> list:
        """Return TPO data: [{price, periods: int, letters: str}]"""
        tpo = self.tpo.get(symbol, {})
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        # Build period → letter mapping
        all_periods = set()
        for periods in tpo.values():
            all_periods.update(periods)
        sorted_periods = sorted(all_periods)
        period_letter = {p: letters[i % len(letters)] for i, p in enumerate(sorted_periods)}

        result = []
        for price_bucket in sorted(tpo.keys()):
            periods = tpo[price_bucket]
            tpo_letters = "".join(period_letter.get(p, ".") for p in sorted(periods))
            result.append({
                "price": float(price_bucket),
                "periods": len(periods),
                "letters": tpo_letters,
            })
        return result


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════

def _price_bucket(price: float, symbol: str) -> float:
    """Round price to a meaningful bucket size based on the asset."""
    if "BTC" in symbol:
        step = 50.0      # $50 buckets for BTC
    elif "ETH" in symbol:
        step = 5.0        # $5 buckets for ETH
    else:
        step = 0.5        # $0.50 buckets for SOL etc.
    return round(math.floor(price / step) * step, 2)


# ══════════════════════════════════════════════════════════════
#  GLOBAL INSTANCES (shared across the application)
# ══════════════════════════════════════════════════════════════

orderflow = OrderFlowEngine()
liquidity = LiquidityEngine()
derivatives = DerivativesEngine()
profile = ProfileEngine()
