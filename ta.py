"""
Technical Analysis Library
ATR, EMA, Volume analysis, Breakout detection, Price Action (SMC)
"""

import statistics
from typing import List, Dict, Optional, Tuple


def ema(values: List[float], period: int) -> List[float]:
    """Exponential Moving Average"""
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


def sma(values: List[float], period: int) -> List[float]:
    """Simple Moving Average"""
    return [
        sum(values[i:i+period]) / period
        for i in range(len(values) - period + 1)
    ]


def atr(candles: List[Dict], period: int = 14) -> List[float]:
    """Average True Range"""
    trs = []
    for i in range(1, len(candles)):
        high  = candles[i]["high"]
        low   = candles[i]["low"]
        prev_close = candles[i-1]["close"]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low  - prev_close)
        )
        trs.append(tr)

    if len(trs) < period:
        return []

    # Wilder smoothing
    result = [sum(trs[:period]) / period]
    for tr in trs[period:]:
        result.append((result[-1] * (period - 1) + tr) / period)
    return result


def volume_sma(candles: List[Dict], period: int = 20) -> List[float]:
    vols = [c["volume"] for c in candles]
    return sma(vols, period)


def donchian_channel(candles: List[Dict], period: int = 20) -> Dict:
    """Donchian Channel — classic breakout range"""
    if len(candles) < period:
        return {}
    window = candles[-period:]
    return {
        "upper": max(c["high"]  for c in window),
        "lower": min(c["low"]   for c in window),
        "mid":   (max(c["high"] for c in window) +
                  min(c["low"]  for c in window)) / 2
    }


def detect_breakout(candles: List[Dict],
                    lookback: int = 20,
                    volume_mult: float = 1.5) -> Optional[Dict]:
    """
    Breakout detection logic:
    - Price closes ABOVE the highest high of last N candles (bullish)
    - Price closes BELOW the lowest low of last N candles (bearish)
    - Volume must be volume_mult * avg volume (confirmation)
    - Returns signal dict or None
    """
    if len(candles) < lookback + 2:
        return None

    # Use candles BEFORE the last one for range (don't include current)
    range_candles = candles[-(lookback+1):-1]
    current       = candles[-1]

    range_high = max(c["high"]  for c in range_candles)
    range_low  = min(c["low"]   for c in range_candles)
    range_size = range_high - range_low

    avg_volume = statistics.mean(c["volume"] for c in range_candles)
    cur_volume = current["volume"]

    volume_confirmed = cur_volume >= avg_volume * volume_mult
    close = current["close"]

    if close > range_high and volume_confirmed:
        return {
            "direction":   "BUY",
            "close":       close,
            "range_high":  range_high,
            "range_low":   range_low,
            "range_size":  range_size,
            "volume_ratio": round(cur_volume / avg_volume, 2),
            "breakout_pct": round((close - range_high) / range_high * 100, 4),
        }

    if close < range_low and volume_confirmed:
        return {
            "direction":   "SELL",
            "close":       close,
            "range_high":  range_high,
            "range_low":   range_low,
            "range_size":  range_size,
            "volume_ratio": round(cur_volume / avg_volume, 2),
            "breakout_pct": round((range_low - close) / range_low * 100, 4),
        }

    return None


def compute_sl_tp(direction: str, entry: float,
                  atr_value: float,
                  atr_mult: float = 1.0,
                  rr_ratio: float = 2.0) -> Dict:
    """Compute Stop Loss and Take Profit from ATR"""
    risk = atr_value * atr_mult

    if direction == "BUY":
        sl = entry - risk
        tp = entry + risk * rr_ratio
    else:
        sl = entry + risk
        tp = entry - risk * rr_ratio

    return {
        "sl":      round(sl, 6),
        "tp":      round(tp, 6),
        "risk_usd_per_unit": round(risk, 6),
        "rr_ratio": rr_ratio
    }


def position_size(max_usd: float, entry: float, sl: float) -> float:
    """Risk-based position sizing"""
    risk_per_unit = abs(entry - sl)
    if risk_per_unit <= 0:
        return 0
    size = max_usd / risk_per_unit
    return round(size, 6)


def is_trending(candles: List[Dict], ema_period: int = 50) -> Optional[str]:
    """Returns 'UP', 'DOWN', or None if no clear trend"""
    closes = [c["close"] for c in candles]
    emas   = ema(closes, ema_period)
    if len(emas) < 3:
        return None
    if emas[-1] > emas[-2] > emas[-3]:
        return "UP"
    if emas[-1] < emas[-2] < emas[-3]:
        return "DOWN"
    return None


def higher_timeframe_bias(candles_4h: List[Dict]) -> Optional[str]:
    """4h EMA trend as HTF filter"""
    return is_trending(candles_4h, ema_period=21)


# ══════════════════════════════════════════════════════════════
#  Extended Indicators for Signal Engine
# ══════════════════════════════════════════════════════════════

def rsi(candles: List[Dict], period: int = 14) -> List[float]:
    """Relative Strength Index (Wilder smoothing)."""
    closes = [c["close"] for c in candles]
    if len(closes) < period + 1:
        return []
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(0, d) for d in deltas]
    losses = [max(0, -d) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    result = []
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 0 else 100
        result.append(round(100 - 100 / (1 + rs), 2))
    return result


def macd(candles: List[Dict], fast: int = 12, slow: int = 26,
         signal_period: int = 9) -> Dict:
    """MACD line, signal line, and histogram."""
    closes = [c["close"] for c in candles]
    fast_ema = ema(closes, fast)
    slow_ema = ema(closes, slow)
    if not fast_ema or not slow_ema:
        return {}
    # Align lengths
    diff = len(fast_ema) - len(slow_ema)
    macd_line = [fast_ema[diff + i] - slow_ema[i] for i in range(len(slow_ema))]
    signal_line = ema(macd_line, signal_period)
    if not signal_line:
        return {}
    # Align for histogram
    d = len(macd_line) - len(signal_line)
    histogram = [macd_line[d + i] - signal_line[i] for i in range(len(signal_line))]
    return {
        "macd": macd_line,
        "signal": signal_line,
        "histogram": histogram,
    }


def fibonacci_levels(candles: List[Dict], lookback: int = 50) -> Dict:
    """Fibonacci retracement levels from recent swing high/low."""
    if len(candles) < lookback:
        return {}
    window = candles[-lookback:]
    high = max(c["high"] for c in window)
    low = min(c["low"] for c in window)
    diff = high - low
    if diff <= 0:
        return {}
    return {
        "high": high,
        "low": low,
        "level_0": high,                          # 0%
        "level_236": round(high - diff * 0.236, 2),  # 23.6%
        "level_382": round(high - diff * 0.382, 2),  # 38.2%
        "level_500": round(high - diff * 0.500, 2),  # 50%
        "level_618": round(high - diff * 0.618, 2),  # 61.8% (golden ratio)
        "level_786": round(high - diff * 0.786, 2),  # 78.6%
        "level_1000": low,                         # 100%
    }


def death_cross(candles: List[Dict], fast: int = 50, slow: int = 200) -> Optional[Dict]:
    """Detect Death Cross (fast EMA crosses below slow EMA) or Golden Cross (above)."""
    closes = [c["close"] for c in candles]
    fast_ema = ema(closes, fast)
    slow_ema = ema(closes, slow)
    if len(fast_ema) < 2 or len(slow_ema) < 2:
        return None
    # Align
    d = len(fast_ema) - len(slow_ema)
    if d < 1:
        return None
    prev_fast = fast_ema[d - 1 + len(slow_ema) - 2]
    prev_slow = slow_ema[-2]
    curr_fast = fast_ema[-1]
    curr_slow = slow_ema[-1]

    if prev_fast >= prev_slow and curr_fast < curr_slow:
        return {"type": "death_cross", "direction": "SELL",
                "fast_ema": round(curr_fast, 2), "slow_ema": round(curr_slow, 2)}
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        return {"type": "golden_cross", "direction": "BUY",
                "fast_ema": round(curr_fast, 2), "slow_ema": round(curr_slow, 2)}
    return None


def ema_crossover(candles: List[Dict], fast: int = 9, slow: int = 21) -> Optional[Dict]:
    """Detect EMA crossover (fast crosses slow)."""
    closes = [c["close"] for c in candles]
    fast_ema = ema(closes, fast)
    slow_ema = ema(closes, slow)
    if len(fast_ema) < 2 or len(slow_ema) < 2:
        return None
    d = len(fast_ema) - len(slow_ema)
    if d < 1:
        return None
    pf = fast_ema[-2]
    ps = slow_ema[-2]
    cf = fast_ema[-1]
    cs = slow_ema[-1]
    if pf <= ps and cf > cs:
        return {"direction": "BUY", "fast": round(cf, 2), "slow": round(cs, 2),
                "strength": round(abs(cf - cs) / cs * 100, 4)}
    if pf >= ps and cf < cs:
        return {"direction": "SELL", "fast": round(cf, 2), "slow": round(cs, 2),
                "strength": round(abs(cf - cs) / cs * 100, 4)}
    return None


def detect_pattern(candles: List[Dict]) -> Optional[Dict]:
    """Detect simple candlestick patterns from last 3 candles."""
    if len(candles) < 3:
        return None
    c0, c1, c2 = candles[-3], candles[-2], candles[-1]
    body0 = c0["close"] - c0["open"]
    body1 = c1["close"] - c1["open"]
    body2 = c2["close"] - c2["open"]
    range2 = c2["high"] - c2["low"]

    # Bullish engulfing
    if body1 < 0 and body2 > 0 and abs(body2) > abs(body1) * 1.5:
        return {"pattern": "bullish_engulfing", "direction": "BUY",
                "confidence": min(90, 60 + abs(body2 / body1) * 10)}

    # Bearish engulfing
    if body1 > 0 and body2 < 0 and abs(body2) > abs(body1) * 1.5:
        return {"pattern": "bearish_engulfing", "direction": "SELL",
                "confidence": min(90, 60 + abs(body2 / body1) * 10)}

    # Hammer (small body, long lower wick, bullish)
    if range2 > 0:
        lower_wick = min(c2["open"], c2["close"]) - c2["low"]
        upper_wick = c2["high"] - max(c2["open"], c2["close"])
        body_size = abs(body2)
        if lower_wick > body_size * 2 and upper_wick < body_size * 0.5:
            return {"pattern": "hammer", "direction": "BUY", "confidence": 65}

    # Shooting star (small body, long upper wick, bearish)
    if range2 > 0:
        lower_wick = min(c2["open"], c2["close"]) - c2["low"]
        upper_wick = c2["high"] - max(c2["open"], c2["close"])
        body_size = abs(body2)
        if upper_wick > body_size * 2 and lower_wick < body_size * 0.5:
            return {"pattern": "shooting_star", "direction": "SELL", "confidence": 65}

    # Three white soldiers (3 consecutive bullish candles with higher closes)
    if body0 > 0 and body1 > 0 and body2 > 0 and c2["close"] > c1["close"] > c0["close"]:
        return {"pattern": "three_white_soldiers", "direction": "BUY", "confidence": 75}

    # Three black crows
    if body0 < 0 and body1 < 0 and body2 < 0 and c2["close"] < c1["close"] < c0["close"]:
        return {"pattern": "three_black_crows", "direction": "SELL", "confidence": 75}

    # Doji (tiny body relative to range)
    if range2 > 0 and abs(body2) / range2 < 0.1:
        return {"pattern": "doji", "direction": "NEUTRAL", "confidence": 50}

    return None


def vwap(candles: List[Dict]) -> List[float]:
    """Volume-Weighted Average Price."""
    result = []
    cum_vol_price = 0.0
    cum_vol = 0.0
    for c in candles:
        typical = (c["high"] + c["low"] + c["close"]) / 3
        cum_vol_price += typical * c["volume"]
        cum_vol += c["volume"]
        result.append(round(cum_vol_price / cum_vol, 2) if cum_vol > 0 else 0)
    return result


def bollinger_bands(candles: List[Dict], period: int = 20,
                    num_std: float = 2.0) -> Dict:
    """Bollinger Bands — middle, upper, lower."""
    closes = [c["close"] for c in candles]
    if len(closes) < period:
        return {}
    mid = sma(closes, period)
    if not mid:
        return {}
    upper = []
    lower = []
    for i in range(len(mid)):
        window = closes[i:i + period]
        std = statistics.stdev(window) if len(window) >= 2 else 0
        upper.append(round(mid[i] + num_std * std, 2))
        lower.append(round(mid[i] - num_std * std, 2))
    return {"middle": mid, "upper": upper, "lower": lower}


# ══════════════════════════════════════════════════════════════
#  Price Action / Smart Money Concepts (SMC) for Sniper Strategies
# ══════════════════════════════════════════════════════════════

def detect_swing_points(candles: List[Dict], lookback: int = 5) -> Dict:
    """Detect swing highs and swing lows using N-bar lookback.
    A swing high = high is higher than N bars on each side.
    Returns: {swing_highs: [(index, price)], swing_lows: [(index, price)]}
    """
    highs = []
    lows = []
    if len(candles) < lookback * 2 + 1:
        return {"swing_highs": highs, "swing_lows": lows}

    for i in range(lookback, len(candles) - lookback):
        h = candles[i]["high"]
        l = candles[i]["low"]
        is_sh = all(h >= candles[i - j]["high"] and h >= candles[i + j]["high"]
                     for j in range(1, lookback + 1))
        is_sl = all(l <= candles[i - j]["low"] and l <= candles[i + j]["low"]
                     for j in range(1, lookback + 1))
        if is_sh:
            highs.append((i, h))
        if is_sl:
            lows.append((i, l))

    return {"swing_highs": highs, "swing_lows": lows}


def detect_order_blocks(candles: List[Dict], lookback: int = 20) -> List[Dict]:
    """Detect bullish and bearish order blocks (institutional entry zones).

    Bullish OB: last bearish candle before a strong bullish impulse move.
    Bearish OB: last bullish candle before a strong bearish impulse move.
    Returns list of: {type, direction, ob_high, ob_low, index, strength}
    """
    if len(candles) < lookback:
        return []

    blocks = []
    window = candles[-lookback:]
    avg_body = sum(abs(c["close"] - c["open"]) for c in window) / len(window)
    if avg_body <= 0:
        return []

    impulse_threshold = avg_body * 2.5  # Impulse = 2.5x average body

    for i in range(1, len(window) - 1):
        prev = window[i - 1]
        curr = window[i]
        nxt = window[i + 1]

        prev_body = prev["close"] - prev["open"]
        nxt_body = nxt["close"] - nxt["open"]

        # Bullish OB: prev candle is bearish, next candle is strong bullish impulse
        if prev_body < 0 and nxt_body > impulse_threshold:
            blocks.append({
                "type": "bullish_ob",
                "direction": "BUY",
                "ob_high": prev["high"],
                "ob_low": prev["low"],
                "index": len(candles) - lookback + i - 1,
                "strength": round(nxt_body / avg_body, 1),
            })

        # Bearish OB: prev candle is bullish, next candle is strong bearish impulse
        if prev_body > 0 and nxt_body < -impulse_threshold:
            blocks.append({
                "type": "bearish_ob",
                "direction": "SELL",
                "ob_high": prev["high"],
                "ob_low": prev["low"],
                "index": len(candles) - lookback + i - 1,
                "strength": round(abs(nxt_body) / avg_body, 1),
            })

    return blocks


def detect_fvg(candles: List[Dict]) -> List[Dict]:
    """Detect Fair Value Gaps (imbalances between 3 consecutive candles).

    Bullish FVG: candle 3's low > candle 1's high (gap up, unfilled).
    Bearish FVG: candle 1's low > candle 3's high (gap down, unfilled).
    Returns list of: {type, direction, gap_high, gap_low, gap_size, index}
    """
    if len(candles) < 3:
        return []

    gaps = []
    for i in range(2, len(candles)):
        c1 = candles[i - 2]
        c3 = candles[i]

        # Bullish FVG: gap between candle 1 high and candle 3 low
        if c3["low"] > c1["high"]:
            gap_size = c3["low"] - c1["high"]
            gaps.append({
                "type": "bullish_fvg",
                "direction": "BUY",
                "gap_high": c3["low"],
                "gap_low": c1["high"],
                "gap_size": gap_size,
                "index": i,
            })

        # Bearish FVG: gap between candle 1 low and candle 3 high
        if c1["low"] > c3["high"]:
            gap_size = c1["low"] - c3["high"]
            gaps.append({
                "type": "bearish_fvg",
                "direction": "SELL",
                "gap_high": c1["low"],
                "gap_low": c3["high"],
                "gap_size": gap_size,
                "index": i,
            })

    return gaps


def detect_bos(candles: List[Dict], lookback: int = 20) -> Optional[Dict]:
    """Detect Break of Structure — price breaking a recent swing point.

    Bullish BOS: price breaks above a recent swing high (trend continuation up).
    Bearish BOS: price breaks below a recent swing low (trend continuation down).
    Returns: {type, direction, broken_level, close, strength} or None
    """
    swings = detect_swing_points(candles[:-1], lookback=3)
    if not swings["swing_highs"] and not swings["swing_lows"]:
        return None

    current = candles[-1]
    close = current["close"]

    # Check bullish BOS (break above most recent swing high)
    if swings["swing_highs"]:
        last_sh = swings["swing_highs"][-1]
        sh_price = last_sh[1]
        if close > sh_price:
            return {
                "type": "bullish_bos",
                "direction": "BUY",
                "broken_level": sh_price,
                "close": close,
                "strength": round((close - sh_price) / sh_price * 100, 4),
            }

    # Check bearish BOS (break below most recent swing low)
    if swings["swing_lows"]:
        last_sl = swings["swing_lows"][-1]
        sl_price = last_sl[1]
        if close < sl_price:
            return {
                "type": "bearish_bos",
                "direction": "SELL",
                "broken_level": sl_price,
                "close": close,
                "strength": round((sl_price - close) / sl_price * 100, 4),
            }

    return None


def detect_liquidity_sweep(candles: List[Dict], lookback: int = 20) -> Optional[Dict]:
    """Detect liquidity sweeps (stop hunts) — price wicks beyond a swing
    point then closes back inside, trapping breakout traders.

    Bullish sweep: wick below swing low, close back above (buy signal).
    Bearish sweep: wick above swing high, close back below (sell signal).
    Returns: {type, direction, swept_level, wick, close, reclaim_pct} or None
    """
    window = candles[-lookback:] if len(candles) >= lookback else candles
    if len(window) < 8:
        return None

    swings = detect_swing_points(window[:-2], lookback=3)
    current = candles[-1]
    prev = candles[-2]

    # Bullish sweep: wick below a swing low, close back above it
    if swings["swing_lows"]:
        for _, sl_price in reversed(swings["swing_lows"]):
            if (current["low"] < sl_price and
                    current["close"] > sl_price and
                    prev["close"] > sl_price):
                return {
                    "type": "bullish_sweep",
                    "direction": "BUY",
                    "swept_level": sl_price,
                    "wick": current["low"],
                    "close": current["close"],
                    "reclaim_pct": round((current["close"] - sl_price) / sl_price * 100, 4),
                }

    # Bearish sweep: wick above a swing high, close back below it
    if swings["swing_highs"]:
        for _, sh_price in reversed(swings["swing_highs"]):
            if (current["high"] > sh_price and
                    current["close"] < sh_price and
                    prev["close"] < sh_price):
                return {
                    "type": "bearish_sweep",
                    "direction": "SELL",
                    "swept_level": sh_price,
                    "wick": current["high"],
                    "close": current["close"],
                    "reclaim_pct": round((sh_price - current["close"]) / sh_price * 100, 4),
                }

    return None


def detect_choch(candles: List[Dict], lookback: int = 20) -> Optional[Dict]:
    """Detect Change of Character — first break of structure against the trend.

    In an uptrend, CHoCH = price breaks below a swing low (reversal signal → SELL).
    In a downtrend, CHoCH = price breaks above a swing high (reversal signal → BUY).
    Returns: {type, direction, broken_level, close, trend_was} or None
    """
    window = candles[-lookback:] if len(candles) >= lookback else candles
    if len(window) < 12:
        return None

    swings = detect_swing_points(window[:-1], lookback=3)
    shs = swings["swing_highs"]
    sls = swings["swing_lows"]
    close = candles[-1]["close"]

    if len(shs) < 2 or len(sls) < 2:
        return None

    # Determine trend from swing sequence
    # Uptrend: higher highs and higher lows
    hh = shs[-1][1] > shs[-2][1]
    hl = sls[-1][1] > sls[-2][1]
    # Downtrend: lower highs and lower lows
    lh = shs[-1][1] < shs[-2][1]
    ll = sls[-1][1] < sls[-2][1]

    if hh and hl:
        # Uptrend — CHoCH if price breaks below last swing low
        last_sl = sls[-1][1]
        if close < last_sl:
            return {
                "type": "bearish_choch",
                "direction": "SELL",
                "broken_level": last_sl,
                "close": close,
                "trend_was": "UP",
            }

    if lh and ll:
        # Downtrend — CHoCH if price breaks above last swing high
        last_sh = shs[-1][1]
        if close > last_sh:
            return {
                "type": "bullish_choch",
                "direction": "BUY",
                "broken_level": last_sh,
                "close": close,
                "trend_was": "DOWN",
            }

    return None

