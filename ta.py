"""
Technical Analysis Library
ATR, EMA, Volume analysis, Breakout detection
"""

import statistics
from typing import List, Dict, Optional


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
