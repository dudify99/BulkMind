"""
Signal Engine — AI-Powered Trading Signal Generator for Alpha Rush
Pre-computes 6 sniper strategies every candle update. Sub-20ms response time.
All signals backed by real TA from ta.py — no random, no fake.
"""

import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from ta import (
    ema, sma, atr, rsi, macd, vwap,
    fibonacci_levels, death_cross, ema_crossover,
    detect_breakout, detect_pattern, donchian_channel,
    bollinger_bands, compute_sl_tp, position_size,
    detect_order_blocks, detect_fvg, detect_bos,
    detect_liquidity_sweep, detect_choch,
)


# ══════════════════════════════════════════════════════════════
#  Signal Types
# ══════════════════════════════════════════════════════════════

STRATEGIES = {
    "breakout":       {"name": "Scalp Breakout",      "emoji": "💥", "desc": "Donchian channel break + volume spike"},
    "fibonacci":      {"name": "Fibonacci Bounce",     "emoji": "🌀", "desc": "Price reversal at key Fib level"},
    "ema_cross":      {"name": "EMA Crossover",        "emoji": "✂️", "desc": "Fast EMA crosses slow EMA"},
    "death_cross":    {"name": "Death/Golden Cross",   "emoji": "💀", "desc": "50 EMA crosses 200 EMA"},
    "pattern":        {"name": "Candlestick Pattern",  "emoji": "🕯️", "desc": "Engulfing, hammer, 3 soldiers/crows"},
    "mean_reversion": {"name": "Mean Reversion",       "emoji": "🔄", "desc": "RSI extreme + price beyond Bollinger"},
    # Price Action / SMC Sniper Strategies (high-leverage precision entries)
    "order_block":    {"name": "Order Block",          "emoji": "🏦", "desc": "Institutional entry zone — tight SL, 3:1+ R:R"},
    "fvg":            {"name": "Fair Value Gap",       "emoji": "⚡", "desc": "Imbalance fill — price returns to unfilled gap"},
    "liq_sweep":      {"name": "Liquidity Sweep",      "emoji": "🎯", "desc": "Stop hunt reversal — swept level + reclaim"},
    "bos":            {"name": "Break of Structure",   "emoji": "🔨", "desc": "Trend continuation — swing point broken"},
    "choch":          {"name": "Change of Character",  "emoji": "🔀", "desc": "Trend reversal — first counter-trend break"},
}


@dataclass
class Signal:
    strategy: str           # Key from STRATEGIES
    direction: str          # "BUY" or "SELL"
    confidence: float       # 0-100
    entry: float
    target: float
    stop: float
    rr_ratio: float         # Risk/reward ratio
    reason: str             # Human-readable why
    details: dict = field(default_factory=dict)
    timestamp: float = 0.0


@dataclass
class StrategyResult:
    """Backtest result for a single strategy."""
    strategy: str
    total_signals: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    avg_pnl_pct: float = 0.0
    total_pnl_pct: float = 0.0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0
    avg_rr: float = 0.0


class SignalEngine:
    """
    Pre-computes signals from candle data. Zero DB queries.
    Call update_candles() on each candle → signals are cached.
    Call get_signals() to retrieve in <1ms.
    """

    def __init__(self):
        # Cache: {symbol: [Signal, ...]} — latest signals per symbol
        self._signals: Dict[str, List[Signal]] = {}
        # Candle cache: {symbol: [candle, ...]}
        self._candles: Dict[str, List[dict]] = {}
        # Backtest results: {symbol: {strategy: StrategyResult}}
        self._backtest: Dict[str, Dict[str, StrategyResult]] = {}
        # Timing
        self._last_update: Dict[str, float] = {}

    def update_candles(self, symbol: str, candles: List[dict]):
        """Feed new candle data. Recomputes all signals. ~5-15ms."""
        start = time.perf_counter()
        self._candles[symbol] = candles
        signals = []

        if len(candles) < 30:
            self._signals[symbol] = []
            return

        close = candles[-1]["close"]
        atr_val = self._safe_atr(candles)

        # Skip signal generation if ATR is still 0 (no usable data)
        if atr_val <= 0:
            self._signals[symbol] = []
            return

        # 1. Breakout
        sig = self._check_breakout(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 2. Fibonacci
        sig = self._check_fibonacci(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 3. EMA Crossover
        sig = self._check_ema_cross(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 4. Death/Golden Cross
        sig = self._check_death_cross(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 5. Candlestick Pattern
        sig = self._check_pattern(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 6. Mean Reversion
        sig = self._check_mean_reversion(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # ── Price Action / SMC Sniper Strategies ─────────────
        # 7. Order Block
        sig = self._check_order_block(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 8. Fair Value Gap
        sig = self._check_fvg(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 9. Liquidity Sweep
        sig = self._check_liq_sweep(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 10. Break of Structure
        sig = self._check_bos(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # 11. Change of Character
        sig = self._check_choch(symbol, candles, close, atr_val)
        if sig:
            signals.append(sig)

        # Sort by confidence (highest first)
        signals.sort(key=lambda s: s.confidence, reverse=True)
        self._signals[symbol] = signals
        self._last_update[symbol] = time.time()

        elapsed_ms = (time.perf_counter() - start) * 1000
        if elapsed_ms > 20:
            print(f"⚠️ Signal engine slow: {symbol} took {elapsed_ms:.1f}ms")

    def get_signals(self, symbol: str, limit: int = 5) -> List[dict]:
        """Get cached signals. <1ms response."""
        signals = self._signals.get(symbol, [])[:limit]
        return [self._signal_to_dict(s) for s in signals]

    def get_best_signal(self, symbol: str) -> Optional[dict]:
        """Get the single best signal by confidence."""
        signals = self._signals.get(symbol, [])
        if not signals:
            return None
        return self._signal_to_dict(signals[0])

    def get_backtest(self, symbol: str) -> Dict[str, dict]:
        """Get backtest results per strategy."""
        return {k: self._result_to_dict(v)
                for k, v in self._backtest.get(symbol, {}).items()}

    def run_backtest(self, symbol: str, candles: List[dict]):
        """Backtest all strategies on historical candles. Run once on startup."""
        results: Dict[str, StrategyResult] = {}
        for key in STRATEGIES:
            results[key] = StrategyResult(strategy=key)

        if len(candles) < 60:
            self._backtest[symbol] = results
            return

        # Walk forward: compute signal at each bar, check if target or stop hit within 5 bars
        for i in range(50, len(candles) - 5):
            window = candles[:i+1]
            close = window[-1]["close"]
            atr_val = self._safe_atr(window)

            checks = [
                ("breakout", self._check_breakout),
                ("fibonacci", self._check_fibonacci),
                ("ema_cross", self._check_ema_cross),
                ("death_cross", self._check_death_cross),
                ("pattern", self._check_pattern),
                ("mean_reversion", self._check_mean_reversion),
                ("order_block", self._check_order_block),
                ("fvg", self._check_fvg),
                ("liq_sweep", self._check_liq_sweep),
                ("bos", self._check_bos),
                ("choch", self._check_choch),
            ]

            for key, fn in checks:
                sig = fn(symbol, window, close, atr_val)
                if not sig:
                    continue

                r = results[key]
                r.total_signals += 1

                # Check next 5 candles for target/stop hit
                future = candles[i+1:i+6]
                hit_target = False
                hit_stop = False
                for fc in future:
                    if sig.direction == "BUY":
                        if fc["high"] >= sig.target:
                            hit_target = True
                            break
                        if fc["low"] <= sig.stop:
                            hit_stop = True
                            break
                    else:
                        if fc["low"] <= sig.target:
                            hit_target = True
                            break
                        if fc["high"] >= sig.stop:
                            hit_stop = True
                            break

                if hit_target:
                    r.wins += 1
                    pnl_pct = abs(sig.target - sig.entry) / sig.entry * 100
                    r.total_pnl_pct += pnl_pct
                    r.best_trade_pct = max(r.best_trade_pct, pnl_pct)
                elif hit_stop:
                    r.losses += 1
                    pnl_pct = -abs(sig.stop - sig.entry) / sig.entry * 100
                    r.total_pnl_pct += pnl_pct
                    r.worst_trade_pct = min(r.worst_trade_pct, pnl_pct)

        # Compute final stats
        for r in results.values():
            total = r.wins + r.losses
            r.win_rate = round(r.wins / total * 100, 1) if total else 0
            r.avg_pnl_pct = round(r.total_pnl_pct / total, 3) if total else 0
            r.avg_rr = round(r.wins / r.losses, 2) if r.losses else 0

        self._backtest[symbol] = results

    # ── Strategy Implementations ──────────────────────────────

    def _check_breakout(self, symbol: str, candles: list,
                        close: float, atr_val: float) -> Optional[Signal]:
        result = detect_breakout(candles, lookback=20, volume_mult=1.3)
        if not result:
            return None
        direction = result["direction"]
        vol_ratio = result.get("volume_ratio", 1)
        confidence = min(95, 55 + vol_ratio * 12 + result.get("breakout_pct", 0) * 20)
        levels = compute_sl_tp(direction, close, atr_val, 1.0, 2.0)
        return Signal(
            strategy="breakout", direction=direction,
            confidence=round(confidence),
            entry=close, target=levels["tp"], stop=levels["sl"],
            rr_ratio=2.0,
            reason=f"Donchian break {'above' if direction=='BUY' else 'below'} "
                   f"with {vol_ratio:.1f}x volume",
            details=result, timestamp=time.time(),
        )

    def _check_fibonacci(self, symbol: str, candles: list,
                         close: float, atr_val: float) -> Optional[Signal]:
        fib = fibonacci_levels(candles, lookback=50)
        if not fib:
            return None
        # Check if price is near a key Fib level (within 0.15%)
        tolerance = close * 0.0015
        for level_name, level_price in fib.items():
            if not level_name.startswith("level_"):
                continue
            if abs(close - level_price) < tolerance:
                pct = level_name.replace("level_", "")
                # At 618/786 levels → likely bounce (mean reversion)
                if pct in ("618", "786"):
                    direction = "BUY" if close <= level_price else "SELL"
                    levels = compute_sl_tp(direction, close, atr_val, 1.2, 2.5)
                    return Signal(
                        strategy="fibonacci", direction=direction,
                        confidence=72,
                        entry=close, target=levels["tp"], stop=levels["sl"],
                        rr_ratio=2.5,
                        reason=f"Price at Fibonacci {pct}% level (${level_price:.0f}) — potential reversal",
                        details=fib, timestamp=time.time(),
                    )
                # At 236/382 levels → trend continuation
                if pct in ("236", "382"):
                    trend = "BUY" if candles[-1]["close"] > candles[-5]["close"] else "SELL"
                    levels = compute_sl_tp(trend, close, atr_val, 1.0, 2.0)
                    return Signal(
                        strategy="fibonacci", direction=trend,
                        confidence=65,
                        entry=close, target=levels["tp"], stop=levels["sl"],
                        rr_ratio=2.0,
                        reason=f"Fibonacci {pct}% pullback — trend continuation",
                        details=fib, timestamp=time.time(),
                    )
        return None

    def _check_ema_cross(self, symbol: str, candles: list,
                         close: float, atr_val: float) -> Optional[Signal]:
        cross = ema_crossover(candles, fast=9, slow=21)
        if not cross:
            return None
        direction = cross["direction"]
        strength = cross.get("strength", 0)
        confidence = min(85, 55 + strength * 100)
        levels = compute_sl_tp(direction, close, atr_val, 1.0, 1.8)
        return Signal(
            strategy="ema_cross", direction=direction,
            confidence=round(confidence),
            entry=close, target=levels["tp"], stop=levels["sl"],
            rr_ratio=1.8,
            reason=f"EMA9 crossed {'above' if direction=='BUY' else 'below'} EMA21 "
                   f"(strength {strength:.3f}%)",
            details=cross, timestamp=time.time(),
        )

    def _check_death_cross(self, symbol: str, candles: list,
                           close: float, atr_val: float) -> Optional[Signal]:
        cross = death_cross(candles, fast=50, slow=200)
        if not cross:
            return None
        direction = cross["direction"]
        cross_type = cross["type"]
        confidence = 80 if cross_type == "golden_cross" else 78
        levels = compute_sl_tp(direction, close, atr_val, 1.5, 3.0)
        return Signal(
            strategy="death_cross", direction=direction,
            confidence=confidence,
            entry=close, target=levels["tp"], stop=levels["sl"],
            rr_ratio=3.0,
            reason=f"{'Golden Cross' if cross_type == 'golden_cross' else 'Death Cross'}: "
                   f"EMA50 ({cross['fast_ema']}) × EMA200 ({cross['slow_ema']})",
            details=cross, timestamp=time.time(),
        )

    def _check_pattern(self, symbol: str, candles: list,
                       close: float, atr_val: float) -> Optional[Signal]:
        pattern = detect_pattern(candles)
        if not pattern or pattern["direction"] == "NEUTRAL":
            return None
        direction = pattern["direction"]
        confidence = pattern.get("confidence", 60)
        levels = compute_sl_tp(direction, close, atr_val, 1.0, 1.5)
        pattern_name = pattern["pattern"].replace("_", " ").title()
        return Signal(
            strategy="pattern", direction=direction,
            confidence=round(confidence),
            entry=close, target=levels["tp"], stop=levels["sl"],
            rr_ratio=1.5,
            reason=f"{pattern_name} pattern detected",
            details=pattern, timestamp=time.time(),
        )

    def _check_mean_reversion(self, symbol: str, candles: list,
                              close: float, atr_val: float) -> Optional[Signal]:
        rsi_vals = rsi(candles, period=14)
        bb = bollinger_bands(candles, period=20)
        if not rsi_vals or not bb or not bb.get("upper"):
            return None

        current_rsi = rsi_vals[-1]
        upper_band = bb["upper"][-1]
        lower_band = bb["lower"][-1]

        # Oversold: RSI < 30 + price below lower Bollinger
        if current_rsi < 30 and close < lower_band:
            levels = compute_sl_tp("BUY", close, atr_val, 1.2, 2.0)
            return Signal(
                strategy="mean_reversion", direction="BUY",
                confidence=min(85, 50 + (30 - current_rsi) * 2),
                entry=close, target=levels["tp"], stop=levels["sl"],
                rr_ratio=2.0,
                reason=f"RSI oversold ({current_rsi:.0f}) + price below lower Bollinger (${lower_band:.0f})",
                details={"rsi": current_rsi, "bb_lower": lower_band},
                timestamp=time.time(),
            )
        # Overbought: RSI > 70 + price above upper Bollinger
        if current_rsi > 70 and close > upper_band:
            levels = compute_sl_tp("SELL", close, atr_val, 1.2, 2.0)
            return Signal(
                strategy="mean_reversion", direction="SELL",
                confidence=min(85, 50 + (current_rsi - 70) * 2),
                entry=close, target=levels["tp"], stop=levels["sl"],
                rr_ratio=2.0,
                reason=f"RSI overbought ({current_rsi:.0f}) + price above upper Bollinger (${upper_band:.0f})",
                details={"rsi": current_rsi, "bb_upper": upper_band},
                timestamp=time.time(),
            )
        return None

    # ── Price Action / SMC Sniper Strategies ────────────────

    def _check_order_block(self, symbol: str, candles: list,
                           close: float, atr_val: float) -> Optional[Signal]:
        """Order Block: price returns to an institutional entry zone. Tight SL at OB edge."""
        blocks = detect_order_blocks(candles, lookback=20)
        if not blocks:
            return None

        for ob in reversed(blocks):  # Check most recent first
            direction = ob["direction"]
            ob_high = ob["ob_high"]
            ob_low = ob["ob_low"]
            ob_mid = (ob_high + ob_low) / 2
            tolerance = atr_val * 0.5

            # Price must be inside or touching the OB zone
            if direction == "BUY" and ob_low - tolerance <= close <= ob_high + tolerance:
                sl = ob_low - atr_val * 0.3  # Tight SL just below OB
                tp = close + (close - sl) * 3.0  # 3:1 R:R
                confidence = min(92, 65 + ob["strength"] * 5)
                return Signal(
                    strategy="order_block", direction="BUY",
                    confidence=round(confidence),
                    entry=close, target=round(tp, 2), stop=round(sl, 2),
                    rr_ratio=3.0,
                    reason=f"Bullish OB zone ${ob_low:.0f}-${ob_high:.0f} "
                           f"(impulse strength {ob['strength']}x)",
                    details=ob, timestamp=time.time(),
                )
            if direction == "SELL" and ob_low - tolerance <= close <= ob_high + tolerance:
                sl = ob_high + atr_val * 0.3
                tp = close - (sl - close) * 3.0
                confidence = min(92, 65 + ob["strength"] * 5)
                return Signal(
                    strategy="order_block", direction="SELL",
                    confidence=round(confidence),
                    entry=close, target=round(tp, 2), stop=round(sl, 2),
                    rr_ratio=3.0,
                    reason=f"Bearish OB zone ${ob_low:.0f}-${ob_high:.0f} "
                           f"(impulse strength {ob['strength']}x)",
                    details=ob, timestamp=time.time(),
                )
        return None

    def _check_fvg(self, symbol: str, candles: list,
                   close: float, atr_val: float) -> Optional[Signal]:
        """Fair Value Gap: price fills an imbalance. Entry at gap edge, SL beyond gap."""
        gaps = detect_fvg(candles[:-3])  # Exclude last 3 candles (gap must be older)
        if not gaps:
            return None

        # Only consider gaps that haven't been filled yet and price is near
        for gap in reversed(gaps):
            gap_high = gap["gap_high"]
            gap_low = gap["gap_low"]
            gap_mid = (gap_high + gap_low) / 2
            tolerance = atr_val * 0.3

            if gap["direction"] == "BUY":
                # Price approaching bullish FVG from above (dipping into gap)
                if gap_low - tolerance <= close <= gap_high + tolerance:
                    sl = gap_low - atr_val * 0.5
                    risk = close - sl
                    tp = close + risk * 3.5  # 3.5:1 R:R
                    return Signal(
                        strategy="fvg", direction="BUY",
                        confidence=78,
                        entry=close, target=round(tp, 2), stop=round(sl, 2),
                        rr_ratio=3.5,
                        reason=f"Bullish FVG fill zone ${gap_low:.0f}-${gap_high:.0f} "
                               f"(gap size ${gap['gap_size']:.0f})",
                        details=gap, timestamp=time.time(),
                    )

            if gap["direction"] == "SELL":
                if gap_low - tolerance <= close <= gap_high + tolerance:
                    sl = gap_high + atr_val * 0.5
                    risk = sl - close
                    tp = close - risk * 3.5
                    return Signal(
                        strategy="fvg", direction="SELL",
                        confidence=78,
                        entry=close, target=round(tp, 2), stop=round(sl, 2),
                        rr_ratio=3.5,
                        reason=f"Bearish FVG fill zone ${gap_low:.0f}-${gap_high:.0f} "
                               f"(gap size ${gap['gap_size']:.0f})",
                        details=gap, timestamp=time.time(),
                    )
        return None

    def _check_liq_sweep(self, symbol: str, candles: list,
                         close: float, atr_val: float) -> Optional[Signal]:
        """Liquidity Sweep: stop hunt detected — high-conviction reversal entry."""
        sweep = detect_liquidity_sweep(candles, lookback=20)
        if not sweep:
            return None

        direction = sweep["direction"]
        reclaim = abs(sweep.get("reclaim_pct", 0))
        confidence = min(95, 70 + reclaim * 30)

        if direction == "BUY":
            sl = sweep["wick"] - atr_val * 0.2  # SL just below the wick
            risk = close - sl
            tp = close + risk * 4.0  # 4:1 R:R — high conviction
            return Signal(
                strategy="liq_sweep", direction="BUY",
                confidence=round(confidence),
                entry=close, target=round(tp, 2), stop=round(sl, 2),
                rr_ratio=4.0,
                reason=f"Bullish liquidity sweep — swept ${sweep['swept_level']:.0f}, "
                       f"reclaimed {reclaim:.2f}%",
                details=sweep, timestamp=time.time(),
            )
        else:
            sl = sweep["wick"] + atr_val * 0.2
            risk = sl - close
            tp = close - risk * 4.0
            return Signal(
                strategy="liq_sweep", direction="SELL",
                confidence=round(confidence),
                entry=close, target=round(tp, 2), stop=round(sl, 2),
                rr_ratio=4.0,
                reason=f"Bearish liquidity sweep — swept ${sweep['swept_level']:.0f}, "
                       f"reclaimed {reclaim:.2f}%",
                details=sweep, timestamp=time.time(),
            )

    def _check_bos(self, symbol: str, candles: list,
                   close: float, atr_val: float) -> Optional[Signal]:
        """Break of Structure: trend continuation after swing point break."""
        bos = detect_bos(candles, lookback=20)
        if not bos:
            return None

        direction = bos["direction"]
        strength = bos.get("strength", 0)
        confidence = min(88, 60 + strength * 40)
        levels = compute_sl_tp(direction, close, atr_val, 0.8, 2.5)

        return Signal(
            strategy="bos", direction=direction,
            confidence=round(confidence),
            entry=close, target=levels["tp"], stop=levels["sl"],
            rr_ratio=2.5,
            reason=f"{'Bullish' if direction == 'BUY' else 'Bearish'} BOS — "
                   f"broke ${bos['broken_level']:.0f} (strength {strength:.3f}%)",
            details=bos, timestamp=time.time(),
        )

    def _check_choch(self, symbol: str, candles: list,
                     close: float, atr_val: float) -> Optional[Signal]:
        """Change of Character: early reversal detection against prior trend."""
        choch = detect_choch(candles, lookback=20)
        if not choch:
            return None

        direction = choch["direction"]
        confidence = 82  # High confidence — structural shift
        levels = compute_sl_tp(direction, close, atr_val, 1.0, 3.0)

        return Signal(
            strategy="choch", direction=direction,
            confidence=confidence,
            entry=close, target=levels["tp"], stop=levels["sl"],
            rr_ratio=3.0,
            reason=f"CHoCH — trend was {choch['trend_was']}, broke "
                   f"${choch['broken_level']:.0f} → reversal {direction}",
            details=choch, timestamp=time.time(),
        )

    # ── Helpers ───────────────────────────────────────────────

    def _safe_atr(self, candles: list) -> float:
        """Return ATR, falling back to recent price range if insufficient data."""
        atr_vals = atr(candles, period=14)
        val = atr_vals[-1] if atr_vals else 0
        if val > 0:
            return val
        # Fallback: use high-low range of last 5 candles as ATR estimate
        if len(candles) >= 2:
            recent = candles[-min(5, len(candles)):]
            highs = [c.get("h", c.get("high", 0)) for c in recent]
            lows = [c.get("l", c.get("low", 0)) for c in recent]
            if highs and lows and max(highs) > 0:
                return (max(highs) - min(lows)) / len(recent)
        # Last resort: 0.5% of close price
        if candles:
            close = candles[-1].get("c", candles[-1].get("close", 0))
            if close > 0:
                return close * 0.005
        return 0

    def _signal_to_dict(self, sig: Signal) -> dict:
        meta = STRATEGIES.get(sig.strategy, {})
        return {
            "strategy": sig.strategy,
            "strategy_name": meta.get("name", sig.strategy),
            "emoji": meta.get("emoji", ""),
            "desc": meta.get("desc", ""),
            "direction": sig.direction,
            "confidence": sig.confidence,
            "entry": round(sig.entry, 2),
            "target": round(sig.target, 2),
            "stop": round(sig.stop, 2),
            "rr_ratio": sig.rr_ratio,
            "reason": sig.reason,
            "timestamp": sig.timestamp,
        }

    def _result_to_dict(self, r: StrategyResult) -> dict:
        meta = STRATEGIES.get(r.strategy, {})
        return {
            "strategy": r.strategy,
            "name": meta.get("name", r.strategy),
            "emoji": meta.get("emoji", ""),
            "total_signals": r.total_signals,
            "wins": r.wins,
            "losses": r.losses,
            "win_rate": r.win_rate,
            "avg_pnl_pct": r.avg_pnl_pct,
            "total_pnl_pct": round(r.total_pnl_pct, 3),
            "best_trade_pct": round(r.best_trade_pct, 3),
            "worst_trade_pct": round(r.worst_trade_pct, 3),
            "avg_rr": r.avg_rr,
        }


# Global instance
signals = SignalEngine()
