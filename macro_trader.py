"""
MacroTrader — Economic Calendar Trading Agent for BulkMind
Positions before high-impact macro events (FOMC, CPI, NFP).
Uses Claude to predict directional impact, trades via BulkExecutor.
"""

import asyncio
import json
import time
import aiohttp
from typing import Optional, List, Dict
from pathlib import Path
from datetime import datetime

from reporter import Reporter
from news_trader import ExchangeVenue
from ta import atr, compute_sl_tp, position_size
from db import (
    log_trade, close_trade, get_open_trades, get_agent_stats, log_issue,
    save_macro_event, mark_macro_traded,
)
from config import (
    MACRO_SYMBOLS, MACRO_CHECK_SEC, MACRO_POSITION_USD,
    MACRO_PRE_EVENT_MIN, MACRO_POST_EVENT_MIN,
    MACRO_PAPER_MODE, MACRO_LLM_MODEL, ANTHROPIC_API_KEY,
    HL_SYMBOL_MAP,
)

AGENT_NAME = "MacroTrader"

# Recurring high-impact macro events
MACRO_CALENDAR = [
    {"name": "FOMC Rate Decision", "type": "fomc", "impact": 10},
    {"name": "US CPI Release", "type": "cpi", "impact": 9},
    {"name": "US NFP (Non-Farm Payrolls)", "type": "nfp", "impact": 9},
    {"name": "US PPI Release", "type": "ppi", "impact": 7},
    {"name": "US GDP Report", "type": "gdp", "impact": 8},
    {"name": "ECB Rate Decision", "type": "ecb", "impact": 8},
    {"name": "US Jobless Claims", "type": "claims", "impact": 6},
    {"name": "BTC ETF Flow Report", "type": "etf", "impact": 7},
]

ANALYSIS_PROMPT = """You are a macro trading analyst for crypto markets.
An upcoming economic event is about to occur. Predict the likely impact on BTC.

Event: {name}
Type: {event_type}
Impact Level: {impact}/10
{extra_context}

Tradeable symbols: {symbols}

Respond in EXACTLY this JSON format:
{{"direction": "BUY" or "SELL" or "NEUTRAL", "confidence": 1-10, "symbols": ["BTC-USD"], "reasoning": "one sentence"}}

Rules:
- FOMC dovish (rate cut/pause): BUY — risk-on, dollar weakens
- FOMC hawkish (rate hike): SELL — risk-off, dollar strengthens
- CPI below expectations: BUY — rate cuts expected
- CPI above expectations: SELL — tightening fears
- Only BUY or SELL if confidence >= 7
- Map to affected symbols
"""


class MacroTrader:
    """Trades around major economic events using LLM analysis."""

    def __init__(self, venues: List[ExchangeVenue],
                 reporter: Reporter,
                 session: aiohttp.ClientSession):
        self.venues = venues
        self.reporter = reporter
        self.session = session
        self.open_trades: Dict[int, dict] = {}
        self._last_events: Dict[str, float] = {}  # event_type → last_trade_ts

    async def fetch_upcoming_events(self) -> List[dict]:
        """Fetch economic calendar. Primary: ForexFactory free API. Fallback: hardcoded."""
        events = []
        try:
            async with self.session.get(
                "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    for item in data:
                        impact_str = (item.get("impact") or "").lower()
                        if impact_str not in ("high", "medium"):
                            continue
                        events.append({
                            "name": item.get("title", "Unknown"),
                            "type": item.get("title", "").lower().replace(" ", "_")[:20],
                            "impact": 9 if impact_str == "high" else 6,
                            "date": item.get("date", ""),
                            "time": item.get("time", ""),
                            "forecast": item.get("forecast", ""),
                            "previous": item.get("previous", ""),
                        })
        except Exception:
            pass

        if not events:
            # Fallback: return known recurring events
            for ev in MACRO_CALENDAR:
                if ev["impact"] >= 7:
                    events.append({**ev, "date": "", "forecast": "", "previous": ""})
        return events

    async def analyze_event(self, event: dict) -> Optional[dict]:
        """Use Claude to predict market direction for a macro event."""
        if not ANTHROPIC_API_KEY:
            return None

        extra = ""
        if event.get("forecast"):
            extra += f"Forecast: {event['forecast']}\n"
        if event.get("previous"):
            extra += f"Previous: {event['previous']}\n"

        prompt = ANALYSIS_PROMPT.format(
            name=event["name"],
            event_type=event.get("type", "unknown"),
            impact=event.get("impact", 5),
            extra_context=extra,
            symbols=", ".join(MACRO_SYMBOLS),
        )

        try:
            async with self.session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": MACRO_LLM_MODEL,
                    "max_tokens": 256,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                text = data["content"][0]["text"]
                # Extract JSON from response
                start = text.find("{")
                end = text.rfind("}") + 1
                if start < 0 or end <= start:
                    return None
                analysis = json.loads(text[start:end])

                if analysis.get("direction") == "NEUTRAL":
                    return None
                if analysis.get("confidence", 0) < 7:
                    return None

                # Filter symbols
                valid_syms = [s for s in analysis.get("symbols", [])
                              if s in MACRO_SYMBOLS]
                if not valid_syms:
                    valid_syms = ["BTC-USD"]
                analysis["symbols"] = valid_syms
                return analysis

        except Exception as e:
            log_issue("LOW", "AGENT_ERROR",
                      f"MacroTrader LLM error: {e}")
            return None

    async def get_signal(self, symbol: str, analysis: dict,
                         venue: ExchangeVenue) -> Optional[dict]:
        """Build a trade signal from macro analysis."""
        direction = analysis["direction"]
        side = "BUY" if direction == "BUY" else "SELL"

        ex_symbol = venue.resolve_symbol(symbol)
        try:
            candles = await venue.client.get_candles(ex_symbol, interval="15m", limit=50)
        except Exception:
            candles = []

        if not candles or len(candles) < 15:
            return None

        atr_vals = atr(candles, period=14)
        atr_val = atr_vals[-1] if atr_vals else 0
        if atr_val <= 0:
            close = candles[-1].get("close", candles[-1].get("c", 0))
            atr_val = close * 0.005 if close > 0 else 0
        if atr_val <= 0:
            return None

        close = candles[-1].get("close", candles[-1].get("c", 0))
        # Wider SL for macro volatility
        levels = compute_sl_tp(side, close, atr_val, 1.5, 2.5)
        size = position_size(MACRO_POSITION_USD, close, levels["sl"])

        return {
            "symbol": symbol,
            "ex_symbol": ex_symbol,
            "exchange": venue.name,
            "direction": side,
            "entry": close,
            "sl": levels["sl"],
            "tp": levels["tp"],
            "size": size,
            "atr": atr_val,
            "rr_ratio": 2.5,
            "reasoning": analysis.get("reasoning", ""),
            "event": analysis.get("event_name", ""),
            "timestamp": time.time(),
        }

    async def execute_signal(self, signal: dict, venue: ExchangeVenue,
                             event_id: int) -> Optional[int]:
        """Execute a macro trade signal."""
        agent_tag = f"{AGENT_NAME}:{venue.name}"
        open_trades = get_open_trades(agent_tag)
        for t in open_trades:
            if t["symbol"] == signal["symbol"]:
                return None  # Already positioned

        result = await venue.executor.place_bracket(
            symbol=signal["ex_symbol"],
            side=signal["direction"],
            entry_price=signal["entry"],
            size=signal["size"],
            sl_price=signal["sl"],
            tp_price=signal["tp"],
        )

        if not result:
            log_issue("HIGH", "EXECUTION",
                      f"{AGENT_NAME} failed to place order on {signal['symbol']}",
                      json.dumps(signal))
            return None

        trade_id = log_trade(
            agent=agent_tag,
            symbol=signal["symbol"],
            side=signal["direction"],
            entry_price=signal["entry"],
            size=signal["size"],
            sl=signal["sl"],
            tp=signal["tp"],
            signal_data=signal,
            paper=MACRO_PAPER_MODE,
            order_id=result.get("order_id", ""),
        )

        self.open_trades[trade_id] = {
            **signal, "trade_id": trade_id,
            "opened_at": time.time(), "venue": venue.name,
        }

        if event_id:
            mark_macro_traded(event_id, trade_id)

        await self.reporter.send(
            f"📅 *{AGENT_NAME} — New Trade*\n"
            f"Event: `{signal.get('reasoning', 'macro event')}`\n"
            f"Symbol: `{signal['symbol']}` ({venue.name})\n"
            f"Side: `{signal['direction']}`\n"
            f"Entry: `{signal['entry']:.2f}`\n"
            f"SL: `{signal['sl']:.2f}` | TP: `{signal['tp']:.2f}`\n"
            f"Paper: `{MACRO_PAPER_MODE}`"
        )
        return trade_id

    async def manage_open_trades(self):
        """Close trades that exceed max hold time."""
        now = time.time()
        to_close = []

        for trade_id, trade in self.open_trades.items():
            elapsed_min = (now - trade["opened_at"]) / 60
            if elapsed_min >= MACRO_POST_EVENT_MIN:
                to_close.append(trade_id)

        for trade_id in to_close:
            trade = self.open_trades.pop(trade_id, None)
            if not trade:
                continue
            # Get current price for exit
            venue = next((v for v in self.venues if v.name == trade["venue"]), None)
            if not venue:
                continue

            try:
                ticker = await venue.client.get_ticker(trade["ex_symbol"])
                price = float(ticker.get("lastPrice", ticker.get("price", trade["entry"])))
            except Exception:
                price = trade["entry"]

            if trade["direction"] == "BUY":
                pnl = (price - trade["entry"]) * trade["size"]
            else:
                pnl = (trade["entry"] - price) * trade["size"]

            status = "WIN" if pnl > 0 else "LOSS"
            close_trade(trade_id, price, status)

            emoji = "✅" if status == "WIN" else "❌"
            await self.reporter.send(
                f"{emoji} *{AGENT_NAME} — Time Exit*\n"
                f"Symbol: `{trade['symbol']}`\n"
                f"PnL: `${pnl:.2f}`\n"
                f"Held: `{MACRO_POST_EVENT_MIN}min`"
            )

    def export_failure_trajectories(self):
        """Export losing trades for EvoSkill self-improvement."""
        stats = get_agent_stats(AGENT_NAME)
        if not stats:
            return

        failures = []
        open_list = get_open_trades(AGENT_NAME)
        # Also check venue-specific agent tags
        for venue in self.venues:
            tag = f"{AGENT_NAME}:{venue.name}"
            st = get_agent_stats(tag)
            if st and st.get("losses", 0) > 0:
                failures.append({
                    "question": f"Should I have traded this macro event on {venue.name}?",
                    "ground_truth": "NO",
                    "agent_answer": "YES",
                    "context": st,
                })

        if failures:
            Path("data").mkdir(exist_ok=True)
            with open("data/macro_failures.json", "w") as f:
                json.dump(failures, f, indent=2)

    async def run(self):
        """Main loop: fetch calendar → analyze → trade → manage."""
        print(f"📅 {AGENT_NAME} starting (paper={MACRO_PAPER_MODE}, "
              f"symbols={MACRO_SYMBOLS})")
        scan_count = 0

        while True:
            try:
                events = await self.fetch_upcoming_events()

                for event in events:
                    event_type = event.get("type", "")
                    # Rate limit: don't trade same event type within 4 hours
                    last_ts = self._last_events.get(event_type, 0)
                    if time.time() - last_ts < 4 * 3600:
                        continue

                    analysis = await self.analyze_event(event)
                    if not analysis:
                        continue

                    event_id = save_macro_event(
                        event_name=event["name"],
                        event_type=event_type,
                        severity=event.get("impact", 5),
                        direction=analysis.get("direction"),
                        symbols=analysis.get("symbols"),
                        source="calendar",
                    )

                    for sym in analysis.get("symbols", ["BTC-USD"]):
                        for venue in self.venues:
                            signal = await self.get_signal(sym, analysis, venue)
                            if signal:
                                result = await self.execute_signal(
                                    signal, venue, event_id)
                                if result:
                                    self._last_events[event_type] = time.time()

                await self.manage_open_trades()

                scan_count += 1
                if scan_count % 20 == 0:
                    self.export_failure_trajectories()

            except Exception as e:
                log_issue("MEDIUM", "AGENT_ERROR", f"{AGENT_NAME} error: {e}")

            await asyncio.sleep(MACRO_CHECK_SEC)
