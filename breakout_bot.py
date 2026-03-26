"""
BreakoutBot — Technical Analysis Trading Agent
Strategy: Donchian Channel breakout with volume confirmation + ATR-based SL/TP
Integrates with EvoSkill for self-improvement via failed trade analysis
"""

import asyncio
import json
from datetime import datetime
from typing import Optional, List, Dict

from executor import BulkClient, BulkExecutor
from ta import (
    detect_breakout, atr, compute_sl_tp,
    position_size, is_trending, higher_timeframe_bias,
    donchian_channel
)
from db import (
    log_trade, close_trade, save_candle,
    get_candles, get_open_trades, get_agent_stats, log_issue
)
from reporter import Reporter
from config import (
    BREAKOUT_SYMBOLS, BREAKOUT_TIMEFRAME_MIN,
    BREAKOUT_LOOKBACK, BREAKOUT_VOLUME_MULT,
    BREAKOUT_ATR_MULT, BREAKOUT_TP_RATIO,
    BREAKOUT_MAX_POSITION_USD, BREAKOUT_PAPER_MODE
)

AGENT_NAME = "BreakoutBot"


class BreakoutBot:
    def __init__(self, executor: BulkExecutor,
                 client: BulkClient,
                 reporter: Reporter):
        self.executor = executor
        self.client   = client
        self.reporter = reporter
        self.open_trades: Dict[int, dict] = {}  # trade_id → trade info

    # ── Signal Generation ─────────────────────────────────────

    async def get_signal(self, symbol: str) -> Optional[dict]:
        """
        Full signal pipeline:
        1. Fetch candles from Bulk API
        2. Detect breakout (Donchian + volume)
        3. Confirm with HTF trend filter
        4. Compute SL/TP via ATR
        5. Size the position
        """

        # Fetch 15m candles
        raw = await self.client.get_candles(
            symbol,
            interval=f"{BREAKOUT_TIMEFRAME_MIN}m",
            limit=BREAKOUT_LOOKBACK + 10
        )

        if len(raw) < BREAKOUT_LOOKBACK + 2:
            print(f"  [{symbol}] Not enough candles: {len(raw)}")
            return None

        # Persist to DB
        for c in raw:
            save_candle(symbol, BREAKOUT_TIMEFRAME_MIN,
                        c["ts"], c["open"], c["high"], c["low"],
                        c["close"], c["volume"])

        # Breakout detection
        signal = detect_breakout(raw, BREAKOUT_LOOKBACK, BREAKOUT_VOLUME_MULT)
        if not signal:
            return None

        # ATR for SL/TP
        atr_vals = atr(raw, period=14)
        if not atr_vals:
            return None
        current_atr = atr_vals[-1]

        # Trend filter — don't trade against HTF trend
        trend = is_trending(raw, ema_period=50)
        if trend and trend != ("UP" if signal["direction"] == "BUY" else "DOWN"):
            print(f"  [{symbol}] Breakout filtered: HTF trend={trend}, signal={signal['direction']}")
            return None

        # SL/TP
        entry  = signal["close"]
        levels = compute_sl_tp(
            signal["direction"], entry,
            current_atr, BREAKOUT_ATR_MULT, BREAKOUT_TP_RATIO
        )

        # Position size
        size = position_size(BREAKOUT_MAX_POSITION_USD, entry, levels["sl"])

        signal.update({
            "symbol":    symbol,
            "entry":     entry,
            "sl":        levels["sl"],
            "tp":        levels["tp"],
            "size":      size,
            "atr":       round(current_atr, 6),
            "rr_ratio":  levels["rr_ratio"],
            "trend":     trend,
            "timestamp": datetime.utcnow().isoformat(),
        })

        return signal

    # ── Trade Execution ───────────────────────────────────────

    async def execute_signal(self, signal: dict) -> Optional[int]:
        symbol = signal["symbol"]
        side   = signal["direction"]

        # Check for existing open trade on same symbol
        open_trades = get_open_trades(AGENT_NAME)
        for t in open_trades:
            if t["symbol"] == symbol:
                print(f"  [{symbol}] Already have open trade, skipping")
                return None

        # Place bracket order (entry + SL + TP atomically)
        result = await self.executor.place_bracket(
            symbol     = symbol,
            side       = side,
            entry_price = signal["entry"],
            size       = signal["size"],
            sl_price   = signal["sl"],
            tp_price   = signal["tp"],
        )

        if not result:
            log_issue("HIGH", "EXECUTION",
                      f"BreakoutBot failed to place order on {symbol}",
                      json.dumps(signal))
            return None

        # Log to DB
        trade_id = log_trade(
            agent       = AGENT_NAME,
            symbol      = symbol,
            side        = side,
            entry_price = signal["entry"],
            size        = signal["size"],
            sl          = signal["sl"],
            tp          = signal["tp"],
            signal_data = signal,
            paper       = BREAKOUT_PAPER_MODE,
            order_id    = result.get("order_id", "")
        )

        self.open_trades[trade_id] = {
            "symbol":   symbol,
            "side":     side,
            "entry":    signal["entry"],
            "sl":       signal["sl"],
            "tp":       signal["tp"],
            "size":     signal["size"],
        }

        # Notify
        await self.reporter.send(
            f"🚀 *BreakoutBot — New Trade*\n"
            f"Symbol: `{symbol}`\n"
            f"Side: `{side}`\n"
            f"Entry: `{signal['entry']}`\n"
            f"SL: `{signal['sl']}`\n"
            f"TP: `{signal['tp']}`\n"
            f"Size: `{signal['size']}`\n"
            f"Vol Ratio: `{signal['volume_ratio']}x`\n"
            f"ATR: `{signal['atr']}`\n"
            f"Paper: `{BREAKOUT_PAPER_MODE}`"
        )

        return trade_id

    # ── Trade Management ──────────────────────────────────────

    async def manage_open_trades(self):
        """Check open trades — close if TP/SL hit (paper simulation)"""
        if not self.open_trades:
            return

        for trade_id, trade in list(self.open_trades.items()):
            ticker = await self.client.get_ticker(trade["symbol"])
            if not ticker:
                continue

            price = float(ticker.get("lastPrice") or
                          ticker.get("last_price") or
                          ticker.get("price", 0))
            if not price:
                continue

            side   = trade["side"]
            sl     = trade["sl"]
            tp     = trade["tp"]
            status = None

            if side == "BUY":
                if price <= sl:
                    status = "LOSS"
                elif price >= tp:
                    status = "WIN"
            else:
                if price >= sl:
                    status = "LOSS"
                elif price <= tp:
                    status = "WIN"

            if status:
                pnl = close_trade(trade_id, price, status)
                del self.open_trades[trade_id]

                emoji = "✅" if status == "WIN" else "❌"
                await self.reporter.send(
                    f"{emoji} *BreakoutBot — Trade Closed*\n"
                    f"Symbol: `{trade['symbol']}`\n"
                    f"Status: `{status}`\n"
                    f"Exit: `{price}`\n"
                    f"PnL: `${pnl:.2f}`\n"
                    f"Trade ID: `{trade_id}`"
                )

    # ── Performance Report ────────────────────────────────────

    async def report_performance(self):
        stats = get_agent_stats(AGENT_NAME)
        if not stats or not stats.get("total"):
            return

        total  = stats["total"] or 0
        wins   = stats["wins"] or 0
        losses = stats["losses"] or 0
        pnl    = stats["total_pnl"] or 0
        wr     = (wins / total * 100) if total > 0 else 0

        await self.reporter.send(
            f"📈 *BreakoutBot Performance*\n"
            f"Total Trades: `{total}`\n"
            f"Wins: `{wins}` | Losses: `{losses}`\n"
            f"Win Rate: `{wr:.1f}%`\n"
            f"Total PnL: `${pnl:.2f}`\n"
            f"Avg PnL%: `{stats.get('avg_pnl_pct', 0):.2f}%`"
        )

    # ── EvoSkill: Export failure trajectories ─────────────────

    def export_failure_trajectories(self, output_path: str = "data/failures.json"):
        """
        Export losing trades as failure trajectories for EvoSkill
        EvoSkill Proposer will analyze these and suggest skill improvements
        """
        conn_db = __import__('db').get_conn()
        rows = conn_db.execute(
            """SELECT t.*, t.signal_data
               FROM trades t
               WHERE agent=? AND status='LOSS'
               ORDER BY ts DESC LIMIT 100""",
            (AGENT_NAME,)
        ).fetchall()
        conn_db.close()

        trajectories = []
        for row in rows:
            d = dict(row)
            signal = json.loads(d.get("signal_data") or "{}")
            trajectories.append({
                "question":      f"Should I have taken this {d['side']} breakout on {d['symbol']}?",
                "ground_truth":  "NO",
                "agent_answer":  "YES",
                "context": {
                    "entry":         d["entry_price"],
                    "sl":            d["sl_price"],
                    "tp":            d["tp_price"],
                    "exit":          d["exit_price"],
                    "pnl_pct":       d["pnl_pct"],
                    "signal":        signal,
                }
            })

        import json as _json
        from pathlib import Path
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            _json.dump(trajectories, f, indent=2)

        print(f"📦 Exported {len(trajectories)} failure trajectories → {output_path}")
        return output_path

    # ── Main Loop ─────────────────────────────────────────────

    async def run(self):
        print(f"🤖 {AGENT_NAME} started — Paper mode: {BREAKOUT_PAPER_MODE}")
        scan_count = 0

        while True:
            try:
                print(f"\n🔍 [{AGENT_NAME}] Scanning {len(BREAKOUT_SYMBOLS)} symbols...")

                for symbol in BREAKOUT_SYMBOLS:
                    signal = await self.get_signal(symbol)
                    if signal:
                        print(f"  🎯 BREAKOUT SIGNAL: {symbol} {signal['direction']}")
                        await self.execute_signal(signal)
                    else:
                        print(f"  [{symbol}] No signal")

                # Manage open trades every scan
                await self.manage_open_trades()

                # Performance report every 10 scans
                scan_count += 1
                if scan_count % 10 == 0:
                    await self.report_performance()

                # Export EvoSkill trajectories every 50 scans
                if scan_count % 50 == 0:
                    self.export_failure_trajectories()

            except Exception as e:
                print(f"BreakoutBot error: {e}")
                log_issue("HIGH", "AGENT_ERROR",
                          "BreakoutBot runtime error", str(e))

            # Wait for next candle close (15 min)
            await asyncio.sleep(BREAKOUT_TIMEFRAME_MIN * 60)
