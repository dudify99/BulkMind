"""
HLCopier — Hyperliquid Whale Copy-Trading Agent
Polls whale wallets on Hyperliquid, detects new position opens/closes,
and replicates trades on Bulk exchange via BulkExecutor with ATR-based SL/TP.
Auto-closes copies when the whale exits or after max hold time.
"""

import asyncio
import json
import aiohttp
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from config import (
    COPIER_WALLETS, COPIER_SYMBOLS, COPIER_CHECK_SEC,
    COPIER_POSITION_USD, COPIER_MAX_POSITIONS, COPIER_MIN_SIZE_USD,
    COPIER_PAPER_MODE, HL_API_BASE, HL_SYMBOL_MAP, HL_SYMBOL_MAP_REVERSE,
    SYMBOL_CONFIGS,
)
from db import (
    log_trade, close_trade, get_open_trades, get_agent_stats, log_issue,
    save_copier_position, close_copier_position, get_open_copier_positions,
)
from reporter import Reporter
from executor import BulkExecutor
from hyperliquid import HyperliquidClient
from ta import atr, compute_sl_tp, position_size

AGENT_NAME = "HLCopier"
MAX_HOLD_SEC = 4 * 3600               # force-close after 4 hours
SIZE_INCREASE_THRESHOLD = 0.30         # 30% size jump = "added to position"


class HLCopier:
    def __init__(self, executor: BulkExecutor,
                 hl_client: HyperliquidClient,
                 reporter: Reporter,
                 session: aiohttp.ClientSession):
        self.executor  = executor
        self.hl_client = hl_client
        self.reporter  = reporter
        self.session   = session
        self._whale_snapshots: Dict[str, dict] = {}
        self._active_copies: Dict[int, dict] = {}

    async def poll_whale(self, wallet: str) -> Optional[Dict[str, dict]]:
        """Poll one wallet. Returns {hl_symbol: {size, entry, side}} or None."""
        try:
            state = await self.hl_client.get_user_state(wallet)
            if not state:
                return None
            positions = {}
            for ap in state.get("assetPositions", []):
                pos = ap.get("position", ap)
                coin      = pos.get("coin", "")
                size_val  = float(pos.get("szi", "0"))
                entry_val = float(pos.get("entryPx", "0"))
                if abs(size_val) < 1e-12 or entry_val <= 0:
                    continue
                if coin not in HL_SYMBOL_MAP_REVERSE:
                    continue
                positions[coin] = {
                    "size": abs(size_val), "entry": entry_val,
                    "side": "BUY" if size_val > 0 else "SELL",
                }
            return positions
        except Exception as e:
            log_issue("MEDIUM", "API_ERROR",
                      f"HLCopier poll failed for {wallet[:8]}", str(e))
            return None

    async def detect_new_trades(self, wallet: str,
                                current_positions: Dict[str, dict]) -> List[dict]:
        """Compare with snapshot; return list of {action, symbol, side, size, entry}."""
        previous = self._whale_snapshots.get(wallet, {})
        trades: List[dict] = []
        for symbol, pos in current_positions.items():
            if symbol not in previous:
                trades.append({"action": "open", "symbol": symbol,
                               "side": pos["side"], "size": pos["size"],
                               "entry": pos["entry"]})
            else:
                delta = pos["size"] - previous[symbol]["size"]
                if delta > previous[symbol]["size"] * SIZE_INCREASE_THRESHOLD:
                    trades.append({"action": "add", "symbol": symbol,
                                   "side": pos["side"], "size": delta,
                                   "entry": pos["entry"]})
        for symbol in previous:
            if symbol not in current_positions:
                p = previous[symbol]
                trades.append({"action": "close", "symbol": symbol,
                               "side": p["side"], "size": p["size"],
                               "entry": p["entry"]})
        return trades

    async def copy_trade(self, wallet: str, trade: dict):
        """Copy a detected whale open to Bulk exchange."""
        if trade["action"] != "open":
            return
        hl_symbol = trade["symbol"]
        whale_notional = trade["size"] * trade["entry"]
        if whale_notional < COPIER_MIN_SIZE_USD:
            print(f"  [{AGENT_NAME}] Skip {hl_symbol} — ${whale_notional:,.0f} < min")
            return
        bulk_symbol = HL_SYMBOL_MAP_REVERSE.get(hl_symbol)
        if not bulk_symbol or bulk_symbol not in COPIER_SYMBOLS:
            return
        open_copies = get_open_copier_positions()
        if len(open_copies) >= COPIER_MAX_POSITIONS:
            print(f"  [{AGENT_NAME}] Max positions ({COPIER_MAX_POSITIONS}) reached")
            return
        for cp in open_copies:
            if cp["whale_wallet"] == wallet and cp["symbol"] == bulk_symbol:
                return

        # ATR-based SL/TP from Bulk candles
        from executor import BulkClient
        raw_candles = await BulkClient(self.session).get_candles(
            bulk_symbol, interval="15m", limit=30)
        if len(raw_candles) < 16:
            return
        atr_vals = atr(raw_candles, period=14)
        if not atr_vals:
            return
        current_atr = atr_vals[-1]
        entry_price, side = trade["entry"], trade["side"]
        levels = compute_sl_tp(side, entry_price, current_atr, 1.5, 2.0)
        copy_size = position_size(COPIER_POSITION_USD, entry_price, levels["sl"])
        if copy_size <= 0:
            return
        sym_cfg = SYMBOL_CONFIGS.get(bulk_symbol)
        if sym_cfg and copy_size < sym_cfg.min_size:
            return

        result = await self.executor.place_bracket(
            symbol=bulk_symbol, side=side, entry_price=entry_price,
            size=copy_size, sl_price=levels["sl"], tp_price=levels["tp"])
        if not result:
            log_issue("HIGH", "ORDER_REJECT",
                      f"HLCopier bracket failed {bulk_symbol}", json.dumps(trade))
            return

        agent_label = f"{AGENT_NAME}:{wallet[:8]}"
        signal_data = {"whale_wallet": wallet, "whale_size": trade["size"],
                       "whale_notional": whale_notional, "hl_symbol": hl_symbol,
                       "atr": round(current_atr, 6)}
        trade_id = log_trade(
            agent=agent_label, symbol=bulk_symbol, side=side,
            entry_price=entry_price, size=copy_size,
            sl=levels["sl"], tp=levels["tp"],
            signal_data=signal_data, paper=COPIER_PAPER_MODE,
            order_id=result.get("order_id", ""))
        cp_id = save_copier_position(
            whale_wallet=wallet, symbol=bulk_symbol, side=side,
            whale_size=trade["size"], copy_size=copy_size,
            entry_price=entry_price, trade_id=trade_id)
        self._active_copies[cp_id] = {
            "trade_id": trade_id, "wallet": wallet, "symbol": bulk_symbol,
            "side": side, "entry": entry_price, "copy_size": copy_size,
            "opened_at": datetime.utcnow()}

        await self.reporter.send(
            f"*{AGENT_NAME} -- New Copy Trade*\n"
            f"Whale: `{wallet[:8]}...` | `{bulk_symbol}` {side}\n"
            f"Whale size: `{trade['size']}` (${whale_notional:,.0f})\n"
            f"Copy: `{copy_size}` @ `{entry_price}`\n"
            f"SL: `{levels['sl']}` | TP: `{levels['tp']}` | Paper: `{COPIER_PAPER_MODE}`")
        print(f"  [{AGENT_NAME}] Copied {side} {bulk_symbol} from {wallet[:8]}")

    async def manage_copies(self):
        """Close copies when whale exits or max hold time exceeded."""
        open_copies = get_open_copier_positions()
        if not open_copies:
            return
        now = datetime.utcnow()
        for cp in open_copies:
            cp_id, wallet = cp["id"], cp["whale_wallet"]
            bulk_symbol, side = cp["symbol"], cp["side"]
            entry, trade_id = cp["entry_price"], cp.get("trade_id", 0)
            hl_symbol = HL_SYMBOL_MAP.get(bulk_symbol)
            if not hl_symbol:
                continue

            should_close, close_reason = False, ""
            if hl_symbol not in self._whale_snapshots.get(wallet, {}):
                should_close, close_reason = True, "whale exited"
            elapsed = (now - datetime.fromisoformat(cp["ts"])).total_seconds()
            if elapsed > MAX_HOLD_SEC:
                should_close, close_reason = True, f"max hold ({MAX_HOLD_SEC // 3600}h)"
            if not should_close:
                continue

            from executor import BulkClient
            ticker = await BulkClient(self.session).get_ticker(bulk_symbol)
            if not ticker:
                continue
            exit_price = float(ticker.get("lastPrice") or
                               ticker.get("last_price") or
                               ticker.get("price", 0))
            if not exit_price:
                continue

            copy_size = cp["copy_size"]
            pnl_pct = ((exit_price - entry) / entry * 100 if side == "BUY"
                        else (entry - exit_price) / entry * 100)
            pnl_usd = pnl_pct / 100 * entry * copy_size
            status = "WIN" if pnl_usd > 0 else "LOSS"

            if trade_id:
                close_trade(trade_id, exit_price, status)
            close_copier_position(cp_id, exit_price, round(pnl_usd, 2))
            self._active_copies.pop(cp_id, None)

            await self.reporter.send(
                f"*{AGENT_NAME} -- Copy Closed*\n"
                f"Whale: `{wallet[:8]}...` | `{bulk_symbol}` {side}\n"
                f"Entry: `{entry}` -> Exit: `{exit_price}`\n"
                f"PnL: `${pnl_usd:+.2f}` ({pnl_pct:+.2f}%) | {close_reason}")
            print(f"  [{AGENT_NAME}] Closed {bulk_symbol} — {close_reason}")

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
            f"*{AGENT_NAME} Performance*\n"
            f"Copies: `{total}` | W: `{wins}` L: `{losses}` ({wr:.1f}%)\n"
            f"PnL: `${pnl:.2f}` | Avg: `{stats.get('avg_pnl_pct', 0):.2f}%`\n"
            f"Active: `{len(get_open_copier_positions())}`"
        )

    # ── EvoSkill: Export failure trajectories ────────────────

    def export_failure_trajectories(self,
                                    output_path: str = "data/copier_failures.json"):
        """Export losing copy trades for EvoSkill analysis."""
        import db as _db
        conn = _db.get_conn()
        rows = conn.execute(
            """SELECT t.*, t.signal_data FROM trades t
               WHERE t.agent LIKE ? AND t.status='LOSS'
               ORDER BY t.ts DESC LIMIT 100""",
            (f"{AGENT_NAME}:%",)
        ).fetchall()
        _db.release_conn(conn)

        trajectories = []
        for row in rows:
            d = dict(row)
            signal = json.loads(d.get("signal_data") or "{}")
            trajectories.append({
                "question":     (f"Should I have copied this {d['side']} on "
                                 f"{d['symbol']} from {signal.get('whale_wallet', '?')[:8]}?"),
                "ground_truth": "NO",
                "agent_answer": "YES",
                "context": {
                    "entry": d["entry_price"], "sl": d["sl_price"],
                    "tp": d["tp_price"], "exit": d["exit_price"],
                    "pnl_pct": d["pnl_pct"],
                    "whale_notional": signal.get("whale_notional"),
                    "whale_size": signal.get("whale_size"),
                    "signal": signal,
                }
            })

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(trajectories, f, indent=2)
        print(f"Exported {len(trajectories)} HLCopier failure trajectories -> {output_path}")
        return output_path

    # ── Main Loop ────────────────────────────────────────────

    async def run(self):
        print(f"[{AGENT_NAME}] Started — watching {len(COPIER_WALLETS)} whale wallets "
              f"| Paper: {COPIER_PAPER_MODE} | Interval: {COPIER_CHECK_SEC}s")
        if not COPIER_WALLETS:
            print(f"[{AGENT_NAME}] WARNING: No wallets in COPIER_WALLETS (config.py)")

        scan_count = 0
        while True:
            try:
                for wallet in COPIER_WALLETS:
                    positions = await self.poll_whale(wallet)
                    if positions is not None:
                        new_trades = await self.detect_new_trades(wallet, positions)
                        for trade in new_trades:
                            await self.copy_trade(wallet, trade)
                        self._whale_snapshots[wallet] = positions
                await self.manage_copies()

                scan_count += 1
                if scan_count % 100 == 0:
                    await self.report_performance()
                if scan_count % 200 == 0:
                    self.export_failure_trajectories()

            except Exception as e:
                print(f"[{AGENT_NAME}] Error: {e}")
                log_issue("MEDIUM", "AGENT_ERROR", f"HLCopier error: {e}", str(e))

            await asyncio.sleep(COPIER_CHECK_SEC)
