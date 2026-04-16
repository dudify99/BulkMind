"""
FundingArb — Cross-Exchange Funding Rate Arbitrage Agent
Strategy: Monitor funding rate differentials between Bulk and Hyperliquid,
open delta-neutral positions (long on one exchange, short on the other)
to capture funding payments. Positions are closed when the differential
narrows below FUNDING_CLOSE_DIFF_BPS.

Delta-neutral logic:
  - If Bulk funding > HL funding → short on Bulk (pay less), long on HL (receive more)
  - If HL funding > Bulk funding → short on HL, long on Bulk
  - In both cases, the net market exposure is zero; profit comes from the funding spread.
"""

import asyncio
import json
from datetime import datetime
from typing import Optional, List, Dict

import aiohttp

from executor import BulkClient, BulkExecutor
from hyperliquid import HyperliquidClient, HyperliquidExecutor
from reporter import Reporter
from ta import atr, compute_sl_tp, position_size
from db import (
    log_trade, close_trade, get_open_trades, get_agent_stats, log_issue,
    save_arb_position, close_arb_position, get_open_arb_positions,
    get_conn, release_conn,
)
from config import (
    FUNDING_SYMBOLS, FUNDING_CHECK_SEC, FUNDING_MIN_DIFF_BPS,
    FUNDING_POSITION_USD, FUNDING_MAX_POSITIONS, FUNDING_CLOSE_DIFF_BPS,
    FUNDING_PAPER_MODE, HL_SYMBOL_MAP, SYMBOL_CONFIGS, HL_API_BASE,
)

AGENT_NAME = "FundingArb"


class FundingArb:
    def __init__(self,
                 bulk_executor: BulkExecutor,
                 hl_executor: HyperliquidExecutor,
                 bulk_client: BulkClient,
                 hl_client: HyperliquidClient,
                 reporter: Reporter,
                 session: aiohttp.ClientSession):
        self.bulk_executor = bulk_executor
        self.hl_executor   = hl_executor
        self.bulk_client   = bulk_client
        self.hl_client     = hl_client
        self.reporter      = reporter
        self.session       = session

    # ── Funding Rate Fetching ────────────────────────────────────

    async def fetch_funding_rates(self) -> Dict[str, dict]:
        """
        Fetch current funding rates from both Bulk and Hyperliquid.
        Returns: {symbol: {"bulk": rate, "hl": rate, "diff_bps": abs_diff}}
        """
        rates: Dict[str, dict] = {}

        # Fetch HL metadata (contains funding rates for all assets)
        hl_data = None
        try:
            async with self.session.post(
                HL_API_BASE + "/info",
                json={"type": "metaAndAssetCtxs"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    hl_data = await resp.json(content_type=None)
        except Exception as e:
            log_issue("MEDIUM", "API_ERROR",
                      "FundingArb: HL metaAndAssetCtxs failed", str(e))

        # Parse HL funding rates into a lookup by coin name
        hl_funding: Dict[str, float] = {}
        if hl_data and isinstance(hl_data, list) and len(hl_data) >= 2:
            meta_info = hl_data[0]  # {universe: [{name: "BTC", ...}, ...]}
            asset_ctxs = hl_data[1]  # [{funding: "0.0001", ...}, ...]
            universe = meta_info.get("universe", [])
            for i, asset_ctx in enumerate(asset_ctxs):
                if i < len(universe):
                    coin = universe[i].get("name", "")
                    funding_str = asset_ctx.get("funding", "0")
                    try:
                        hl_funding[coin] = float(funding_str)
                    except (ValueError, TypeError):
                        hl_funding[coin] = 0.0

        # For each tracked symbol, get both exchange rates
        for symbol in FUNDING_SYMBOLS:
            hl_coin = HL_SYMBOL_MAP.get(symbol, symbol.replace("-USD", ""))

            # Bulk funding rate from ticker
            bulk_rate = 0.0
            try:
                ticker = await self.bulk_client.get_ticker(symbol)
                if ticker:
                    bulk_rate = float(ticker.get("fundingRate", 0) or 0)
            except Exception as e:
                print(f"  [FundingArb] Bulk ticker fetch error for {symbol}: {e}")

            # HL funding rate from pre-fetched data
            hl_rate = hl_funding.get(hl_coin, 0.0)

            # Compute differential in basis points (1 bps = 0.01%)
            diff = abs(bulk_rate - hl_rate) * 10000

            rates[symbol] = {
                "bulk": bulk_rate,
                "hl": hl_rate,
                "diff_bps": round(diff, 2),
            }

        return rates

    # ── Opportunity Detection ────────────────────────────────────

    async def check_opportunities(self, rates: Dict[str, dict]):
        """
        Scan funding rates for arb opportunities.
        Opens delta-neutral positions when diff >= FUNDING_MIN_DIFF_BPS.
        """
        open_positions = get_open_arb_positions()
        open_symbols = {pos["symbol"] for pos in open_positions}

        if len(open_positions) >= FUNDING_MAX_POSITIONS:
            return

        for symbol, rate_info in rates.items():
            if symbol in open_symbols:
                continue
            if len(open_positions) >= FUNDING_MAX_POSITIONS:
                break

            diff_bps = rate_info["diff_bps"]
            if diff_bps < FUNDING_MIN_DIFF_BPS:
                continue

            # Determine which exchange to long/short:
            # Long on the exchange with NEGATIVE funding (you get paid)
            # Short on the exchange with POSITIVE funding (you pay less or get paid)
            bulk_rate = rate_info["bulk"]
            hl_rate = rate_info["hl"]

            if bulk_rate > hl_rate:
                # Bulk has higher funding → short Bulk, long HL
                long_exchange = "hl"
                short_exchange = "bulk"
            else:
                # HL has higher funding → short HL, long Bulk
                long_exchange = "bulk"
                short_exchange = "hl"

            print(f"  [FundingArb] Opportunity: {symbol} diff={diff_bps:.1f}bps "
                  f"(bulk={bulk_rate:.6f}, hl={hl_rate:.6f}) "
                  f"→ long {long_exchange}, short {short_exchange}")

            await self.open_arb(symbol, long_exchange, short_exchange, diff_bps)
            open_positions = get_open_arb_positions()

    # ── Open Arb Position ────────────────────────────────────────

    async def open_arb(self, symbol: str, long_exchange: str,
                       short_exchange: str, diff_bps: float):
        """
        Open both legs of the delta-neutral arb.
        Long leg on one exchange, short leg on the other.
        """
        hl_coin = HL_SYMBOL_MAP.get(symbol, symbol.replace("-USD", ""))
        cfg = SYMBOL_CONFIGS.get(symbol)
        min_size = cfg.min_size if cfg else 0.001

        # Get current price from the long exchange for sizing
        if long_exchange == "bulk":
            ticker = await self.bulk_client.get_ticker(symbol)
        else:
            ticker = await self.hl_client.get_ticker(hl_coin)
        if not ticker:
            log_issue("MEDIUM", "AGENT_ERROR",
                      f"FundingArb: no ticker for {symbol}", "")
            return

        price = float(
            ticker.get("lastPrice") or
            ticker.get("last_price") or
            ticker.get("price", 0)
        )
        if not price:
            return

        size = round(FUNDING_POSITION_USD / price, 8)
        if size < min_size:
            print(f"  [FundingArb] {symbol} size {size} below min {min_size}, skipping")
            return

        # Resolve exchange-specific symbols and executors
        bulk_sym = symbol
        hl_sym = hl_coin

        # ── Place long leg ───────────────────────────────────────
        if long_exchange == "bulk":
            long_result = await self.bulk_executor.place_bracket(
                symbol=bulk_sym, side="BUY", entry_price=price,
                size=size, sl_price=0, tp_price=0,
            )
        else:
            long_result = await self.hl_executor.place_bracket(
                symbol=hl_sym, side="BUY", entry_price=price,
                size=size, sl_price=0, tp_price=0,
            )

        if not long_result:
            log_issue("HIGH", "AGENT_ERROR",
                      f"FundingArb: long leg failed on {long_exchange}/{symbol}",
                      "")
            return

        # ── Place short leg ──────────────────────────────────────
        if short_exchange == "bulk":
            short_result = await self.bulk_executor.place_bracket(
                symbol=bulk_sym, side="SELL", entry_price=price,
                size=size, sl_price=0, tp_price=0,
            )
        else:
            short_result = await self.hl_executor.place_bracket(
                symbol=hl_sym, side="SELL", entry_price=price,
                size=size, sl_price=0, tp_price=0,
            )

        if not short_result:
            log_issue("HIGH", "AGENT_ERROR",
                      f"FundingArb: short leg failed on {short_exchange}/{symbol}",
                      "")
            return

        # ── Log both legs to trades table ────────────────────────
        long_trade_id = log_trade(
            agent=f"{AGENT_NAME}:{long_exchange}",
            symbol=symbol,
            side="BUY",
            entry_price=price,
            size=size,
            sl=0, tp=0,
            signal_data={
                "type": "funding_arb_long",
                "diff_bps": diff_bps,
                "exchange": long_exchange,
            },
            paper=FUNDING_PAPER_MODE,
            order_id=long_result.get("order_id", ""),
        )

        short_trade_id = log_trade(
            agent=f"{AGENT_NAME}:{short_exchange}",
            symbol=symbol,
            side="SELL",
            entry_price=price,
            size=size,
            sl=0, tp=0,
            signal_data={
                "type": "funding_arb_short",
                "diff_bps": diff_bps,
                "exchange": short_exchange,
            },
            paper=FUNDING_PAPER_MODE,
            order_id=short_result.get("order_id", ""),
        )

        # ── Record arb position ──────────────────────────────────
        arb_id = save_arb_position(
            symbol=symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            entry_diff_bps=diff_bps,
            position_usd=FUNDING_POSITION_USD,
            long_trade_id=long_trade_id,
            short_trade_id=short_trade_id,
        )

        await self.reporter.send(
            f"*FundingArb -- New Position*\n"
            f"Symbol: `{symbol}`\n"
            f"Long: `{long_exchange}` | Short: `{short_exchange}`\n"
            f"Size: `{size}` (~${FUNDING_POSITION_USD})\n"
            f"Entry Diff: `{diff_bps:.1f} bps`\n"
            f"Arb ID: `{arb_id}`\n"
            f"Paper: `{FUNDING_PAPER_MODE}`"
        )

    # ── Position Management ──────────────────────────────────────

    async def manage_positions(self, rates: Dict[str, dict]):
        """
        Check open arb positions. Close both legs when the funding
        differential has narrowed below FUNDING_CLOSE_DIFF_BPS.
        """
        open_positions = get_open_arb_positions()
        if not open_positions:
            return

        for pos in open_positions:
            symbol = pos["symbol"]
            rate_info = rates.get(symbol)
            if not rate_info:
                continue

            current_diff = rate_info["diff_bps"]

            if current_diff > FUNDING_CLOSE_DIFF_BPS:
                continue

            # Funding diff has narrowed — close both legs
            print(f"  [FundingArb] Closing {symbol}: diff narrowed to {current_diff:.1f}bps "
                  f"(threshold={FUNDING_CLOSE_DIFF_BPS}bps)")

            # Get current price for PnL calculation
            hl_coin = HL_SYMBOL_MAP.get(symbol, symbol.replace("-USD", ""))
            ticker = await self.bulk_client.get_ticker(symbol)
            if not ticker:
                ticker = await self.hl_client.get_ticker(hl_coin)
            exit_price = float(
                ticker.get("lastPrice") or
                ticker.get("last_price") or
                ticker.get("price", 0)
            ) if ticker else 0

            # Close the trade records for both legs
            long_pnl = 0
            short_pnl = 0
            if pos.get("long_trade_id"):
                long_pnl = close_trade(pos["long_trade_id"], exit_price, "WIN") or 0
            if pos.get("short_trade_id"):
                short_pnl = close_trade(pos["short_trade_id"], exit_price, "WIN") or 0

            # Net PnL is approximately the accumulated funding payments
            # In delta-neutral, price moves cancel out; profit = funding collected
            net_pnl = round(long_pnl + short_pnl, 2)

            close_arb_position(
                arb_id=pos["id"],
                exit_diff_bps=current_diff,
                pnl_usd=net_pnl,
            )

            await self.reporter.send(
                f"*FundingArb -- Position Closed*\n"
                f"Symbol: `{symbol}`\n"
                f"Entry Diff: `{pos['entry_diff_bps']:.1f} bps` -> Exit Diff: `{current_diff:.1f} bps`\n"
                f"Net PnL: `${net_pnl:.2f}`\n"
                f"Arb ID: `{pos['id']}`"
            )

    # ── Performance Report ───────────────────────────────────────

    async def report_performance(self):
        """Aggregate and report performance across both exchanges."""
        for exchange in ("bulk", "hl"):
            agent_tag = f"{AGENT_NAME}:{exchange}"
            stats = get_agent_stats(agent_tag)
            if not stats or not stats.get("total"):
                continue

            total  = stats["total"] or 0
            wins   = stats["wins"] or 0
            losses = stats["losses"] or 0
            pnl    = stats["total_pnl"] or 0
            wr     = (wins / total * 100) if total > 0 else 0

            await self.reporter.send(
                f"*FundingArb Performance ({exchange})*\n"
                f"Total Trades: `{total}`\n"
                f"Wins: `{wins}` | Losses: `{losses}`\n"
                f"Win Rate: `{wr:.1f}%`\n"
                f"Total PnL: `${pnl:.2f}`\n"
                f"Avg PnL%: `{stats.get('avg_pnl_pct', 0):.2f}%`"
            )

    # ── EvoSkill: Export failure trajectories ────────────────────

    def export_failure_trajectories(self,
                                    output_path: str = "data/funding_failures.json"):
        """
        Export losing arb positions as failure trajectories for EvoSkill.
        EvoSkill Proposer will analyze these and suggest improvements.
        """
        conn_db = get_conn()
        rows = conn_db.execute(
            """SELECT * FROM funding_arb_positions
               WHERE status='closed' AND pnl_usd < 0
               ORDER BY closed_at DESC LIMIT 100"""
        ).fetchall()
        release_conn(conn_db)

        trajectories = []
        for row in rows:
            d = dict(row)
            trajectories.append({
                "question":     (f"Should I have opened this funding arb on {d['symbol']} "
                                 f"(long {d['long_exchange']}, short {d['short_exchange']})?"),
                "ground_truth": "NO",
                "agent_answer": "YES",
                "context": {
                    "symbol":          d["symbol"],
                    "long_exchange":   d["long_exchange"],
                    "short_exchange":  d["short_exchange"],
                    "entry_diff_bps":  d["entry_diff_bps"],
                    "exit_diff_bps":   d.get("exit_diff_bps"),
                    "position_usd":    d["position_usd"],
                    "pnl_usd":         d["pnl_usd"],
                },
            })

        from pathlib import Path
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(trajectories, f, indent=2)

        print(f"FundingArb: exported {len(trajectories)} failure trajectories -> {output_path}")
        return output_path

    # ── Main Loop ────────────────────────────────────────────────

    async def run(self):
        print(f"{AGENT_NAME} started — Paper mode: {FUNDING_PAPER_MODE} | "
              f"Symbols: {FUNDING_SYMBOLS} | "
              f"Min diff: {FUNDING_MIN_DIFF_BPS}bps | "
              f"Check interval: {FUNDING_CHECK_SEC}s")

        scan_count = 0

        while True:
            try:
                print(f"\n[{AGENT_NAME}] Checking funding rates across "
                      f"{len(FUNDING_SYMBOLS)} symbols...")

                rates = await self.fetch_funding_rates()

                # Log current rates
                for symbol, info in rates.items():
                    print(f"  {symbol}: bulk={info['bulk']:.6f} "
                          f"hl={info['hl']:.6f} diff={info['diff_bps']:.1f}bps")

                # Manage existing positions first (close if diff narrowed)
                await self.manage_positions(rates)

                # Then check for new opportunities
                await self.check_opportunities(rates)

                scan_count += 1
                if scan_count % 12 == 0:  # ~every hour at 5-min interval
                    await self.report_performance()

                if scan_count % 50 == 0:
                    self.export_failure_trajectories()

            except Exception as e:
                print(f"FundingArb error: {e}")
                log_issue("MEDIUM", "AGENT_ERROR",
                          f"FundingArb error: {e}", str(e))

            await asyncio.sleep(FUNDING_CHECK_SEC)
