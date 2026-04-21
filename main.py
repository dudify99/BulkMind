"""
BulkMind — Main Orchestrator
Runs BulkWatch + BulkStream + BulkProfile + BulkSOL + BreakoutBot + Dashboard
"""

import asyncio
import json
import aiohttp
from db import init_db
from reporter import Reporter
from executor import BulkClient, BulkExecutor
from bulk_watch import BulkWatch
from bulk_stream import BulkStream
from bulk_profile import BulkProfile
from bulk_sol import BulkSOL
from breakout_bot import BreakoutBot
from news_trader import NewsTrader, ExchangeVenue
from funding_arb import FundingArb
from hl_copier import HLCopier
from macro_trader import MacroTrader
from war_trader import WarTrader
from smc_bot import SMCBot
from hyperliquid import HyperliquidClient, HyperliquidExecutor
from hl_stream import HLStream
from dashboard import Dashboard
from evoskill_integration import run_evoskill_loop
from agent_monitor import supervise
from config import (
    BREAKOUT_PAPER_MODE, NEWS_PAPER_MODE, NEWS_EXCHANGES,
    HL_PAPER_MODE, DASHBOARD_PORT,
    FUNDING_PAPER_MODE, COPIER_PAPER_MODE,
    MACRO_PAPER_MODE, WAR_PAPER_MODE,
    SMC_PAPER_MODE,
)


async def main():
    print("=" * 50)
    print("  🧠 BulkMind Starting")
    print(f"  BreakoutBot:  {'PAPER' if BREAKOUT_PAPER_MODE else '🔴 LIVE'}")
    print(f"  NewsTrader:   {'PAPER' if NEWS_PAPER_MODE else '🔴 LIVE'}")
    print(f"  FundingArb:   {'PAPER' if FUNDING_PAPER_MODE else '🔴 LIVE'}")
    print(f"  HLCopier:     {'PAPER' if COPIER_PAPER_MODE else '🔴 LIVE'}")
    print(f"  MacroTrader:  {'PAPER' if MACRO_PAPER_MODE else '🔴 LIVE'}")
    print(f"  WarTrader:    {'PAPER' if WAR_PAPER_MODE else '🔴 LIVE'}")
    print(f"  SMCBot:       {'PAPER' if SMC_PAPER_MODE else '🔴 LIVE'}")
    print(f"  Dashboard: http://localhost:{DASHBOARD_PORT}")
    print("=" * 50)

    # Init DB
    init_db()

    # Shared components
    reporter  = Reporter()

    # Init modules
    watch     = BulkWatch(reporter)
    stream    = BulkStream(reporter)
    hl_stream = HLStream(reporter)
    profile   = BulkProfile(reporter)
    bulksol   = BulkSOL(reporter)

    async with aiohttp.ClientSession() as session:
        # ── Bulk exchange ────────────────────────────────────
        client   = BulkClient(session)

        # ── Hyperliquid exchange ─────────────────────────────
        hl_client = HyperliquidClient(session)
        hl_exec   = HyperliquidExecutor(hl_client, paper=HL_PAPER_MODE)

        # ── Shared venue list (Bulk + Hyperliquid) ───────────
        bulk_venue = ExchangeVenue("bulk", client,
                                   BulkExecutor(client, paper=BREAKOUT_PAPER_MODE),
                                   paper=BREAKOUT_PAPER_MODE)
        hl_venue   = ExchangeVenue("hyperliquid", hl_client, hl_exec,
                                   paper=HL_PAPER_MODE)
        all_venues = [bulk_venue, hl_venue]

        # ── BreakoutBot (multi-exchange) ─────────────────────
        bot = BreakoutBot(all_venues, reporter)

        # ── HyperBulk executors for trade API ────────────────
        hb_bulk_exec = BulkExecutor(client, paper=NEWS_PAPER_MODE)
        hb_hl_exec   = HyperliquidExecutor(hl_client, paper=HL_PAPER_MODE)
        dashboard = Dashboard(reporter, bulksol,
                              bulk_executor=hb_bulk_exec,
                              hl_executor=hb_hl_exec)

        # ── Multi-exchange NewsTrader ────────────────────────
        news_venues = []
        if "bulk" in NEWS_EXCHANGES:
            news_venues.append(
                ExchangeVenue("bulk", client,
                              BulkExecutor(client, paper=NEWS_PAPER_MODE),
                              paper=NEWS_PAPER_MODE))
        if "hyperliquid" in NEWS_EXCHANGES:
            news_venues.append(
                ExchangeVenue("hyperliquid", hl_client, hl_exec,
                              paper=HL_PAPER_MODE))
        news_trader = NewsTrader(news_venues, reporter, session)

        # ── FundingArb (delta-neutral funding rate arb) ──────
        funding_arb = FundingArb(
            bulk_executor=BulkExecutor(client, paper=FUNDING_PAPER_MODE),
            hl_executor=HyperliquidExecutor(hl_client, paper=HL_PAPER_MODE),
            bulk_client=client,
            hl_client=hl_client,
            reporter=reporter,
            session=session,
        )

        # ── HLCopier (whale wallet mirroring) ────────────────
        hl_copier = HLCopier(
            executor=BulkExecutor(client, paper=COPIER_PAPER_MODE),
            hl_client=hl_client,
            reporter=reporter,
            session=session,
        )

        # ── MacroTrader (economic calendar) ───────────────────
        macro_venues = []
        if "bulk" in NEWS_EXCHANGES:
            macro_venues.append(
                ExchangeVenue("bulk", client,
                              BulkExecutor(client, paper=MACRO_PAPER_MODE),
                              paper=MACRO_PAPER_MODE)
            )
        if "hyperliquid" in NEWS_EXCHANGES:
            macro_venues.append(
                ExchangeVenue("hyperliquid", hl_client, hl_exec,
                              paper=HL_PAPER_MODE)
            )
        macro_trader = MacroTrader(macro_venues, reporter, session)

        # ── WarTrader (geopolitical event classifier) ─────────
        war_trader = WarTrader(macro_venues, reporter, session)

        # ── SMCBot (Smart Money Concepts, multi-exchange) ─────
        smc_venues = [
            ExchangeVenue("bulk", client,
                          BulkExecutor(client, paper=SMC_PAPER_MODE),
                          paper=SMC_PAPER_MODE),
            ExchangeVenue("hyperliquid", hl_client, hl_exec,
                          paper=HL_PAPER_MODE),
        ]
        smc_bot = SMCBot(smc_venues, reporter)

        venue_str = ", ".join(v.name for v in news_venues)
        await reporter.send(
            "🟢 *BulkMind Online*\n"
            f"BulkWatch: ✅\n"
            f"BulkStream: ✅\n"
            f"BulkProfile: ✅\n"
            f"BulkSOL: ✅\n"
            f"BreakoutBot: ✅\n"
            f"NewsTrader: ✅ ({venue_str})\n"
            f"FundingArb: ✅\n"
            f"HLCopier: ✅\n"
            f"MacroTrader: ✅\n"
            f"WarTrader: ✅\n"
            f"SMCBot: ✅\n"
            f"Dashboard: ✅\n"
            f"Mode: `{'PAPER' if BREAKOUT_PAPER_MODE else 'LIVE'}`"
        )

        # Run all loops concurrently — each agent wrapped in supervisor for auto-restart
        await asyncio.gather(
            dashboard.run(),                              # Web dashboard + API (no supervisor — crash = site down)
            supervise("BulkWatch",    watch.run),
            supervise("BulkStream",   stream.run),
            supervise("HLStream",     hl_stream.run),
            supervise("BulkProfile",  profile.run),
            supervise("BulkSOL",      bulksol.run),
            supervise("BreakoutBot",  bot.run),
            supervise("NewsTrader",   news_trader.run),
            supervise("FundingArb",   funding_arb.run),
            supervise("HLCopier",     hl_copier.run),
            supervise("MacroTrader",  macro_trader.run),
            supervise("WarTrader",    war_trader.run),
            supervise("SMCBot",       smc_bot.run),
            hb_pnl_loop(reporter, dashboard),
            hb_analytics_loop(client, hl_client),
            dice_settle_loop(client, hl_client, reporter),
            evoskill_schedule(),
        )


async def hb_pnl_loop(reporter, dashboard):
    """Broadcast live PnL updates for open HyperBulk positions every 3 seconds."""
    from db import hb_get_open_trades
    while True:
        try:
            open_trades = hb_get_open_trades()
            if open_trades:
                updates = []
                for trade in open_trades:
                    symbol = trade["symbol"]
                    ex = trade.get("exchange", "bulk")
                    current_price = await dashboard._get_price(symbol, ex)
                    if current_price and trade["entry_price"]:
                        if trade["side"] in ("BUY", "buy"):
                            pnl = (current_price - trade["entry_price"]) * trade["size"]
                        else:
                            pnl = (trade["entry_price"] - current_price) * trade["size"]
                        updates.append({
                            "trade_id": trade["id"],
                            "current_price": current_price,
                            "pnl_usd": round(pnl, 2),
                        })
                if updates:
                    await reporter._ws_broadcast("pnl_update", json.dumps(updates))
        except Exception:
            pass
        await asyncio.sleep(3)


async def hb_analytics_loop(bulk_client, hl_client):
    """Poll orderbooks, OI, funding, and candles for analytics + signal engine."""
    from analytics import liquidity, derivatives
    from signal_engine import signals as signal_engine
    from config import WATCHED_SYMBOLS
    while True:
        try:
            for symbol in WATCHED_SYMBOLS:
                # Bulk orderbook
                ob = await bulk_client.get_orderbook(symbol, nlevels=30)
                if ob:
                    bids = [(l[0], l[1]) for l in ob.get("bids", ob.get("levels", {}).get("bids", []))] if isinstance(ob, dict) else []
                    asks = [(l[0], l[1]) for l in ob.get("asks", ob.get("levels", {}).get("asks", []))] if isinstance(ob, dict) else []
                    if bids or asks:
                        liquidity.record_snapshot(symbol, bids, asks)

                # Bulk ticker for OI + funding + price cache
                ticker = await bulk_client.get_ticker(symbol)
                if ticker:
                    last_p = float(ticker.get("lastPrice", 0))
                    if last_p:
                        Dashboard.cache_price(symbol, "bulk", last_p)
                    oi = float(ticker.get("openInterest", 0))
                    if oi:
                        derivatives.record_oi(symbol, oi)
                    bulk_funding = float(ticker.get("fundingRate", 0))

                    # HL funding via allMids (already available)
                    hl_funding = 0.0
                    derivatives.record_funding(symbol, bulk_funding, hl_funding)

                # Fetch candles for signal engine (1m candles for Alpha Rush)
                candles = await bulk_client.get_candles(symbol, interval="1m", limit=200)
                if candles and len(candles) > 30:
                    signal_engine.update_candles(symbol, candles)
        except Exception as e:
            print(f"Analytics loop error: {e}")
        await asyncio.sleep(10)


async def dice_settle_loop(bulk_client, hl_client, reporter):
    """Auto-settle dice rolls every 500ms. Push results via WebSocket instantly."""
    from dice_engine import dice
    from db import settle_dice_game, hb_update_balance
    while True:
        try:
            for roll_id, roll in list(dice.active_rolls.items()):
                if roll.status != "live":
                    continue
                # Fetch price from the roll's exchange
                if roll.exchange == "hyperliquid":
                    from config import HL_SYMBOL_MAP
                    hl_sym = HL_SYMBOL_MAP.get(roll.symbol, roll.symbol)
                    ticker = await hl_client.get_ticker(hl_sym)
                else:
                    ticker = await bulk_client.get_ticker(roll.symbol)

                if not ticker:
                    continue
                price = float(ticker.get("lastPrice") or
                              ticker.get("last_price") or
                              ticker.get("price", 0))
                if not price:
                    continue

                price_str = f"{price}"
                dice.tick(roll_id, price, price_str)

                # Push live face animation during window
                if roll.status == "live":
                    await reporter._ws_broadcast("dice_tick", json.dumps({
                        "roll_id": roll_id,
                        "live_face": roll.live_face,
                        "time_remaining": round(roll.time_remaining, 1),
                        "current_price": price,
                    }))

                if roll.status in ("won", "lost"):
                    settle_dice_game(
                        roll_id, roll.entry_price_str, roll.settlement_price_str,
                        roll.raw_digits, roll.dice_result, roll.won,
                        roll.payout_multiplier, roll.payout_usd, roll.pnl_usd,
                    )
                    if roll.won:
                        hb_update_balance(roll.user_id, roll.payout_usd)
                    await reporter._ws_broadcast("dice_result", json.dumps({
                        "roll_id": roll_id,
                        "dice_result": roll.dice_result,
                        "won": roll.won,
                        "payout_usd": roll.payout_usd,
                        "pnl_usd": roll.pnl_usd,
                        "game_type": roll.game_type,
                        "bet_amount": roll.bet_amount,
                    }))
        except Exception:
            pass
        await asyncio.sleep(0.5)


async def evoskill_schedule():
    """Run EvoSkill improvement loop every 6 hours"""
    while True:
        await asyncio.sleep(6 * 3600)
        print("\n🧬 Starting scheduled EvoSkill improvement loop...")
        try:
            await run_evoskill_loop()
        except Exception as e:
            print(f"EvoSkill loop error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
