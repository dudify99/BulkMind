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
from hyperliquid import HyperliquidClient, HyperliquidExecutor
from hl_stream import HLStream
from dashboard import Dashboard
from evoskill_integration import run_evoskill_loop
from config import (
    BREAKOUT_PAPER_MODE, NEWS_PAPER_MODE, NEWS_EXCHANGES,
    HL_PAPER_MODE, DASHBOARD_PORT,
    FUNDING_PAPER_MODE, COPIER_PAPER_MODE,
    MACRO_PAPER_MODE, WAR_PAPER_MODE,
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
        executor = BulkExecutor(client, paper=BREAKOUT_PAPER_MODE)
        bot      = BreakoutBot(executor, client, reporter)

        # ── Hyperliquid exchange ─────────────────────────────
        hl_client = HyperliquidClient(session)
        hl_exec   = HyperliquidExecutor(hl_client, paper=HL_PAPER_MODE)

        # ── HyperBulk executors for trade API ────────────────
        hb_bulk_exec = BulkExecutor(client, paper=NEWS_PAPER_MODE)
        hb_hl_exec   = HyperliquidExecutor(hl_client, paper=HL_PAPER_MODE)
        dashboard = Dashboard(reporter, bulksol,
                              bulk_executor=hb_bulk_exec,
                              hl_executor=hb_hl_exec)

        # ── Multi-exchange NewsTrader ────────────────────────
        news_venues = []
        if "bulk" in NEWS_EXCHANGES:
            bulk_news_exec = BulkExecutor(client, paper=NEWS_PAPER_MODE)
            news_venues.append(
                ExchangeVenue("bulk", client, bulk_news_exec, paper=NEWS_PAPER_MODE)
            )
        if "hyperliquid" in NEWS_EXCHANGES:
            news_venues.append(
                ExchangeVenue("hyperliquid", hl_client, hl_exec, paper=HL_PAPER_MODE)
            )
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
            f"Dashboard: ✅\n"
            f"Mode: `{'PAPER' if BREAKOUT_PAPER_MODE else 'LIVE'}`"
        )

        # Run all loops concurrently
        await asyncio.gather(
            dashboard.run(),            # Web dashboard + API
            watch.run(),                # BulkWatch: exchange health
            stream.run(),               # BulkStream: Bulk live trade feed
            hl_stream.run(),            # HLStream: Hyperliquid live trade feed
            profile.run(),              # BulkProfile: wallet discovery
            bulksol.run(),              # BulkSOL: staking analytics
            bot.run(),                  # BreakoutBot: TA trading agent
            news_trader.run(),          # NewsTrader: LLM news agent
            funding_arb.run(),          # FundingArb: funding rate arb
            hl_copier.run(),            # HLCopier: whale copy trading
            macro_trader.run(),         # MacroTrader: economic calendar
            war_trader.run(),           # WarTrader: geopolitical events
            hb_pnl_loop(reporter, dashboard),
            hb_analytics_loop(client, hl_client),
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

                # Bulk ticker for OI + funding
                ticker = await bulk_client.get_ticker(symbol)
                if ticker:
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
