"""
BulkMind — Main Orchestrator
Runs BulkWatch + BulkStream + BulkProfile + BulkSOL + BreakoutBot + Dashboard
"""

import asyncio
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
from hyperliquid import HyperliquidClient, HyperliquidExecutor
from dashboard import Dashboard
from evoskill_integration import run_evoskill_loop
from config import (
    BREAKOUT_PAPER_MODE, NEWS_PAPER_MODE, NEWS_EXCHANGES,
    HL_PAPER_MODE, DASHBOARD_PORT,
)


async def main():
    print("=" * 50)
    print("  🧠 BulkMind Starting")
    print(f"  BreakoutBot: {'PAPER' if BREAKOUT_PAPER_MODE else '🔴 LIVE'}")
    print(f"  NewsTrader:  {'PAPER' if NEWS_PAPER_MODE else '🔴 LIVE'}")
    print(f"  Dashboard: http://localhost:{DASHBOARD_PORT}")
    print("=" * 50)

    # Init DB
    init_db()

    # Shared components
    reporter  = Reporter()

    # Init modules
    watch    = BulkWatch(reporter)
    stream   = BulkStream(reporter)
    profile  = BulkProfile(reporter)
    bulksol  = BulkSOL(reporter)
    dashboard = Dashboard(reporter, bulksol)

    async with aiohttp.ClientSession() as session:
        # ── Bulk exchange ────────────────────────────────────
        client   = BulkClient(session)
        executor = BulkExecutor(client, paper=BREAKOUT_PAPER_MODE)
        bot      = BreakoutBot(executor, client, reporter)

        # ── Multi-exchange NewsTrader ────────────────────────
        news_venues = []
        if "bulk" in NEWS_EXCHANGES:
            bulk_news_exec = BulkExecutor(client, paper=NEWS_PAPER_MODE)
            news_venues.append(
                ExchangeVenue("bulk", client, bulk_news_exec, paper=NEWS_PAPER_MODE)
            )
        if "hyperliquid" in NEWS_EXCHANGES:
            hl_client = HyperliquidClient(session)
            hl_exec   = HyperliquidExecutor(hl_client, paper=HL_PAPER_MODE)
            news_venues.append(
                ExchangeVenue("hyperliquid", hl_client, hl_exec, paper=HL_PAPER_MODE)
            )
        news_trader = NewsTrader(news_venues, reporter, session)

        venue_str = ", ".join(v.name for v in news_venues)
        await reporter.send(
            "🟢 *BulkMind Online*\n"
            f"BulkWatch: ✅\n"
            f"BulkStream: ✅\n"
            f"BulkProfile: ✅\n"
            f"BulkSOL: ✅\n"
            f"BreakoutBot: ✅\n"
            f"NewsTrader: ✅ ({venue_str})\n"
            f"Dashboard: ✅\n"
            f"Mode: `{'PAPER' if BREAKOUT_PAPER_MODE else 'LIVE'}`"
        )

        # Run all loops concurrently
        await asyncio.gather(
            dashboard.run(),        # Web dashboard + API
            watch.run(),            # BulkWatch: exchange health
            stream.run(),           # BulkStream: live trade feed
            profile.run(),          # BulkProfile: wallet discovery
            bulksol.run(),          # BulkSOL: staking analytics
            bot.run(),              # BreakoutBot: TA trading agent
            news_trader.run(),      # NewsTrader: LLM news agent
            evoskill_schedule(),    # Periodic EvoSkill improvement
        )


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
