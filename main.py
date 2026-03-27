"""
BulkMind — Main Orchestrator
Runs BulkWatch + BreakoutBot + Dashboard in parallel async loops
"""

import asyncio
import aiohttp
from db import init_db
from reporter import Reporter
from executor import BulkClient, BulkExecutor
from bulk_watch import BulkWatch
from breakout_bot import BreakoutBot
from dashboard import Dashboard
from evoskill_integration import run_evoskill_loop
from config import BREAKOUT_PAPER_MODE, DASHBOARD_PORT


async def main():
    print("=" * 50)
    print("  🧠 BulkMind Starting")
    print(f"  Mode: {'PAPER' if BREAKOUT_PAPER_MODE else '🔴 LIVE'}")
    print(f"  Dashboard: http://localhost:{DASHBOARD_PORT}")
    print("=" * 50)

    # Init DB
    init_db()

    # Shared components
    reporter  = Reporter()
    dashboard = Dashboard(reporter)

    async with aiohttp.ClientSession() as session:
        client   = BulkClient(session)
        executor = BulkExecutor(client, paper=BREAKOUT_PAPER_MODE)

        watch    = BulkWatch(reporter, client=client)
        bot      = BreakoutBot(executor, client, reporter)

        await reporter.send(
            "🟢 *BulkMind Online*\n"
            f"BulkWatch: ✅\n"
            f"BreakoutBot: ✅\n"
            f"Dashboard: ✅\n"
            f"Mode: `{'PAPER' if BREAKOUT_PAPER_MODE else 'LIVE'}`"
        )

        # Run all loops concurrently
        await asyncio.gather(
            dashboard.run(),     # Web dashboard + API
            watch.run(),         # BulkWatch monitoring loop
            bot.run(),           # BreakoutBot trading loop
            evoskill_schedule(), # Periodic EvoSkill improvement
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
