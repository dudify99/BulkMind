"""
BulkProfile — Wallet Discovery & Profiling
Queries POST /account for discovered wallets, builds leaderboard from real data
"""

import asyncio
import aiohttp
from datetime import datetime
from db import (
    log_issue, get_pending_wallets, mark_wallet_profiled,
    upsert_wallet_balance, upsert_trader_record
)
from config import (
    BULK_API_BASE, WALLET_PROFILE_INTERVAL_SEC,
    WALLET_PROFILE_BATCH_SIZE
)
from reporter import Reporter


class BulkProfile:
    def __init__(self, reporter: Reporter):
        self.reporter = reporter

    # ── Main Loop ─────────────────────────────────────────────

    async def run(self):
        """Loop: pick discovered wallets, query POST /account,
        populate traders + wallet_balances tables for leaderboard."""
        print("👛 BulkProfile started — wallet discovery & profiling")
        await asyncio.sleep(30)  # let BulkStream discover some wallets first

        while True:
            try:
                wallets = get_pending_wallets(WALLET_PROFILE_BATCH_SIZE)
                if wallets:
                    print(f"👛 Profiling {len(wallets)} wallets...")

                for wallet in wallets:
                    try:
                        await self._profile_wallet(wallet)
                    except Exception as e:
                        print(f"👛 Error profiling {wallet[:12]}...: {e}")
                    await asyncio.sleep(1)  # rate limit between wallets

            except Exception as e:
                print(f"👛 BulkProfile error: {e}")
                log_issue("HIGH", "SYSTEM",
                          "BulkProfile error", str(e))

            await asyncio.sleep(WALLET_PROFILE_INTERVAL_SEC)

    # ── Profile a Single Wallet ───────────────────────────────

    async def _profile_wallet(self, wallet: str):
        """Query a wallet's full account via POST /account and store results."""
        async with aiohttp.ClientSession() as session:
            url = f"{BULK_API_BASE}/account"

            async with session.post(
                url, json={"type": "fullAccount", "user": wallet},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    mark_wallet_profiled(wallet)
                    return
                data = await resp.json(content_type=None)

            if not data or not isinstance(data, list):
                mark_wallet_profiled(wallet)
                return

            for item in data:
                account = item.get("fullAccount")
                if not account:
                    continue

                margin = account.get("margin", {})
                total_balance = float(margin.get("totalBalance", 0))
                margin_used = float(margin.get("marginUsed", 0))
                unrealized = float(margin.get("unrealizedPnl", 0))
                equity = total_balance + unrealized

                # Store balance
                upsert_wallet_balance(
                    wallet=wallet,
                    balance_usd=total_balance,
                    equity_usd=equity,
                    unrealized_pnl=unrealized,
                    margin_used=margin_used
                )

                # Extract positions and compute per-symbol PnL
                positions = account.get("positions", [])
                for pos in positions:
                    symbol = pos.get("symbol", "")
                    size = float(pos.get("size", 0))
                    rpnl = float(pos.get("realizedPnl", 0))
                    notional = float(pos.get("notional", 0))
                    side = "BUY" if size > 0 else "SELL"
                    pnl_pct = (rpnl / notional * 100) if notional else 0

                    if symbol and notional > 0:
                        upsert_trader_record(
                            wallet=wallet,
                            symbol=symbol,
                            side=side,
                            pnl_usd=rpnl,
                            pnl_pct=pnl_pct,
                            volume_usd=abs(notional),
                            trades_count=1
                        )

                break  # only process first fullAccount item

        mark_wallet_profiled(wallet)
