"""
BulkSOL — Staking & LST Analytics Module
Tracks: supply, holders, APY, DeFi deployments, validator earnings, protocol yields

Data sources & citations:
  - Token mint: https://app.sanctum.so/explore/BulkSOL
  - Stake pool: https://solanacompass.com/stake-pools/3aUmJDNpMHjkxunQEkHTj2chzyryKoH2uQj6YACLD174
  - Supply: Solana RPC getTokenSupply (live)
  - APY & SOL value: Sanctum API https://extra-api.sanctum.so/v1/apy/latest (live)
  - Fee structure: Solana Compass stake pool page (verified 2026-03-27)
  - 12.5% validator fee share: https://chainflow.io/bulk-exchange-the-architecture-that-pays-everyone-to-win/
  - Taker fee ~6bps: ESTIMATED — Bulk docs do not publish exact fee schedule
  - Exponent data: https://app.exponent.finance/income/bulksol-20Jun26 (fetched 2026-03-27)
  - Loopscale: https://app.loopscale.com/loops/bulksol-20jun26-sol
  - Holder count: Solflare (point-in-time, not live — no free API for holder count)
  - Liquidity: Solflare (point-in-time, not live)
"""

import asyncio
import aiohttp
import json
from datetime import datetime
from db import get_conn, log_issue
from config import BULK_API_BASE
from reporter import Reporter

# ── Constants ─────────────────────────────────────────────────
BULKSOL_MINT = "BULKoNSGzxtCqzwTvg5hFJg8fx6dqZRScyXe5LYMfxrn"
BULKSOL_STAKE_POOL = "3aUmJDNpMHjkxunQEkHTj2chzyryKoH2uQj6YACLD174"
BULKSOL_VOTE_ACCOUNT = "votem3UdGx5xWFbY9EFbyZ1X2pBuswfR5yd2oB3JAaj"

SOLANA_RPC = "https://api.mainnet-beta.solana.com"
SANCTUM_API = "https://extra-api.sanctum.so/v1"

# Competitor LSTs for comparison
LST_COMPETITORS = {
    "mSOL":    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    "jitoSOL": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
    "jupSOL":  "jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v",
}

# Known DeFi protocol addresses holding BulkSOL
DEFI_PROTOCOLS = {
    "Exponent Finance": {
        "description": "PT/YT yield splitting",
        "maturity": "Jun 20, 2026",
        "url": "https://app.exponent.finance/income/bulksol-20Jun26",
    },
    "Loopscale": {
        "description": "Leveraged yield loops",
        "maturity": "Jun 2026",
        "url": "https://app.loopscale.com/loops/bulksol-20jun26-sol",
    },
    "Bulk Exchange": {
        "description": "Perp trading collateral (earns yield while margined)",
        "url": "https://early.bulk.trade",
    },
    "Kamino": {
        "description": "Lending/borrowing collateral",
        "url": "https://app.kamino.finance",
    },
    "Sanctum": {
        "description": "LST infrastructure (mint/redeem/swap)",
        "url": "https://app.sanctum.so/explore/BulkSOL",
    },
}

# Fee structure — verified via Solana Compass stake pool page (2026-03-27)
# https://solanacompass.com/stake-pools/3aUmJDNpMHjkxunQEkHTj2chzyryKoH2uQj6YACLD174
STAKING_REWARDS_FEE = 0.025       # 2.5% pool rewards fee ✅ verified
SOL_WITHDRAWAL_FEE = 0.001        # 0.1% withdrawal fee ✅ verified
# SOL deposit fee: 0% ✅ verified
# Stake withdrawal fee: 0.1% ✅ verified

# Validator fee share — from Chainflow article:
# https://chainflow.io/bulk-exchange-the-architecture-that-pays-everyone-to-win/
# "BULK distributes 12.5% of all taker fees directly to validators in USDC"
BULK_VALIDATOR_FEE_SHARE = 0.125  # ✅ cited

# ⚠️ ESTIMATED: taker fee rate not published in Bulk docs
# Using 6bps (0.06%) as industry-standard perp DEX taker fee
# Actual fee may differ — update when Bulk publishes fee schedule
ESTIMATED_TAKER_FEE_RATE = 0.0006

SNAPSHOT_INTERVAL_SEC = 300  # 5 minutes between snapshots


class BulkSOL:
    def __init__(self, reporter: Reporter):
        self.reporter = reporter
        self._last_supply = None
        self._last_holders = None

    # ── Solana RPC Queries ────────────────────────────────────

    async def _rpc_call(self, session: aiohttp.ClientSession,
                        method: str, params: list) -> dict:
        """Make a Solana RPC call."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        try:
            async with session.post(
                SOLANA_RPC, json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json(content_type=None)
                if "error" in data:
                    return {"error": data["error"]}
                return data.get("result", {})
        except Exception as e:
            return {"error": str(e)}

    async def get_supply(self, session: aiohttp.ClientSession) -> dict:
        """Get BulkSOL total supply from Solana RPC."""
        result = await self._rpc_call(session, "getTokenSupply", [BULKSOL_MINT])
        if "error" in result:
            return {"error": result["error"]}
        value = result.get("value", {})
        return {
            "supply_raw": int(value.get("amount", 0)),
            "supply": float(value.get("uiAmount", 0)),
            "decimals": value.get("decimals", 9),
        }

    async def get_largest_holders(self, session: aiohttp.ClientSession) -> list:
        """Get top 20 BulkSOL holders from Solana RPC."""
        result = await self._rpc_call(
            session, "getTokenLargestAccounts", [BULKSOL_MINT]
        )
        if "error" in result:
            return []
        accounts = result.get("value", [])
        holders = []
        for acc in accounts:
            holders.append({
                "address": acc.get("address", ""),
                "amount": float(acc.get("uiAmount", 0)),
                "amount_raw": int(acc.get("amount", 0)),
            })
        return holders

    # ── Sanctum API Queries ───────────────────────────────────

    async def get_apy(self, session: aiohttp.ClientSession) -> dict:
        """Get BulkSOL APY from Sanctum, with competitor comparison."""
        all_lsts = {BULKSOL_MINT: "BulkSOL"}
        all_lsts.update({v: k for k, v in LST_COMPETITORS.items()})

        params = "&".join(f"lst={mint}" for mint in all_lsts.keys())
        url = f"{SANCTUM_API}/apy/latest?{params}"

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json(content_type=None)
                apys = data.get("apys", {})

                result = {}
                for mint, name in all_lsts.items():
                    if mint in apys:
                        result[name] = round(apys[mint] * 100, 2)

                return result
        except Exception as e:
            return {"error": str(e)}

    async def get_sol_value(self, session: aiohttp.ClientSession) -> dict:
        """Get BulkSOL/SOL exchange rate from Sanctum."""
        all_lsts = {BULKSOL_MINT: "BulkSOL"}
        all_lsts.update({v: k for k, v in LST_COMPETITORS.items()})

        params = "&".join(f"lst={mint}" for mint in all_lsts.keys())
        url = f"{SANCTUM_API}/sol-value/current?{params}"

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json(content_type=None)
                values = data.get("solValues", {})

                result = {}
                for mint, name in all_lsts.items():
                    if mint in values:
                        # Value is in lamports per 1 token (with 9 decimals)
                        raw = int(values[mint])
                        sol_value = raw / 1e9
                        result[name] = round(sol_value, 6)

                return result
        except Exception as e:
            return {"error": str(e)}

    # ── Validator Earnings Estimation ─────────────────────────

    async def estimate_validator_earnings(self, session: aiohttp.ClientSession) -> dict:
        """Estimate validator earnings from Bulk exchange fee share.
        12.5% of ALL taker fees → validators → delegators (stakers)."""
        try:
            # Get 24h volume from Bulk exchange
            async with session.get(
                f"{BULK_API_BASE}/stats?period=1d",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json(content_type=None)

            total_volume_24h = 0
            markets = data if isinstance(data, list) else data.get("markets", [])

            if isinstance(data, dict) and "totalVolume" in data:
                total_volume_24h = float(data["totalVolume"])
            else:
                for market in markets:
                    vol = float(market.get("volumeUsd", 0) or market.get("volume_value", 0))
                    total_volume_24h += vol

            # ⚠️ ESTIMATED taker fee — see ESTIMATED_TAKER_FEE_RATE comment
            taker_fee_rate = ESTIMATED_TAKER_FEE_RATE
            total_fees_24h = total_volume_24h * taker_fee_rate
            validator_share_24h = total_fees_24h * BULK_VALIDATOR_FEE_SHARE

            return {
                "exchange_volume_24h_usd": round(total_volume_24h, 2),
                "estimated_taker_fees_24h_usd": round(total_fees_24h, 2),
                "validator_share_24h_usd": round(validator_share_24h, 2),
                "validator_share_annual_usd": round(validator_share_24h * 365, 2),
                "fee_share_pct": BULK_VALIDATOR_FEE_SHARE * 100,
                "note": "12.5% of taker fees paid in USDC to validators, then to delegators",
                "fee_share_citation": "https://chainflow.io/bulk-exchange-the-architecture-that-pays-everyone-to-win/",
                "taker_fee_note": "⚠️ ESTIMATED at 6bps — Bulk does not publish exact fee schedule",
            }
        except Exception as e:
            return {"error": str(e)}

    # ── DeFi Protocol Earnings ────────────────────────────────

    def get_protocol_deployments(self) -> list:
        """Return known DeFi protocol deployments with their yield data.
        Each entry includes a 'citation' field for data provenance.
        ⚠️ = point-in-time snapshot (may be stale), ✅ = live/verified
        """
        return [
            {
                "protocol": "Sanctum (Base Staking)",
                "type": "LST Infrastructure",
                "bulksol_deposited": None,  # = total supply, fetched live
                "apy": None,  # fetched live from Sanctum API
                "yield_source": "SOL inflation + Jito MEV + Bulk fee share",
                "maturity": None,
                "url": "https://app.sanctum.so/explore/BulkSOL",
                "earnings_note": "Base layer — all BulkSOL earns this",
                "citation": "https://app.sanctum.so/explore/BulkSOL",
                "data_freshness": "live",
            },
            {
                "protocol": "Exponent Finance",
                "type": "PT/YT Yield Splitting",
                "bulksol_deposited": 17943,  # ⚠️ fetched 2026-03-27T00:50Z
                "apy": "8.04% (implied)",    # ⚠️ fetched 2026-03-27T00:50Z
                "underlying_yield": "5.70%", # ⚠️ fetched 2026-03-27T00:50Z
                "pt_price": 0.9815,          # ⚠️ fetched 2026-03-27T00:50Z
                "yt_implied_rate": "8.04%",  # ⚠️ fetched 2026-03-27T00:50Z
                "fixed_rate": "7.32%",       # ⚠️ fetched 2026-03-27T00:50Z
                "vault_fee": "5.50%",        # ⚠️ fetched 2026-03-27T00:50Z
                "maturity": "Jun 20, 2026",
                "url": "https://app.exponent.finance/income/bulksol-20Jun26",
                "earnings_note": "PT holders lock in 7.32% fixed; YT holders get floating yield",
                "citation": "https://app.exponent.finance/income/bulksol-20Jun26",
                "data_freshness": "snapshot_2026-03-27",
            },
            {
                "protocol": "Loopscale",
                "type": "Leveraged Yield Loops",
                "bulksol_deposited": None,  # page was client-rendered, couldn't scrape
                "apy": None,  # variable, depends on leverage
                "maturity": "Jun 2026",
                "url": "https://app.loopscale.com/loops/bulksol-20jun26-sol",
                "earnings_note": "Leveraged yield via order-book lending",
                "citation": "https://app.loopscale.com/loops/bulksol-20jun26-sol",
                "data_freshness": "unverified — page not scrapable",
            },
            {
                "protocol": "Bulk Exchange",
                "type": "Perp Trading Collateral",
                "bulksol_deposited": None,  # not exposed via API
                "apy": None,  # same as base staking APY
                "url": "https://early.bulk.trade",
                "earnings_note": "BulkSOL earns staking yield even when posted as perp collateral",
                "citation": "https://chainflow.io/bulk-exchange-the-architecture-that-pays-everyone-to-win/",
                "data_freshness": "architecture confirmed, no live deposit data",
            },
            {
                "protocol": "Kamino",
                "type": "Lending/Borrowing",
                "bulksol_deposited": None,  # mentioned in Chainflow article, not verified on Kamino
                "apy": None,
                "url": "https://app.kamino.finance",
                "earnings_note": "Mentioned as composable collateral integration",
                "citation": "https://chainflow.io/bulk-exchange-the-architecture-that-pays-everyone-to-win/",
                "data_freshness": "mentioned in article, not independently verified",
            },
        ]

    # ── Snapshot & Persistence ────────────────────────────────

    def save_snapshot(self, supply: float, sol_value: float,
                      apy: float, holders: int, sol_price_usd: float,
                      validator_earnings_24h: float):
        """Save a point-in-time snapshot for historical charts."""
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bulksol_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ts          TEXT NOT NULL,
                supply      REAL NOT NULL,
                sol_value   REAL NOT NULL,
                apy_pct     REAL NOT NULL,
                holders     INTEGER,
                price_usd   REAL,
                market_cap_usd REAL,
                total_sol_staked REAL,
                validator_earnings_24h_usd REAL
            )
        """)
        price_usd = sol_value * sol_price_usd
        market_cap = supply * price_usd
        total_sol = supply * sol_value

        conn.execute("""
            INSERT INTO bulksol_snapshots
            (ts, supply, sol_value, apy_pct, holders, price_usd,
             market_cap_usd, total_sol_staked, validator_earnings_24h_usd)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            datetime.utcnow().isoformat(), supply, sol_value, apy,
            holders, round(price_usd, 2), round(market_cap, 2),
            round(total_sol, 2), round(validator_earnings_24h, 2)
        ))
        conn.commit()
        conn.close()

    def get_snapshots(self, hours: int = 168) -> list:
        """Get historical snapshots for charts."""
        conn = get_conn()
        # Ensure table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bulksol_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL, supply REAL, sol_value REAL,
                apy_pct REAL, holders INTEGER, price_usd REAL,
                market_cap_usd REAL, total_sol_staked REAL,
                validator_earnings_24h_usd REAL
            )
        """)
        rows = conn.execute("""
            SELECT * FROM bulksol_snapshots
            WHERE ts > datetime('now', ?)
            ORDER BY ts ASC
        """, (f"-{hours} hours",)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Full Dashboard Data ───────────────────────────────────

    async def get_full_stats(self, session: aiohttp.ClientSession) -> dict:
        """Aggregate all BulkSOL data for dashboard display."""
        # Run all queries concurrently
        supply_task = self.get_supply(session)
        apy_task = self.get_apy(session)
        sol_value_task = self.get_sol_value(session)
        earnings_task = self.estimate_validator_earnings(session)

        supply, apys, sol_values, earnings = await asyncio.gather(
            supply_task, apy_task, sol_value_task, earnings_task
        )

        bulksol_supply = supply.get("supply", 0) if "error" not in supply else 0
        bulksol_apy = apys.get("BulkSOL", 0) if "error" not in apys else 0
        bulksol_sol_value = sol_values.get("BulkSOL", 1.0) if "error" not in sol_values else 1.0

        # Get SOL price from Bulk API ticker (live)
        sol_price_usd = 0  # no fallback — must fetch live
        try:
            async with session.get(
                f"{BULK_API_BASE}/ticker/SOL-USD",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                ticker = await resp.json(content_type=None)
                if isinstance(ticker, dict) and "last" in ticker:
                    sol_price_usd = float(ticker["last"])
                elif isinstance(ticker, dict) and "data" in ticker:
                    data = ticker["data"]
                    if isinstance(data, list) and data:
                        sol_price_usd = float(data[0].get("last", sol_price_usd))
        except Exception:
            pass

        bulksol_price = bulksol_sol_value * sol_price_usd

        result = {
            "token": {
                "mint": BULKSOL_MINT,
                "name": "BULK Staked SOL",
                "symbol": "BulkSOL",
                "launched": "2025-02-24",
                "program": "SanctumSplMulti",
                "stake_pool": BULKSOL_STAKE_POOL,
            },
            "supply": {
                "total_bulksol": round(bulksol_supply, 2),
                "total_sol_staked": round(bulksol_supply * bulksol_sol_value, 2),
                "sol_value": bulksol_sol_value,
                # ⚠️ Holder count: no free live API available
                # Snapshot from Solflare 2026-03-27. Updates require manual check or paid API.
                "holders": 2334,
                "holders_citation": "https://www.solflare.com/prices/bulk-staked-sol/BULKoNSGzxtCqzwTvg5hFJg8fx6dqZRScyXe5LYMfxrn/",
                "holders_freshness": "snapshot_2026-03-27",
            },
            "price": {
                "bulksol_usd": round(bulksol_price, 2),
                "sol_usd": round(sol_price_usd, 2),
                "market_cap_usd": round(bulksol_supply * bulksol_price, 2),
                # ⚠️ Liquidity: from Solflare, point-in-time, no live API
                "liquidity_usd": 12120000,
                "liquidity_citation": "https://www.solflare.com/prices/bulk-staked-sol/BULKoNSGzxtCqzwTvg5hFJg8fx6dqZRScyXe5LYMfxrn/",
                "liquidity_freshness": "snapshot_2026-03-27",
            },
            "yield": {
                "bulksol_apy_pct": bulksol_apy,
                "bulksol_apy_citation": "Sanctum API /v1/apy/latest (live)",
                "yield_sources": [
                    {"source": "SOL inflation rewards", "citation": "Solana staking baseline"},
                    {"source": "Jito MEV tip distributions", "citation": "Bulk runs Bulk-agave (forked Jito-agave)"},
                    {"source": "12.5% of Bulk taker fees (USDC)", "citation": "https://chainflow.io/bulk-exchange-the-architecture-that-pays-everyone-to-win/"},
                ],
                "competitor_apys": {k: v for k, v in apys.items() if k != "BulkSOL"} if "error" not in apys else {},
            },
            "fees": {
                "rewards_fee_pct": STAKING_REWARDS_FEE * 100,
                "sol_deposit_fee_pct": 0,
                "sol_withdrawal_fee_pct": SOL_WITHDRAWAL_FEE * 100,
                "validator_fee_share_pct": BULK_VALIDATOR_FEE_SHARE * 100,
            },
            "validator_earnings": earnings if "error" not in earnings else {},
            "defi_deployments": self.get_protocol_deployments(),
            "staking_page": "https://early.bulk.trade/stake",
            "chart_data": {
                "snapshots": self.get_snapshots(hours=168),
            },
            "fetched_at": datetime.utcnow().isoformat(),
        }

        # Save snapshot for historical tracking
        validator_earnings_24h = earnings.get("validator_share_24h_usd", 0) if "error" not in earnings else 0
        if bulksol_supply > 0:
            self.save_snapshot(
                supply=bulksol_supply,
                sol_value=bulksol_sol_value,
                apy=bulksol_apy,
                holders=2334,
                sol_price_usd=sol_price_usd,
                validator_earnings_24h=validator_earnings_24h,
            )

        return result

    # ── Main Loop ─────────────────────────────────────────────

    async def run(self):
        """Periodic BulkSOL stats collection loop."""
        print("🪙  BulkSOL analytics started")
        await asyncio.sleep(10)  # let other services start first

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    stats = await self.get_full_stats(session)
                    supply = stats["supply"]["total_bulksol"]
                    apy = stats["yield"]["bulksol_apy_pct"]
                    price = stats["price"]["bulksol_usd"]
                    earnings = stats.get("validator_earnings", {})
                    val_24h = earnings.get("validator_share_24h_usd", "N/A")

                    print(f"🪙  BulkSOL: {supply:,.0f} supply | "
                          f"APY {apy}% | ${price:.2f} | "
                          f"Validator earnings 24h: ${val_24h}")

                    # Alert on significant changes
                    if self._last_supply and supply > 0:
                        change_pct = (supply - self._last_supply) / self._last_supply * 100
                        if abs(change_pct) > 5:
                            await self.reporter.alert(
                                f"🪙 BulkSOL Supply Change: {change_pct:+.1f}%\n"
                                f"Previous: {self._last_supply:,.0f}\n"
                                f"Current: {supply:,.0f}"
                            )
                    self._last_supply = supply

                except Exception as e:
                    print(f"🪙  BulkSOL error: {e}")
                    log_issue("MEDIUM", "SYSTEM",
                              "BulkSOL analytics error", str(e))

                await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)
