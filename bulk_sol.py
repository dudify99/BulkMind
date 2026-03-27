"""
BulkSOL — Staking & LST Analytics Module
Tracks: supply, holders, APY, DeFi deployments, validator earnings, protocol yields
Data sources: Solana RPC, Sanctum API, Exponent, Solana Compass
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

# Fee structure
BULK_VALIDATOR_FEE_SHARE = 0.125  # 12.5% of taker fees go to validators
STAKING_REWARDS_FEE = 0.025       # 2.5% pool rewards fee
SOL_WITHDRAWAL_FEE = 0.001        # 0.1% withdrawal fee

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

            # Estimate fees: typical taker fee is ~6bps (0.06%)
            taker_fee_rate = 0.0006
            total_fees_24h = total_volume_24h * taker_fee_rate
            validator_share_24h = total_fees_24h * BULK_VALIDATOR_FEE_SHARE

            return {
                "exchange_volume_24h_usd": round(total_volume_24h, 2),
                "estimated_taker_fees_24h_usd": round(total_fees_24h, 2),
                "validator_share_24h_usd": round(validator_share_24h, 2),
                "validator_share_annual_usd": round(validator_share_24h * 365, 2),
                "fee_share_pct": BULK_VALIDATOR_FEE_SHARE * 100,
                "note": "12.5% of taker fees paid in USDC to validators, then to delegators",
            }
        except Exception as e:
            return {"error": str(e)}

    # ── DeFi Protocol Earnings ────────────────────────────────

    def get_protocol_deployments(self) -> list:
        """Return known DeFi protocol deployments with their yield data."""
        return [
            {
                "protocol": "Sanctum (Base Staking)",
                "type": "LST Infrastructure",
                "bulksol_deposited": "All supply (127,257 BulkSOL)",
                "apy": "5.77%",
                "yield_source": "SOL inflation + Jito MEV + Bulk fee share",
                "maturity": None,
                "url": "https://app.sanctum.so/explore/BulkSOL",
                "earnings_note": "Base layer — all BulkSOL earns this",
            },
            {
                "protocol": "Exponent Finance",
                "type": "PT/YT Yield Splitting",
                "bulksol_deposited": "17,943 BulkSOL",
                "deposited_value_usd": 17943 * 92.56,
                "apy": "8.04% (implied)",
                "underlying_yield": "5.70%",
                "pt_price": 0.9815,
                "yt_implied_rate": "8.04%",
                "fixed_rate": "7.32%",
                "vault_fee": "5.50%",
                "maturity": "Jun 20, 2026",
                "url": "https://app.exponent.finance/income/bulksol-20Jun26",
                "earnings_note": "PT holders lock in 7.32% fixed; YT holders get floating yield",
            },
            {
                "protocol": "Loopscale",
                "type": "Leveraged Yield Loops",
                "bulksol_deposited": "Active (exact TBD)",
                "apy": "Variable (leveraged)",
                "maturity": "Jun 2026",
                "url": "https://app.loopscale.com/loops/bulksol-20jun26-sol",
                "earnings_note": "Leveraged yield via order-book lending",
            },
            {
                "protocol": "Bulk Exchange",
                "type": "Perp Trading Collateral",
                "bulksol_deposited": "Unknown (native)",
                "apy": "5.77% (continues earning while used as margin)",
                "url": "https://early.bulk.trade",
                "earnings_note": "BulkSOL earns staking yield even when posted as perp collateral",
            },
            {
                "protocol": "Kamino",
                "type": "Lending/Borrowing",
                "bulksol_deposited": "Listed as integration",
                "apy": "Variable",
                "url": "https://app.kamino.finance",
                "earnings_note": "Supply-side lending yield on BulkSOL",
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

        # Get SOL price from Bulk API ticker
        sol_price_usd = 86.28  # fallback
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
                "holders": 2334,  # from Solflare (updated via snapshots)
            },
            "price": {
                "bulksol_usd": round(bulksol_price, 2),
                "sol_usd": round(sol_price_usd, 2),
                "market_cap_usd": round(bulksol_supply * bulksol_price, 2),
                "liquidity_usd": 12120000,  # from Solflare
            },
            "yield": {
                "bulksol_apy_pct": bulksol_apy,
                "yield_sources": [
                    "SOL inflation rewards (~6.5%)",
                    "Jito MEV tip distributions",
                    f"12.5% of Bulk taker fees (USDC)",
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
