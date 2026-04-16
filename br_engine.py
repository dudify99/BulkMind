"""
Battle Royale — Last Trader Standing for HyperBulk
Everyone opens same direction. SL tightens every 30s. Last alive wins the pot.
All positions real on-chain via Bulk/Hyperliquid.
"""

import time
import math
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, List


class BRStatus(Enum):
    LOBBY = "lobby"           # Waiting for players
    COUNTDOWN = "countdown"   # 10s countdown before start
    LIVE = "live"             # Game running, eliminations happening
    SETTLED = "settled"       # Winner(s) determined
    CANCELLED = "cancelled"   # Not enough players


class PlayerStatus(Enum):
    ALIVE = "alive"
    ELIMINATED = "eliminated"
    WINNER = "winner"
    CASHED_OUT = "cashed_out"


@dataclass
class BRConfig:
    symbol: str = "BTC-USD"
    direction: str = "long"       # Everyone goes long (or short)
    entry_fee: float = 10.0
    min_players: int = 3
    max_players: int = 100
    rake_pct: float = 10.0
    lobby_timeout_sec: int = 120  # Auto-start after 2 min if min_players met
    # Shrinking stop-loss config
    initial_sl_pct: float = 2.0   # Start with 2% SL distance
    shrink_interval_sec: int = 30  # Tighten SL every 30 seconds
    shrink_amount_pct: float = 0.1 # Tighten by 0.1% each interval
    min_sl_pct: float = 0.2       # Minimum SL distance (0.2%)
    # Payout structure
    payouts: Dict[int, float] = field(default_factory=lambda: {
        1: 0.50,   # 1st: 50%
        2: 0.30,   # 2nd: 30%
        3: 0.20,   # 3rd: 20%
    })


@dataclass
class BRPlayer:
    user_id: int
    wallet: str
    username: str
    status: str = "alive"
    entry_price: float = 0.0
    eliminated_price: float = 0.0
    eliminated_at: float = 0.0
    rank: int = 0
    payout_usd: float = 0.0
    survival_sec: float = 0.0
    order_id: str = ""
    # Per-player SL offset (0.0 to 0.1%) to prevent simultaneous elimination
    sl_offset_pct: float = 0.0


@dataclass
class BRGame:
    game_id: int
    config: BRConfig
    status: str = "lobby"
    players: List[BRPlayer] = field(default_factory=list)
    # Timing
    created_at: float = 0.0
    started_at: float = 0.0
    ended_at: float = 0.0
    elapsed_sec: float = 0.0
    # Market
    entry_price: float = 0.0
    current_price: float = 0.0
    current_sl_pct: float = 2.0
    current_sl_price: float = 0.0
    # Pot
    pot_usd: float = 0.0
    rake_usd: float = 0.0
    prize_pool_usd: float = 0.0
    # Elimination log: [(timestamp, username, price, rank)]
    eliminations: list = field(default_factory=list)
    # Shrink counter
    last_shrink_at: float = 0.0
    shrink_count: int = 0


class BattleRoyaleEngine:
    """Manages Battle Royale games."""

    def __init__(self):
        self.games: Dict[int, BRGame] = {}
        self._next_id = 1

    def create_game(self, config: BRConfig = None) -> BRGame:
        if config is None:
            config = BRConfig()
        game_id = self._next_id
        self._next_id += 1
        game = BRGame(
            game_id=game_id,
            config=config,
            created_at=time.time(),
            current_sl_pct=config.initial_sl_pct,
        )
        self.games[game_id] = game
        return game

    def join_game(self, game_id: int, user_id: int, wallet: str,
                  username: str) -> Optional[str]:
        """Join a game. Returns error string or None on success."""
        game = self.games.get(game_id)
        if not game:
            return "Game not found"
        if game.status != "lobby":
            return "Game already started"
        if len(game.players) >= game.config.max_players:
            return "Game is full"
        for p in game.players:
            if p.user_id == user_id:
                return "Already in this game"

        game.players.append(BRPlayer(
            user_id=user_id, wallet=wallet, username=username,
        ))
        game.pot_usd = len(game.players) * game.config.entry_fee
        return None

    def start_game(self, game_id: int, entry_price: float):
        """Start the game — all players enter at same price."""
        game = self.games.get(game_id)
        if not game or game.status != "lobby":
            return
        if len(game.players) < game.config.min_players:
            game.status = "cancelled"
            return

        game.status = "live"
        game.started_at = time.time()
        game.last_shrink_at = game.started_at
        game.entry_price = entry_price
        game.current_price = entry_price

        # Calculate initial SL price
        if game.config.direction == "long":
            game.current_sl_price = entry_price * (1 - game.config.initial_sl_pct / 100)
        else:
            game.current_sl_price = entry_price * (1 + game.config.initial_sl_pct / 100)

        for p in game.players:
            p.entry_price = entry_price
            p.status = "alive"
            # Random SL offset (0.00% to 0.10%) so players eliminate one at a time
            p.sl_offset_pct = random.uniform(0.0, 0.10)

    def tick(self, game_id: int, current_price: float) -> Optional[BRGame]:
        """Process a price tick. Checks eliminations and SL shrinking."""
        game = self.games.get(game_id)
        if not game or game.status != "live":
            return game

        game.current_price = current_price
        now = time.time()
        game.elapsed_sec = now - game.started_at

        # Shrink SL if interval passed
        if now - game.last_shrink_at >= game.config.shrink_interval_sec:
            game.shrink_count += 1
            game.last_shrink_at = now
            game.current_sl_pct = max(
                game.config.min_sl_pct,
                game.current_sl_pct - game.config.shrink_amount_pct,
            )
            # Recalculate SL price from entry
            if game.config.direction == "long":
                game.current_sl_price = game.entry_price * (1 - game.current_sl_pct / 100)
            else:
                game.current_sl_price = game.entry_price * (1 + game.current_sl_pct / 100)

        # Check eliminations — each player has a unique SL (base + offset)
        # to prevent all players being eliminated on the same tick.
        alive = [p for p in game.players if p.status == "alive"]
        # Sort by tightest SL first so eliminations happen in order
        alive.sort(key=lambda p: p.sl_offset_pct)
        for player in alive:
            # Player's individual SL = base SL adjusted by their offset
            player_sl_pct = game.current_sl_pct - player.sl_offset_pct
            player_sl_pct = max(game.config.min_sl_pct * 0.5, player_sl_pct)
            if game.config.direction == "long":
                player_sl_price = game.entry_price * (1 - player_sl_pct / 100)
                eliminated = current_price <= player_sl_price
            else:
                player_sl_price = game.entry_price * (1 + player_sl_pct / 100)
                eliminated = current_price >= player_sl_price

            if eliminated:
                player.status = "eliminated"
                player.eliminated_price = current_price
                player.eliminated_at = now
                player.survival_sec = now - game.started_at
                remaining = len([p for p in game.players if p.status == "alive"])
                player.rank = remaining + 1
                game.eliminations.append({
                    "time": now,
                    "username": player.username,
                    "price": current_price,
                    "rank": player.rank,
                    "survival_sec": round(player.survival_sec, 1),
                })

        # Check if game over (1 or 0 alive)
        alive = [p for p in game.players if p.status == "alive"]
        if len(alive) <= 1:
            self._settle(game, alive)

        return game

    def _settle(self, game: BRGame, alive: list):
        """Settle the game — distribute prizes."""
        # Mark winner
        for p in alive:
            p.status = "winner"
            p.rank = 1
            p.survival_sec = time.time() - game.started_at

        game.status = "settled"
        game.ended_at = time.time()
        game.rake_usd = round(game.pot_usd * game.config.rake_pct / 100, 2)
        game.prize_pool_usd = round(game.pot_usd - game.rake_usd, 2)

        # Sort by rank
        ranked = sorted(game.players, key=lambda p: p.rank if p.rank > 0 else 999)
        for p in ranked:
            if p.rank in game.config.payouts:
                p.payout_usd = round(game.prize_pool_usd * game.config.payouts[p.rank], 2)

    def get_state(self, game_id: int) -> Optional[dict]:
        """Serialize game state for API."""
        game = self.games.get(game_id)
        if not game:
            return None

        alive_count = len([p for p in game.players if p.status == "alive"])
        total = len(game.players)

        return {
            "game_id": game.game_id,
            "status": game.status,
            "symbol": game.config.symbol,
            "direction": game.config.direction,
            "entry_fee": game.config.entry_fee,
            "player_count": total,
            "alive_count": alive_count,
            "max_players": game.config.max_players,
            "pot_usd": game.pot_usd,
            "prize_pool_usd": game.prize_pool_usd,
            "rake_usd": game.rake_usd,
            "entry_price": game.entry_price,
            "current_price": game.current_price,
            "current_sl_pct": round(game.current_sl_pct, 2),
            "current_sl_price": round(game.current_sl_price, 2),
            "elapsed_sec": round(game.elapsed_sec, 1),
            "shrink_count": game.shrink_count,
            "next_shrink_in": round(max(0,
                game.config.shrink_interval_sec - (time.time() - game.last_shrink_at)
            ), 1) if game.status == "live" else 0,
            "players": [
                {
                    "username": p.username,
                    "wallet": p.wallet[:6] + "..." + p.wallet[-4:] if len(p.wallet) > 10 else p.wallet,
                    "status": p.status,
                    "rank": p.rank,
                    "payout_usd": p.payout_usd,
                    "survival_sec": round(p.survival_sec, 1),
                }
                for p in sorted(game.players, key=lambda p: p.rank if p.rank > 0 else 999)
            ],
            "eliminations": game.eliminations[-20:],  # Last 20 eliminations
        }

    def get_active_games(self) -> list:
        return [
            self.get_state(gid)
            for gid, g in self.games.items()
            if g.status in ("lobby", "live")
        ]


# Global instance
battle_royale = BattleRoyaleEngine()
