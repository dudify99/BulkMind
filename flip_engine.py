"""
Flip It — 60-Second Binary Prediction Game for HyperBulk
Pick UP or DOWN. 60 seconds. Win = 1.8x payout. Streaks multiply.
All positions real on-chain via Bulk/Hyperliquid.
"""

import time
import math
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict


class FlipStatus(Enum):
    PENDING = "pending"       # Waiting for countdown
    LIVE = "live"             # Position open, timer running
    WON = "won"               # Correct direction
    LOST = "lost"             # Wrong direction
    EXPIRED = "expired"       # Time ran out (settles based on direction)


class FlipDirection(Enum):
    UP = "up"
    DOWN = "down"


# Streak bonus multipliers
STREAK_MULTIPLIERS = {
    0: 1.8,   # Base payout: 1.8x (house edge ~10%)
    3: 2.0,   # 3-streak: 2x
    5: 2.5,   # 5-streak: 2.5x
    7: 3.0,   # 7-streak: 3x
    10: 5.0,  # 10-streak: 5x (legendary)
}


def get_payout_multiplier(streak: int) -> float:
    """Get payout multiplier based on current win streak."""
    result = 1.8
    for threshold, mult in sorted(STREAK_MULTIPLIERS.items()):
        if streak >= threshold:
            result = mult
    return result


@dataclass
class FlipGame:
    game_id: int
    user_id: int
    symbol: str
    exchange: str
    direction: str            # "up" or "down"
    bet_amount: float
    status: str = "pending"

    # Position
    entry_price: float = 0.0
    exit_price: float = 0.0
    size: float = 0.0
    order_id: str = ""

    # Timing
    duration_sec: int = 60
    started_at: float = 0.0
    ends_at: float = 0.0
    ended_at: float = 0.0
    time_remaining: float = 60.0

    # Result
    price_change_pct: float = 0.0
    won: bool = False
    streak: int = 0
    payout_multiplier: float = 1.8
    payout_usd: float = 0.0
    pnl_usd: float = 0.0

    # Live tracking
    current_price: float = 0.0
    live_pnl: float = 0.0


class FlipEngine:
    """Manages Flip It games."""

    def __init__(self):
        self.active_games: Dict[int, FlipGame] = {}
        self._next_id = 1

    def create_game(self, user_id: int, symbol: str, exchange: str,
                    direction: str, bet_amount: float,
                    streak: int = 0, duration_sec: int = 60) -> FlipGame:
        """Create a new Flip It game."""
        game_id = self._next_id
        self._next_id += 1

        game = FlipGame(
            game_id=game_id,
            user_id=user_id,
            symbol=symbol,
            exchange=exchange,
            direction=direction.lower(),
            bet_amount=bet_amount,
            duration_sec=duration_sec,
            streak=streak,
            payout_multiplier=get_payout_multiplier(streak),
        )
        self.active_games[game_id] = game
        return game

    def start_game(self, game_id: int, entry_price: float,
                   size: float, order_id: str = ""):
        """Called after position is opened on exchange."""
        game = self.active_games.get(game_id)
        if not game:
            return
        game.entry_price = entry_price
        game.current_price = entry_price
        game.size = size
        game.order_id = order_id
        game.started_at = time.time()
        game.ends_at = game.started_at + game.duration_sec
        game.status = "live"

    def tick(self, game_id: int, current_price: float) -> Optional[FlipGame]:
        """Process a price tick. Auto-settles when time expires."""
        game = self.active_games.get(game_id)
        if not game or game.status != "live":
            return game

        game.current_price = current_price
        now = time.time()
        game.time_remaining = max(0, game.ends_at - now)

        # Live PnL
        if game.entry_price > 0:
            change = (current_price - game.entry_price) / game.entry_price
            if game.direction == "up":
                game.live_pnl = change * game.bet_amount * 10  # Simple 10x for visual
            else:
                game.live_pnl = -change * game.bet_amount * 10

        # Auto-settle when timer hits zero
        if game.time_remaining <= 0:
            self.settle(game_id, current_price)

        return game

    def settle(self, game_id: int, exit_price: float) -> Optional[FlipGame]:
        """Settle a game — determine win/loss."""
        game = self.active_games.get(game_id)
        if not game or game.status not in ("live", "pending"):
            return game

        game.exit_price = exit_price
        game.ended_at = time.time()

        # Calculate price change
        if game.entry_price > 0:
            game.price_change_pct = ((exit_price - game.entry_price) / game.entry_price) * 100

        # Did the player win?
        price_went_up = exit_price > game.entry_price
        if game.direction == "up":
            game.won = price_went_up
        else:
            game.won = not price_went_up

        # Handle exact same price = loss (house wins ties)
        if exit_price == game.entry_price:
            game.won = False

        # Calculate payout
        if game.won:
            game.status = "won"
            game.payout_usd = round(game.bet_amount * game.payout_multiplier, 2)
            game.pnl_usd = round(game.payout_usd - game.bet_amount, 2)
        else:
            game.status = "lost"
            game.payout_usd = 0.0
            game.pnl_usd = -game.bet_amount

        # Remove from active
        if game_id in self.active_games:
            del self.active_games[game_id]

        return game

    def to_dict(self, game: FlipGame) -> dict:
        """Serialize game state for API."""
        return {
            "game_id": game.game_id,
            "user_id": game.user_id,
            "symbol": game.symbol,
            "exchange": game.exchange,
            "direction": game.direction,
            "bet_amount": game.bet_amount,
            "status": game.status,
            "entry_price": game.entry_price,
            "exit_price": game.exit_price,
            "current_price": game.current_price,
            "size": game.size,
            "duration_sec": game.duration_sec,
            "time_remaining": round(game.time_remaining, 1),
            "price_change_pct": round(game.price_change_pct, 4),
            "won": game.won,
            "streak": game.streak,
            "payout_multiplier": game.payout_multiplier,
            "payout_usd": game.payout_usd,
            "pnl_usd": game.pnl_usd,
            "live_pnl": round(game.live_pnl, 2),
            "next_streak_mult": get_payout_multiplier(game.streak + 1 if game.won else 0),
        }


# Global instance
flip = FlipEngine()
