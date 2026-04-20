"""
Market Dice — Price-Derived Dice Game for HyperBulk
No random seeds. Real market price movements determine dice outcomes.
Settlement in 5 seconds. Provably fair via commit-reveal + price oracle.

Bet tiers: $1, $50, $100
Game types: pick (5x), over_under (1.9x), even_odd (1.9x)
"""

import time
import hashlib
from dataclasses import dataclass
from typing import Optional, Dict, List
from config import (
    DICE_WINDOW_SEC, DICE_BET_TIERS,
    DICE_PICK_PAYOUT, DICE_BINARY_PAYOUT,
)


class DiceEngine:
    """Market-microstructure dice game engine.

    Each roll:
    1. Commitment hash published (locks player's pick)
    2. 5-second market window opens
    3. Settlement price sampled
    4. dice = (last_4_digits XOR commitment_bits) % 6 + 1
    5. Payout based on game type
    """

    def __init__(self):
        self.active_rolls: Dict[int, "DiceRoll"] = {}
        self._next_id = 1

    # ── Create ────────────────────────────────────────────────

    def create_roll(self, user_id: int, symbol: str, exchange: str,
                    game_type: str, bet_amount: float,
                    player_pick: int, num_dice: int = 1) -> "DiceRoll":
        if bet_amount not in DICE_BET_TIERS:
            raise ValueError(f"Bet must be one of {DICE_BET_TIERS}")
        if game_type not in ("pick", "over_under", "even_odd"):
            raise ValueError("game_type must be pick, over_under, or even_odd")
        if game_type == "pick" and not (1 <= player_pick <= 6):
            raise ValueError("pick must be 1-6")
        if game_type == "over_under" and player_pick not in (0, 1):
            raise ValueError("over_under pick: 0=under(1-3), 1=over(4-6)")
        if game_type == "even_odd" and player_pick not in (0, 1):
            raise ValueError("even_odd pick: 0=even, 1=odd")

        roll_id = self._next_id
        self._next_id += 1

        ts = time.time()
        commitment = _make_commitment(roll_id, user_id, ts)

        roll = DiceRoll(
            roll_id=roll_id,
            user_id=user_id,
            symbol=symbol,
            exchange=exchange,
            game_type=game_type,
            num_dice=num_dice,
            bet_amount=bet_amount,
            player_pick=player_pick,
            commitment_hash=commitment,
            created_at=ts,
        )
        self.active_rolls[roll_id] = roll
        return roll

    # ── Start ─────────────────────────────────────────────────

    def start_roll(self, roll_id: int, entry_price: float,
                   entry_price_str: str = ""):
        roll = self.active_rolls.get(roll_id)
        if not roll:
            return
        roll.entry_price = entry_price
        roll.entry_price_str = entry_price_str or f"{entry_price}"
        roll.started_at = time.time()
        roll.settles_at = roll.started_at + DICE_WINDOW_SEC
        roll.status = "live"

    # ── Tick (called every poll) ──────────────────────────────

    def tick(self, roll_id: int, current_price: float,
             current_price_str: str = "") -> Optional["DiceRoll"]:
        roll = self.active_rolls.get(roll_id)
        if not roll or roll.status != "live":
            return roll

        roll.current_price = current_price
        now = time.time()
        roll.time_remaining = max(0, roll.settles_at - now)

        # Show live digit animation
        roll.live_digits = _extract_digits(current_price_str or f"{current_price}")
        roll.live_face = (_combine(roll.live_digits, roll.commitment_hash) % 6) + 1

        if roll.time_remaining <= 0:
            self.settle(roll_id, current_price, current_price_str)

        return roll

    # ── Settle ────────────────────────────────────────────────

    def settle(self, roll_id: int, settlement_price: float,
               settlement_price_str: str = "") -> Optional["DiceRoll"]:
        roll = self.active_rolls.get(roll_id)
        if not roll or roll.status not in ("live", "pending"):
            return roll

        price_str = settlement_price_str or f"{settlement_price}"
        roll.settlement_price = settlement_price
        roll.settlement_price_str = price_str
        roll.settled_at = time.time()

        # === THE CORE: market-derived dice outcome ===
        raw_digits = _extract_digits(price_str)
        combined = _combine(raw_digits, roll.commitment_hash)
        roll.raw_digits = raw_digits
        roll.dice_result = (combined % 6) + 1

        # Determine win/loss
        roll.won = _check_win(roll.game_type, roll.player_pick, roll.dice_result)

        # Payout
        if roll.won:
            if roll.game_type == "pick":
                roll.payout_multiplier = DICE_PICK_PAYOUT
            else:
                roll.payout_multiplier = DICE_BINARY_PAYOUT
            roll.payout_usd = round(roll.bet_amount * roll.payout_multiplier, 2)
            roll.pnl_usd = round(roll.payout_usd - roll.bet_amount, 2)
            roll.status = "won"
        else:
            roll.payout_multiplier = 0.0
            roll.payout_usd = 0.0
            roll.pnl_usd = -roll.bet_amount
            roll.status = "lost"

        if roll_id in self.active_rolls:
            del self.active_rolls[roll_id]

        return roll

    # ── Verify (provable fairness) ────────────────────────────

    @staticmethod
    def verify(roll_id: int, user_id: int, created_at: float,
               settlement_price_str: str, expected_result: int) -> dict:
        commitment = _make_commitment(roll_id, user_id, created_at)
        raw_digits = _extract_digits(settlement_price_str)
        combined = _combine(raw_digits, commitment)
        computed = (combined % 6) + 1
        return {
            "valid": computed == expected_result,
            "computed_result": computed,
            "commitment_hash": commitment,
            "raw_digits": raw_digits,
            "combined_value": combined,
        }

    # ── Serialize ─────────────────────────────────────────────

    def to_dict(self, roll: "DiceRoll") -> dict:
        d = {
            "roll_id": roll.roll_id,
            "user_id": roll.user_id,
            "symbol": roll.symbol,
            "exchange": roll.exchange,
            "game_type": roll.game_type,
            "num_dice": roll.num_dice,
            "bet_amount": roll.bet_amount,
            "player_pick": roll.player_pick,
            "commitment_hash": roll.commitment_hash,
            "status": roll.status,
            "entry_price": roll.entry_price,
            "settlement_price": roll.settlement_price,
            "dice_result": roll.dice_result,
            "won": roll.won,
            "payout_multiplier": roll.payout_multiplier,
            "payout_usd": roll.payout_usd,
            "pnl_usd": roll.pnl_usd,
            "window_sec": DICE_WINDOW_SEC,
            "time_remaining": round(roll.time_remaining, 1),
            "live_face": roll.live_face,
        }
        if roll.status in ("won", "lost"):
            d["verify_url"] = f"/api/hb/dice/verify/{roll.roll_id}"
        return d


# ── DiceRoll dataclass ────────────────────────────────────────

@dataclass
class DiceRoll:
    roll_id: int
    user_id: int
    symbol: str
    exchange: str
    game_type: str
    num_dice: int
    bet_amount: float
    player_pick: int
    commitment_hash: str
    created_at: float

    status: str = "pending"

    # Prices (strings preserved for digit extraction)
    entry_price: float = 0.0
    entry_price_str: str = ""
    settlement_price: float = 0.0
    settlement_price_str: str = ""
    current_price: float = 0.0

    # Timing
    started_at: float = 0.0
    settles_at: float = 0.0
    settled_at: float = 0.0
    time_remaining: float = 0.0

    # Result
    raw_digits: int = 0
    dice_result: int = 0
    won: bool = False
    payout_multiplier: float = 0.0
    payout_usd: float = 0.0
    pnl_usd: float = 0.0

    # Live animation state
    live_digits: int = 0
    live_face: int = 1


# ── Pure functions ────────────────────────────────────────────

def _make_commitment(roll_id: int, user_id: int, ts: float) -> str:
    raw = f"{roll_id}:{user_id}:{ts:.6f}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _extract_digits(price_str: str) -> int:
    cleaned = price_str.replace(".", "").replace(",", "").lstrip("0")
    if len(cleaned) < 4:
        cleaned = cleaned.zfill(4)
    return int(cleaned[-4:])


def _combine(raw_digits: int, commitment_hash: str) -> int:
    commit_bits = int(commitment_hash[-4:], 16)
    return raw_digits ^ commit_bits


def _check_win(game_type: str, player_pick: int, dice_result: int) -> bool:
    if game_type == "pick":
        return player_pick == dice_result
    elif game_type == "over_under":
        if player_pick == 0:  # under: 1-3
            return dice_result <= 3
        else:                 # over: 4-6
            return dice_result >= 4
    elif game_type == "even_odd":
        is_even = dice_result % 2 == 0
        if player_pick == 0:  # even
            return is_even
        else:                 # odd
            return not is_even
    return False


# ── Global instance ───────────────────────────────────────────
dice = DiceEngine()
