"""
Sniper — Price Prediction Game Engine for HyperBulk
Skill-based: predict the exact price in X minutes. Closest guess wins the pot.
All entries + settlements on-chain via Bulk/Hyperliquid orders.
"""

import time
import math
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict


class RoundStatus(Enum):
    OPEN = "open"             # Accepting predictions
    LOCKED = "locked"         # No more predictions, waiting for settlement
    SETTLED = "settled"       # Winners determined, payouts done
    CANCELLED = "cancelled"   # Not enough players, refunded


class AccuracyTier(Enum):
    PERFECT = "perfect"   # Within 0.01% — legendary
    SNIPER = "sniper"     # Within 0.05%
    SHARP = "sharp"       # Within 0.1%
    CLOSE = "close"       # Within 0.5%
    MISS = "miss"         # > 0.5%


@dataclass
class SniperConfig:
    symbol: str = "BTC-USD"
    duration_sec: int = 300       # 5 minutes to settlement
    entry_fee: float = 5.0        # USD per entry
    min_players: int = 2          # Minimum to run (else cancel + refund)
    max_players: int = 100
    rake_pct: float = 10.0        # Founder takes 10% of pot
    # Payout structure (% of prize pool after rake)
    payouts: Dict[int, float] = field(default_factory=lambda: {
        1: 0.50,    # 1st place: 50%
        2: 0.30,    # 2nd place: 30%
        3: 0.20,    # 3rd place: 20%
    })


@dataclass
class Prediction:
    user_id: int
    wallet: str
    username: str
    predicted_price: float
    submitted_at: float           # unix timestamp
    # Filled after settlement
    accuracy_pct: float = 0.0     # How close (0 = perfect)
    accuracy_tier: str = ""
    rank: int = 0
    payout_usd: float = 0.0
    distance_usd: float = 0.0    # Absolute $ distance from actual


@dataclass
class SniperRound:
    round_id: int
    config: SniperConfig
    status: RoundStatus = RoundStatus.OPEN
    predictions: List[Prediction] = field(default_factory=list)
    # Timing
    created_at: float = 0.0
    locks_at: float = 0.0         # No more entries after this
    settles_at: float = 0.0       # When actual price is checked
    settled_at: float = 0.0
    # Settlement data
    actual_price: float = 0.0
    pot_usd: float = 0.0
    rake_usd: float = 0.0
    prize_pool_usd: float = 0.0


class SniperEngine:
    """Manages Sniper game rounds."""

    def __init__(self):
        self.rounds: Dict[int, SniperRound] = {}
        self._next_round_id = 1

    def create_round(self, config: SniperConfig = None) -> SniperRound:
        """Create a new prediction round."""
        if config is None:
            config = SniperConfig()

        now = time.time()
        round_id = self._next_round_id
        self._next_round_id += 1

        # Predictions accepted for first 50% of duration, then locked.
        # This prevents late-submission exploit where players observe
        # price trend before predicting (80% was too late).
        lock_time = now + (config.duration_sec * 0.5)
        settle_time = now + config.duration_sec

        rnd = SniperRound(
            round_id=round_id,
            config=config,
            status=RoundStatus.OPEN,
            created_at=now,
            locks_at=lock_time,
            settles_at=settle_time,
        )
        self.rounds[round_id] = rnd
        return rnd

    def submit_prediction(self, round_id: int, user_id: int, wallet: str,
                          username: str, predicted_price: float) -> Optional[str]:
        """Submit a price prediction. Returns error string or None on success."""
        rnd = self.rounds.get(round_id)
        if not rnd:
            return "Round not found"
        if rnd.status != RoundStatus.OPEN:
            return "Round is no longer accepting predictions"
        if time.time() > rnd.locks_at:
            rnd.status = RoundStatus.LOCKED
            return "Predictions locked — round settling soon"
        if len(rnd.predictions) >= rnd.config.max_players:
            return "Round is full"

        # Check duplicate
        for p in rnd.predictions:
            if p.user_id == user_id:
                return "Already submitted a prediction for this round"

        if predicted_price <= 0:
            return "Price must be positive"

        rnd.predictions.append(Prediction(
            user_id=user_id,
            wallet=wallet,
            username=username,
            predicted_price=predicted_price,
            submitted_at=time.time(),
        ))
        rnd.pot_usd = len(rnd.predictions) * rnd.config.entry_fee
        return None  # success

    def check_lock(self, round_id: int) -> bool:
        """Check if round should be locked. Returns True if just locked."""
        rnd = self.rounds.get(round_id)
        if not rnd or rnd.status != RoundStatus.OPEN:
            return False
        if time.time() > rnd.locks_at:
            rnd.status = RoundStatus.LOCKED
            return True
        return False

    def settle(self, round_id: int, actual_price: float) -> Optional[SniperRound]:
        """Settle a round with the actual price. Computes rankings + payouts."""
        rnd = self.rounds.get(round_id)
        if not rnd:
            return None
        if rnd.status == RoundStatus.SETTLED:
            return rnd

        # Check minimum players
        if len(rnd.predictions) < rnd.config.min_players:
            rnd.status = RoundStatus.CANCELLED
            rnd.settled_at = time.time()
            return rnd

        rnd.actual_price = actual_price
        rnd.pot_usd = len(rnd.predictions) * rnd.config.entry_fee
        rnd.rake_usd = round(rnd.pot_usd * rnd.config.rake_pct / 100, 2)
        rnd.prize_pool_usd = round(rnd.pot_usd - rnd.rake_usd, 2)

        # Score each prediction
        for pred in rnd.predictions:
            pred.distance_usd = abs(pred.predicted_price - actual_price)
            if actual_price > 0:
                pred.accuracy_pct = (pred.distance_usd / actual_price) * 100
            else:
                pred.accuracy_pct = 100.0

            # Accuracy tier
            if pred.accuracy_pct <= 0.01:
                pred.accuracy_tier = AccuracyTier.PERFECT.value
            elif pred.accuracy_pct <= 0.05:
                pred.accuracy_tier = AccuracyTier.SNIPER.value
            elif pred.accuracy_pct <= 0.1:
                pred.accuracy_tier = AccuracyTier.SHARP.value
            elif pred.accuracy_pct <= 0.5:
                pred.accuracy_tier = AccuracyTier.CLOSE.value
            else:
                pred.accuracy_tier = AccuracyTier.MISS.value

        # Rank by accuracy (closest first)
        rnd.predictions.sort(key=lambda p: (p.distance_usd, p.submitted_at))
        for i, pred in enumerate(rnd.predictions):
            pred.rank = i + 1

        # Distribute prizes
        for rank, pct in rnd.config.payouts.items():
            if rank <= len(rnd.predictions):
                rnd.predictions[rank - 1].payout_usd = round(
                    rnd.prize_pool_usd * pct, 2
                )

        rnd.status = RoundStatus.SETTLED
        rnd.settled_at = time.time()
        return rnd

    def get_round_state(self, round_id: int) -> Optional[dict]:
        """Serialize round state for API response."""
        rnd = self.rounds.get(round_id)
        if not rnd:
            return None

        now = time.time()
        time_remaining = max(0, rnd.settles_at - now)

        result = {
            "round_id": rnd.round_id,
            "status": rnd.status.value,
            "symbol": rnd.config.symbol,
            "entry_fee": rnd.config.entry_fee,
            "duration_sec": rnd.config.duration_sec,
            "player_count": len(rnd.predictions),
            "max_players": rnd.config.max_players,
            "pot_usd": rnd.pot_usd,
            "rake_pct": rnd.config.rake_pct,
            "time_remaining_sec": round(time_remaining, 1),
            "locks_in_sec": round(max(0, rnd.locks_at - now), 1),
            "created_at": rnd.created_at,
            "settles_at": rnd.settles_at,
        }

        # Include predictions (hide others' predictions if round is open)
        if rnd.status == RoundStatus.OPEN:
            result["predictions"] = [
                {"username": p.username, "submitted": True}
                for p in rnd.predictions
            ]
        elif rnd.status in (RoundStatus.LOCKED, RoundStatus.SETTLED):
            result["predictions"] = [
                {
                    "username": p.username,
                    "wallet": p.wallet[:6] + "..." + p.wallet[-4:] if len(p.wallet) > 10 else p.wallet,
                    "predicted_price": p.predicted_price,
                    "rank": p.rank,
                    "accuracy_pct": round(p.accuracy_pct, 4),
                    "accuracy_tier": p.accuracy_tier,
                    "distance_usd": round(p.distance_usd, 2),
                    "payout_usd": p.payout_usd,
                }
                for p in rnd.predictions
            ]

        if rnd.status == RoundStatus.SETTLED:
            result["actual_price"] = rnd.actual_price
            result["prize_pool_usd"] = rnd.prize_pool_usd
            result["rake_usd"] = rnd.rake_usd

        return result

    def get_active_rounds(self) -> list:
        """List all non-settled rounds."""
        return [
            self.get_round_state(rid)
            for rid, rnd in self.rounds.items()
            if rnd.status in (RoundStatus.OPEN, RoundStatus.LOCKED)
        ]


# Global instance
sniper = SniperEngine()
