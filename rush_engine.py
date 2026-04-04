"""
Alpha Rush — AI-Guided Micro-Trading Game for HyperBulk
5 rounds × 1 minute each. AI gives you a sniper signal each round.
Execute or skip. Your PnL is real. Single + multiplayer.
"""

import time
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from signal_engine import signals as signal_engine, STRATEGIES


class RushStatus:
    WAITING = "waiting"
    LIVE = "live"
    BETWEEN = "between_rounds"  # Waiting for next signal
    FINISHED = "finished"


@dataclass
class RushRound:
    round_num: int           # 1-5
    signal: dict             # Signal from signal engine
    decision: str = ""       # "execute" or "skip"
    entry_price: float = 0.0
    exit_price: float = 0.0
    pnl_usd: float = 0.0
    won: bool = False
    started_at: float = 0.0
    ended_at: float = 0.0
    order_id: str = ""


@dataclass
class RushGame:
    game_id: int
    user_id: int
    symbol: str
    exchange: str
    bet_amount: float        # Total stake for the game
    per_round: float         # bet_amount / 5
    status: str = "waiting"
    current_round: int = 0   # 0 = not started, 1-5 = active round
    rounds: List[RushRound] = field(default_factory=list)
    # Scoring
    total_pnl: float = 0.0
    rounds_won: int = 0
    rounds_played: int = 0
    ai_accuracy: int = 0     # How many AI signals were correct
    # Timing
    round_duration_sec: int = 60
    created_at: float = 0.0
    started_at: float = 0.0
    ended_at: float = 0.0
    current_round_ends_at: float = 0.0


class RushEngine:
    """Manages Alpha Rush games."""

    def __init__(self):
        self.games: Dict[int, RushGame] = {}
        self._next_id = 1

    def create_game(self, user_id: int, symbol: str, exchange: str,
                    bet_amount: float = 5.0) -> RushGame:
        game_id = self._next_id
        self._next_id += 1
        game = RushGame(
            game_id=game_id,
            user_id=user_id,
            symbol=symbol,
            exchange=exchange,
            bet_amount=bet_amount,
            per_round=round(bet_amount / 5, 2),
            created_at=time.time(),
        )
        self.games[game_id] = game
        return game

    def start_game(self, game_id: int) -> Optional[RushGame]:
        """Start the game — generates first signal."""
        game = self.games.get(game_id)
        if not game:
            return None
        game.status = "live"
        game.started_at = time.time()
        self._next_round(game)
        return game

    def _next_round(self, game: RushGame):
        """Advance to next round with a fresh AI signal."""
        game.current_round += 1
        if game.current_round > 5:
            self._finish(game)
            return

        # Get best signal from engine
        sigs = signal_engine.get_signals(game.symbol, limit=6)
        if sigs:
            # Rotate through strategies — don't repeat same strategy
            used = {r.signal.get("strategy") for r in game.rounds}
            sig = None
            for s in sigs:
                if s["strategy"] not in used:
                    sig = s
                    break
            if not sig:
                sig = sigs[0]
        else:
            # Fallback signal if engine has no data
            sig = {
                "strategy": "ema_cross", "strategy_name": "EMA Crossover",
                "emoji": "✂️", "direction": "BUY", "confidence": 60,
                "entry": 0, "target": 0, "stop": 0, "rr_ratio": 1.5,
                "reason": "Awaiting live data...",
            }

        rnd = RushRound(
            round_num=game.current_round,
            signal=sig,
            started_at=time.time(),
        )
        game.rounds.append(rnd)
        game.current_round_ends_at = time.time() + game.round_duration_sec
        game.status = "live"

    def execute_round(self, game_id: int, entry_price: float,
                      order_id: str = "") -> Optional[RushGame]:
        """Player chooses to EXECUTE the AI signal."""
        game = self.games.get(game_id)
        if not game or game.status != "live" or not game.rounds:
            return game
        rnd = game.rounds[-1]
        if rnd.decision:
            return game  # Already decided
        rnd.decision = "execute"
        rnd.entry_price = entry_price
        rnd.order_id = order_id
        game.rounds_played += 1
        return game

    def skip_round(self, game_id: int) -> Optional[RushGame]:
        """Player chooses to SKIP the AI signal."""
        game = self.games.get(game_id)
        if not game or game.status != "live" or not game.rounds:
            return game
        rnd = game.rounds[-1]
        if rnd.decision:
            return game
        rnd.decision = "skip"
        rnd.ended_at = time.time()
        # Move to next round immediately
        game.status = "between_rounds"
        return game

    def settle_round(self, game_id: int, exit_price: float) -> Optional[RushGame]:
        """Settle the current round (called when timer expires or manually)."""
        game = self.games.get(game_id)
        if not game or not game.rounds:
            return game
        rnd = game.rounds[-1]
        if rnd.ended_at > 0:
            return game  # Already settled

        rnd.ended_at = time.time()
        rnd.exit_price = exit_price

        if rnd.decision == "execute" and rnd.entry_price > 0:
            direction = rnd.signal.get("direction", "BUY")
            if direction == "BUY":
                pnl_pct = (exit_price - rnd.entry_price) / rnd.entry_price
            else:
                pnl_pct = (rnd.entry_price - exit_price) / rnd.entry_price
            rnd.pnl_usd = round(pnl_pct * game.per_round * 10, 2)  # 10x for visible PnL
            rnd.won = rnd.pnl_usd > 0
            game.total_pnl += rnd.pnl_usd
            if rnd.won:
                game.rounds_won += 1

            # Track AI accuracy (did the signal direction match the move?)
            actual_up = exit_price > rnd.entry_price
            ai_said_buy = direction == "BUY"
            if actual_up == ai_said_buy:
                game.ai_accuracy += 1

        elif rnd.decision == "skip":
            # Check if AI would have been right (for accuracy tracking)
            pass

        # Advance to next round
        game.status = "between_rounds"
        return game

    def advance_round(self, game_id: int) -> Optional[RushGame]:
        """Move to next round after settlement."""
        game = self.games.get(game_id)
        if not game or game.status != "between_rounds":
            return game
        self._next_round(game)
        return game

    def _finish(self, game: RushGame):
        """Game over — 5 rounds complete."""
        game.status = "finished"
        game.ended_at = time.time()
        game.current_round = 5

    def tick(self, game_id: int, current_price: float) -> Optional[RushGame]:
        """Check if current round should auto-settle."""
        game = self.games.get(game_id)
        if not game or game.status != "live":
            return game
        if not game.rounds:
            return game

        rnd = game.rounds[-1]
        now = time.time()

        # Auto-settle if round timer expired
        if now >= game.current_round_ends_at and rnd.decision and rnd.ended_at == 0:
            self.settle_round(game_id, current_price)

        # If no decision made and timer expired, force skip
        if now >= game.current_round_ends_at and not rnd.decision:
            self.skip_round(game_id)

        return game

    def to_dict(self, game: RushGame) -> dict:
        now = time.time()
        current_signal = None
        round_time_left = 0

        if game.rounds and game.status == "live":
            rnd = game.rounds[-1]
            current_signal = rnd.signal
            round_time_left = max(0, game.current_round_ends_at - now)

        return {
            "game_id": game.game_id,
            "status": game.status,
            "symbol": game.symbol,
            "exchange": game.exchange,
            "bet_amount": game.bet_amount,
            "per_round": game.per_round,
            "current_round": game.current_round,
            "total_rounds": 5,
            "round_time_left": round(round_time_left, 1),
            "total_pnl": round(game.total_pnl, 2),
            "rounds_won": game.rounds_won,
            "rounds_played": game.rounds_played,
            "ai_accuracy": game.ai_accuracy,
            "current_signal": current_signal,
            "current_decision": game.rounds[-1].decision if game.rounds else "",
            "rounds": [
                {
                    "round_num": r.round_num,
                    "strategy": r.signal.get("strategy_name", ""),
                    "emoji": r.signal.get("emoji", ""),
                    "direction": r.signal.get("direction", ""),
                    "confidence": r.signal.get("confidence", 0),
                    "decision": r.decision,
                    "pnl_usd": round(r.pnl_usd, 2),
                    "won": r.won,
                    "entry": round(r.entry_price, 2),
                    "exit": round(r.exit_price, 2),
                }
                for r in game.rounds
            ],
        }


# Global instance
rush = RushEngine()
