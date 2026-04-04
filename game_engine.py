"""
Moon or Doom — Game Engine for HyperBulk
Crash-style game with real 50x leveraged positions on Bulk + Hyperliquid.
Based on Bulkie iOS SDK patterns.
"""

import time
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


class RiskLevel(Enum):
    SAFE = "safe"         # Within 5% of peak (ratio > 0.95)
    RISING = "rising"     # 5-20% drawdown from peak (0.80-0.95)
    CRITICAL = "critical" # >20% drawdown from peak (< 0.80)


class GameStatus(Enum):
    WAITING = "waiting"       # Lobby, not started
    COUNTDOWN = "countdown"   # 3-2-1-GO countdown
    LIVE = "live"             # Position open, multiplier running
    CASHED_OUT = "cashed_out" # User ejected with profit
    CRASHED = "crashed"       # Price hit crash threshold
    ERROR = "error"           # Something went wrong


@dataclass
class GameConfig:
    symbol: str = "BTC-USD"
    bet_amount: float = 10.0        # USD bet
    leverage: float = 50.0          # 50x leverage
    crash_threshold_pct: float = 0.02  # 2% adverse move = crash (entry × (1 - 0.02))
    auto_cashout_mult: float = 0.0  # 0 = disabled, e.g. 2.0 = auto cash at 2x
    exchange: str = "bulk"          # "bulk" or "hyperliquid" or "both"


@dataclass
class GameState:
    config: GameConfig
    status: GameStatus = GameStatus.WAITING

    # Position data
    entry_price: float = 0.0
    current_price: float = 0.0
    size: float = 0.0
    order_id: str = ""

    # Multiplier tracking
    multiplier: float = 1.0
    high_water_mark: float = 1.0
    risk_level: RiskLevel = RiskLevel.SAFE

    # PnL
    pnl_usd: float = 0.0
    pnl_pct: float = 0.0

    # Crash detection
    crash_price: float = 0.0  # Price at which game crashes

    # Timing
    started_at: float = 0.0   # unix timestamp
    ended_at: float = 0.0
    elapsed_sec: float = 0.0

    # Price history for chart (last 300 ticks)
    price_history: list = field(default_factory=list)

    # Result
    exit_price: float = 0.0
    exit_multiplier: float = 0.0


class MoonOrDoomEngine:
    """
    Core game logic. Processes price ticks, computes multiplier, detects crashes.
    Thread-safe — designed to be called from async WebSocket handler.
    """

    def __init__(self, config: GameConfig):
        self.state = GameState(config=config)

    def start_game(self, entry_price: float, size: float, order_id: str = ""):
        """Called after position is opened. Sets entry price and starts the game."""
        self.state.entry_price = entry_price
        self.state.current_price = entry_price
        self.state.size = size
        self.state.order_id = order_id
        self.state.crash_price = entry_price * (1.0 - self.state.config.crash_threshold_pct)
        self.state.started_at = time.time()
        self.state.status = GameStatus.LIVE
        self.state.multiplier = 1.0
        self.state.high_water_mark = 1.0
        self.state.price_history = [{"time": time.time(), "price": entry_price, "mult": 1.0}]

    def process_tick(self, price: float) -> GameState:
        """Process a price tick. Updates multiplier, risk, PnL. Returns current state."""
        if self.state.status != GameStatus.LIVE:
            return self.state

        self.state.current_price = price
        self.state.elapsed_sec = time.time() - self.state.started_at

        # Multiplier: 1.0 + (price_change_pct × leverage)
        if self.state.entry_price > 0:
            change = (price - self.state.entry_price) / self.state.entry_price
            self.state.multiplier = max(0.0, 1.0 + change * self.state.config.leverage)

        # High water mark
        if self.state.multiplier > self.state.high_water_mark:
            self.state.high_water_mark = self.state.multiplier

        # Risk level (drawdown from peak)
        if self.state.high_water_mark > 0:
            ratio = self.state.multiplier / self.state.high_water_mark
            if ratio > 0.95:
                self.state.risk_level = RiskLevel.SAFE
            elif ratio > 0.80:
                self.state.risk_level = RiskLevel.RISING
            else:
                self.state.risk_level = RiskLevel.CRITICAL

        # PnL
        self.state.pnl_usd = (self.state.multiplier - 1.0) * self.state.config.bet_amount
        self.state.pnl_pct = (self.state.multiplier - 1.0) * 100

        # Price history (cap at 300 points for chart)
        self.state.price_history.append({
            "time": time.time(),
            "price": price,
            "mult": round(self.state.multiplier, 4),
        })
        if len(self.state.price_history) > 300:
            self.state.price_history = self.state.price_history[-300:]

        # Crash detection
        if price <= self.state.crash_price:
            self.state.status = GameStatus.CRASHED
            self.state.exit_price = price
            self.state.exit_multiplier = self.state.multiplier
            self.state.ended_at = time.time()
            self.state.pnl_usd = -self.state.config.bet_amount  # Lost the bet

        # Auto cash-out
        elif (self.state.config.auto_cashout_mult > 0 and
              self.state.multiplier >= self.state.config.auto_cashout_mult):
            self.cash_out(price)

        return self.state

    def cash_out(self, current_price: float = None) -> GameState:
        """User ejects — cash out at current multiplier."""
        if self.state.status != GameStatus.LIVE:
            return self.state

        price = current_price or self.state.current_price
        self.state.status = GameStatus.CASHED_OUT
        self.state.exit_price = price
        self.state.exit_multiplier = self.state.multiplier
        self.state.ended_at = time.time()
        # Final PnL = (multiplier - 1) × bet
        self.state.pnl_usd = (self.state.multiplier - 1.0) * self.state.config.bet_amount
        return self.state

    def add_to_position(self, new_entry_price: float, additional_size: float):
        """Add to position — recalculate weighted average entry."""
        if self.state.status != GameStatus.LIVE:
            return
        old_notional = self.state.entry_price * self.state.size
        new_notional = new_entry_price * additional_size
        total_size = self.state.size + additional_size
        if total_size > 0:
            self.state.entry_price = (old_notional + new_notional) / total_size
            self.state.size = total_size
            # Recalculate crash price
            self.state.crash_price = self.state.entry_price * (1.0 - self.state.config.crash_threshold_pct)

    def to_dict(self) -> dict:
        """Serialize game state for API/WebSocket response."""
        return {
            "status": self.state.status.value,
            "symbol": self.state.config.symbol,
            "exchange": self.state.config.exchange,
            "bet_amount": self.state.config.bet_amount,
            "leverage": self.state.config.leverage,
            "entry_price": self.state.entry_price,
            "current_price": self.state.current_price,
            "crash_price": round(self.state.crash_price, 2),
            "multiplier": round(self.state.multiplier, 4),
            "high_water_mark": round(self.state.high_water_mark, 4),
            "risk_level": self.state.risk_level.value,
            "pnl_usd": round(self.state.pnl_usd, 2),
            "pnl_pct": round(self.state.pnl_pct, 2),
            "elapsed_sec": round(self.state.elapsed_sec, 1),
            "auto_cashout": self.state.config.auto_cashout_mult,
            "size": self.state.size,
            "order_id": self.state.order_id,
        }
