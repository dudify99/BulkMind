"""
BulkMind Input Validation
Centralized validation for all API endpoints.
"""

import re
from typing import Optional, Tuple
from config import WATCHED_SYMBOLS

# Valid symbols whitelist
VALID_SYMBOLS = set(WATCHED_SYMBOLS)  # {"BTC-USD", "ETH-USD", "SOL-USD"}
VALID_EXCHANGES = {"bulk", "hyperliquid", "both"}
VALID_SIDES = {"BUY", "SELL"}
VALID_DIRECTIONS = {"up", "down", "long", "short"}
VALID_PERIODS = {"daily", "weekly", "alltime", "24h", "7d", "30d"}
VALID_INTERVALS = {"1m", "5m", "15m", "1h", "4h", "1d"}

# Wallet: base58 (Solana) or hex (EVM) — 32-88 chars, alphanumeric
WALLET_PATTERN = re.compile(r"^[a-zA-Z0-9]{32,88}$")
# Username: 2-20 chars, alphanumeric + underscore + dash
USERNAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{2,20}$")


def validate_int(raw, *, default: int = 0, min_val: int = 0,
                 max_val: int = 10000) -> int:
    """Parse and clamp an integer value safely."""
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return default
    return max(min_val, min(val, max_val))


def validate_float(raw, *, default: float = 0.0, min_val: float = 0.0,
                   max_val: float = 1_000_000.0) -> float:
    """Parse and clamp a float value safely. Rejects NaN/Inf."""
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return default
    if val != val or val == float("inf") or val == float("-inf"):
        return default
    return max(min_val, min(val, max_val))


def validate_wallet(wallet: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Validate wallet address format. Returns (wallet, error)."""
    if not wallet or not isinstance(wallet, str):
        return None, "wallet is required"
    wallet = wallet.strip()
    if not WALLET_PATTERN.match(wallet):
        return None, "invalid wallet format (expected 32-88 alphanumeric characters)"
    return wallet, None


def validate_symbol(symbol: Optional[str], default: str = "BTC-USD") -> str:
    """Validate symbol against whitelist. Returns valid symbol or default."""
    if not symbol or not isinstance(symbol, str):
        return default
    symbol = symbol.strip().upper()
    if symbol in VALID_SYMBOLS:
        return symbol
    return default


def validate_exchange(exchange: Optional[str], default: str = "bulk") -> str:
    """Validate exchange against whitelist."""
    if not exchange or not isinstance(exchange, str):
        return default
    exchange = exchange.strip().lower()
    if exchange in VALID_EXCHANGES:
        return exchange
    return default


def validate_side(side: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Validate trade side. Returns (side, error)."""
    if not side or not isinstance(side, str):
        return None, "side is required"
    side = side.strip().upper()
    if side in VALID_SIDES:
        return side, None
    return None, "side must be BUY or SELL"


def validate_direction(direction: Optional[str],
                       default: str = "up") -> str:
    """Validate direction (up/down/long/short)."""
    if not direction or not isinstance(direction, str):
        return default
    direction = direction.strip().lower()
    if direction in VALID_DIRECTIONS:
        return direction
    return default


def validate_period(period: Optional[str], default: str = "alltime") -> str:
    """Validate leaderboard period."""
    if not period or not isinstance(period, str):
        return default
    period = period.strip().lower()
    if period in VALID_PERIODS:
        return period
    return default


def validate_interval(interval: Optional[str],
                      default: str = "15m") -> str:
    """Validate candle interval."""
    if not interval or not isinstance(interval, str):
        return default
    interval = interval.strip().lower()
    if interval in VALID_INTERVALS:
        return interval
    return default


def validate_username(username: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Validate username format. Returns (username, error)."""
    if not username or not isinstance(username, str):
        return None, "username is required"
    username = username.strip()
    if not USERNAME_PATTERN.match(username):
        return None, "username must be 2-20 characters (letters, numbers, _ or -)"
    return username, None
