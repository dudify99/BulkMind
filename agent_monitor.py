"""
AgentMonitor — Central registry for all BulkMind agent health tracking.

Each agent calls monitor.heartbeat(name) at the top of every scan loop.
The supervisor in main.py wraps each agent's run() and calls monitor.set_status()
on crash/restart. Dashboard reads monitor.get_all() for the /api/agents endpoint.
"""

import asyncio
from datetime import datetime
from typing import Dict, Optional

from db import log_issue


# ── Status constants ──────────────────────────────────────────────────────────
STATUS_STARTING  = "starting"
STATUS_RUNNING   = "running"
STATUS_ERROR     = "error"
STATUS_RESTARTING = "restarting"
STATUS_STOPPED   = "stopped"


class AgentState:
    __slots__ = (
        "status", "last_heartbeat", "started_at",
        "scan_count", "signal_count", "trade_count",
        "error_count", "last_error", "restart_count",
    )

    def __init__(self):
        self.status         = STATUS_STARTING
        self.last_heartbeat: Optional[str] = None
        self.started_at     = datetime.utcnow().isoformat()
        self.scan_count     = 0
        self.signal_count   = 0
        self.trade_count    = 0
        self.error_count    = 0
        self.last_error: Optional[str] = None
        self.restart_count  = 0

    def to_dict(self) -> dict:
        now = datetime.utcnow().isoformat()
        stale = False
        if self.last_heartbeat:
            try:
                delta = (datetime.utcnow() - datetime.fromisoformat(self.last_heartbeat)).total_seconds()
                stale = delta > 300  # no heartbeat in 5 min = stale
            except Exception:
                pass

        return {
            "status":          self.status,
            "stale":           stale,
            "last_heartbeat":  self.last_heartbeat,
            "started_at":      self.started_at,
            "scan_count":      self.scan_count,
            "signal_count":    self.signal_count,
            "trade_count":     self.trade_count,
            "error_count":     self.error_count,
            "last_error":      self.last_error,
            "restart_count":   self.restart_count,
            "ts":              now,
        }


class AgentMonitor:
    def __init__(self):
        self._agents: Dict[str, AgentState] = {}
        self._lock = asyncio.Lock()
        self._broadcast_fn = None  # injected by dashboard

    def _get_or_create(self, name: str) -> AgentState:
        if name not in self._agents:
            self._agents[name] = AgentState()
        return self._agents[name]

    # ── Called by agents ─────────────────────────────────────────────────────

    def heartbeat(self, name: str):
        """Call at the top of each agent scan loop."""
        s = self._get_or_create(name)
        s.last_heartbeat = datetime.utcnow().isoformat()
        s.status         = STATUS_RUNNING
        s.scan_count    += 1

    def signal_found(self, name: str):
        """Call when an agent finds a tradeable signal."""
        self._get_or_create(name).signal_count += 1

    def trade_placed(self, name: str):
        """Call when an agent successfully places a trade."""
        self._get_or_create(name).trade_count += 1

    def record_error(self, name: str, err: str):
        """Call when an agent hits a non-fatal error."""
        s = self._get_or_create(name)
        s.error_count += 1
        s.last_error   = str(err)[:300]

    # ── Called by supervisor ─────────────────────────────────────────────────

    def set_status(self, name: str, status: str, error: str = None):
        s = self._get_or_create(name)
        s.status = status
        if error:
            s.error_count += 1
            s.last_error   = str(error)[:300]
        if status == STATUS_RESTARTING:
            s.restart_count += 1

    # ── Read ─────────────────────────────────────────────────────────────────

    def get_all(self) -> dict:
        return {name: state.to_dict() for name, state in self._agents.items()}

    def get(self, name: str) -> Optional[dict]:
        if name in self._agents:
            return self._agents[name].to_dict()
        return None

    def inject_broadcast(self, fn):
        """Dashboard injects its WebSocket broadcast function here."""
        self._broadcast_fn = fn

    async def push_update(self):
        """Push current agent status to all WebSocket clients."""
        if self._broadcast_fn:
            import json
            try:
                await self._broadcast_fn("agent_status", json.dumps(self.get_all()))
            except Exception:
                pass


# ── Global singleton ──────────────────────────────────────────────────────────
monitor = AgentMonitor()


# ── Supervisor ────────────────────────────────────────────────────────────────

MAX_RESTARTS    = 10   # stop retrying after this many crashes
BASE_RESTART_S  = 30   # initial restart delay
MAX_RESTART_S   = 300  # cap at 5 min


async def supervise(name: str, coro_fn, *args, **kwargs):
    """
    Supervisor wrapper for agent run() coroutines.
    Auto-restarts crashed agents with exponential backoff.
    Stops after MAX_RESTARTS consecutive crashes.
    """
    monitor.set_status(name, STATUS_STARTING)
    restarts = 0

    while True:
        try:
            monitor.set_status(name, STATUS_RUNNING)
            await coro_fn(*args, **kwargs)
            # run() returned normally (shouldn't happen — all loops are infinite)
            print(f"[Supervisor] {name} exited cleanly — not restarting")
            monitor.set_status(name, STATUS_STOPPED)
            break

        except asyncio.CancelledError:
            monitor.set_status(name, STATUS_STOPPED)
            raise

        except Exception as e:
            restarts += 1
            err_str = str(e)
            monitor.set_status(name, STATUS_ERROR, error=err_str)
            print(f"[Supervisor] {name} crashed (#{restarts}): {err_str}")

            severity = "CRITICAL" if restarts >= MAX_RESTARTS else "HIGH"
            log_issue(severity, "AGENT_ERROR",
                      f"{name} crashed (restart #{restarts})", err_str)

            if restarts >= MAX_RESTARTS:
                print(f"[Supervisor] {name} exceeded {MAX_RESTARTS} restarts — stopping")
                monitor.set_status(name, STATUS_STOPPED)
                break

            delay = min(BASE_RESTART_S * (2 ** (restarts - 1)), MAX_RESTART_S)
            print(f"[Supervisor] Restarting {name} in {delay}s...")
            monitor.set_status(name, STATUS_RESTARTING)
            await asyncio.sleep(delay)
