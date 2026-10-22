"""NinjaTrader 8 bidirectional executor -- TCP client to PythonBridge AddOn.

Replaces the v1 OIF file approach. Connects to the NinjaScript AddOn
running inside NT8 (see ninjatrader_addon/PythonBridge.cs) on a TCP socket
and exchanges newline-delimited JSON messages.

Bidirectional means:
- Python -> NT: PLACE commands (with correlation id for matching back)
- NT -> Python: ACK / REJECT (synchronous within timeout) +
  ORDER_UPDATE (Working / Filled / Cancelled / Rejected) +
  POSITION (account-level long/short/flat with avg price).

Why this matters for parallel-live: the OIF version had no way to know if
NT actually accepted an entry. If NT rejected and the bot kept firing SELL
closes, NT could open shorts on a flat netting account. With bidirectional,
place_order BLOCKS for ACK or REJECT, and a desync flag records the case
where NT diverges from the primary -- subsequent close orders skip NT
until the next ENTRY resyncs.

Environment variables
---------------------
NT_BRIDGE_HOST       AddOn TCP host (default 127.0.0.1).
NT_BRIDGE_PORT       AddOn TCP port (default 36973 -- must match PORT in
                     PythonBridge.cs).
NT_ACCOUNT_NAME      NT account name as shown in the Connections panel
                     (e.g., "Sim101").
NT_INSTRUMENT        NT-format symbol, e.g., "MGC 06-26".
NT_LIVE              "1" or "true" to send real PLACE commands. Anything
                     else keeps broker in dry-run mode (no socket, just logs).
NT_ACK_TIMEOUT       Seconds to wait for ACK/REJECT (default 5.0).

Lifecycle
---------
- build_from_env() returns an instance or None
- caller calls start() once -- connects + starts reader thread; non-fatal
  if AddOn not yet running (background reconnect loop will retry).
- place_order() is synchronous: blocks for ACK/REJECT or ack_timeout.
- stop() closes socket and joins reader thread.

Desync semantics
----------------
The flag _position_synced starts True. It flips to False on:
  - ACK timeout (we don't know if NT placed the order)
  - REJECT (NT refused the order)
  - Late ORDER_UPDATE state=Rejected (after ACK)
  - Disconnect mid-flight (any pending acks resolve as rejected)
While desynced, place_order returns failure for non-entry orders. The next
ENTRY resets the flag and the mirror resumes.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Defaults match PythonBridge.cs
_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 36973
_DEFAULT_ACK_TIMEOUT = 5.0

# Reconnect backoff schedule (seconds) -- capped at the last value.
_RECONNECT_BACKOFF = [1, 2, 4, 8, 16, 30]


class _PendingAck:
    """Holds a threading.Event + result dict for one in-flight PLACE."""
    __slots__ = ("event", "result")

    def __init__(self):
        self.event = threading.Event()
        self.result: Dict[str, Any] = {}


class NinjaTraderExecutor:
    """TCP client to the PythonBridge NinjaScript AddOn.

    Mirrors WebullMGCBroker.place_order signature for use in
    MultiBrokerFanout. place_order blocks for ACK/REJECT (with timeout) so
    the caller's primary state machine sees a synchronous result.

    Usage:
        nt = NinjaTraderExecutor.build_from_env()
        if nt is not None:
            nt.start()                       # non-fatal if AddOn down
            r = nt.place_order(side="BUY", quantity=2,
                               limit_price=3250.5, comment="LONG Entry")
    """

    def __init__(
        self,
        host: str,
        port: int,
        account_name: str,
        instrument: str,
        live_orders: bool = False,
        ack_timeout: float = _DEFAULT_ACK_TIMEOUT,
    ):
        self.host = host
        self.port = port
        self.account_name = account_name
        self.instrument = instrument
        self.live_orders = live_orders
        self.ack_timeout = ack_timeout

        self._sock: Optional[socket.socket] = None
        self._sock_lock = threading.Lock()    # serialize sends + close
        self._stop_event = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self._reconnect_thread: Optional[threading.Thread] = None

        self._pending: Dict[str, _PendingAck] = {}
        self._pending_lock = threading.Lock()

        self._state_lock = threading.Lock()
        self._position_synced = True
        self._connected = False

        self._order_seq = 0
        self._last_position: Dict[str, Any] = {}

        logger.info(
            "[NT] Bidirectional broker constructed: live=%s, %s:%d, "
            "account=%s, instrument=%s",
            live_orders, host, port, account_name, instrument,
        )

    @classmethod
    def build_from_env(cls) -> Optional["NinjaTraderExecutor"]:
        host = os.environ.get("NT_BRIDGE_HOST", _DEFAULT_HOST).strip()
        port_str = os.environ.get("NT_BRIDGE_PORT", str(_DEFAULT_PORT)).strip()
        account = os.environ.get("NT_ACCOUNT_NAME", "").strip()
        instrument = os.environ.get("NT_INSTRUMENT", "").strip()
        live_flag = os.environ.get("NT_LIVE", "").strip().lower()
        live = live_flag in ("1", "true", "yes")
        try:
            ack_timeout = float(
                os.environ.get("NT_ACK_TIMEOUT", str(_DEFAULT_ACK_TIMEOUT))
            )
        except ValueError:
            ack_timeout = _DEFAULT_ACK_TIMEOUT

        if not account or not instrument:
            logger.info(
                "[NT] Disabled -- set NT_ACCOUNT_NAME + NT_INSTRUMENT to enable. "
                "(current: account=%r instrument=%r)", account, instrument,
            )
            return None

        try:
            port = int(port_str)
        except ValueError:
            logger.error("[NT] Invalid NT_BRIDGE_PORT=%r -- disabling", port_str)
            return None

        return cls(
            host=host,
            port=port,
            account_name=account,
            instrument=instrument,
            live_orders=live,
            ack_timeout=ack_timeout,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Connect + start reader. Non-fatal if AddOn not yet running.

        On failure, schedules a background reconnect loop. place_order()
        returns failure with "not connected" until reconnect succeeds.
        """
        if not self.live_orders:
            logger.info("[NT] Dry-run mode -- skipping TCP connect")
            return

        self._stop_event.clear()
        self._reconnect_thread = threading.Thread(
            target=self._reconnect_loop, daemon=True, name="nt-reconnect",
        )
        self._reconnect_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._sock_lock:
            sock = self._sock
            self._sock = None
            self._connected = False
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------
    def _reconnect_loop(self) -> None:
        """Maintain socket: dial, retry with exponential backoff on failure."""
        attempt = 0
        while not self._stop_event.is_set():
            if self._connected:
                # Sleep until disconnected (set by reader thread on EOF/error)
                self._stop_event.wait(1.0)
                continue

            try:
                logger.info("[NT] Connecting to %s:%d...", self.host, self.port)
                sock = socket.create_connection(
                    (self.host, self.port), timeout=5.0,
                )
                sock.settimeout(None)  # blocking reads from now on
                with self._sock_lock:
                    self._sock = sock
                    self._connected = True
                attempt = 0  # reset backoff
                logger.info("[NT] Connected")

                # Reader thread for THIS socket. The thread exits when the
                # socket closes; reconnect loop will then dial again.
                self._reader_thread = threading.Thread(
                    target=self._reader_loop, args=(sock,),
                    daemon=True, name="nt-reader",
                )
                self._reader_thread.start()

                # Heartbeat -- confirms the AddOn is alive and JSON works.
                try:
                    self._send_json({"cmd": "PING"})
                except Exception as exc:
                    logger.warning("[NT] PING after connect failed: %s", exc)
            except (OSError, socket.timeout) as exc:
                wait = _RECONNECT_BACKOFF[
                    min(attempt, len(_RECONNECT_BACKOFF) - 1)
                ]
                logger.warning(
                    "[NT] Connect failed (%s) -- retrying in %ds (attempt %d)",
                    exc, wait, attempt + 1,
                )
                attempt += 1
                self._stop_event.wait(wait)

    def _reader_loop(self, sock: socket.socket) -> None:
        """Read newline-delimited JSON events until socket closes."""
        try:
            f = sock.makefile("r", encoding="utf-8", newline="\n")
            while not self._stop_event.is_set():
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                except Exception as exc:
                    logger.warning("[NT] Bad event JSON: %r (err=%s)", line, exc)
                    continue
                self._dispatch_event(evt)
        except Exception as exc:
            logger.warning("[NT] Reader error: %s", exc)
        finally:
            with self._sock_lock:
                if self._sock is sock:
                    self._sock = None
                    self._connected = False
            try:
                sock.close()
            except Exception:
                pass
            logger.info("[NT] Disconnected -- reconnect loop will retry")

            # Resolve any in-flight acks as failed so callers don't hang.
            with self._pending_lock:
                for _, p in list(self._pending.items()):
                    if not p.event.is_set():
                        p.result = {
                            "rejected": True,
                            "reason": "NT disconnected before ACK",
                        }
                        p.event.set()
                self._pending.clear()

    # ------------------------------------------------------------------
    # Event dispatch
    # ------------------------------------------------------------------
    def _dispatch_event(self, evt: dict) -> None:
        kind = evt.get("event", "")
        if kind == "ACK":
            self._on_ack(evt)
        elif kind == "REJECT":
            self._on_reject(evt)
        elif kind == "ORDER_UPDATE":
            self._on_order_update(evt)
        elif kind == "POSITION":
            self._on_position(evt)
        elif kind == "PONG":
            logger.info("[NT] PONG (heartbeat)")
        elif kind == "ERROR":
            logger.warning("[NT] AddOn ERROR: %s", evt.get("msg", ""))
        else:
            logger.info("[NT] Unknown event: %s", evt)

    def _on_ack(self, evt: dict) -> None:
        pid = evt.get("id", "")
        with self._pending_lock:
            p = self._pending.pop(pid, None)
        if p is None:
            logger.warning("[NT] ACK for unknown id=%s", pid)
            return
        p.result = {"rejected": False, "nt_order_id": evt.get("ntOrderId")}
        p.event.set()

    def _on_reject(self, evt: dict) -> None:
        pid = evt.get("id", "")
        reason = evt.get("reason", "")
        with self._pending_lock:
            p = self._pending.pop(pid, None)
        if p is None:
            logger.warning("[NT] REJECT for unknown id=%s reason=%s", pid, reason)
            return
        p.result = {"rejected": True, "reason": reason}
        p.event.set()

    def _on_order_update(self, evt: dict) -> None:
        state = evt.get("state", "")
        pid = evt.get("id", "")
        nt_oid = evt.get("ntOrderId", "")
        filled = evt.get("filledQty", 0)
        avg = evt.get("avgFillPrice", 0)
        reason = evt.get("reason", "")
        logger.info(
            "[NT] ORDER_UPDATE id=%s nt=%s state=%s filled=%s avg=%s reason=%s",
            pid, nt_oid, state, filled, avg, reason,
        )
        # Late rejection (after ACK): mirror is now out of sync.
        if state == "Rejected":
            self._mark_desync(f"late reject id={pid} reason={reason}")

    def _on_position(self, evt: dict) -> None:
        self._last_position = evt
        logger.info(
            "[NT] POSITION account=%s instrument=%s qty=%s avg=%s mp=%s",
            evt.get("account"), evt.get("instrument"),
            evt.get("qty"), evt.get("avgPrice"), evt.get("marketPosition"),
        )

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------
    def place_order(
        self,
        side: str,
        quantity: int,
        limit_price: Optional[float] = None,
        comment: str = "",
    ) -> dict:
        side_norm = side.upper().strip()
        if side_norm not in ("BUY", "SELL"):
            return {
                "success": False,
                "error": f"Invalid side: {side!r}",
                "dry_run": True,
            }
        if quantity <= 0:
            return {
                "success": False,
                "error": f"Invalid quantity: {quantity}",
                "dry_run": True,
            }

        is_entry = "ENTRY" in (comment or "").upper()

        # An entry kicks off a fresh trade -- clear any stale desync flag.
        if is_entry:
            with self._state_lock:
                if not self._position_synced:
                    logger.info("[NT] Resyncing on new entry")
                    self._position_synced = True

        # Don't mirror close orders if the prior entry didn't sync.
        with self._state_lock:
            synced = self._position_synced
        if not synced and not is_entry:
            logger.warning(
                "[NT] Skipping mirror (desynced) -- side=%s qty=%s comment=%s",
                side_norm, quantity, comment,
            )
            return {
                "success": False,
                "error": "NT desynced; skipping until next entry",
                "dry_run": False,
            }

        # Dry-run: log + return success without touching the socket.
        if not self.live_orders:
            self._order_seq += 1
            stub_id = f"dryrun_{self._order_seq:06d}"
            logger.info(
                "[NT] DRY-RUN PLACE id=%s side=%s qty=%s account=%s instr=%s tv_price=%s",
                stub_id, side_norm, quantity, self.account_name,
                self.instrument, limit_price,
            )
            return {"success": True, "order_id": stub_id, "dry_run": True}

        if not self._connected:
            return {
                "success": False,
                "error": "NT bridge not connected",
                "dry_run": False,
            }

        py_id = self._next_id()
        cmd = {
            "cmd": "PLACE",
            "id": py_id,
            "account": self.account_name,
            "instrument": self.instrument,
            "action": side_norm,
            "qty": int(quantity),
            "orderType": "MARKET",
            "tif": "DAY",
        }

        pending = _PendingAck()
        with self._pending_lock:
            self._pending[py_id] = pending

        try:
            self._send_json(cmd)
        except Exception as exc:
            with self._pending_lock:
                self._pending.pop(py_id, None)
            logger.exception("[NT] Send failed: %s", exc)
            return {
                "success": False,
                "error": f"Send failed: {exc}",
                "order_id": py_id,
                "dry_run": False,
            }

        if not pending.event.wait(self.ack_timeout):
            with self._pending_lock:
                self._pending.pop(py_id, None)
            self._mark_desync(f"ACK timeout id={py_id}")
            return {
                "success": False,
                "error": f"ACK timeout after {self.ack_timeout}s",
                "order_id": py_id,
                "dry_run": False,
            }

        result = pending.result
        if result.get("rejected"):
            self._mark_desync(
                f"REJECT id={py_id} reason={result.get('reason')}"
            )
            return {
                "success": False,
                "error": f"NT rejected: {result.get('reason', '')}",
                "order_id": py_id,
                "dry_run": False,
            }

        return {
            "success": True,
            "order_id": py_id,
            "nt_order_id": result.get("nt_order_id"),
            "dry_run": False,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _send_json(self, msg: dict) -> None:
        line = (json.dumps(msg) + "\n").encode("utf-8")
        with self._sock_lock:
            if self._sock is None:
                raise ConnectionError("not connected")
            self._sock.sendall(line)

    def _next_id(self) -> str:
        self._order_seq += 1
        ts = int(time.time() * 1_000_000)
        return f"{ts}_{self._order_seq:04d}"

    def _mark_desync(self, reason: str) -> None:
        with self._state_lock:
            if self._position_synced:
                logger.error(
                    "[NT] DESYNC: %s -- mirror disabled until next entry",
                    reason,
                )
            self._position_synced = False

    # ------------------------------------------------------------------
    # Status (for /status endpoint)
    # ------------------------------------------------------------------
    def get_status(self) -> dict:
        with self._state_lock:
            synced = self._position_synced
        with self._pending_lock:
            pending_count = len(self._pending)
        return {
            "kind": "ninjatrader_tcp",
            "live_orders": self.live_orders,
            "host": self.host,
            "port": self.port,
            "connected": self._connected,
            "position_synced": synced,
            "pending_acks": pending_count,
            "account": self.account_name,
            "instrument": self.instrument,
            "last_position": self._last_position,
            "orders_sent": self._order_seq,
        }

    def get_live_equity(self, force_refresh: bool = False) -> Optional[float]:
        """Not exposed in v1. Could be added by extending the AddOn protocol."""
        return None
