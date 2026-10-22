"""Unit tests for NinjaTraderExecutor (TCP bidirectional) + MultiBrokerFanout.

Self-contained: spins up a mock TCP server in-process to play the role of
the NinjaScript AddOn. No NT install or network required. Run:

    .venv\\Scripts\\python.exe -m unittest test_nt_executor -v

The mock AddOn (FakeBridge) is a single-client TCP server that:
  - reads one or more JSON commands from Python
  - replies according to a script (ACK / REJECT / silence) configured per test
  - can push follow-up events (ORDER_UPDATE, POSITION) on demand
"""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
import unittest
from unittest.mock import MagicMock

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.gold_scalper.ninjatrader_executor import NinjaTraderExecutor
from src.gold_scalper.multi_broker_fanout import MultiBrokerFanout


# ---------------------------------------------------------------------------
# In-process mock AddOn
# ---------------------------------------------------------------------------

class FakeBridge:
    """One-shot TCP server mimicking PythonBridge.cs.

    Configure behavior with:
      - reply_for_place: dict with keys "ack" (bool), "reason" (str if reject),
                         "delay" (seconds), "nt_order_id" (str)
                         OR "silent" (bool) to suppress the ACK entirely
                         (useful for ACK-timeout tests)
      - drop_after_place: bool — close socket after first PLACE
    Push extra events with .push_event(dict).

    Lifecycle: bridge.start() launches the listener thread on an auto-port,
    bridge.port returns the bound port. bridge.stop() shuts down.
    """

    def __init__(self):
        self.reply_for_place = {"ack": True, "delay": 0, "nt_order_id": "NT-1"}
        self.drop_after_place = False

        self._listener: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self.port = self._listener.getsockname()[1]

        self._client: socket.socket | None = None
        self._client_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

        self.received_commands: list[dict] = []
        self.connected_event = threading.Event()

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            self._listener.close()
        except Exception:
            pass
        with self._client_lock:
            if self._client is not None:
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = None
        self._thread.join(timeout=2)

    def push_event(self, evt: dict):
        """Send an out-of-band event (e.g., ORDER_UPDATE / POSITION)."""
        with self._client_lock:
            if self._client is None:
                return
            data = (json.dumps(evt) + "\n").encode("utf-8")
            try:
                self._client.sendall(data)
            except Exception:
                pass

    def _loop(self):
        try:
            self._listener.settimeout(2.0)
            sock, _ = self._listener.accept()
        except Exception:
            return
        with self._client_lock:
            self._client = sock
        self.connected_event.set()

        f = sock.makefile("r", encoding="utf-8", newline="\n")
        try:
            while not self._stop.is_set():
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:
                    continue
                self.received_commands.append(msg)
                self._handle(sock, msg)
        finally:
            try:
                sock.close()
            except Exception:
                pass
            with self._client_lock:
                self._client = None

    def _handle(self, sock: socket.socket, msg: dict):
        cmd = msg.get("cmd", "")
        if cmd == "PING":
            self._send(sock, {"event": "PONG"})
        elif cmd == "PLACE":
            cfg = self.reply_for_place
            if cfg.get("delay", 0) > 0:
                time.sleep(cfg["delay"])
            if cfg.get("silent"):
                return
            if cfg.get("ack", True):
                self._send(sock, {
                    "event": "ACK",
                    "id": msg.get("id"),
                    "ntOrderId": cfg.get("nt_order_id", "NT-1"),
                })
            else:
                self._send(sock, {
                    "event": "REJECT",
                    "id": msg.get("id"),
                    "reason": cfg.get("reason", "test reject"),
                })
            if self.drop_after_place:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass

    @staticmethod
    def _send(sock: socket.socket, evt: dict):
        try:
            sock.sendall((json.dumps(evt) + "\n").encode("utf-8"))
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_executor(bridge: FakeBridge, ack_timeout=2.0) -> NinjaTraderExecutor:
    nt = NinjaTraderExecutor(
        host="127.0.0.1",
        port=bridge.port,
        account_name="Sim101",
        instrument="MGC 06-26",
        live_orders=True,
        ack_timeout=ack_timeout,
    )
    nt.start()
    # Give the reconnect thread a moment to dial in.
    bridge.connected_event.wait(timeout=2.0)
    # And let the PING round-trip so the executor is fully primed.
    deadline = time.time() + 2.0
    while time.time() < deadline and not nt._connected:
        time.sleep(0.05)
    return nt


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class NTConnectAndPlaceTests(unittest.TestCase):
    def setUp(self):
        self.bridge = FakeBridge()
        self.bridge.start()

    def tearDown(self):
        self.bridge.stop()

    def test_ack_path_returns_success(self):
        nt = _make_executor(self.bridge)
        try:
            result = nt.place_order(
                side="BUY", quantity=2, limit_price=3250.5, comment="LONG Entry",
            )
            self.assertTrue(result["success"])
            self.assertEqual(result["nt_order_id"], "NT-1")
            self.assertFalse(result["dry_run"])
            # Verify the command shape the bridge received
            place = next(m for m in self.bridge.received_commands if m.get("cmd") == "PLACE")
            self.assertEqual(place["account"], "Sim101")
            self.assertEqual(place["instrument"], "MGC 06-26")
            self.assertEqual(place["action"], "BUY")
            self.assertEqual(place["qty"], 2)
            self.assertEqual(place["orderType"], "MARKET")
        finally:
            nt.stop()

    def test_reject_returns_failure_and_marks_desync(self):
        self.bridge.reply_for_place = {
            "ack": False, "reason": "Account not found: Sim101",
        }
        nt = _make_executor(self.bridge)
        try:
            result = nt.place_order(side="BUY", quantity=1, comment="LONG Entry")
            self.assertFalse(result["success"])
            self.assertIn("Account not found", result["error"])
            # Subsequent close skipped due to desync
            close = nt.place_order(side="SELL", quantity=1, comment="TP1 close")
            self.assertFalse(close["success"])
            self.assertIn("desynced", close["error"])
        finally:
            nt.stop()

    def test_new_entry_resets_desync(self):
        # First entry rejected -> desync
        self.bridge.reply_for_place = {"ack": False, "reason": "test reject"}
        nt = _make_executor(self.bridge)
        try:
            r1 = nt.place_order(side="BUY", quantity=1, comment="LONG Entry")
            self.assertFalse(r1["success"])
            # Reconfigure bridge to ack the next one
            self.bridge.reply_for_place = {
                "ack": True, "nt_order_id": "NT-2",
            }
            # New ENTRY should resync and succeed
            r2 = nt.place_order(side="BUY", quantity=1, comment="LONG Entry")
            self.assertTrue(r2["success"])
            self.assertEqual(r2["nt_order_id"], "NT-2")
        finally:
            nt.stop()

    def test_ack_timeout_marks_desync(self):
        self.bridge.reply_for_place = {"silent": True}
        nt = _make_executor(self.bridge, ack_timeout=0.5)
        try:
            result = nt.place_order(side="BUY", quantity=1, comment="LONG Entry")
            self.assertFalse(result["success"])
            self.assertIn("ACK timeout", result["error"])
            self.assertFalse(nt._position_synced)
        finally:
            nt.stop()

    def test_late_rejection_via_order_update_marks_desync(self):
        nt = _make_executor(self.bridge)
        try:
            r = nt.place_order(side="BUY", quantity=1, comment="LONG Entry")
            self.assertTrue(r["success"])
            self.assertTrue(nt._position_synced)
            # Now push a late Rejected ORDER_UPDATE
            self.bridge.push_event({
                "event": "ORDER_UPDATE", "id": r["order_id"],
                "ntOrderId": "NT-1", "state": "Rejected", "filledQty": 0,
                "avgFillPrice": 0, "reason": "InsufficientMargin",
            })
            # Give the reader thread a moment to process
            deadline = time.time() + 1.0
            while time.time() < deadline and nt._position_synced:
                time.sleep(0.05)
            self.assertFalse(nt._position_synced)
        finally:
            nt.stop()

    def test_position_event_recorded(self):
        nt = _make_executor(self.bridge)
        try:
            self.bridge.push_event({
                "event": "POSITION", "account": "Sim101",
                "instrument": "MGC 06-26", "qty": 2, "avgPrice": 3250.5,
                "marketPosition": "Long",
            })
            deadline = time.time() + 1.0
            while time.time() < deadline and not nt._last_position:
                time.sleep(0.05)
            self.assertEqual(nt._last_position.get("qty"), 2)
            self.assertEqual(nt._last_position.get("marketPosition"), "Long")
        finally:
            nt.stop()


class NTNotConnectedTests(unittest.TestCase):
    def test_place_order_when_no_addon_running(self):
        # Pick a port nothing is listening on -- expect immediate "not connected"
        nt = NinjaTraderExecutor(
            host="127.0.0.1", port=1,  # privileged port, guaranteed refused
            account_name="Sim101", instrument="MGC 06-26",
            live_orders=True, ack_timeout=0.5,
        )
        nt.start()
        # Reconnect loop will be looping; place_order should fail fast
        # because _connected is False.
        try:
            result = nt.place_order(side="BUY", quantity=1, comment="LONG Entry")
            self.assertFalse(result["success"])
            self.assertIn("not connected", result["error"])
        finally:
            nt.stop()


class NTDryRunTests(unittest.TestCase):
    def test_dry_run_returns_success_without_socket(self):
        nt = NinjaTraderExecutor(
            host="127.0.0.1", port=1,
            account_name="Sim101", instrument="MGC 06-26",
            live_orders=False, ack_timeout=0.5,
        )
        nt.start()  # no-op in dry-run
        try:
            r = nt.place_order(side="BUY", quantity=2,
                               limit_price=3250.5, comment="LONG Entry")
            self.assertTrue(r["success"])
            self.assertTrue(r["dry_run"])
            self.assertTrue(r["order_id"].startswith("dryrun_"))
        finally:
            nt.stop()


class NTValidationTests(unittest.TestCase):
    def test_invalid_side(self):
        nt = NinjaTraderExecutor(
            host="127.0.0.1", port=1,
            account_name="Sim101", instrument="MGC 06-26",
            live_orders=False,
        )
        r = nt.place_order(side="HOLD", quantity=1)
        self.assertFalse(r["success"])
        self.assertIn("Invalid side", r["error"])

    def test_invalid_qty(self):
        nt = NinjaTraderExecutor(
            host="127.0.0.1", port=1,
            account_name="Sim101", instrument="MGC 06-26",
            live_orders=False,
        )
        r = nt.place_order(side="BUY", quantity=0)
        self.assertFalse(r["success"])
        self.assertIn("Invalid quantity", r["error"])


class NTBuildFromEnvTests(unittest.TestCase):
    def setUp(self):
        self._saved = {
            k: os.environ.get(k) for k in
            ("NT_BRIDGE_HOST", "NT_BRIDGE_PORT", "NT_ACCOUNT_NAME",
             "NT_INSTRUMENT", "NT_LIVE", "NT_ACK_TIMEOUT")
        }
        for k in self._saved:
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_returns_none_when_unconfigured(self):
        self.assertIsNone(NinjaTraderExecutor.build_from_env())

    def test_returns_none_when_partial_env(self):
        os.environ["NT_ACCOUNT_NAME"] = "Sim101"
        # NT_INSTRUMENT missing
        self.assertIsNone(NinjaTraderExecutor.build_from_env())


# ---------------------------------------------------------------------------
# Fanout
# ---------------------------------------------------------------------------

class FanoutTests(unittest.TestCase):
    def _make_broker(self, success=True, order_id="abc"):
        m = MagicMock()
        m.place_order.return_value = {
            "success": success, "order_id": order_id, "dry_run": False,
        }
        m.get_status.return_value = {"kind": "mock"}
        return m

    def test_both_brokers_called(self):
        primary = self._make_broker(order_id="webull-1")
        secondary = self._make_broker(order_id="nt-1")
        fan = MultiBrokerFanout(primary=primary, secondary=secondary)
        result = fan.place_order(side="BUY", quantity=2, limit_price=3250.5)
        primary.place_order.assert_called_once()
        secondary.place_order.assert_called_once()
        self.assertEqual(result["order_id"], "webull-1")

    def test_secondary_exception_does_not_block_primary(self):
        primary = self._make_broker(order_id="webull-1")
        secondary = MagicMock()
        secondary.place_order.side_effect = RuntimeError("NT bridge dropped")
        fan = MultiBrokerFanout(primary=primary, secondary=secondary)
        result = fan.place_order(side="BUY", quantity=1)
        self.assertTrue(result["success"])
        self.assertEqual(result["order_id"], "webull-1")

    def test_secondary_rejection_does_not_block_primary(self):
        primary = self._make_broker(order_id="webull-1")
        secondary = self._make_broker(success=False)
        secondary.place_order.return_value = {
            "success": False, "error": "NT desynced", "dry_run": False,
        }
        fan = MultiBrokerFanout(primary=primary, secondary=secondary)
        result = fan.place_order(side="SELL", quantity=1)
        self.assertTrue(result["success"])

    def test_unknown_attr_proxies_to_primary(self):
        primary = self._make_broker()
        primary.get_live_equity = MagicMock(return_value=12345.67)
        primary.symbol = "MGCM6"
        secondary = self._make_broker()
        fan = MultiBrokerFanout(primary=primary, secondary=secondary)
        self.assertEqual(fan.get_live_equity(), 12345.67)
        self.assertEqual(fan.symbol, "MGCM6")


if __name__ == "__main__":
    unittest.main(verbosity=2)
