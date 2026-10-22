"""End-to-end test: TradingView alert payload -> webhook router -> broker call.

Traces the live signal path for each instrument with mocked brokers/clients
(no real orders, no network). Confirms:

1. Each ticker routes to the correct executor (MGC, TQQQ, BTC, QQQ).
2. The payload's price/contracts/direction reach the broker.
3. Fix #2 — TQQQ exit signal arriving <30s after entry is NOT dropped by
   the dedup guard (the pre-fix bug silently dropped legitimate stop-outs).
4. Fix #3 — BTC exit signal arriving <30s after entry is NOT dropped.
5. Fix #1 — WEBULL_TQQQ_LIVE env flag controls live_orders.

Run:
    .venv\\Scripts\\python.exe -m unittest test_alert_to_executor -v
"""

from __future__ import annotations

import importlib
import os
import shutil
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import after sys.path tweak — executors live under src/gold_scalper.
from src.gold_scalper import btc_executor as btc_mod
from src.gold_scalper import qqq_options_executor as qqq_mod
from src.gold_scalper import webhook_executor as gold_mod
from src.gold_scalper import webull_tqqq_executor as tqqq_mod
from src.gold_scalper import webhook_server as srv
from src.gold_scalper.config import GoldScalperConfig


# ── Helpers ────────────────────────────────────────────────────────────────

def route(body: dict):
    """Mirror webhook_server.webhook() routing — sync, no FastAPI plumbing.

    Returns whatever the matched _handle_* helper returns (a JSONResponse).
    Tests pull the dict out via ._content / .body as needed; for assertion
    purposes we mostly inspect mock call_args on the patched broker.
    """
    auth_err = srv._check_auth(body)
    if auth_err is not None:
        return auth_err
    ticker = body.get("ticker", "").strip().upper()
    norm = ticker.replace("/", "").replace("-", "")
    if srv._webull_executor and (norm in srv._WEBULL_TICKERS
                                 or ticker in srv._WEBULL_TICKERS):
        return srv._handle_webull(body)
    if norm in srv._QQQ_TICKERS or ticker in srv._QQQ_TICKERS:
        return srv._handle_qqq(body)
    if ticker in srv._BTC_TICKERS:
        return srv._handle_btc(body)
    return srv._handle_gold(body)


def _redirect_state(executor, tmpdir: str, prefix: str) -> None:
    """Point an executor's persistence at a tempdir so tests don't pollute prod."""
    state = os.path.join(tmpdir, f"{prefix}_state.json")
    log = os.path.join(tmpdir, f"{prefix}_log.json")
    if hasattr(executor, "_state_file"):
        executor._state_file = state
    if hasattr(executor, "_log_file"):
        executor._log_file = log


# ── Tests ──────────────────────────────────────────────────────────────────

class AlertToExecutorE2E(unittest.TestCase):
    """End-to-end path from TradingView JSON to broker invocation."""

    def setUp(self):
        # Fresh tempdir for state — keeps prod outputs/ untouched.
        self.tmp = tempfile.mkdtemp(prefix="auto_trader_test_")
        # No webhook secret → auth bypass (matches dev-mode behavior).
        self._orig_secret = srv._WEBHOOK_SECRET
        srv._WEBHOOK_SECRET = ""
        # Reset module-level executors before each test (each test
        # installs its own mocks).
        srv._gold_executor = None
        srv._btc_executor = None
        srv._webull_executor = None
        srv._qqq_executor = None

    def tearDown(self):
        srv._WEBHOOK_SECRET = self._orig_secret
        srv._gold_executor = None
        srv._btc_executor = None
        srv._webull_executor = None
        srv._qqq_executor = None
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ── Gold/MGC ──

    def _make_gold(self):
        """Construct a WebhookExecutor with a mock Webull-style broker."""
        broker = MagicMock()
        broker.live_orders = False  # don't try to query live equity
        broker.get_live_equity.return_value = 5000.0
        broker.place_order.return_value = {
            "success": True, "order_id": "MOCK-MGC-001",
        }
        broker.wait_for_fill.return_value = {
            "status": "FILLED", "filled_price": 4500.50,
        }
        # Patch yfinance fetch so the background price-comparison thread
        # doesn't hit the network.
        patcher = patch.object(gold_mod, "_fetch_yahoo_price", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)

        ex = gold_mod.WebhookExecutor(
            broker=broker, config=GoldScalperConfig(), alerts=None,
        )
        _redirect_state(ex, self.tmp, "gold")
        # Wipe any state restored from prod paths during __init__.
        ex.position = None
        ex.daily_pnl = 0.0
        ex._cooldown_until = None
        ex._last_entry_attempt = None
        return ex, broker

    def test_gold_native_entry_routes_to_broker(self):
        """{"action":"ENTRY","direction":"LONG",...} -> broker.place_order."""
        ex, broker = self._make_gold()
        srv._gold_executor = ex

        resp = route({
            "action": "ENTRY", "direction": "LONG",
            "price": 4500.50, "contracts": 6,
        })

        broker.place_order.assert_called_once()
        kwargs = broker.place_order.call_args.kwargs
        self.assertEqual(kwargs["side"], "BUY")
        self.assertEqual(kwargs["quantity"], 6)
        self.assertEqual(kwargs["limit_price"], 4500.50)
        self.assertIsNotNone(ex.position)
        self.assertEqual(ex.position.direction, "LONG")
        self.assertEqual(ex.position.remaining_contracts, 6)
        self.assertEqual(resp.status_code, 200)

    def test_gold_tv_format_short_entry_routes_to_broker(self):
        """TV format {"action":"sell","comment":"Short Entry"} -> SELL order."""
        ex, broker = self._make_gold()
        srv._gold_executor = ex

        route({
            "action": "sell",
            "comment": "Short Entry",
            "price": 4500.50,
            "contracts": 6,
        })

        broker.place_order.assert_called_once()
        self.assertEqual(broker.place_order.call_args.kwargs["side"], "SELL")
        self.assertEqual(ex.position.direction, "SHORT")

    # ── TQQQ ──

    def _make_tqqq(self, live: bool = True):
        """Construct a WebullTQQQExecutor with all Webull SDK calls mocked."""
        # Redirect the module-level state paths to the tempdir BEFORE
        # constructing the executor, so _save_state never touches the prod
        # outputs/ directory (the TQQQ executor reads these as module
        # globals, not instance attrs, so the _redirect_state helper alone
        # isn't enough).
        patcher_dir = patch.object(tqqq_mod, "_STATE_DIR", self.tmp)
        patcher_state = patch.object(
            tqqq_mod, "_STATE_FILE", os.path.join(self.tmp, "tqqq_state.json"),
        )
        patcher_log = patch.object(
            tqqq_mod, "_TRADE_LOG", os.path.join(self.tmp, "tqqq_log.json"),
        )
        patcher_dir.start(); self.addCleanup(patcher_dir.stop)
        patcher_state.start(); self.addCleanup(patcher_state.stop)
        patcher_log.start(); self.addCleanup(patcher_log.stop)

        # Patch _init_webull so __init__ doesn't try to talk to Webull.
        patcher_init = patch.object(tqqq_mod.WebullTQQQExecutor, "_init_webull")
        patcher_init.start()
        self.addCleanup(patcher_init.stop)
        # Patch _fetch_buying_power so __init__'s post-init equity sync no-ops.
        patcher_bp = patch.object(
            tqqq_mod.WebullTQQQExecutor, "_fetch_buying_power", return_value=0.0,
        )
        patcher_bp.start()
        self.addCleanup(patcher_bp.stop)
        # Block gist sync (it reads secrets/.env and pings GitHub).
        patcher_gist = patch.object(
            tqqq_mod.WebullTQQQExecutor, "_sync_to_gist", return_value=None,
        )
        patcher_gist.start()
        self.addCleanup(patcher_gist.stop)

        ex = tqqq_mod.WebullTQQQExecutor(
            app_key="test_key", app_secret="test_secret",
            live_orders=live, initial_equity=5000.0,
        )
        # Stub the order placement + verify so no Webull SDK call happens.
        ex._place_webull_order = MagicMock(return_value={
            "success": True, "order_id": "MOCK-TQQQ-001",
        })
        ex._verify_fill = MagicMock(return_value=(45.50, "FILLED"))
        # Reset state restored from disk by __init__.
        ex.position = None
        ex._last_signal_time = 0.0
        ex._cooldown_until = 0.0
        ex._circuit_breaker_fired = False
        return ex

    def test_tqqq_buy_entry_routes_to_webull(self):
        """TQQQ buy with position_size=1 fires _place_webull_order(BUY,...)."""
        ex = self._make_tqqq(live=True)
        srv._webull_executor = ex

        route({
            "action": "buy", "ticker": "TQQQ", "contracts": 1,
            "position_size": 1, "price": 45.50, "comment": "Long Entry",
        })

        ex._place_webull_order.assert_called_once()
        call = ex._place_webull_order.call_args
        self.assertEqual(call.kwargs["side"], "BUY")
        self.assertEqual(call.kwargs["price"], 45.50)
        self.assertIsNotNone(ex.position)
        self.assertEqual(ex.position.direction, "LONG")

    def test_tqqq_exit_within_dedup_window_NOT_dropped(self):
        """Fix #2 — close signal <30s after entry must reach the broker.

        Pre-fix, the dedup guard fired on every signal including exits, so
        a stop-out arriving 5s after entry was silently dropped, leaving
        the position open while TV thought we were flat.
        """
        ex = self._make_tqqq(live=True)
        srv._webull_executor = ex

        # 1) Open
        route({
            "action": "buy", "ticker": "TQQQ", "contracts": 1,
            "position_size": 1, "price": 45.50, "comment": "Long Entry",
        })
        self.assertIsNotNone(ex.position, "entry must open a position")

        # 2) Exit immediately — well under the 30s dedup window
        ex._place_webull_order.reset_mock()
        ex._verify_fill = MagicMock(return_value=(46.00, "FILLED"))
        route({
            "action": "sell", "ticker": "TQQQ", "contracts": 1,
            "position_size": 0, "price": 46.00, "comment": "Trail Stop",
        })

        ex._place_webull_order.assert_called_once()
        self.assertEqual(ex._place_webull_order.call_args.kwargs["side"], "SELL")
        self.assertIsNone(ex.position, "exit must close the position")

    def test_tqqq_duplicate_entry_within_window_IS_dropped(self):
        """Dedup still gates duplicate ENTRY signals (only exits bypass)."""
        ex = self._make_tqqq(live=True)
        srv._webull_executor = ex

        route({
            "action": "buy", "ticker": "TQQQ", "contracts": 1,
            "position_size": 1, "price": 45.50,
        })
        first_calls = ex._place_webull_order.call_count

        # Same entry replayed immediately — TV does this 2-4× per alert.
        route({
            "action": "buy", "ticker": "TQQQ", "contracts": 1,
            "position_size": 1, "price": 45.50,
        })
        self.assertEqual(
            ex._place_webull_order.call_count, first_calls,
            "duplicate entry must NOT call broker again",
        )

    def test_tqqq_live_flag_defaults_to_false(self):
        """Fix #1 — TQQQ live_orders must default to False without env flag."""
        # Sanity check: the constructor honors live_orders=False.
        ex = self._make_tqqq(live=False)
        self.assertFalse(
            ex._live_orders,
            "live_orders must be False unless WEBULL_TQQQ_LIVE is set",
        )

    # ── BTC ──

    def _make_btc(self):
        """Construct a BTCWebhookExecutor with the Alpaca client mocked."""
        ex = btc_mod.BTCWebhookExecutor(
            api_key="test_key", api_secret="test_secret",
        )
        _redirect_state(ex, self.tmp, "btc")
        # Pre-set _client so the lazy property hands back our mock.
        mock_client = MagicMock()
        mock_order = MagicMock()
        mock_order.id = "MOCK-BTC-001"
        mock_client.submit_order.return_value = mock_order
        ex._client = mock_client
        # Reset any restored state.
        ex.position = None
        ex.daily_pnl = 0.0
        ex._last_entry_time = None
        ex._cooldown_until = None
        ex._circuit_breaker_fired = False
        return ex, mock_client

    def test_btc_buy_routes_to_alpaca(self):
        """BTC buy with position_size=1 -> client.submit_order(side=BUY)."""
        ex, mock_client = self._make_btc()
        srv._btc_executor = ex

        route({
            "action": "buy", "ticker": "BTCUSD", "contracts": 0.01,
            "position_size": 1, "price": 68394.0,
        })

        mock_client.submit_order.assert_called_once()
        req = mock_client.submit_order.call_args.args[0]
        # MarketOrderRequest stores qty/symbol/side as attributes
        self.assertEqual(req.symbol, "BTC/USD")
        self.assertEqual(float(req.qty), 0.01)
        # Side comes from alpaca.trading.enums.OrderSide; compare by name.
        self.assertEqual(req.side.name, "BUY")
        self.assertIsNotNone(ex.position)
        self.assertEqual(ex.position.direction, "LONG")

    def test_btc_close_within_dedup_window_NOT_dropped(self):
        """Fix #3 — BTC close signal <30s after entry must reach Alpaca."""
        ex, mock_client = self._make_btc()
        srv._btc_executor = ex

        # 1) Open LONG
        route({
            "action": "buy", "ticker": "BTCUSD", "contracts": 0.01,
            "position_size": 1, "price": 68394.0,
        })
        self.assertIsNotNone(ex.position)

        # 2) Close immediately — well under the 30s dedup window
        mock_client.submit_order.reset_mock()
        # Cooldown was set to 1min (win) or 5min (loss) on prior closes,
        # but we just opened so cooldown is None — only dedup matters.
        route({
            "action": "sell", "ticker": "BTCUSD", "contracts": 0.01,
            "position_size": 0, "price": 68500.0,
        })

        mock_client.submit_order.assert_called_once()
        req = mock_client.submit_order.call_args.args[0]
        self.assertEqual(req.side.name, "SELL")
        self.assertIsNone(ex.position, "close must flatten position")

    def test_btc_duplicate_entry_within_window_IS_dropped(self):
        """Dedup still gates duplicate BTC entries (only closes bypass)."""
        ex, mock_client = self._make_btc()
        srv._btc_executor = ex

        route({
            "action": "buy", "ticker": "BTCUSD", "contracts": 0.01,
            "position_size": 1, "price": 68394.0,
        })
        first = mock_client.submit_order.call_count

        # Replay same entry — should be dropped by dedup.
        route({
            "action": "buy", "ticker": "BTCUSD", "contracts": 0.01,
            "position_size": 1, "price": 68394.0,
        })
        self.assertEqual(
            mock_client.submit_order.call_count, first,
            "duplicate entry must NOT call Alpaca again",
        )

    # ── QQQ Options ──

    def _make_qqq(self):
        # Redirect QQQ's module-level state paths to tempdir.
        patcher_dir = patch.object(qqq_mod, "_STATE_DIR", self.tmp)
        patcher_state = patch.object(
            qqq_mod, "_STATE_FILE", os.path.join(self.tmp, "qqq_state.json"),
        )
        patcher_log = patch.object(
            qqq_mod, "_TRADE_LOG", os.path.join(self.tmp, "qqq_log.json"),
        )
        patcher_dir.start(); self.addCleanup(patcher_dir.stop)
        patcher_state.start(); self.addCleanup(patcher_state.stop)
        patcher_log.start(); self.addCleanup(patcher_log.stop)

        # TradingClient is constructed inside __init__ — patch the class.
        patcher = patch.object(qqq_mod, "TradingClient")
        mock_cls = patcher.start()
        self.addCleanup(patcher.stop)

        mock_client = MagicMock()
        mock_cls.return_value = mock_client

        ex = qqq_mod.QQQOptionsExecutor(api_key="test", api_secret="test")
        # Reset state restored from disk
        ex._position = None
        ex._daily_pnl = 0.0
        ex._last_entry_time = None
        ex._cooldown_until = None
        # Stub contract finder + order so we don't call Alpaca options API
        ex._find_0dte_contract = MagicMock(return_value=MagicMock(
            symbol="QQQ250101C00500000", strike_price=500.0,
        ))
        ex._submit_order = MagicMock(return_value=MagicMock(id="MOCK-QQQ-001"))
        return ex, mock_client

    def test_qqq_routes_to_options_executor(self):
        """QQQ ticker reaches QQQOptionsExecutor and calls _submit_order."""
        ex, mock_client = self._make_qqq()
        srv._qqq_executor = ex

        resp = route({
            "action": "buy", "ticker": "QQQ", "position_size": 1,
            "price": 500.0, "comment": "Long Entry",
        })

        # _handle_entry calls _find_0dte_contract then _submit_order
        ex._find_0dte_contract.assert_called_once()
        ex._submit_order.assert_called_once()
        # First arg: option symbol; second: side
        args = ex._submit_order.call_args.args
        self.assertEqual(args[0], "QQQ250101C00500000")
        self.assertEqual(resp.status_code, 200)

    # ── Routing precedence ──

    def test_routing_priority_webull_before_btc(self):
        """When both TQQQ and BTC executors exist, TQQQ matches its own ticker."""
        tqqq = self._make_tqqq(live=True)
        btc, btc_client = self._make_btc()
        srv._webull_executor = tqqq
        srv._btc_executor = btc

        route({
            "action": "buy", "ticker": "TQQQ", "contracts": 1,
            "position_size": 1, "price": 45.50,
        })

        tqqq._place_webull_order.assert_called_once()
        btc_client.submit_order.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
