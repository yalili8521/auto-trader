"""Unit tests for TradovateExecutor + MultiBrokerFanout integration.

Self-contained: mocks all requests.post / requests.get calls with
unittest.mock — no Tradovate account or network required. Run:

    .venv\\Scripts\\python.exe -m unittest test_tradovate_executor -v
"""

from __future__ import annotations

import os
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.gold_scalper.multi_broker_fanout import MultiBrokerFanout
from src.gold_scalper.tradovate_executor import (
    TradovateExecutor,
    _extract_error,
    _parse_iso8601,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code=200, json_body=None, text="") -> MagicMock:
    r = MagicMock()
    r.status_code = status_code
    r.text = text or (str(json_body) if json_body else "")
    r.json.return_value = json_body if json_body is not None else {}
    if status_code >= 400:
        r.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    else:
        r.raise_for_status.return_value = None
    return r


def _make_executor(live=False, account_id=None, max_qty=None) -> TradovateExecutor:
    return TradovateExecutor(
        username="testuser",
        password="testpass",
        cid="42",
        secret="00000000-0000-0000-0000-000000000000",
        app_id="iii-test",
        app_version="1.0",
        device_id="11111111-1111-1111-1111-111111111111",
        api_url="https://demo.tradovateapi.com/v1",
        symbol="MGCM6",
        live_orders=live,
        account_id=account_id,
        max_position_qty=max_qty,
    )


def _iso_in(seconds_from_now: int) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=seconds_from_now)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ---------------------------------------------------------------------------
# build_from_env
# ---------------------------------------------------------------------------

class TestBuildFromEnv(unittest.TestCase):

    def setUp(self):
        # Wipe any Tradovate vars the caller's shell might have.
        self._saved = {}
        for key in list(os.environ.keys()):
            if key.startswith("TRADOVATE_"):
                self._saved[key] = os.environ.pop(key)

    def tearDown(self):
        for key in list(os.environ.keys()):
            if key.startswith("TRADOVATE_"):
                del os.environ[key]
        for key, value in self._saved.items():
            os.environ[key] = value

    def test_returns_none_when_creds_blank(self):
        self.assertIsNone(TradovateExecutor.build_from_env())

    def test_returns_none_when_only_some_filled(self):
        os.environ["TRADOVATE_USERNAME"] = "u"
        os.environ["TRADOVATE_PASSWORD"] = "p"
        # Missing CID, SECRET, DEVICE_ID, SYMBOL
        self.assertIsNone(TradovateExecutor.build_from_env())

    def test_returns_instance_when_all_required_set(self):
        os.environ.update({
            "TRADOVATE_USERNAME": "u",
            "TRADOVATE_PASSWORD": "p",
            "TRADOVATE_CID": "42",
            "TRADOVATE_SECRET": "abc",
            "TRADOVATE_DEVICE_ID": "dev",
            "TRADOVATE_SYMBOL": "MGCM6",
        })
        td = TradovateExecutor.build_from_env()
        self.assertIsNotNone(td)
        self.assertEqual(td.username, "u")
        self.assertEqual(td.symbol, "MGCM6")
        self.assertFalse(td.live_orders)  # default

    def test_live_flag_parsing(self):
        base = {
            "TRADOVATE_USERNAME": "u", "TRADOVATE_PASSWORD": "p",
            "TRADOVATE_CID": "42", "TRADOVATE_SECRET": "abc",
            "TRADOVATE_DEVICE_ID": "dev", "TRADOVATE_SYMBOL": "MGCM6",
        }
        for live_val, expected in [
            ("1", True), ("true", True), ("yes", True),
            ("0", False), ("false", False), ("", False),
        ]:
            os.environ.update(base)
            os.environ["TRADOVATE_LIVE"] = live_val
            td = TradovateExecutor.build_from_env()
            self.assertEqual(td.live_orders, expected, f"live={live_val!r}")

    def test_account_id_parsed_as_int(self):
        os.environ.update({
            "TRADOVATE_USERNAME": "u", "TRADOVATE_PASSWORD": "p",
            "TRADOVATE_CID": "42", "TRADOVATE_SECRET": "abc",
            "TRADOVATE_DEVICE_ID": "dev", "TRADOVATE_SYMBOL": "MGCM6",
            "TRADOVATE_ACCOUNT_ID": "987654",
        })
        td = TradovateExecutor.build_from_env()
        self.assertEqual(td.account_id, 987654)

    def test_invalid_account_id_falls_back_to_resolve(self):
        os.environ.update({
            "TRADOVATE_USERNAME": "u", "TRADOVATE_PASSWORD": "p",
            "TRADOVATE_CID": "42", "TRADOVATE_SECRET": "abc",
            "TRADOVATE_DEVICE_ID": "dev", "TRADOVATE_SYMBOL": "MGCM6",
            "TRADOVATE_ACCOUNT_ID": "not-a-number",
        })
        td = TradovateExecutor.build_from_env()
        self.assertIsNone(td.account_id)


# ---------------------------------------------------------------------------
# Validation + dry-run path
# ---------------------------------------------------------------------------

class TestPlaceOrderValidation(unittest.TestCase):

    def test_invalid_side_rejected(self):
        td = _make_executor(live=False)
        r = td.place_order(side="HOLD", quantity=1)
        self.assertFalse(r["success"])
        self.assertIn("Invalid side", r["error"])

    def test_zero_quantity_rejected(self):
        td = _make_executor(live=False)
        r = td.place_order(side="BUY", quantity=0)
        self.assertFalse(r["success"])
        self.assertIn("Invalid quantity", r["error"])

    def test_negative_quantity_rejected(self):
        td = _make_executor(live=False)
        r = td.place_order(side="BUY", quantity=-1)
        self.assertFalse(r["success"])

    def test_max_position_qty_enforced(self):
        td = _make_executor(live=False, max_qty=2)
        r = td.place_order(side="BUY", quantity=3)
        self.assertFalse(r["success"])
        self.assertIn("TRADOVATE_MAX_POSITION_QTY", r["error"])
        # 2 is allowed
        r2 = td.place_order(side="BUY", quantity=2)
        self.assertTrue(r2["success"])

    def test_dry_run_returns_success_without_network(self):
        td = _make_executor(live=False)
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post"
        ) as mock_post:
            r = td.place_order(side="BUY", quantity=1, comment="LONG Entry")
        self.assertTrue(r["success"])
        self.assertTrue(r["dry_run"])
        self.assertIn("dryrun_", r["order_id"])
        mock_post.assert_not_called()

    def test_side_buy_and_sell_both_accepted(self):
        td = _make_executor(live=False)
        for side in ("BUY", "SELL", "buy", "sell"):
            r = td.place_order(side=side, quantity=1)
            self.assertTrue(r["success"], f"side={side}")


# ---------------------------------------------------------------------------
# Auth + token caching
# ---------------------------------------------------------------------------

class TestAuth(unittest.TestCase):

    def test_authenticate_posts_correct_body_and_caches_token(self):
        td = _make_executor(live=True)
        auth_resp = _make_response(json_body={
            "accessToken": "tok-abc",
            "mdAccessToken": "md-abc",
            "expirationTime": _iso_in(3600),
            "userId": 999,
        })
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            return_value=auth_resp,
        ) as mock_post:
            td._authenticate()

        # One call, correct URL + body shape
        self.assertEqual(mock_post.call_count, 1)
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://demo.tradovateapi.com/v1/auth/accesstokenrequest")
        body = kwargs["json"]
        self.assertEqual(body["name"], "testuser")
        self.assertEqual(body["password"], "testpass")
        self.assertEqual(body["cid"], "42")  # sent as STRING per Tradovate's official curl
        self.assertEqual(body["sec"], "00000000-0000-0000-0000-000000000000")
        self.assertEqual(body["appId"], "iii-test")
        self.assertEqual(body["deviceId"], "11111111-1111-1111-1111-111111111111")

        # Token cached
        self.assertEqual(td._access_token, "tok-abc")
        self.assertGreater(td._token_expiry_ts, time.time())

    def test_authenticate_raises_when_no_access_token(self):
        td = _make_executor(live=True)
        bad_resp = _make_response(json_body={
            "errorText": "Invalid credentials",
        })
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            return_value=bad_resp,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                td._authenticate()
        self.assertIn("Invalid credentials", str(ctx.exception))

    def test_ensure_token_skips_refresh_when_fresh(self):
        td = _make_executor(live=True)
        td._access_token = "still-good"
        td._token_expiry_ts = time.time() + 3600
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post"
        ) as mock_post:
            td._ensure_token()
        mock_post.assert_not_called()

    def test_ensure_token_refreshes_when_near_expiry(self):
        td = _make_executor(live=True)
        td._access_token = "old"
        td._token_expiry_ts = time.time() + 60  # within 5-min buffer
        auth_resp = _make_response(json_body={
            "accessToken": "new-tok",
            "expirationTime": _iso_in(3600),
        })
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            return_value=auth_resp,
        ):
            td._ensure_token()
        self.assertEqual(td._access_token, "new-tok")


# ---------------------------------------------------------------------------
# Account resolution
# ---------------------------------------------------------------------------

class TestAccountResolution(unittest.TestCase):

    def test_resolve_picks_matching_username(self):
        td = _make_executor(live=True)
        td._access_token = "tok"
        get_resp = _make_response(json_body=[
            {"id": 100, "name": "other", "active": True},
            {"id": 200, "name": "testuser", "active": True},
            {"id": 300, "name": "third", "active": True},
        ])
        with patch(
            "src.gold_scalper.tradovate_executor.requests.get",
            return_value=get_resp,
        ):
            td._resolve_account_id()
        self.assertEqual(td.account_id, 200)

    def test_resolve_falls_back_to_first_when_no_match(self):
        td = _make_executor(live=True)
        td._access_token = "tok"
        get_resp = _make_response(json_body=[
            {"id": 100, "name": "other", "active": True},
        ])
        with patch(
            "src.gold_scalper.tradovate_executor.requests.get",
            return_value=get_resp,
        ):
            td._resolve_account_id()
        self.assertEqual(td.account_id, 100)

    def test_resolve_raises_when_empty(self):
        td = _make_executor(live=True)
        td._access_token = "tok"
        get_resp = _make_response(json_body=[])
        with patch(
            "src.gold_scalper.tradovate_executor.requests.get",
            return_value=get_resp,
        ):
            with self.assertRaises(RuntimeError):
                td._resolve_account_id()


# ---------------------------------------------------------------------------
# Live place_order — happy paths
# ---------------------------------------------------------------------------

class TestPlaceOrderLive(unittest.TestCase):

    def _stub_authed_executor(self, account_id=12345):
        td = _make_executor(live=True, account_id=account_id)
        td._access_token = "tok"
        td._token_expiry_ts = time.time() + 3600
        return td

    def test_live_place_order_posts_correct_body(self):
        td = self._stub_authed_executor()
        place_resp = _make_response(json_body={"orderId": 777})
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            return_value=place_resp,
        ) as mock_post:
            r = td.place_order(side="BUY", quantity=1, comment="LONG Entry")

        self.assertTrue(r["success"])
        self.assertEqual(r["order_id"], 777)
        self.assertFalse(r["dry_run"])

        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://demo.tradovateapi.com/v1/order/placeorder")
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer tok")
        body = kwargs["json"]
        self.assertEqual(body["accountSpec"], "testuser")
        self.assertEqual(body["accountId"], 12345)
        self.assertEqual(body["action"], "Buy")
        self.assertEqual(body["symbol"], "MGCM6")
        self.assertEqual(body["orderQty"], 1)
        self.assertEqual(body["orderType"], "Market")
        self.assertTrue(body["isAutomated"])

    def test_sell_action_is_capitalized(self):
        td = self._stub_authed_executor()
        place_resp = _make_response(json_body={"orderId": 778})
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            return_value=place_resp,
        ) as mock_post:
            td.place_order(side="SELL", quantity=2)
        body = mock_post.call_args.kwargs["json"]
        self.assertEqual(body["action"], "Sell")
        self.assertEqual(body["orderQty"], 2)


# ---------------------------------------------------------------------------
# Live place_order — error paths
# ---------------------------------------------------------------------------

class TestPlaceOrderErrors(unittest.TestCase):

    def _stub_authed_executor(self):
        td = _make_executor(live=True, account_id=12345)
        td._access_token = "tok"
        td._token_expiry_ts = time.time() + 3600
        return td

    def test_http_error_returns_failure(self):
        td = self._stub_authed_executor()
        bad_resp = _make_response(
            status_code=401,
            json_body={"errorText": "Unauthorized"},
        )
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            return_value=bad_resp,
        ):
            r = td.place_order(side="BUY", quantity=1)
        self.assertFalse(r["success"])
        self.assertEqual(r["status_code"], 401)
        self.assertIn("Unauthorized", r["error"])

    def test_soft_reject_with_failure_reason(self):
        td = self._stub_authed_executor()
        soft_reject = _make_response(json_body={
            "failureReason": "InsufficientFunds",
            "failureText": "Margin call",
        })
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            return_value=soft_reject,
        ):
            r = td.place_order(side="BUY", quantity=1)
        self.assertFalse(r["success"])
        self.assertIn("InsufficientFunds", r["error"])

    def test_unexpected_shape_returns_failure(self):
        td = self._stub_authed_executor()
        weird = _make_response(json_body={"something": "else"})
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            return_value=weird,
        ):
            r = td.place_order(side="BUY", quantity=1)
        self.assertFalse(r["success"])
        self.assertIn("Unexpected response", r["error"])

    def test_network_error_returns_failure(self):
        td = self._stub_authed_executor()
        import requests as _r
        with patch(
            "src.gold_scalper.tradovate_executor.requests.post",
            side_effect=_r.ConnectionError("DNS fail"),
        ):
            r = td.place_order(side="BUY", quantity=1)
        self.assertFalse(r["success"])
        self.assertIn("Network error", r["error"])


# ---------------------------------------------------------------------------
# Fanout integration
# ---------------------------------------------------------------------------

class TestFanoutIntegration(unittest.TestCase):

    def test_works_as_fanout_secondary(self):
        """Webull primary + Tradovate secondary: primary result returned,
        Tradovate failure does not propagate."""
        primary = MagicMock()
        primary.place_order.return_value = {
            "success": True, "order_id": "WB-1", "dry_run": False,
        }

        td = _make_executor(live=False)  # dry-run secondary
        fanout = MultiBrokerFanout(primary=primary, secondary=td)

        r = fanout.place_order(
            side="BUY", quantity=1, limit_price=None, comment="LONG Entry",
        )

        # Primary's result is what propagates
        self.assertTrue(r["success"])
        self.assertEqual(r["order_id"], "WB-1")
        # Both brokers got the call
        primary.place_order.assert_called_once()
        # Tradovate ran its dry-run path
        self.assertEqual(td._orders_sent, 1)

    def test_fanout_proxies_status(self):
        primary = MagicMock()
        primary.get_status.return_value = {"kind": "webull_mgc"}
        td = _make_executor(live=False)

        fanout = MultiBrokerFanout(primary=primary, secondary=td)
        status = fanout.get_status()

        self.assertEqual(status["primary"]["kind"], "webull_mgc")
        self.assertEqual(status["secondary"]["kind"], "tradovate_rest")


# ---------------------------------------------------------------------------
# Status + helpers
# ---------------------------------------------------------------------------

class TestStatusAndHelpers(unittest.TestCase):

    def test_get_status_shape(self):
        td = _make_executor(live=False)
        s = td.get_status()
        for key in (
            "kind", "live_orders", "api_url", "symbol", "account_id",
            "connected", "has_token", "token_remaining_sec", "orders_sent",
        ):
            self.assertIn(key, s)
        self.assertEqual(s["kind"], "tradovate_rest")
        self.assertFalse(s["has_token"])

    def test_parse_iso8601_z_suffix(self):
        ts = _parse_iso8601("2030-01-01T00:00:00.000Z")
        expected = datetime(2030, 1, 1, tzinfo=timezone.utc).timestamp()
        self.assertAlmostEqual(ts, expected, delta=1)

    def test_parse_iso8601_offset(self):
        ts = _parse_iso8601("2030-01-01T00:00:00+00:00")
        expected = datetime(2030, 1, 1, tzinfo=timezone.utc).timestamp()
        self.assertAlmostEqual(ts, expected, delta=1)

    def test_parse_iso8601_falls_back_when_missing(self):
        ts = _parse_iso8601(None)
        # Should be roughly 80 min from now (within a generous window)
        self.assertGreater(ts, time.time() + 60 * 60)
        self.assertLess(ts, time.time() + 100 * 60)

    def test_extract_error_falls_back_to_text(self):
        r = MagicMock()
        r.json.side_effect = Exception("not json")
        r.text = "Plain text error from server"
        msg = _extract_error(r)
        self.assertIn("Plain text error", msg)


# ---------------------------------------------------------------------------
# Reconciler hooks: get_order_status + get_position_qty (added 2026-05-11)
# ---------------------------------------------------------------------------

class TestGetOrderStatus(unittest.TestCase):

    def _live_executor(self):
        td = _make_executor(live=True, account_id=1000001)
        td._access_token = "tok"
        td._token_expiry_ts = time.time() + 3600
        return td

    def test_dry_run_returns_synthetic_filled(self):
        td = _make_executor(live=False)
        with patch("src.gold_scalper.tradovate_executor.requests.get") as mock_get:
            result = td.get_order_status(123)
        self.assertEqual(result["status"], "FILLED")
        self.assertTrue(result["dry_run"])
        mock_get.assert_not_called()

    def test_status_mapping_filled(self):
        td = self._live_executor()
        resp = _make_response(json_body={
            "ordStatus": "Filled",
            "cumQty": 2,
            "orderQty": 2,
            "avgPx": 4728.5,
        })
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=resp):
            r = td.get_order_status(123)
        self.assertEqual(r["status"], "FILLED")
        self.assertEqual(r["filled_quantity"], 2.0)
        self.assertEqual(r["total_quantity"], 2.0)
        self.assertEqual(r["filled_price"], 4728.5)

    def test_status_mapping_working_to_submitted(self):
        td = self._live_executor()
        resp = _make_response(json_body={
            "ordStatus": "Working",
            "cumQty": 0,
            "orderQty": 1,
            "avgPx": 0,
        })
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=resp):
            r = td.get_order_status(123)
        self.assertEqual(r["status"], "SUBMITTED")
        self.assertEqual(r["filled_quantity"], 0.0)

    def test_status_mapping_cancelled(self):
        td = self._live_executor()
        for raw_status in ("Cancelled", "Canceled", "Expired"):
            resp = _make_response(json_body={"ordStatus": raw_status, "cumQty": 0, "orderQty": 1, "avgPx": 0})
            with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=resp):
                r = td.get_order_status(123)
            self.assertEqual(r["status"], "CANCELLED", f"raw={raw_status}")

    def test_status_mapping_rejected(self):
        td = self._live_executor()
        resp = _make_response(json_body={"ordStatus": "Rejected", "cumQty": 0, "orderQty": 1, "avgPx": 0})
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=resp):
            r = td.get_order_status(123)
        self.assertEqual(r["status"], "REJECTED")

    def test_unknown_status_falls_back(self):
        td = self._live_executor()
        resp = _make_response(json_body={"ordStatus": "Weirdness", "cumQty": 0, "orderQty": 1, "avgPx": 0})
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=resp):
            r = td.get_order_status(123)
        self.assertEqual(r["status"], "UNKNOWN")

    def test_http_error_returns_unknown_with_error(self):
        td = self._live_executor()
        resp = _make_response(status_code=500, json_body={"errorText": "Server error"})
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=resp):
            r = td.get_order_status(123)
        self.assertEqual(r["status"], "UNKNOWN")
        self.assertEqual(r["status_code"], 500)
        self.assertIn("Server error", r["error"])

    def test_network_error_returns_unknown(self):
        td = self._live_executor()
        import requests as _r
        with patch(
            "src.gold_scalper.tradovate_executor.requests.get",
            side_effect=_r.ConnectionError("DNS fail"),
        ):
            r = td.get_order_status(123)
        self.assertEqual(r["status"], "UNKNOWN")
        self.assertIn("network", r["error"])


class TestGetPositionQty(unittest.TestCase):

    def _live_executor(self):
        td = _make_executor(live=True, account_id=1000001)
        td._access_token = "tok"
        td._token_expiry_ts = time.time() + 3600
        return td

    def test_dry_run_returns_zero(self):
        td = _make_executor(live=False)
        self.assertEqual(td.get_position_qty(), 0)

    def test_live_no_account_id_returns_none(self):
        td = _make_executor(live=True, account_id=None)
        td._access_token = "tok"
        td._token_expiry_ts = time.time() + 3600
        self.assertIsNone(td.get_position_qty())

    def test_resolves_contract_then_finds_matching_position(self):
        td = self._live_executor()
        contract_resp = _make_response(json_body={"id": 2287764, "name": "MGCM6"})
        position_resp = _make_response(json_body=[
            {"accountId": 999, "contractId": 9999, "netPos": 5},
            {"accountId": 1000001, "contractId": 2287764, "netPos": 2},  # match
        ])
        with patch(
            "src.gold_scalper.tradovate_executor.requests.get",
            side_effect=[contract_resp, position_resp],
        ):
            qty = td.get_position_qty()
        self.assertEqual(qty, 2)
        self.assertEqual(td._contract_id, 2287764)  # cached

    def test_no_matching_position_returns_zero(self):
        td = self._live_executor()
        td._contract_id = 2287764  # pre-cached
        position_resp = _make_response(json_body=[
            {"accountId": 999, "contractId": 9999, "netPos": 5},
        ])
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=position_resp):
            qty = td.get_position_qty()
        self.assertEqual(qty, 0)

    def test_short_position_returns_negative(self):
        td = self._live_executor()
        td._contract_id = 2287764
        position_resp = _make_response(json_body=[
            {"accountId": 1000001, "contractId": 2287764, "netPos": -3},
        ])
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=position_resp):
            qty = td.get_position_qty()
        self.assertEqual(qty, -3)

    def test_contract_resolution_failure_returns_none(self):
        td = self._live_executor()
        # /contract/find returns empty / 404
        bad_resp = _make_response(status_code=404, json_body={"errorText": "not found"})
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=bad_resp):
            qty = td.get_position_qty()
        self.assertIsNone(qty)

    def test_position_http_error_returns_none(self):
        td = self._live_executor()
        td._contract_id = 2287764
        bad_resp = _make_response(status_code=500, json_body={})
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=bad_resp):
            qty = td.get_position_qty()
        self.assertIsNone(qty)


class TestResolveContractId(unittest.TestCase):

    def test_caches_after_first_resolution(self):
        td = _make_executor(live=True, account_id=1000001)
        td._access_token = "tok"
        resp = _make_response(json_body={"id": 2287764, "name": "MGCM6"})
        with patch(
            "src.gold_scalper.tradovate_executor.requests.get",
            return_value=resp,
        ) as mock_get:
            cid1 = td._resolve_contract_id()
            cid2 = td._resolve_contract_id()  # should hit cache
        self.assertEqual(cid1, 2287764)
        self.assertEqual(cid2, 2287764)
        self.assertEqual(mock_get.call_count, 1)  # cached, second call did not hit network

    def test_returns_none_on_no_id_in_response(self):
        td = _make_executor(live=True, account_id=1000001)
        td._access_token = "tok"
        resp = _make_response(json_body={})  # empty response
        with patch("src.gold_scalper.tradovate_executor.requests.get", return_value=resp):
            cid = td._resolve_contract_id()
        self.assertIsNone(cid)


if __name__ == "__main__":
    unittest.main(verbosity=2)
