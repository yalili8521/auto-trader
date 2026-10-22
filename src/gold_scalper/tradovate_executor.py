"""Tradovate REST API executor — direct futures execution without NT8 bridge.

Calls the Tradovate REST API directly (no NinjaTrader desktop required).
Designed as a drop-in for MultiBrokerFanout — same place_order signature
and return shape as NinjaTraderExecutor and WebullMGCBroker.

Architecture
------------
- Auth via POST /auth/accesstokenrequest with name + password + cid + sec.
- Token cached in-memory with expiry check; re-auth when <5 min remain.
- Account ID fetched from /account/list on first auth if TRADOVATE_ACCOUNT_ID
  is blank in env; otherwise used directly.
- place_order -> POST /order/placeorder. Market orders only in v1.
- No WebSocket fills yet — caller treats success/error from the REST
  response as authoritative.

Sim vs Live
-----------
TRADOVATE_LIVE=0 (default) hits TRADOVATE_API_URL which defaults to
demo.tradovateapi.com — real Tradovate sim account, real auth, real
orders that fill on paper. Flip TRADOVATE_LIVE=1 and the live URL to go
real. Live trades cost real money — sim first, always.

Environment variables (defined in secrets/.env)
-----------------------------------------------
TRADOVATE_LIVE          "1"/"true" to send real orders. Else dry-run
                        (logs only, no HTTP calls).
TRADOVATE_USERNAME      Tradovate login name.
TRADOVATE_PASSWORD      Tradovate login password.
TRADOVATE_CID           Numeric client id (from "Generate API Key").
TRADOVATE_SECRET        UUID secret (from "Generate API Key").
TRADOVATE_APP_ID        Free-form app label (default: iii-gold-scalper).
TRADOVATE_APP_VERSION   Free-form version label (default: 1.0).
TRADOVATE_DEVICE_ID     Stable UUID generated once; keep across restarts.
TRADOVATE_API_URL       Base URL. Demo: https://demo.tradovateapi.com/v1
                        Live: https://live.tradovateapi.com/v1
TRADOVATE_ACCOUNT_ID    Optional. If blank, resolved on first auth via
                        /account/list (logs the resolved id; copy into
                        .env for fast subsequent boots).
TRADOVATE_SYMBOL        Contract symbol in Tradovate format, e.g.
                        "MGCM6" (MGC June 2026). NOT TradingView's
                        "MGC1!" continuation.
TRADOVATE_MAX_POSITION_QTY   Optional safety cap. Rejects place_order
                             when requested qty exceeds this value.

Lifecycle
---------
- build_from_env() -> instance or None (None if any required cred blank).
- caller calls start() once — performs initial auth + resolves account
  id. Non-fatal if auth fails: place_order will retry and surface the
  error to the caller.
- place_order(side, quantity, limit_price=None, comment="") — sync,
  returns dict with success/error/order_id/dry_run.
- stop() is a no-op (HTTP is stateless; no socket to close).

Caveat: response field names (accessToken / expirationTime / errorText /
failureReason / orderId etc.) come from my recollection of Tradovate's
API. Verify against current docs at https://api.tradovate.com/ before
first live run — if Tradovate has renamed any of these in a recent API
version, the _authenticate / place_order parsers will need a small fix.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

# Re-auth when token has less than this many seconds remaining.
_TOKEN_REFRESH_BUFFER_SEC = 300

# HTTP timeout for every Tradovate request. Tradovate p99 is well under
# a second, but allow generous headroom for first-auth handshakes.
_HTTP_TIMEOUT_SEC = 10.0


class TradovateExecutor:
    """Direct Tradovate REST API broker.

    Mirrors NinjaTraderExecutor.place_order signature so it can drop
    into MultiBrokerFanout. Market orders only in v1.

    Usage:
        td = TradovateExecutor.build_from_env()
        if td is not None:
            td.start()
            r = td.place_order(side="BUY", quantity=1, comment="LONG Entry")
    """

    def __init__(
        self,
        username: str,
        password: str,
        cid: str,
        secret: str,
        app_id: str,
        app_version: str,
        device_id: str,
        api_url: str,
        symbol: str,
        live_orders: bool,
        account_id: Optional[int] = None,
        max_position_qty: Optional[int] = None,
    ):
        self.username = username
        self._password = password
        self._cid = cid
        self._secret = secret
        self.app_id = app_id
        self.app_version = app_version
        self.device_id = device_id
        self.api_url = api_url.rstrip("/")
        self.symbol = symbol
        self.live_orders = live_orders
        self.account_id = account_id
        self.max_position_qty = max_position_qty

        # Token cache (lock guards both fields).
        self._access_token: Optional[str] = None
        self._token_expiry_ts: float = 0.0
        self._auth_lock = threading.Lock()

        # Contract ID cached after first resolution (for position queries).
        self._contract_id: Optional[int] = None
        self._equity_cache: Optional[float] = None
        self._equity_cache_ts: float = 0.0

        # Stats / state for /status endpoint.
        self._orders_sent = 0
        self._last_order_result: Dict[str, Any] = {}
        self._connected = False
        self._last_auth_error: Optional[str] = None

        logger.info(
            "[TD] Tradovate broker constructed: live=%s, url=%s, "
            "symbol=%s, user=%s, account=%s",
            live_orders, self.api_url, symbol, username,
            account_id if account_id else "(to-resolve)",
        )

    @classmethod
    def build_from_env(cls) -> Optional["TradovateExecutor"]:
        username = os.environ.get("TRADOVATE_USERNAME", "").strip()
        password = os.environ.get("TRADOVATE_PASSWORD", "").strip()
        cid = os.environ.get("TRADOVATE_CID", "").strip()
        secret = os.environ.get("TRADOVATE_SECRET", "").strip()
        app_id = os.environ.get("TRADOVATE_APP_ID", "iii-gold-scalper").strip()
        app_version = os.environ.get("TRADOVATE_APP_VERSION", "1.0").strip()
        device_id = os.environ.get("TRADOVATE_DEVICE_ID", "").strip()
        api_url = os.environ.get(
            "TRADOVATE_API_URL", "https://demo.tradovateapi.com/v1"
        ).strip()
        symbol = os.environ.get("TRADOVATE_SYMBOL", "").strip()
        live_flag = os.environ.get("TRADOVATE_LIVE", "").strip().lower()
        live = live_flag in ("1", "true", "yes")

        acct_str = os.environ.get("TRADOVATE_ACCOUNT_ID", "").strip()
        account_id: Optional[int] = None
        if acct_str:
            try:
                account_id = int(acct_str)
            except ValueError:
                logger.warning(
                    "[TD] Invalid TRADOVATE_ACCOUNT_ID=%r — will resolve at start()",
                    acct_str,
                )

        max_qty_str = os.environ.get("TRADOVATE_MAX_POSITION_QTY", "").strip()
        max_position_qty: Optional[int] = None
        if max_qty_str:
            try:
                max_position_qty = int(max_qty_str)
            except ValueError:
                logger.warning(
                    "[TD] Invalid TRADOVATE_MAX_POSITION_QTY=%r — ignoring",
                    max_qty_str,
                )

        missing = []
        if not username:  missing.append("TRADOVATE_USERNAME")
        if not password:  missing.append("TRADOVATE_PASSWORD")
        if not cid:       missing.append("TRADOVATE_CID")
        if not secret:    missing.append("TRADOVATE_SECRET")
        if not device_id: missing.append("TRADOVATE_DEVICE_ID")
        if not symbol:    missing.append("TRADOVATE_SYMBOL")

        if missing:
            logger.info(
                "[TD] Disabled — missing env vars: %s. Fill them in "
                "secrets/.env to enable.", ", ".join(missing),
            )
            return None

        return cls(
            username=username,
            password=password,
            cid=cid,
            secret=secret,
            app_id=app_id,
            app_version=app_version,
            device_id=device_id,
            api_url=api_url,
            symbol=symbol,
            live_orders=live,
            account_id=account_id,
            max_position_qty=max_position_qty,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Authenticate + resolve account_id. Non-fatal if it fails."""
        if not self.live_orders:
            logger.info("[TD] Dry-run mode — skipping initial auth")
            self._connected = False
            return

        try:
            self._authenticate()
            if self.account_id is None:
                self._resolve_account_id()
            self._connected = True
            logger.info(
                "[TD] Auth OK — account_id=%s, token expires in %.0fs",
                self.account_id,
                self._token_expiry_ts - time.time(),
            )
        except Exception as exc:
            self._last_auth_error = str(exc)
            self._connected = False
            logger.exception(
                "[TD] Initial auth failed: %s — place_order will retry",
                exc,
            )

    def stop(self) -> None:
        # HTTP is stateless — nothing to clean up.
        self._connected = False

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    def _authenticate(self) -> None:
        body = {
            "name": self.username,
            "password": self._password,
            "appId": self.app_id,
            "appVersion": self.app_version,
            "deviceId": self.device_id,
            "cid": self._cid,  # send as STRING per Tradovate's official curl example
            "sec": self._secret,
        }
        url = f"{self.api_url}/auth/accesstokenrequest"
        logger.info("[TD] Auth POST %s", url)
        r = requests.post(url, json=body, timeout=_HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        payload = r.json()

        token = payload.get("accessToken")
        if not token:
            err = payload.get("errorText") or str(payload)
            raise RuntimeError(f"Tradovate auth returned no token: {err}")

        expiry_ts = _parse_iso8601(payload.get("expirationTime"))

        with self._auth_lock:
            self._access_token = token
            self._token_expiry_ts = expiry_ts
            self._last_auth_error = None

    def _ensure_token(self) -> None:
        with self._auth_lock:
            remaining = self._token_expiry_ts - time.time()
            need_refresh = (
                self._access_token is None
                or remaining < _TOKEN_REFRESH_BUFFER_SEC
            )
        if need_refresh:
            self._authenticate()

    def _resolve_account_id(self) -> None:
        """Call /account/list, log all accounts, pick a default."""
        url = f"{self.api_url}/account/list"
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {self._access_token}"},
            timeout=_HTTP_TIMEOUT_SEC,
        )
        r.raise_for_status()
        accounts = r.json()
        if not isinstance(accounts, list) or not accounts:
            raise RuntimeError("No accounts returned from /account/list")

        # Log every account so the user can spot the right id at boot.
        for acct in accounts:
            logger.info(
                "[TD] Account found: id=%s name=%s active=%s",
                acct.get("id"), acct.get("name"), acct.get("active"),
            )

        # Prefer the first active account that matches the username.
        chosen = None
        for acct in accounts:
            if acct.get("name") == self.username and acct.get("active", True):
                chosen = acct
                break
        if chosen is None:
            chosen = accounts[0]

        self.account_id = int(chosen["id"])
        logger.info(
            "[TD] Using account_id=%s (name=%s). "
            "For faster boots, save to .env: TRADOVATE_ACCOUNT_ID=%s",
            self.account_id, chosen.get("name"), self.account_id,
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
        if self.max_position_qty and quantity > self.max_position_qty:
            return {
                "success": False,
                "error": (
                    f"Quantity {quantity} exceeds TRADOVATE_MAX_POSITION_QTY="
                    f"{self.max_position_qty}"
                ),
                "dry_run": False,
            }

        # Dry-run path: log + return success without touching the network.
        if not self.live_orders:
            self._orders_sent += 1
            stub_id = f"dryrun_{self._orders_sent:06d}"
            logger.info(
                "[TD] DRY-RUN PLACE id=%s side=%s qty=%s symbol=%s "
                "tv_price=%s comment=%s",
                stub_id, side_norm, quantity, self.symbol, limit_price, comment,
            )
            self._last_order_result = {
                "success": True, "order_id": stub_id, "dry_run": True,
            }
            return self._last_order_result

        # Live path — token + account_id must be ready before posting.
        try:
            self._ensure_token()
        except Exception as exc:
            logger.exception("[TD] Token refresh failed: %s", exc)
            self._last_auth_error = str(exc)
            return {
                "success": False,
                "error": f"Auth failed: {exc}",
                "dry_run": False,
            }

        if self.account_id is None:
            try:
                self._resolve_account_id()
            except Exception as exc:
                logger.exception("[TD] account_id resolve failed: %s", exc)
                return {
                    "success": False,
                    "error": f"account_id unresolved: {exc}",
                    "dry_run": False,
                }

        body = {
            "accountSpec": self.username,
            "accountId": self.account_id,
            "action": "Buy" if side_norm == "BUY" else "Sell",
            "symbol": self.symbol,
            "orderQty": int(quantity),
            "orderType": "Market",
            "isAutomated": True,
        }
        url = f"{self.api_url}/order/placeorder"

        try:
            r = requests.post(
                url,
                json=body,
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=_HTTP_TIMEOUT_SEC,
            )
        except requests.RequestException as exc:
            logger.exception("[TD] place_order network error: %s", exc)
            return {
                "success": False,
                "error": f"Network error: {exc}",
                "dry_run": False,
            }

        self._orders_sent += 1

        if r.status_code >= 400:
            err = _extract_error(r)
            logger.error(
                "[TD] place_order HTTP %s — %s",
                r.status_code, err,
            )
            self._last_order_result = {
                "success": False,
                "error": err,
                "status_code": r.status_code,
                "dry_run": False,
            }
            return self._last_order_result

        try:
            payload = r.json()
        except Exception:
            payload = {}

        order_id = payload.get("orderId")
        failure_reason = (
            payload.get("failureReason") or payload.get("failureText")
        )

        if order_id is None and failure_reason:
            logger.error(
                "[TD] place_order soft-reject: %s — body=%s",
                failure_reason, payload,
            )
            self._last_order_result = {
                "success": False,
                "error": str(failure_reason),
                "dry_run": False,
            }
            return self._last_order_result

        if order_id is None:
            logger.warning(
                "[TD] place_order: unexpected response shape: %s", payload,
            )
            self._last_order_result = {
                "success": False,
                "error": f"Unexpected response: {payload}",
                "dry_run": False,
            }
            return self._last_order_result

        logger.info(
            "[TD] PLACE OK id=%s side=%s qty=%s symbol=%s comment=%s",
            order_id, side_norm, quantity, self.symbol, comment,
        )
        self._last_order_result = {
            "success": True,
            "order_id": order_id,
            "dry_run": False,
        }
        return self._last_order_result

    # ------------------------------------------------------------------
    # Status (for /status endpoint)
    # ------------------------------------------------------------------
    def get_status(self) -> dict:
        with self._auth_lock:
            token_remaining = max(0.0, self._token_expiry_ts - time.time())
            has_token = self._access_token is not None
        return {
            "kind": "tradovate_rest",
            "live_orders": self.live_orders,
            "api_url": self.api_url,
            "symbol": self.symbol,
            "account_id": self.account_id,
            "connected": self._connected,
            "has_token": has_token,
            "token_remaining_sec": int(token_remaining),
            "orders_sent": self._orders_sent,
            "last_auth_error": self._last_auth_error,
            "last_order_result": self._last_order_result,
            "max_position_qty": self.max_position_qty,
        }

    def get_live_equity(self, force_refresh: bool = False) -> Optional[float]:
        """Real account equity via /cashBalance/getCashBalanceSnapshot.

        2026-10-14: implemented so the Plan A contract cap and the daily
        breaker anchor on the real account instead of stale paper equity.
        Returns netLiq (falls back to totalCashValue) or None on any error.
        Cached for 60s unless force_refresh.
        """
        if not self.live_orders:
            return None
        now = time.time()
        if (not force_refresh
                and self._equity_cache is not None
                and now - self._equity_cache_ts < 60.0):
            return self._equity_cache
        if self.account_id is None:
            return None
        try:
            self._ensure_token()
        except Exception as exc:
            logger.warning("[TD] get_live_equity: auth refresh failed: %s", exc)
            return None
        try:
            r = requests.post(
                f"{self.api_url}/cashBalance/getcashbalancesnapshot",
                json={"accountId": self.account_id},
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=_HTTP_TIMEOUT_SEC,
            )
        except requests.RequestException as exc:
            logger.warning("[TD] get_live_equity: network error: %s", exc)
            return None
        if r.status_code >= 400:
            logger.warning(
                "[TD] get_live_equity HTTP %s: %s", r.status_code, r.text[:200],
            )
            return None
        try:
            payload = r.json()
        except Exception:
            return None
        val = payload.get("netLiq")
        if val is None:
            val = payload.get("totalCashValue")
        if val is None:
            logger.warning("[TD] get_live_equity: no netLiq/totalCashValue in %s",
                           str(payload)[:200])
            return None
        self._equity_cache = float(val)
        self._equity_cache_ts = now
        return self._equity_cache

    # ------------------------------------------------------------------
    # Reconciler hooks (added 2026-05-11 — match WebullMGCBroker shape)
    # ------------------------------------------------------------------
    def _resolve_contract_id(self) -> Optional[int]:
        """Look up the contract ID for self.symbol via /contract/find.

        Caches result in self._contract_id. Returns None on failure.
        """
        if self._contract_id is not None:
            return self._contract_id
        try:
            r = requests.get(
                f"{self.api_url}/contract/find",
                params={"name": self.symbol},
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=_HTTP_TIMEOUT_SEC,
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("id"):
                self._contract_id = int(data["id"])
                logger.info(
                    "[TD] Resolved contract_id=%s for symbol=%s",
                    self._contract_id, self.symbol,
                )
                return self._contract_id
        except Exception as exc:
            logger.warning(
                "[TD] _resolve_contract_id failed for symbol=%s: %s",
                self.symbol, exc,
            )
        return None

    def get_order_status(self, client_order_id) -> dict:
        """Reconciler hook — fetch current order state.

        Returns a dict shaped like WebullMGCBroker.get_order_status:
            {
              "status": "FILLED" | "SUBMITTED" | "CANCELLED" | "REJECTED" | "UNKNOWN",
              "filled_quantity": float,
              "total_quantity": float,
              "filled_price": float (avg fill, 0.0 if unfilled),
              "raw": dict (full Tradovate response),
              "error": str (only if query failed),
              "dry_run": bool (only in dry-run mode),
            }
        Dry-run mode returns synthetic FILLED so the reconciler is a no-op
        and doesn't probe paper trades.
        """
        if not self.live_orders:
            return {
                "status": "FILLED",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "raw": {},
                "dry_run": True,
            }

        try:
            self._ensure_token()
        except Exception as exc:
            return {
                "status": "UNKNOWN",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "raw": {},
                "error": f"auth_failed: {exc}",
            }

        try:
            r = requests.get(
                f"{self.api_url}/order/item",
                params={"id": client_order_id},
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=_HTTP_TIMEOUT_SEC,
            )
        except requests.RequestException as exc:
            return {
                "status": "UNKNOWN",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "raw": {},
                "error": f"network: {exc}",
            }

        if r.status_code >= 400:
            return {
                "status": "UNKNOWN",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "raw": {},
                "error": _extract_error(r),
                "status_code": r.status_code,
            }

        try:
            data = r.json()
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}

        ord_status = (data.get("ordStatus") or "").strip()
        status_map = {
            "Working": "SUBMITTED",
            "PendingNew": "SUBMITTED",
            "PendingReplace": "SUBMITTED",
            "Filled": "FILLED",
            "Cancelled": "CANCELLED",
            "Canceled": "CANCELLED",
            "Rejected": "REJECTED",
            "Expired": "CANCELLED",
        }
        return {
            "status": status_map.get(ord_status, "UNKNOWN"),
            "filled_quantity": float(data.get("cumQty") or 0.0),
            "total_quantity": float(data.get("orderQty") or 0.0),
            "filled_price": float(data.get("avgPx") or 0.0),
            "raw": data,
        }

    def get_position_qty(self, force_refresh: bool = False) -> Optional[int]:
        """Reconciler hook — signed broker position quantity for self.symbol.

        Positive = long, negative = short, 0 = flat. Returns None on error
        (auth fail, network error, contract unresolved, http error).
        Dry-run mode returns 0 (no real position).
        """
        if not self.live_orders:
            return 0
        if self.account_id is None:
            return None

        try:
            self._ensure_token()
        except Exception as exc:
            logger.warning("[TD] get_position_qty: auth refresh failed: %s", exc)
            return None

        contract_id = self._contract_id or self._resolve_contract_id()
        if contract_id is None:
            return None

        try:
            r = requests.get(
                f"{self.api_url}/position/list",
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=_HTTP_TIMEOUT_SEC,
            )
        except requests.RequestException as exc:
            logger.warning("[TD] get_position_qty: network error: %s", exc)
            return None

        if r.status_code >= 400:
            logger.warning(
                "[TD] get_position_qty HTTP %s: %s",
                r.status_code, r.text[:200],
            )
            return None

        try:
            positions = r.json()
        except Exception:
            positions = []
        if not isinstance(positions, list):
            positions = []

        for pos in positions:
            if (pos.get("accountId") == self.account_id
                    and pos.get("contractId") == contract_id):
                return int(pos.get("netPos") or 0)
        return 0  # no matching position = flat


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_iso8601(s: Optional[str]) -> float:
    """Parse Tradovate's expirationTime (ISO 8601 UTC) to unix seconds.

    Falls back to "now + 80 min" (Tradovate's documented token TTL) if the
    field is missing or unparseable.
    """
    if not s:
        return time.time() + 80 * 60
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception as exc:
        logger.warning(
            "[TD] Could not parse expirationTime=%r (%s) — assuming 80 min",
            s, exc,
        )
        return time.time() + 80 * 60


def _extract_error(resp: requests.Response) -> str:
    """Pull a human error string out of a Tradovate error response."""
    try:
        j = resp.json()
        if isinstance(j, dict):
            return (
                j.get("errorText")
                or j.get("failureReason")
                or j.get("failureText")
                or str(j)
            )
        return str(j)
    except Exception:
        return resp.text[:200]
