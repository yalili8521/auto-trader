"""Webull MGC Broker — live futures order placement via Webull OpenAPI v3.

This is a thin broker layer that WebhookExecutor (paper multi-TP tracker) can
inject to mirror entries/TP hits/exits as real orders on Webull. The paper
tracker stays the single source of truth for position state; this class just
converts (side, qty, price) tuples into Webull v3 futures orders.

Design goals
------------
1. Default dry-run — real orders require explicit WEBULL_MGC_LIVE=1.
2. Same Webull account/credentials as the TQQQ executor (equity + futures
   entitlement on one account_id).
3. No state of its own — WebhookExecutor owns position/PnL state. Broker is
   stateless between calls except for cached api_client and instrument info.
4. Fail-closed — any error returns {"success": False} so the caller rolls
   back its paper-state mutation.

Endpoint used
-------------
``POST /openapi/trade/order/place`` (v3) — unified endpoint for stocks /
options / futures / crypto. The SDK auto-sets the ``category`` request
header from ``market + "_" + instrument_type``, so passing
``market="US", instrument_type="FUTURES"`` produces ``category: US_FUTURES``.

Futures order body fields (from Webull developer docs)::

    {
      "combo_type": "NORMAL",
      "client_order_id": "<unique>",
      "symbol": "MGCM6",          # specific month code (auto-rolled)
      "instrument_type": "FUTURES",
      "market": "US",
      "order_type": "LIMIT",
      "limit_price": "3250.50",
      "quantity": "6",
      "side": "BUY",              # or SELL to close long / open short
      "time_in_force": "DAY",
      "entrust_type": "QTY"
    }

Environment variables
---------------------
WEBULL_APP_KEY       — shared with TQQQ executor (same account)
WEBULL_APP_SECRET    — shared with TQQQ executor (same account)
WEBULL_MGC_SYMBOL    — specific contract code Webull accepts (e.g., "MGCM6").
                       Defaults to "MGCM6" (June 2026). Webull's order API
                       does NOT accept the continuous alias "MGCmain" — it
                       returns FUTURES_TICKER_ID_CAN_NOT_TRADE. Must use
                       explicit month codes: G=Feb J=Apr M=Jun Q=Aug V=Oct Z=Dec.
WEBULL_MGC_LIVE      — "1" or "true" to fire real orders. Any other value
                       (incl. unset) keeps the broker in dry-run mode.
WEBULL_MGC_ACCOUNT_ID — optional explicit account_id override. If unset,
                        uses the first account returned by /account/list.

Safety notes
------------
- MGC tick size is $0.10 (10 oz micro). We round limit_price to 1 decimal
  to avoid "invalid price tick" rejects. Verify this matches Webull's
  expected precision on first live order.
- MGC contract multiplier is 10 oz, so 1 tick = $1 P&L per contract.
- Quantity is INT — Webull expects whole contracts, no fractions.
- Side semantics: assuming standard netting (BUY opens long / closes short;
  SELL opens short / closes long). This is the documented behavior for most
  futures APIs but was NOT explicitly confirmed in Webull's public docs —
  the first live round-trip will verify.
"""

from __future__ import annotations

import calendar
import logging
import math
import os
import time
import uuid
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Rounding precision for MGC limit prices (ticks at $0.10)
_MGC_PRICE_DECIMALS = 1

# MGC tick size in dollars (10oz micro; $1 P&L per contract per tick)
_MGC_TICK_SIZE = 0.10

# Marketable-LIMIT offset (in ticks). We push BUY above / SELL below the
# TradingView signal price by this many ticks so the order crosses the
# spread and fills immediately. 10 ticks = $1.00 max slippage per contract.
#
# Why this matters on a netting futures account: if a SELL-to-close limit
# order rests unfilled (price moves away), we're still long on paper but
# short-selling fails. Worse, the next opposite-direction alert could
# silently open a new short position because the original long never
# closed. Guaranteeing the fill trades a known $1 slippage cost for
# eliminated silent-position-flip risk.
_MARKETABLE_OFFSET_TICKS = 10

# Canonical futures field values
_COMBO_TYPE = "NORMAL"
_INSTRUMENT_TYPE = "FUTURES"
_MARKET = "US"
_TIME_IN_FORCE = "DAY"
_ENTRUST_TYPE = "QTY"


# =====================================================================
# Auto-roll: determine current MGC front-month contract from date alone
# =====================================================================
#
# CME Micro Gold (MGC) has bi-monthly delivery months.  Webull's ORDER
# API rejects the continuous alias ``MGCmain`` — it only accepts
# specific month codes like ``MGCM6``.  This module computes the active
# front-month code so the webhook broker never needs a hardcoded symbol.
#
# Roll rule:
#   Front month = first contract whose First Notice Date (FND) is more
#   than _ROLL_DAYS_BEFORE_FND calendar days in the future.
#
# FND for each MGC contract = last business day of the month preceding
# the delivery month.  Verified against Webull's get_futures_instrument
# data (see scripts/webull_mgc_lookup.py):
#
#   MGCJ6 (Apr 26) FND=2026-03-31   MGCM6 (Jun 26) FND=2026-05-29
#   MGCQ6 (Aug 26) FND=2026-07-31   MGCV6 (Oct 26) FND=2026-09-30
#   MGCZ6 (Dec 26) FND=2026-11-30   MGCG7 (Feb 27) FND=2027-01-29
#
# TradingView naming:
#   TV continuous:  COMEX:MGC1!     (auto-rolls by volume)
#   TV specific:    COMEX:MGCM2026  (4-digit year)
#   Webull specific: MGCM6          (1-digit year)
# =====================================================================

# (month_code_letter, delivery_month_number)
_MGC_CONTRACT_SCHEDULE = [
    ("G", 2),   # February
    ("J", 4),   # April
    ("M", 6),   # June
    ("Q", 8),   # August
    ("V", 10),  # October
    ("Z", 12),  # December
]

# Calendar days before First Notice Date to switch to the next contract.
# CME open interest typically migrates 5-10 days before FND; TradingView's
# MGC1! rolls based on volume in roughly the same window.  7 days is
# conservative enough to never trade an expiring contract, yet tight
# enough that we stay aligned with TV's continuous-contract pricing.
_ROLL_DAYS_BEFORE_FND = 7


def _last_business_day_of(year: int, month: int) -> date:
    """Return last Mon-Fri of the given month."""
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    while d.weekday() >= 5:  # Sat=5, Sun=6
        d -= timedelta(days=1)
    return d


def _first_notice_date(delivery_year: int, delivery_month: int) -> date:
    """FND = last business day of the month before delivery month."""
    fnd_month = delivery_month - 1
    fnd_year = delivery_year
    if fnd_month < 1:
        fnd_month = 12
        fnd_year -= 1
    return _last_business_day_of(fnd_year, fnd_month)


def get_front_month_symbol(
    reference_date: Optional[date] = None,
    roll_days_before_fnd: int = _ROLL_DAYS_BEFORE_FND,
) -> str:
    """Return the Webull symbol for the active MGC front-month contract.

    Pure date arithmetic — no API calls, no network.

    Examples::

        >>> get_front_month_symbol(date(2026, 4, 16))   # before Jun FND
        'MGCM6'
        >>> get_front_month_symbol(date(2026, 5, 23))   # after Jun roll date
        'MGCQ6'
        >>> get_front_month_symbol(date(2026, 11, 25))  # after Dec roll date
        'MGCG7'

    Args:
        reference_date: evaluation date (default: today).
        roll_days_before_fnd: how many calendar days before FND to roll.

    Returns:
        Webull symbol string, e.g. ``"MGCM6"`` (June 2026).
    """
    if reference_date is None:
        reference_date = date.today()

    for year_offset in range(3):          # current year + next 2
        year = reference_date.year + year_offset
        for code, month in _MGC_CONTRACT_SCHEDULE:
            fnd = _first_notice_date(year, month)
            roll_date = fnd - timedelta(days=roll_days_before_fnd)
            if reference_date < roll_date:
                year_digit = year % 10    # 2026 → 6
                return f"MGC{code}{year_digit}"

    # Unreachable: 3 years × 6 contracts = 18 contracts ahead
    logger.warning(
        "[AUTO-ROLL] Could not determine front month — fallback MGCM6"
    )
    return "MGCM6"


def get_tv_symbol(webull_symbol: str) -> str:
    """Convert Webull MGC symbol to TradingView specific-month symbol.

    ``MGCM6`` → ``MGCM2026``  (COMEX:MGCM2026 on TradingView)

    The TradingView continuous contract is always ``COMEX:MGC1!``,
    which should track the same specific month during normal trading.
    """
    if len(webull_symbol) < 5 or not webull_symbol.startswith("MGC"):
        return f"UNKNOWN({webull_symbol})"

    month_code = webull_symbol[3]
    if not webull_symbol[4].isdigit():
        return f"UNKNOWN({webull_symbol})"
    year_digit = int(webull_symbol[4])

    # Resolve single digit → full year (assumes 2020s decade)
    current_decade = (date.today().year // 10) * 10   # 2020
    full_year = current_decade + year_digit
    # Guard: if >5 years in the past, bump to next decade
    if full_year < date.today().year - 5:
        full_year += 10

    return f"MGC{month_code}{full_year}"


def log_auto_roll_info(reference_date: Optional[date] = None) -> str:
    """Compute front-month symbol and log full diagnostics at startup.

    Returns the Webull symbol (caller can use it directly).
    """
    if reference_date is None:
        reference_date = date.today()

    symbol = get_front_month_symbol(reference_date)
    tv_symbol = get_tv_symbol(symbol)

    # Derive FND / roll date for the chosen contract
    month_code = symbol[3]
    year_digit = int(symbol[4])
    delivery_month = next(
        (m for code, m in _MGC_CONTRACT_SCHEDULE if code == month_code),
        None,
    )
    fnd_str = roll_str = "unknown"
    if delivery_month is not None:
        current_decade = (reference_date.year // 10) * 10
        full_year = current_decade + year_digit
        fnd = _first_notice_date(full_year, delivery_month)
        roll_date = fnd - timedelta(days=_ROLL_DAYS_BEFORE_FND)
        fnd_str = fnd.isoformat()
        roll_str = roll_date.isoformat()

    logger.info(
        "[AUTO-ROLL] date=%s -> Webull=%s  TV=COMEX:%s (~COMEX:MGC1!)  "
        "FND=%s  roll_by=%s",
        reference_date.isoformat(),
        symbol,
        tv_symbol,
        fnd_str,
        roll_str,
    )
    return symbol


class WebullMGCBroker:
    """Place MGC futures orders on Webull OpenAPI.

    Usage:
        broker = WebullMGCBroker(
            app_key=os.environ["WEBULL_APP_KEY"],
            app_secret=os.environ["WEBULL_APP_SECRET"],
            # symbol auto-resolved from date if omitted
            live_orders=False,   # default dry-run
        )
        result = broker.place_order(
            side="BUY", quantity=6, limit_price=3250.5, comment="Long Entry",
        )
        if not result["success"]:
            # caller (WebhookExecutor) rolls back paper-state mutation
            ...
    """

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        symbol: Optional[str] = None,
        live_orders: bool = False,
        account_id: Optional[str] = None,
        bp_cache_ttl_seconds: float = 300.0,
    ):
        self._app_key = app_key
        self._app_secret = app_secret
        self._symbol = symbol or log_auto_roll_info()
        self._live_orders = live_orders
        self._explicit_account_id = account_id

        # Lazy-initialized Webull clients
        self._api_client = None
        self._trade_client = None
        self._account_id: Optional[str] = account_id

        # Buying-power cache — avoid hammering the Webull API on every
        # webhook call. The contract cap in webhook_executor asks for
        # live equity on every entry, so without a cache we'd add 200ms+
        # of round-trip latency to every TradingView alert. 5-min TTL
        # strikes a balance: account balance doesn't change faster than
        # trade settlement anyway, and explicit force_refresh=True is
        # available for post-fill reconciliation.
        self._bp_cache: float = 0.0
        self._bp_cache_ts: float = 0.0
        self._bp_cache_ttl: float = bp_cache_ttl_seconds

        # Position-qty cache — short TTL (5s) so rapid-fire webhooks don't
        # each hit Webull's get_account_position endpoint. Long enough to
        # dedup TP-level retries from a single TV bar, short enough that
        # drift detection still feels near-real-time. Only positive fetches
        # are cached; None (error/unknown) skips the cache to force retry
        # on the next call.
        self._pos_cache_value: Optional[int] = None
        self._pos_cache_ts: float = 0.0
        self._pos_cache_ttl: float = 5.0

        if live_orders:
            self._init_webull()

        logger.info(
            "[WEBULL_MGC] Broker ready: live=%s, symbol=%s, account=%s",
            live_orders, symbol, self._account_id or "pending",
        )

    # ------------------------------------------------------------------
    # Webull client initialization
    # ------------------------------------------------------------------
    def _init_webull(self) -> None:
        """Connect to Webull and discover the account_id (if not provided)."""
        try:
            from webull.core.client import ApiClient
            from webull.core.common.region import Region
            from webull.trade.trade_client import TradeClient
            from src.gold_scalper.sdk_redaction import disable_sdk_default_logger
        except ImportError as e:
            logger.error("[WEBULL_MGC] webull SDK not installed: %s", e)
            return

        try:
            self._api_client = ApiClient(
                app_key=self._app_key,
                app_secret=self._app_secret,
                region_id=Region.US.value,
            )
            # SECURITY: neutralize the SDK's default logger install BEFORE
            # TradeClient construction. Without these two flags the SDK
            # writes every request (including x-app-key headers) to stdout
            # and to webull_trade_sdk.log. See src/gold_scalper/sdk_redaction.py
            disable_sdk_default_logger(self._api_client)
            self._trade_client = TradeClient(self._api_client)

            # Use explicit account if provided, else discover
            if self._explicit_account_id:
                self._account_id = self._explicit_account_id
                logger.info("[WEBULL_MGC] Using explicit account: %s",
                            self._account_id)
                return

            resp = self._trade_client.account_v2.get_account_list()
            data = resp.json() if hasattr(resp, "json") else resp
            if isinstance(data, list):
                accounts = data
            else:
                accounts = data.get("data", data.get("accounts", []))

            if accounts:
                # NOTE: if you have multiple Webull accounts (e.g., separate
                # equity and futures sub-accounts), set WEBULL_MGC_ACCOUNT_ID
                # explicitly. Otherwise we pick the first one returned.
                first = accounts[0]
                self._account_id = str(
                    first.get("account_id", first.get("accountId", ""))
                )
                logger.info("[WEBULL_MGC] Discovered account: %s",
                            self._account_id)
            else:
                logger.error(
                    "[WEBULL_MGC] No accounts in response: %s", str(data)[:300]
                )

        except Exception as exc:
            logger.error("[WEBULL_MGC] Failed to initialize Webull: %s", exc)
            logger.warning(
                "[WEBULL_MGC] Client init failed — will retry on first order"
            )

    # ------------------------------------------------------------------
    # Account balance — used by webhook_executor's contract cap
    # ------------------------------------------------------------------
    def _fetch_buying_power(self) -> float:
        """Fetch raw buying power from Webull's /openapi/assets/balance.

        Copied from ``webull_tqqq_executor._fetch_buying_power`` (line 228)
        — same SDK, same endpoint, same response shape, same fallback
        chain. Keeping both in sync is important: if Webull changes the
        response format, both executors need the same fix.

        Returns 0.0 on any failure (network error, SDK error, parse fail,
        empty response). Callers MUST check for 0 and fall back gracefully
        — never treat 0 as "account has zero dollars".
        """
        if not self._trade_client or not self._account_id:
            return 0.0
        try:
            resp = self._trade_client.account_v2.get_account_balance(
                self._account_id
            )
            data = resp.json() if hasattr(resp, "json") else resp
            # Primary path: account_currency_assets[0].buying_power
            # This is the shape Webull US returns for a funded account.
            assets = data.get("account_currency_assets", [])
            if assets:
                bp = float(assets[0].get("buying_power", 0))
                # Finite-check: if Webull ever returns "inf" / "nan"
                # in the JSON body, float() happily converts it and
                # bp > 0 would be True for inf — poisoning the cache
                # and crashing compute_contract_cap downstream. Guard
                # at the earliest entry point.
                if bp > 0 and math.isfinite(bp):
                    logger.info(
                        "[WEBULL_MGC] Buying power from Webull: $%.2f", bp
                    )
                    return bp
            # Fallback: top-level keys. Older SDKs and some account
            # shapes (sub-accounts, margin vs cash) put the relevant
            # field one level up with different names. Try each in
            # priority order and take the first positive finite hit.
            for key in (
                "total_cash_balance",
                "total_net_liquidation_value",
                "buying_power",
                "cash_balance",
            ):
                val = data.get(key)
                if val is not None:
                    bp = float(val)
                    if bp > 0 and math.isfinite(bp):
                        logger.info(
                            "[WEBULL_MGC] Buying power (fallback %s): $%.2f",
                            key, bp,
                        )
                        return bp
            logger.warning(
                "[WEBULL_MGC] Could not parse buying power from: %s",
                str(data)[:500],
            )
            return 0.0
        except Exception as exc:
            logger.error(
                "[WEBULL_MGC] Failed to fetch buying power: %s", exc
            )
            return 0.0

    def get_live_equity(self, force_refresh: bool = False) -> float:
        """Return cached Webull buying power (aka live account equity).

        Cached for ``bp_cache_ttl_seconds`` (default 5 min) to avoid
        hammering the API on every webhook call. Every TradingView alert
        calls the executor's contract cap which in turn calls this
        method — without caching we'd add a full round-trip to every
        order, increasing the time between TV signal and order
        placement measurably.

        Args:
            force_refresh: bypass the cache and re-fetch. Use after a
                fill settles so the cap sees the new balance on the
                next alert rather than waiting for TTL expiry.

        Returns:
            Positive float (buying power in USD) on success.
            0.0 when broker is in dry-run mode (live_orders=False),
            when SDK is not yet initialized, or when the fetch fails.
            Callers should treat 0.0 as "unknown" and fall back to
            their own paper-equity tracking rather than trusting it
            as "zero dollars available".
        """
        if not self._live_orders:
            return 0.0
        now = time.time()
        if (
            not force_refresh
            and self._bp_cache > 0
            and now - self._bp_cache_ts < self._bp_cache_ttl
        ):
            return self._bp_cache
        bp = self._fetch_buying_power()
        if bp > 0:
            self._bp_cache = bp
            self._bp_cache_ts = now
        return bp

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------
    def place_order(
        self,
        side: str,
        quantity: int,
        limit_price: float,
        comment: str = "",
    ) -> dict:
        """Place a single MGC futures order.

        Args:
            side: "BUY" (opens long / closes short) or
                  "SELL" (opens short / closes long).
            quantity: number of contracts (positive integer).
            limit_price: limit price in USD per oz (e.g., 3250.5).
            comment: free-text tag for logging (e.g., "Long Entry", "TP1").

        Returns:
            {"success": bool, "order_id": str, "error": str (if failure),
             "dry_run": bool, "response": dict (raw Webull response)}
        """
        # Input sanitation
        side_upper = side.upper().strip()
        if side_upper not in ("BUY", "SELL"):
            return {
                "success": False,
                "error": f"Invalid side: {side!r} (must be BUY or SELL)",
            }
        try:
            quantity_int = int(quantity)
        except (TypeError, ValueError):
            return {"success": False, "error": f"Invalid quantity: {quantity!r}"}
        if quantity_int < 1:
            return {
                "success": False,
                "error": f"Quantity must be >=1, got {quantity_int}",
            }
        if limit_price <= 0:
            return {
                "success": False,
                "error": f"Invalid limit_price: {limit_price}",
            }

        # Compute marketable-LIMIT price:
        #   BUY  → base + N ticks (cross the offer)
        #   SELL → base − N ticks (cross the bid)
        # Keeps the order book LIMIT (has price protection) but ensures
        # it executes immediately at the current market. See comment on
        # _MARKETABLE_OFFSET_TICKS for rationale.
        base = float(limit_price)
        offset = _MGC_TICK_SIZE * _MARKETABLE_OFFSET_TICKS
        if side_upper == "BUY":
            aggressive_price = base + offset
        else:
            aggressive_price = base - offset

        # Defense in depth: if the offset ever pushes a SELL price to zero
        # or negative (impossible for real MGC but possible for a bogus
        # test input), reject instead of sending garbage to Webull.
        if aggressive_price <= 0:
            return {
                "success": False,
                "error": (
                    f"Aggressive price non-positive after offset: "
                    f"base={base} side={side_upper} offset={offset}"
                ),
            }

        # Round to MGC tick precision (0.10)
        limit_rounded = round(aggressive_price, _MGC_PRICE_DECIMALS)
        client_order_id = f"mgc_{uuid.uuid4().hex[:16]}"

        order_body = {
            "combo_type": _COMBO_TYPE,
            "client_order_id": client_order_id,
            "symbol": self._symbol,
            "instrument_type": _INSTRUMENT_TYPE,
            "market": _MARKET,
            "order_type": "LIMIT",
            "limit_price": f"{limit_rounded:.{_MGC_PRICE_DECIMALS}f}",
            "quantity": str(quantity_int),
            "side": side_upper,
            "time_in_force": _TIME_IN_FORCE,
            "entrust_type": _ENTRUST_TYPE,
        }

        # Dry-run mode — log and return success without sending
        if not self._live_orders:
            logger.info(
                "[WEBULL_MGC] DRY-RUN (live=False) — would place: "
                "%s %d %s @ $%.1f (base=$%.1f, offset=$%.1f) (%s) — "
                "client_order_id=%s",
                side_upper, quantity_int, self._symbol,
                limit_rounded, base, offset, comment, client_order_id,
            )
            logger.debug("[WEBULL_MGC] DRY-RUN body: %s", order_body)
            return {
                "success": True,
                "order_id": client_order_id,
                "dry_run": True,
            }

        # Real-order path — retry init if clients are None
        if not self._trade_client or not self._account_id:
            logger.info("[WEBULL_MGC] Retrying Webull init before order...")
            self._init_webull()
        if not self._trade_client or not self._account_id:
            logger.error("[WEBULL_MGC] Cannot place order: client not initialized")
            return {"success": False, "error": "webull_not_initialized"}

        try:
            logger.info(
                "[WEBULL_MGC] Placing LIVE %s %d %s @ $%.1f "
                "(base=$%.1f, offset=$%.1f) (%s)",
                side_upper, quantity_int, self._symbol,
                limit_rounded, base, offset, comment,
            )
            resp = self._trade_client.order_v3.place_order(
                account_id=self._account_id,
                new_orders=[order_body],
            )
            data = resp.json() if hasattr(resp, "json") else resp
            logger.info("[WEBULL_MGC] Order response: %s", str(data)[:500])

            # Webull sometimes returns error_code in the body instead of raising
            if isinstance(data, dict) and data.get("error_code"):
                err_msg = data.get("message", data.get("error_code"))
                logger.error("[WEBULL_MGC] Order rejected: %s", err_msg)
                return {"success": False, "error": err_msg}

            # Invalidate position cache -- the 5s TTL would otherwise return
            # pre-fill qty on the next get_position_qty() call within 5s,
            # causing webhook's _sync_position_from_broker to rebuild paper
            # from stale data and double-close (see 2026-04-24 12:18 overshoot).
            self._pos_cache_value = None
            self._pos_cache_ts = 0.0

            return {
                "success": True,
                "order_id": client_order_id,
                "response": data,
            }

        except Exception as exc:
            logger.error("[WEBULL_MGC] Order exception: %s", exc)
            return {"success": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # Order status / fill verification
    # ------------------------------------------------------------------
    def get_order_status(self, client_order_id: str) -> dict:
        """Query Webull for the current status of an order.

        Returns a dict with normalized fields:
            {
              "status": "FILLED" | "SUBMITTED" | "CANCELLED" | "REJECTED" | "UNKNOWN",
              "filled_quantity": float,
              "total_quantity": float,
              "filled_price": float (avg fill price, 0.0 if unfilled),
              "raw": dict (full Webull response for debugging),
              "error": str (only if query failed),
            }

        For dry-run mode, returns a synthetic FILLED response so callers
        don't need to special-case dry vs live.
        """
        if not self._live_orders:
            return {
                "status": "FILLED",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "raw": {},
                "dry_run": True,
            }

        if not self._trade_client or not self._account_id:
            return {
                "status": "UNKNOWN",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "error": "broker_not_initialized",
            }

        try:
            resp = self._trade_client.order_v3.get_order_detail(
                account_id=self._account_id,
                client_order_id=client_order_id,
            )
            data = resp.json() if hasattr(resp, "json") else resp

            # Webull responses can be a dict {orders: [...]} or a single order
            sub_orders = []
            if isinstance(data, dict):
                if "orders" in data and isinstance(data["orders"], list):
                    sub_orders = data["orders"]
                else:
                    sub_orders = [data]
            elif isinstance(data, list):
                sub_orders = data

            if not sub_orders:
                return {
                    "status": "UNKNOWN",
                    "filled_quantity": 0.0,
                    "total_quantity": 0.0,
                    "filled_price": 0.0,
                    "raw": data,
                    "error": "no_order_in_response",
                }

            # Take first sub-order (we only place one order per place_order call)
            sub = sub_orders[0]
            status_raw = (sub.get("status") or "").upper()

            # Normalize Webull's status strings to our 5 categories
            if status_raw in ("FILLED",):
                status = "FILLED"
            elif status_raw in ("CANCELLED", "CANCELED"):
                status = "CANCELLED"
            elif status_raw in ("REJECTED", "FAILED"):
                status = "REJECTED"
            elif status_raw in ("SUBMITTED", "PENDING", "PARTIAL_FILLED",
                                "WORKING", "ACCEPTED"):
                status = "SUBMITTED"
            else:
                status = "UNKNOWN"

            try:
                filled_qty = float(sub.get("filled_quantity", 0))
            except (TypeError, ValueError):
                filled_qty = 0.0
            try:
                total_qty = float(sub.get("total_quantity", 0))
            except (TypeError, ValueError):
                total_qty = 0.0
            try:
                filled_price = float(sub.get("filled_price", 0) or 0)
            except (TypeError, ValueError):
                filled_price = 0.0

            return {
                "status": status,
                "filled_quantity": filled_qty,
                "total_quantity": total_qty,
                "filled_price": filled_price,
                "raw": sub,
            }

        except Exception as exc:
            logger.error("[WEBULL_MGC] get_order_status failed for %s: %s",
                         client_order_id, exc)
            return {
                "status": "UNKNOWN",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "error": str(exc),
            }

    def get_position_qty(self, force_refresh: bool = False) -> Optional[int]:
        """Return the signed position quantity for ``self._symbol`` on Webull.

        Used by the reconciler's drift detector and the webhook executor's
        pre-dispatch sync. Returns:
            * 0        — Webull confirms the account is flat in this symbol.
            * +N       — Webull reports a LONG position of N contracts.
            * -N       — Webull reports a SHORT position of N contracts.
            * None     — query failed, broker not initialized, or dry-run mode.
                         Caller MUST treat None as "unknown" and skip the drift
                         check this cycle — never treat None as "flat".

        Successful fetches are cached for ``self._pos_cache_ttl`` seconds (5s
        by default) to prevent hammering Webull's API when webhooks fire in
        rapid succession (e.g. TV retries, TP-level bursts within one 5-min
        bar). ``force_refresh=True`` bypasses the cache — the reconciler uses
        this before issuing a correction.

        The dry-run path returns None (not 0) so a dry-run broker doesn't
        mislead the reconciler into auto-clearing paper positions.
        """
        if not self._live_orders:
            return None
        if not self._trade_client or not self._account_id:
            return None

        now = time.time()
        if (not force_refresh
                and self._pos_cache_value is not None
                and (now - self._pos_cache_ts) < self._pos_cache_ttl):
            return self._pos_cache_value

        try:
            resp = self._trade_client.account_v2.get_account_position(
                self._account_id
            )
            data = resp.json() if hasattr(resp, "json") else resp

            if isinstance(data, list):
                positions = data
            elif isinstance(data, dict):
                positions = (
                    data.get("position_list")
                    or data.get("positions")
                    or data.get("data")
                    or []
                )
            else:
                # Unrecognised response shape — treat as unknown, not flat.
                return None

            if not isinstance(positions, list):
                return None

            target = (self._symbol or "").upper()
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                sym = str(pos.get("symbol") or pos.get("ticker_symbol") or "").upper()
                if sym != target:
                    continue
                try:
                    qty = int(float(pos.get("quantity", 0) or 0))
                except (TypeError, ValueError):
                    return None
                direction = str(pos.get("direction", "")).upper()
                if direction == "SHORT":
                    qty = -qty
                self._pos_cache_value = qty
                self._pos_cache_ts = now
                return qty
            # Symbol not present in positions list → account is flat.
            self._pos_cache_value = 0
            self._pos_cache_ts = now
            return 0
        except Exception as exc:
            logger.error("[WEBULL_MGC] get_position_qty failed: %s", exc)
            return None

    def wait_for_fill(
        self,
        client_order_id: str,
        timeout_seconds: float = 5.0,
        poll_interval_seconds: float = 0.5,
    ) -> dict:
        """Poll Webull until the order fills, is cancelled/rejected, or times out.

        This exists because place_order returns success when Webull *accepts*
        the order, not when it *fills*. A LIMIT placed above market for a
        SELL will rest indefinitely — we need to know the difference between
        "filled at $X" and "still resting on the book".

        Returns the same dict shape as get_order_status. Final status will be
        one of: FILLED / CANCELLED / REJECTED / SUBMITTED (timed out).
        Caller decides what to do with SUBMITTED — usually mark as pending
        and let the background reconciler resolve it.
        """
        if not self._live_orders:
            # Dry-run: synthetic instant fill
            return self.get_order_status(client_order_id)

        deadline = time.time() + timeout_seconds
        last_status = None
        while time.time() < deadline:
            result = self.get_order_status(client_order_id)
            status = result.get("status")
            last_status = result
            # Terminal states — return immediately
            if status in ("FILLED", "CANCELLED", "REJECTED"):
                return result
            time.sleep(poll_interval_seconds)

        # Timed out — return whatever the last known status was
        if last_status is None:
            return {
                "status": "UNKNOWN",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "error": "no_status_polled",
            }
        return last_status

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @property
    def live_orders(self) -> bool:
        return self._live_orders

    @property
    def symbol(self) -> str:
        return self._symbol

    def get_status(self) -> dict:
        """Return broker status for /status endpoint."""
        return {
            "symbol": self._symbol,
            "live_orders": self._live_orders,
            "account_id": self._account_id or "pending",
            "initialized": bool(self._trade_client),
        }


def build_from_env(config_symbol: Optional[str] = None) -> Optional[WebullMGCBroker]:
    """Factory — construct WebullMGCBroker from env vars, or None if missing keys.

    Reads:
      WEBULL_APP_KEY / WEBULL_APP_SECRET  (required)
      WEBULL_MGC_SYMBOL                   (optional — auto-resolved from date
                                           if unset; see ``get_front_month_symbol()``)
      WEBULL_MGC_LIVE                     (optional, "1"/"true" = real orders)
      WEBULL_MGC_ACCOUNT_ID               (optional, explicit account override)

    Args:
        config_symbol: fallback symbol if WEBULL_MGC_SYMBOL not set.
            If *also* None, auto-roll determines the front-month contract.

    Returns:
        Configured broker, or None if credentials unavailable.
    """
    app_key = os.environ.get("WEBULL_APP_KEY", "").strip()
    app_secret = os.environ.get("WEBULL_APP_SECRET", "").strip()
    if not app_key or not app_secret:
        logger.warning(
            "[WEBULL_MGC] WEBULL_APP_KEY/SECRET not set — MGC broker disabled"
        )
        return None

    # Priority: env var → config param → auto-roll from today's date
    symbol = (
        os.environ.get("WEBULL_MGC_SYMBOL", "").strip()
        or config_symbol
        or None   # let __init__ → log_auto_roll_info() compute it
    )
    live_flag = os.environ.get("WEBULL_MGC_LIVE", "").strip().lower()
    live_orders = live_flag in ("1", "true", "yes", "on")
    account_id = os.environ.get("WEBULL_MGC_ACCOUNT_ID", "").strip() or None

    return WebullMGCBroker(
        app_key=app_key,
        app_secret=app_secret,
        symbol=symbol,
        live_orders=live_orders,
        account_id=account_id,
    )
