"""Webull TQQQ Executor — receives TradingView webhook signals, places TQQQ orders on Webull.

Signal format (from Pine Script):
    {"action": "buy",  "ticker": "TQQQ", "contracts": 1, "position_size":  1, "price": 45.50, "comment": "Long Entry"}
    {"action": "sell", "ticker": "TQQQ", "contracts": 1, "position_size":  0, "price": 46.00, "comment": "Trail Stop"}
    {"action": "sell", "ticker": "TQQQ", "contracts": 1, "position_size": -1, "price": 44.00, "comment": "Short Entry"}
    {"action": "buy",  "ticker": "TQQQ", "contracts": 1, "position_size":  0, "price": 43.50, "comment": "Trail Stop"}
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from webull.core.client import ApiClient
from webull.core.common.region import Region
from webull.trade.trade_client import TradeClient

from src.gold_scalper.sdk_redaction import disable_sdk_default_logger

logger = logging.getLogger("webull_tqqq")

ET = ZoneInfo("America/New_York")

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_STATE_DIR = os.path.join(_PROJECT_ROOT, "outputs", "paper_state", "webhook")
_STATE_FILE = os.path.join(_STATE_DIR, "webull_tqqq_state.json")
_TRADE_LOG = os.path.join(_STATE_DIR, "webull_tqqq_trade_log.json")

# Strategy capital cap — max dollar amount this strategy is allowed to deploy.
# Sizing = min(cap * 0.80, webull_buying_power * 0.80) / price.
# Override via WEBULL_TQQQ_CAPITAL_CAP env var (default $100).
def _load_capital_cap() -> float:
    raw = os.environ.get("WEBULL_TQQQ_CAPITAL_CAP", "100")
    try:
        val = float(raw)
        if val > 0:
            return val
    except (TypeError, ValueError):
        pass
    return 100.0


_CAPITAL_CAP = _load_capital_cap()


@dataclass
class TQQQPosition:
    direction: str              # "LONG" or "SHORT"
    entry_price: float
    entry_time: str             # ISO format
    qty: int
    realized_pnl: float = 0.0
    # fill_status closes the crash-unsafety window between Webull place_order
    # and the post-fill state save. PENDING_OPEN / PENDING_CLOSE are written
    # to disk BEFORE calling the broker; startup reconcile uses broker truth
    # to resolve any PENDING that outlives the process. Default FILLED keeps
    # existing state files backward-compatible.
    fill_status: str = "FILLED"  # "PENDING_OPEN" | "PENDING_CLOSE" | "FILLED"


class WebullTQQQExecutor:
    """Executes TQQQ trades on Webull via OpenAPI SDK."""

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        live_orders: bool = False,
        initial_equity: float = 5000.0,
    ):
        self._app_key = app_key
        self._app_secret = app_secret
        self._live_orders = live_orders
        self._initial_equity = initial_equity

        # Webull API client
        self._api_client: Optional[ApiClient] = None
        self._trade_client: Optional[TradeClient] = None
        self._account_id: Optional[str] = None

        # Position state
        self.position: Optional[TQQQPosition] = None
        self._equity = initial_equity

        # Daily stats
        self.daily_pnl: float = 0.0
        self.daily_trades: int = 0
        self.daily_wins: int = 0
        self.daily_losses: int = 0
        self._last_reset_date: Optional[str] = None

        # Risk controls
        self._circuit_breaker_fired: bool = False
        self._daily_loss_limit: float = -500.0
        self._last_signal_time: float = 0
        self._dedup_window: float = 30.0  # seconds
        self._cooldown_until: float = 0

        # State persistence
        os.makedirs(_STATE_DIR, exist_ok=True)
        self._load_state()

        # Initialize Webull connection
        if live_orders:
            self._init_webull()
            # Sync equity from real Webull balance
            bp = self._fetch_buying_power()
            if bp > 0:
                self._equity = bp
                logger.info("[WEBULL_TQQQ] Equity synced from Webull: $%.2f", bp)
            # Closes the crash-unsafety window in _open_position /
            # _close_position. Runs AFTER _init_webull so the trade
            # client is ready to query the broker.
            self._reconcile_on_startup()

        logger.info(
            "[WEBULL_TQQQ] Initialized: live=%s, equity=$%.2f, position=%s",
            live_orders, self._equity,
            self.position.direction if self.position else "FLAT",
        )

    def _init_webull(self):
        """Initialize Webull API client and discover account."""
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

            # Discover the CASH account — TQQQ is an equity/ETF, so it must
            # be traded on the Individual Cash account, NOT the Futures account.
            # The account list order varies across SDK calls; picking
            # accounts[0] used to land on Futures, causing 100% of orders to
            # fail with "invalid instrument_type".
            resp = self._trade_client.account_v2.get_account_list()
            data = resp.json() if hasattr(resp, 'json') else resp
            if isinstance(data, list):
                accounts = data
            else:
                accounts = data.get("data", data.get("accounts", []))

            # Prefer explicit env override
            explicit_id = os.environ.get("WEBULL_TQQQ_ACCOUNT_ID", "").strip()
            if explicit_id:
                self._account_id = explicit_id
                logger.info("[WEBULL_TQQQ] Using explicit account: %s", self._account_id)
            elif accounts:
                # Pick the first non-FUTURES account (INDIVIDUAL_CASH or CASH)
                cash_accts = [
                    a for a in accounts
                    if a.get("account_class", "").upper() != "FUTURES"
                ]
                chosen = cash_accts[0] if cash_accts else accounts[0]
                self._account_id = str(
                    chosen.get("account_id", chosen.get("accountId", ""))
                )
                acct_class = chosen.get("account_class", "unknown")
                logger.info(
                    "[WEBULL_TQQQ] Found account: %s (class=%s)",
                    self._account_id, acct_class,
                )
            else:
                logger.error("[WEBULL_TQQQ] No accounts found in response: %s", data)

        except Exception as e:
            logger.error("[WEBULL_TQQQ] Failed to initialize Webull: %s", e)
            logger.warning("[WEBULL_TQQQ] Will retry Webull init on first order")
            # Keep live_orders=True — retry on first trade

    def _sync_position_from_broker(self, signal_price: float = 0.0) -> None:
        """Reconcile self.position to actual Webull position before deciding.

        Paper state is a local cache; Webull is the source of truth. Without
        this sync, drifted paper state causes wrong decisions:
          * Close signal arrives → paper says flat → executor rejects the
            close while Webull bleeds an unhedged share position.
          * Open signal arrives → paper says flat → executor buys more on
            top of a position Webull already has → double exposure.

        Called at the top of ``execute()`` so every decision is grounded in
        reality. If the broker query fails or returns unknown, we keep paper
        state rather than corrupting it — better to act on stale state than
        block legitimate trades because of a flaky API call.

        ``signal_price`` is used as a placeholder ``entry_price`` if we
        discover a broker position paper didn't know about. Using 0.0 would
        make downstream PnL math nonsensical; using the current alert price
        yields approximately 0 realized PnL for this one reconciled trade.
        """
        if not self._live_orders:
            return

        try:
            actual_qty = self.get_broker_position_qty()
        except Exception as exc:
            logger.warning(
                "[WEBULL_TQQQ] get_broker_position_qty raised: %s — "
                "keeping paper state as-is", exc,
            )
            return

        if actual_qty is None:
            # Query failed or unknown — never treat None as flat.
            logger.warning(
                "[WEBULL_TQQQ] get_broker_position_qty returned None — "
                "keeping paper state as-is",
            )
            return

        # Skip sync if paper position was opened in the last few seconds.
        # Webull's position API can lag behind fill confirmation by ~1-2s,
        # and TQQQPosition has no fill_status field (unlike the MGC side)
        # to flag pending state. Without this guard, a fast-follow webhook
        # could sync on a stale "flat" broker response and wipe a valid
        # just-opened position. 3s covers typical fill-registration lag;
        # after that, Webull is authoritative.
        if self.position and self.position.entry_time:
            try:
                entry_ts = datetime.fromisoformat(self.position.entry_time)
                if (datetime.now(ET) - entry_ts).total_seconds() < 3.0:
                    return
            except (ValueError, TypeError):
                pass  # malformed entry_time — fall through to normal sync

        # Signed paper qty: LONG +N, SHORT -N, flat 0.
        if self.position and self.position.qty > 0:
            paper_qty = (
                self.position.qty
                if self.position.direction == "LONG"
                else -self.position.qty
            )
        else:
            paper_qty = 0

        if paper_qty == actual_qty:
            return  # already in sync — common case, no-op

        logger.warning(
            "[WEBULL_TQQQ] PAPER/BROKER DRIFT: paper_qty=%d, broker_qty=%d — "
            "reconciling paper to broker truth", paper_qty, actual_qty,
        )

        if actual_qty == 0:
            # Webull is flat → clear any phantom paper position.
            logger.info(
                "[WEBULL_TQQQ] Clearing phantom paper position (broker is flat)",
            )
            self.position = None
            return

        # Webull has a position — rebuild paper to match. entry_price is
        # unknown without an avg-cost query, so use signal_price as a
        # safe placeholder (near-zero PnL on close vs phantom $thousands).
        direction = "LONG" if actual_qty > 0 else "SHORT"
        qty = abs(actual_qty)
        placeholder_entry = signal_price if signal_price > 0 else 0.0
        self.position = TQQQPosition(
            direction=direction,
            entry_price=placeholder_entry,
            entry_time=datetime.now(ET).isoformat(),
            qty=qty,
        )
        logger.info(
            "[WEBULL_TQQQ] Rebuilt paper position from broker: %s %d sh "
            "(entry_price placeholder=$%.2f)",
            direction, qty, placeholder_entry,
        )

    def execute(self, body: dict) -> dict:
        """Process a TradingView webhook signal.

        Routes:
        - position_size > 0 = go LONG (buy to open)
        - position_size < 0 = go SHORT (sell short to open)
        - position_size == 0 = close position (sell to close or buy to cover)
        """
        now = datetime.now(ET)
        self._maybe_reset_daily(now)

        # Parse signal
        action = body.get("action", "").lower()  # buy or sell
        ticker = body.get("ticker", "TQQQ").upper()
        position_size = body.get("position_size", None)
        price = float(body.get("price", 0))
        comment = body.get("comment", "")

        if ticker != "TQQQ":
            return {"status": "skipped", "reason": f"Not TQQQ: {ticker}"}

        # Broker-truth sync — reconcile self.position to actual Webull
        # position BEFORE any decision. Same rationale as WebhookExecutor
        # for MGC: stale paper state rejects legitimate closes or double-
        # opens into an existing position. See _sync_position_from_broker().
        self._sync_position_from_broker(price)

        now_ts = time.time()

        # Dedup guard — only applies to signals that OPEN a new position.
        # Pre-fix, this gated every signal including close/flip, so a
        # legitimate stop-out arriving <30s after entry was silently
        # dropped — leaving a live position open while TV thought flat.
        # Close/flip signals bypass dedup; they're the whole point of
        # reacting quickly to TV.
        is_opening_signal = self._is_opening_signal(body)
        if is_opening_signal:
            if now_ts - self._last_signal_time < self._dedup_window:
                return {"status": "skipped", "reason": "dedup"}
            self._last_signal_time = now_ts

        # Circuit breaker
        if self._circuit_breaker_fired:
            return {"status": "blocked", "reason": "circuit_breaker"}

        # Cooldown
        if now_ts < self._cooldown_until:
            return {"status": "skipped", "reason": "cooldown"}

        logger.info(
            "[WEBULL_TQQQ] Signal: action=%s pos_size=%s price=%.2f comment=%s",
            action, position_size, price, comment,
        )

        result = {}

        if position_size is not None:
            pos_size = int(position_size)

            if pos_size > 0 and self.position is None:
                # Open LONG
                result = self._open_position("LONG", price, now)

            elif pos_size < 0 and self.position is None:
                # Open SHORT
                result = self._open_position("SHORT", price, now)

            elif pos_size == 0 and self.position is not None:
                # Close position
                result = self._close_position(price, now, comment)

            elif pos_size > 0 and self.position and self.position.direction == "SHORT":
                # Flip: close short, open long
                close_result = self._close_position(price, now, "Flip to Long")
                if close_result.get("status") == "error":
                    result = close_result  # close failed, don't open new
                else:
                    result = self._open_position("LONG", price, now)

            elif pos_size < 0 and self.position and self.position.direction == "LONG":
                # Flip: close long, open short
                close_result = self._close_position(price, now, "Flip to Short")
                if close_result.get("status") == "error":
                    result = close_result  # close failed, don't open new
                else:
                    result = self._open_position("SHORT", price, now)

            else:
                result = {"status": "no_action", "reason": "already_positioned"}
        else:
            # Legacy format: use action field
            if action == "buy" and self.position is None:
                result = self._open_position("LONG", price, now)
            elif action == "sell" and self.position is None:
                result = self._open_position("SHORT", price, now)
            elif action in ("buy", "sell") and self.position is not None:
                result = self._close_position(price, now, comment)
            else:
                result = {"status": "no_action"}

        self._save_state()
        return result

    def _is_opening_signal(self, body: dict) -> bool:
        """True if this webhook would open a new position (not close/flip).

        Dedup should only gate fresh opens — close and flip signals must
        react fast. A signal OPENS when we're currently FLAT and TV asks
        us to go LONG or SHORT. A flip (LONG→SHORT or SHORT→LONG) still
        needs to flow through; we want the close half to fire promptly.
        """
        position_size = body.get("position_size")
        if position_size is not None:
            try:
                pos_size = int(position_size)
            except (TypeError, ValueError):
                return False
            # Fresh open only: currently FLAT and TV wants a direction.
            # Flips are handled here too (not gated) so the close+open
            # pair fires through.
            if pos_size == 0:
                return False  # close, never an open
            return self.position is None  # open only if currently flat
        # Legacy action-only format: buy/sell with no position means open
        action = body.get("action", "").lower()
        if action in ("buy", "sell") and self.position is None:
            return True
        return False

    def _fetch_buying_power(self) -> float:
        """Fetch actual buying power from Webull account."""
        if not self._trade_client or not self._account_id:
            return 0.0
        try:
            resp = self._trade_client.account_v2.get_account_balance(self._account_id)
            data = resp.json() if hasattr(resp, 'json') else resp
            # Primary path: account_currency_assets[0].buying_power
            assets = data.get("account_currency_assets", [])
            if assets:
                bp = float(assets[0].get("buying_power", 0))
                if bp > 0:
                    logger.info("[WEBULL_TQQQ] Buying power from Webull: $%.2f", bp)
                    return bp
            # Fallback: top-level total_cash_balance
            for key in ("total_cash_balance", "total_net_liquidation_value",
                        "buying_power", "cash_balance"):
                val = data.get(key)
                if val is not None:
                    bp = float(val)
                    logger.info("[WEBULL_TQQQ] Buying power (fallback %s): $%.2f", key, bp)
                    return bp
            logger.warning("[WEBULL_TQQQ] Could not parse buying power from: %s", str(data)[:500])
            return 0.0
        except Exception as e:
            logger.error("[WEBULL_TQQQ] Failed to fetch buying power: %s", e)
            return 0.0

    def _calculate_qty(self, price: float) -> int:
        """Calculate position size from strategy capital cap.

        Formula: qty = floor(_CAPITAL_CAP * 0.80 / price)

        The capital cap (_CAPITAL_CAP, default $100) is the strategy's
        total dollar budget. We use 80% of it as deployable capital,
        then divide by price to get share count. If Webull buying power
        is lower than the cap, we use the lower value so we never exceed
        the account's actual capacity.
        """
        if price <= 0:
            return 0

        # Start from strategy capital cap
        capital = _CAPITAL_CAP

        # If live, also check Webull BP — never exceed actual account capacity
        if self._live_orders:
            bp = self._fetch_buying_power()
            if bp > 0:
                capital = min(capital, bp)

        dollar_amount = capital * 0.80
        qty = int(dollar_amount / price)

        logger.info(
            "[WEBULL_TQQQ] Sizing: capital_cap=$%.0f, deployable=$%.2f, "
            "price=$%.2f -> %d shares",
            _CAPITAL_CAP, dollar_amount, price, qty,
        )
        return max(1, qty)

    def _open_position(self, direction: str, price: float, now: datetime) -> dict:
        """Open a new position.

        Crash-safety: paper is written as PENDING_OPEN BEFORE the Webull
        call so a crash between the broker fill and the final save leaves
        an auditable marker on disk. _reconcile_on_startup repairs any
        PENDING_OPEN that outlives the process using broker truth. This
        closes the bug that silently lost 4/17, 4/20, 4/22 TQQQ fills.
        """
        qty = self._calculate_qty(price)

        # STEP 1: pre-write PENDING paper state. Durable marker for the
        # broker-call window below.
        self.position = TQQQPosition(
            direction=direction,
            entry_price=price,          # TV price — replaced with actual fill on success
            entry_time=now.isoformat(),
            qty=qty,
            fill_status="PENDING_OPEN",
        )
        self._save_state()

        # STEP 2: place order on Webull.
        actual_entry_price = price
        fill_status = "FILLED"
        if self._live_orders:
            order_result = self._place_webull_order(
                side="BUY" if direction == "LONG" else "SELL",
                qty=qty,
                price=price,
                comment=f"{direction} Entry",
            )
            if not order_result.get("success"):
                self.position = None    # order didn't place — clear pending
                self._save_state()
                return {"status": "error", "reason": order_result.get("error", "order_failed")}

            # STEP 3: verify fill. Needed because an unfilled BUY LIMIT
            # below market would otherwise record a phantom entry at TV's
            # price (same bug class as the MGC TP3 incident 2026-04-17).
            order_id = order_result.get("order_id")
            actual_entry_price, fill_status = self._verify_fill(
                order_id, price, context=f"ENTRY/{direction}",
            )
            if fill_status in ("CANCELLED", "REJECTED"):
                logger.error(
                    "[WEBULL_TQQQ] Entry order %s on Webull (id=%s) — clearing PENDING paper",
                    fill_status, order_id,
                )
                self.position = None    # broker rejected — clear pending
                self._save_state()
                return {"status": "error", "reason": f"entry_{fill_status.lower()}"}

        # STEP 4: upgrade PENDING → FILLED with actual fill price.
        self.position = TQQQPosition(
            direction=direction,
            entry_price=actual_entry_price,
            entry_time=now.isoformat(),
            qty=qty,
            fill_status="FILLED",
        )

        logger.info(
            "[WEBULL_TQQQ] OPENED %s: %d shares @ $%.2f (fill_status=%s, tv=%.2f) (equity=$%.2f)",
            direction, qty, actual_entry_price, fill_status, price, self._equity,
        )

        return {
            "status": "opened",
            "direction": direction,
            "qty": qty,
            "price": actual_entry_price,
            "fill_status": fill_status,
            "live": self._live_orders,
        }

    def _close_position(self, price: float, now: datetime, comment: str = "") -> dict:
        """Close the current position and calculate P&L using ACTUAL fill price.

        Order changed (vs pre-2026-04-17 version): place order FIRST, verify
        fill, THEN compute PnL from actual fill price. The previous flow
        pre-computed PnL with TV's price, mutated equity, then rolled back
        on order failure — but had no path to correct for SUBMITTED orders
        that filled at a different price than TV claimed.
        """
        if not self.position:
            return {"status": "no_position"}

        pos = self.position

        # Crash-safety: mark PENDING_CLOSE BEFORE calling Webull. If we
        # crash between the broker fill and the self.position=None clear
        # below, startup reconcile uses broker truth: broker=0 → book the
        # close; broker still has position → revert fill_status to FILLED.
        pos.fill_status = "PENDING_CLOSE"
        self._save_state()

        # Place close order BEFORE PnL calc, so we can use the actual fill price
        actual_exit_price = price
        fill_status = "FILLED"
        order_id = None
        if self._live_orders:
            close_side = "SELL" if pos.direction == "LONG" else "BUY"
            order_result = self._place_webull_order(
                side=close_side,
                qty=pos.qty,
                price=price,
                comment=comment or "Close",
            )
            if not order_result.get("success"):
                logger.error("[WEBULL_TQQQ] Close order FAILED — reverting to FILLED: %s",
                             order_result.get("error", "unknown"))
                pos.fill_status = "FILLED"   # close never placed — revert
                self._save_state()
                return {"status": "error", "reason": order_result.get("error", "close_order_failed")}
            order_id = order_result.get("order_id")

            # Verify fill — same reason as MGC. Critical because TV's exit
            # price can differ from real Webull fill (slippage, market move).
            actual_exit_price, fill_status = self._verify_fill(
                order_id, price, context=f"CLOSE/{comment}",
            )
            if fill_status in ("CANCELLED", "REJECTED"):
                logger.error(
                    "[WEBULL_TQQQ] Close order %s on Webull (id=%s) — reverting to FILLED",
                    fill_status, order_id,
                )
                pos.fill_status = "FILLED"   # broker rejected close — revert
                self._save_state()
                return {"status": "error", "reason": f"close_{fill_status.lower()}"}

        # Compute PnL from ACTUAL fill price
        if pos.direction == "LONG":
            pnl_pct = (actual_exit_price - pos.entry_price) / pos.entry_price
        else:
            pnl_pct = (pos.entry_price - actual_exit_price) / pos.entry_price
        pnl_usd = pos.qty * pos.entry_price * pnl_pct

        # Update equity
        self._equity += pnl_usd
        self.daily_pnl += pnl_usd
        self.daily_trades += 1
        if pnl_usd > 0:
            self.daily_wins += 1
        elif pnl_usd < 0:
            self.daily_losses += 1

        logger.info(
            "[WEBULL_TQQQ] CLOSED %s: %d shares @ $%.2f (fill_status=%s, tv=%.2f) | "
            "P&L: $%.2f (%.2f%%) | Equity: $%.2f | %s",
            pos.direction, pos.qty, actual_exit_price, fill_status, price,
            pnl_usd, pnl_pct * 100, self._equity, comment,
        )

        # Log trade
        self._log_trade({
            "direction": pos.direction,
            "entry_price": pos.entry_price,
            "exit_price": actual_exit_price,
            "qty": pos.qty,
            "pnl_usd": round(pnl_usd, 2),
            "pnl_pct": round(pnl_pct * 100, 4),
            "comment": comment,
            "entry_time": pos.entry_time,
            "exit_time": now.isoformat(),
            "equity_after": round(self._equity, 2),
            "live": self._live_orders,
            "fill_status": fill_status,
            "broker_order_id": order_id,
        })

        result = {
            "status": "closed",
            "direction": pos.direction,
            "pnl_usd": round(pnl_usd, 2),
            "pnl_pct": round(pnl_pct * 100, 2),
            "comment": comment,
            "equity": round(self._equity, 2),
            "live": self._live_orders,
            "fill_status": fill_status,
        }

        self.position = None

        # Circuit breaker check
        if self.daily_pnl <= self._daily_loss_limit:
            self._circuit_breaker_fired = True
            logger.warning(
                "[WEBULL_TQQQ] CIRCUIT BREAKER: daily P&L $%.2f hit limit $%.2f",
                self.daily_pnl, self._daily_loss_limit,
            )

        # Cooldown
        if pnl_usd < 0:
            self._cooldown_until = time.time() + 300  # 5 min after loss
        else:
            self._cooldown_until = time.time() + 60   # 1 min after win

        return result

    def _place_webull_order(self, side: str, qty: int, price: float = 0, comment: str = "") -> dict:
        """Place a TQQQ limit order on Webull using the v3 order API.

        Uses ``order_v3.place_order()`` with ``instrument_type="EQUITY"``
        (Webull's v3 order endpoint requires "EQUITY" for stocks/ETFs —
        neither "STOCK" nor "ETF" are accepted despite appearing in data APIs).

        The v3 endpoint (``/openapi/trade/order/place``) is the canonical
        Webull US order API — same as the MGC broker uses for futures.
        """
        # Retry init if needed
        if not self._trade_client or not self._account_id:
            logger.info("[WEBULL_TQQQ] Retrying Webull init before order...")
            self._init_webull()
        if not self._trade_client or not self._account_id:
            logger.error("[WEBULL_TQQQ] Cannot place order: client not initialized")
            return {"success": False, "error": "not_initialized"}

        try:
            client_order_id = f"tqqq_{uuid.uuid4().hex[:16]}"

            # v3 order body — matches Webull OpenAPI documentation for US
            # equities/ETFs. Key findings from API testing (2026-04-16):
            #   - instrument_type MUST be "EQUITY" (not "STOCK" or "ETF")
            #   - support_trading_session MUST be present ("CORE" = regular hours)
            #   - "STOCK" → INVALID_PARAMETER, "ETF" → same error
            order_body = {
                "combo_type": "NORMAL",
                "client_order_id": client_order_id,
                "symbol": "TQQQ",
                "instrument_type": "EQUITY",
                "market": "US",
                "order_type": "LIMIT",
                "limit_price": str(round(price, 2)),
                "quantity": str(qty),
                "side": side,
                "time_in_force": "DAY",
                "entrust_type": "QTY",
                "support_trading_session": "CORE",
            }

            logger.info(
                "[WEBULL_TQQQ] Placing LIMIT %s %d shares @ $%.2f (%s)",
                side, qty, price, comment,
            )

            resp = self._trade_client.order_v3.place_order(
                account_id=self._account_id,
                new_orders=[order_body],
            )

            data = resp.json() if hasattr(resp, "json") else resp
            logger.info("[WEBULL_TQQQ] Order response: %s", str(data)[:500])

            # Check for error in response body (some errors don't raise exceptions)
            if isinstance(data, dict) and data.get("error_code"):
                logger.error("[WEBULL_TQQQ] Order rejected: %s — %s",
                             data.get("error_code"), data.get("message", ""))
                return {"success": False, "error": data.get("message", data["error_code"])}

            return {"success": True, "order_id": client_order_id, "response": data}

        except Exception as e:
            logger.error("[WEBULL_TQQQ] Order failed: %s", e)
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Fill verification — same pattern as WebullMGCBroker, kept inline
    # because TQQQ executor doesn't go through a broker class. If a
    # third instrument joins, factor these into a shared base class.
    # ------------------------------------------------------------------
    def _get_order_status(self, client_order_id: str) -> dict:
        """Query Webull for the current status of an order.

        Returns a normalized dict:
            {status, filled_quantity, total_quantity, filled_price, raw, error?}
        Status is one of FILLED / SUBMITTED / CANCELLED / REJECTED / UNKNOWN.

        For paper-only mode (live_orders=False), returns synthetic FILLED.
        """
        if not self._live_orders:
            return {
                "status": "FILLED",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "dry_run": True,
            }
        if not self._trade_client or not self._account_id:
            return {
                "status": "UNKNOWN",
                "filled_quantity": 0.0,
                "total_quantity": 0.0,
                "filled_price": 0.0,
                "error": "client_not_initialized",
            }
        try:
            resp = self._trade_client.order_v3.get_order_detail(
                account_id=self._account_id,
                client_order_id=client_order_id,
            )
            data = resp.json() if hasattr(resp, "json") else resp
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
                    "status": "UNKNOWN", "filled_quantity": 0.0,
                    "total_quantity": 0.0, "filled_price": 0.0,
                    "error": "no_order_in_response",
                }
            sub = sub_orders[0]
            status_raw = (sub.get("status") or "").upper()
            if status_raw == "FILLED":
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
            logger.error("[WEBULL_TQQQ] _get_order_status failed for %s: %s",
                         client_order_id, exc)
            return {
                "status": "UNKNOWN", "filled_quantity": 0.0,
                "total_quantity": 0.0, "filled_price": 0.0,
                "error": str(exc),
            }

    def _wait_for_fill(
        self, client_order_id: str,
        timeout_seconds: float = 5.0,
        poll_interval_seconds: float = 0.5,
    ) -> dict:
        """Poll until FILLED/CANCELLED/REJECTED, or return last status on timeout."""
        if not self._live_orders:
            return self._get_order_status(client_order_id)
        deadline = time.time() + timeout_seconds
        last_status = None
        while time.time() < deadline:
            result = self._get_order_status(client_order_id)
            last_status = result
            if result.get("status") in ("FILLED", "CANCELLED", "REJECTED"):
                return result
            time.sleep(poll_interval_seconds)
        return last_status or {
            "status": "UNKNOWN", "filled_quantity": 0.0,
            "total_quantity": 0.0, "filled_price": 0.0,
            "error": "no_status_polled",
        }

    def _verify_fill(
        self, client_order_id: Optional[str], tv_price: float, context: str = "",
        timeout_seconds: float = 5.0,
    ) -> "tuple[float, str]":
        """Wait for fill, return (actual_price, status). Falls back to tv_price.

        Same semantics as WebhookExecutor._verify_fill in webhook_executor.py.
        Critical for TQQQ because LIMIT orders above market sit unfilled, and
        without verification we'd record fictional fills (same bug class as
        the MGC TP3 incident on 2026-04-17).
        """
        if not client_order_id:
            return (tv_price, "FILLED")
        result = self._wait_for_fill(client_order_id, timeout_seconds=timeout_seconds)
        if result.get("dry_run"):
            return (tv_price, "FILLED")
        status = result.get("status", "UNKNOWN")
        filled_price = result.get("filled_price", 0.0) or 0.0
        if status == "FILLED" and filled_price > 0:
            logger.info(
                "[WEBULL_TQQQ] %s verified FILLED: id=%s tv=$%.2f actual=$%.2f slip=$%.2f",
                context, client_order_id, tv_price, filled_price, filled_price - tv_price,
            )
            return (filled_price, status)
        if status == "SUBMITTED":
            logger.warning(
                "[WEBULL_TQQQ] %s order STILL SUBMITTED after %.1fs (id=%s) — "
                "recording with TV price as estimate",
                context, timeout_seconds, client_order_id,
            )
            return (tv_price, status)
        logger.error("[WEBULL_TQQQ] %s order on Webull is %s (id=%s)",
                     context, status, client_order_id)
        return (tv_price, status)

    def get_status(self) -> dict:
        """Return current status for /status endpoint."""
        pos_info = None
        if self.position:
            pos_info = {
                "direction": self.position.direction,
                "entry_price": self.position.entry_price,
                "qty": self.position.qty,
                "entry_time": self.position.entry_time,
            }

        # Show real Webull buying power if available
        webull_bp = 0.0
        if self._live_orders:
            webull_bp = self._fetch_buying_power()

        return {
            "position": pos_info,
            "equity": round(self._equity, 2),
            "webull_buying_power": round(webull_bp, 2) if webull_bp > 0 else "unavailable",
            "daily_pnl": round(self.daily_pnl, 2),
            "daily_trades": self.daily_trades,
            "daily_wl": f"{self.daily_wins}W/{self.daily_losses}L",
            "circuit_breaker": self._circuit_breaker_fired,
            "live_orders": self._live_orders,
            "capital_cap": _CAPITAL_CAP,
        }

    def _maybe_reset_daily(self, now: datetime):
        """Reset daily stats at midnight ET."""
        today = now.strftime("%Y-%m-%d")
        if self._last_reset_date != today:
            if self._last_reset_date is not None:
                logger.info(
                    "[WEBULL_TQQQ] Daily reset: prev P&L=$%.2f, %dW/%dL",
                    self.daily_pnl, self.daily_wins, self.daily_losses,
                )
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.daily_wins = 0
            self.daily_losses = 0
            self._circuit_breaker_fired = False
            self._last_reset_date = today

    def get_broker_position_qty(self) -> Optional[int]:
        """Return the signed TQQQ position on Webull (for reconciler drift check).

        Same semantics as ``WebullMGCBroker.get_position_qty``:
            * 0    — Webull confirms flat in TQQQ.
            * +N   — LONG N shares.
            * -N   — SHORT N shares.
            * None — query failed or dry-run mode. Caller MUST NOT treat None
                     as flat.
        """
        if not self._live_orders:
            return None
        if not self._trade_client or not self._account_id:
            return None
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

            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                sym = str(pos.get("symbol") or pos.get("ticker_symbol") or "").upper()
                if sym != "TQQQ":
                    continue
                try:
                    qty = int(float(pos.get("quantity", 0) or 0))
                except (TypeError, ValueError):
                    return None
                direction = str(pos.get("direction", "")).upper()
                if direction == "SHORT":
                    qty = -qty
                return qty
            return 0
        except Exception as exc:
            logger.error("[WEBULL_TQQQ] get_broker_position_qty failed: %s", exc)
            return None

    def force_flatten(self, reason: str) -> None:
        """Clear paper position and persist — no broker call, no PnL accounting.

        Used by the reconciler when Webull reports flat but paper still holds a
        phantom. Skips PnL booking because the matching broker close either
        already happened or never existed, and paper's entry_price may be stale.
        """
        if self.position is None:
            return
        # Don't clear a PENDING_OPEN — it's a transient state during an in-flight
        # Webull place_order call. The reconciler's 60s cadence can race with a
        # 2-5s PENDING window; clearing here would wipe a legit open-in-flight.
        # If the process dies mid-open, _reconcile_on_startup handles the cleanup
        # via broker-truth comparison on next boot.
        if self.position.fill_status == "PENDING_OPEN":
            logger.info(
                "[WEBULL_TQQQ] force_flatten skipped — position is PENDING_OPEN (%s)",
                reason,
            )
            return
        logger.warning(
            "[WEBULL_TQQQ] force_flatten: dropping phantom %s %d sh @ $%.2f — %s",
            self.position.direction,
            self.position.qty,
            self.position.entry_price,
            reason,
        )
        self.position = None
        self._save_state()

    def _get_broker_position_full(self) -> Optional[dict]:
        """Return {'qty': signed_int, 'cost_price': float} or None on query failure.

        Like ``get_broker_position_qty`` but also returns cost_price for startup
        reconcile so we can rebuild paper with the real entry price rather than
        a signal-time placeholder.
        """
        if not self._trade_client or not self._account_id:
            return None
        try:
            resp = self._trade_client.account_v2.get_account_position(self._account_id)
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
                return None
            if not isinstance(positions, list):
                return None

            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                sym = str(pos.get("symbol") or pos.get("ticker_symbol") or "").upper()
                if sym != "TQQQ":
                    continue
                try:
                    qty = int(float(pos.get("quantity", 0) or 0))
                    cost = float(pos.get("cost_price", pos.get("cost", 0)) or 0)
                except (TypeError, ValueError):
                    return None
                if str(pos.get("direction", "")).upper() == "SHORT":
                    qty = -qty
                return {"qty": qty, "cost_price": cost}
            return {"qty": 0, "cost_price": 0.0}
        except Exception as exc:
            logger.error("[WEBULL_TQQQ] _get_broker_position_full failed: %s", exc)
            return None

    def _reconcile_on_startup(self) -> None:
        """Repair paper state against broker truth on boot.

        Closes the crash-unsafety window in _open_position / _close_position.
        Handles every combination of paper (None / FILLED / PENDING_OPEN /
        PENDING_CLOSE) × broker (flat / same / different).
        """
        if not self._live_orders:
            return

        # Retry on None -- Webull's cold-start throttle 429s rapid restarts.
        broker = None
        for attempt in range(3):
            broker = self._get_broker_position_full()
            if broker is not None:
                break
            if attempt < 2:
                delay = 2.0 * (2 ** attempt)
                logger.warning(
                    "[WEBULL_TQQQ] Startup reconcile: broker returned None -- retry %d/3 in %.0fs",
                    attempt + 1, delay,
                )
                time.sleep(delay)
        if broker is None:
            logger.warning(
                "[WEBULL_TQQQ] Startup reconcile skipped: broker unreachable after 3 attempts -- paper unchanged"
            )
            return

        broker_qty = broker["qty"]
        broker_cost = broker["cost_price"]

        # Signed paper qty for comparison.
        paper_qty = 0
        if self.position:
            paper_qty = (
                self.position.qty
                if self.position.direction == "LONG"
                else -self.position.qty
            )
        paper_status = self.position.fill_status if self.position else "FLAT"

        logger.info(
            "[WEBULL_TQQQ] Startup reconcile: paper=%d (%s) broker=%d",
            paper_qty, paper_status, broker_qty,
        )

        # Fast path: already in sync and stable.
        if paper_qty == broker_qty and paper_status in ("FILLED", "FLAT"):
            return

        if broker_qty == 0:
            if paper_status == "PENDING_CLOSE" and self.position:
                # Close succeeded on broker before we could save — book a
                # zero-PnL reconciled trade (actual exit price unknown).
                logger.warning(
                    "[WEBULL_TQQQ] Reconcile: PENDING_CLOSE + broker flat → booking reconciled close"
                )
                self._log_trade({
                    "direction": self.position.direction,
                    "entry_price": self.position.entry_price,
                    "exit_price": self.position.entry_price,
                    "qty": self.position.qty,
                    "pnl_usd": 0.0,
                    "pnl_pct": 0.0,
                    "comment": "RECONCILED: broker close detected on startup",
                    "entry_time": self.position.entry_time,
                    "exit_time": datetime.now(ET).isoformat(),
                    "equity_after": round(self._equity, 2),
                    "live": True,
                    "fill_status": "RECONCILED",
                })
            else:
                logger.warning(
                    "[WEBULL_TQQQ] Reconcile: paper=%s broker=flat → clearing paper",
                    paper_status,
                )
            self.position = None
        else:
            # Broker has a position — rebuild paper to match.
            direction = "LONG" if broker_qty > 0 else "SHORT"
            qty = abs(broker_qty)
            entry_price = broker_cost if broker_cost > 0 else (
                self.position.entry_price if self.position else 0.0
            )
            # Preserve paper's entry_time if it already matches direction+qty;
            # otherwise stamp now (we don't have the broker fill time cheaply).
            entry_time = (
                self.position.entry_time
                if (self.position
                    and self.position.direction == direction
                    and self.position.qty == qty
                    and self.position.entry_time)
                else datetime.now(ET).isoformat()
            )
            self.position = TQQQPosition(
                direction=direction,
                entry_price=entry_price,
                entry_time=entry_time,
                qty=qty,
                fill_status="FILLED",
            )
            logger.warning(
                "[WEBULL_TQQQ] Reconcile: paper rebuilt from broker → %s %d @ $%.2f",
                direction, qty, entry_price,
            )

        self._save_state()

    def _save_state(self):
        """Persist state to disk (atomic write) and sync to Gist."""
        state = {
            "equity": self._equity,
            "initial_equity": self._initial_equity,
            "daily_pnl": self.daily_pnl,
            "daily_trades": self.daily_trades,
            "daily_wins": self.daily_wins,
            "daily_losses": self.daily_losses,
            "last_reset_date": self._last_reset_date,
            "circuit_breaker": self._circuit_breaker_fired,
            "position": asdict(self.position) if self.position else None,
        }
        try:
            fd, tmp = tempfile.mkstemp(dir=_STATE_DIR, suffix=".tmp")
            with os.fdopen(fd, "w") as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, _STATE_FILE)
        except Exception as e:
            logger.error("[WEBULL_TQQQ] Failed to save state: %s", e)

        # Sync to GitHub Gist for Vercel dashboard
        self._sync_to_gist(state)

    def _get_gist_creds(self) -> tuple:
        """Get Gist ID and GitHub token from env."""
        gist_id = os.environ.get("KRAKEN_STATE_GIST_ID", "")
        gh_token = os.environ.get("GITHUB_TOKEN", "")
        if not gist_id or not gh_token:
            env_file = os.path.join(_PROJECT_ROOT, "secrets", ".env")
            if os.path.exists(env_file):
                try:
                    with open(env_file) as f:
                        for line in f:
                            line = line.strip()
                            if not line or line.startswith("#"):
                                continue
                            k, _, v = line.partition("=")
                            k, v = k.strip(), v.strip().strip("\"'")
                            if k == "KRAKEN_STATE_GIST_ID" and not gist_id:
                                gist_id = v
                            elif k == "GITHUB_TOKEN" and not gh_token:
                                gh_token = v
                except Exception:
                    pass
        return gist_id, gh_token

    def _sync_to_gist(self, state: dict) -> None:
        """Upload state + trade log to GitHub Gist for the dashboard."""
        gist_id, gh_token = self._get_gist_creds()
        if not gist_id or not gh_token:
            return
        try:
            import requests
            # Dashboard-compatible state
            gist_state = {
                "equity": state.get("equity", 0),
                "initial_equity": state.get("initial_equity", 0),
                "position": state.get("position"),
                "daily_pnl": state.get("daily_pnl", 0),
                "daily_trades": state.get("daily_trades", 0),
                "daily_wins": state.get("daily_wins", 0),
                "daily_losses": state.get("daily_losses", 0),
                "live_orders": self._live_orders,
            }
            files = {
                "webull_tqqq_state.json": {
                    "content": json.dumps(gist_state, indent=2),
                },
            }
            # Include trade log (last 200 entries)
            trades = []
            if os.path.exists(_TRADE_LOG):
                try:
                    with open(_TRADE_LOG) as f:
                        trades = json.load(f)
                except Exception:
                    pass
            if trades:
                files["webull_tqqq_trade_log.json"] = {
                    "content": json.dumps(trades[-200:], indent=2),
                }
            requests.patch(
                f"https://api.github.com/gists/{gist_id}",
                headers={
                    "Authorization": f"token {gh_token}",
                    "Accept": "application/vnd.github.v3+json",
                },
                json={"files": files},
                timeout=10,
            )
        except Exception as exc:
            logger.debug("[WEBULL_TQQQ] Gist sync error: %s", exc)

    def _load_state(self):
        """Restore state from disk."""
        if not os.path.exists(_STATE_FILE):
            return
        try:
            with open(_STATE_FILE) as f:
                state = json.load(f)
            self._equity = state.get("equity", self._initial_equity)
            self._initial_equity = state.get("initial_equity", self._initial_equity)
            self.daily_pnl = state.get("daily_pnl", 0)
            self.daily_trades = state.get("daily_trades", 0)
            self.daily_wins = state.get("daily_wins", 0)
            self.daily_losses = state.get("daily_losses", 0)
            self._last_reset_date = state.get("last_reset_date")
            self._circuit_breaker_fired = state.get("circuit_breaker", False)

            pos_data = state.get("position")
            if pos_data:
                self.position = TQQQPosition(**pos_data)
            logger.info("[WEBULL_TQQQ] State restored: equity=$%.2f", self._equity)
        except Exception as e:
            logger.error("[WEBULL_TQQQ] Failed to load state: %s", e)

    def _log_trade(self, trade: dict):
        """Append trade to log file."""
        trades = []
        if os.path.exists(_TRADE_LOG):
            try:
                with open(_TRADE_LOG) as f:
                    trades = json.load(f)
            except Exception:
                pass
        trades.append(trade)
        try:
            fd, tmp = tempfile.mkstemp(dir=_STATE_DIR, suffix=".tmp")
            with os.fdopen(fd, "w") as f:
                json.dump(trades, f, indent=2)
            os.replace(tmp, _TRADE_LOG)
        except Exception as e:
            logger.error("[WEBULL_TQQQ] Failed to log trade: %s", e)
