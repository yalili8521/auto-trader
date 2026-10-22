"""Webhook executor — receives TradingView alerts and tracks paper positions.

Signal flow: TradingView → webhook_server → WebhookExecutor → paper P&L tracking
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from src.gold_scalper.alerts import ScalperAlertEngine
from src.gold_scalper.config import GoldScalperConfig

logger = logging.getLogger(__name__)


def _fetch_yahoo_price(symbol: str = "MGC=F") -> Optional[float]:
    """Fetch latest price from Yahoo Finance (non-blocking fallback)."""
    try:
        import yfinance as yf
        t = yf.Ticker(symbol)
        return float(t.fast_info.last_price)
    except Exception as e:
        logger.debug("[PRICE_CMP] Yahoo fetch failed: %s", e)
        return None


def _atomic_replace_with_retry(tmp_path: str, dest_path: str, attempts: int = 4) -> None:
    """os.replace with short exponential backoff for OneDrive sync locks.

    OneDrive's background sync occasionally holds a brief lock on files
    under its synced tree, surfacing as WinError 5 / Access denied (or
    WinError 32 / sharing violation) exactly during the rename in the
    "write to .tmp -> atomic rename" pattern. Observed 2026-04-24 11:05:52
    during state save -- reconciler had to clean up the resulting paper
    drift 60s later. Locks are typically <200ms; retry 4x with 50ms, 100ms,
    200ms backoff (worst-case +350ms added latency). Raises the final
    OSError if every attempt fails so the caller's except clause still
    runs and logs.
    """
    for i in range(attempts):
        try:
            os.replace(tmp_path, dest_path)
            return
        except OSError:
            if i == attempts - 1:
                raise
            time.sleep(0.05 * (2 ** i))  # 50ms, 100ms, 200ms


def compute_contract_cap(
    tv_contracts: int,
    live_equity: float,
    initial_equity: float,
    base_contracts: int,
    scale_per_profit: float,
) -> "tuple[int, int, int]":
    """Plan A contract cap — clamp TV's requested contracts to a
    scale-up budget driven by REAL account equity.

    Why this exists: the Pine strategy computes ``scale_mult`` from
    ``strategy.equity`` — TradingView's simulated strategy equity, which
    compounds through backtest P&L and has nothing to do with the real
    Webull balance. A strategy that's run for months in backtest can
    easily have ``strategy.equity`` at $45K+ while the real account
    holds $148, producing 30-contract alerts that would either get
    rejected at margin or blow up a real account.

    The formula mirrors Pine's intent exactly (so scale-up still
    functions as designed) but anchors on the REAL equity the caller
    passes in (usually ``broker.get_live_equity()``):

        scale_mult = 1 + max(0, floor((live_equity - initial_equity) / scale_per_profit))
        max_contracts = base_contracts * scale_mult
        final = min(tv_contracts, max_contracts)

    At ``base_contracts=6, scale_per_profit=$10K, initial_equity=$5K``:

    ======================  ==========  ===============  ===============
    Live equity             scale_mult  max_contracts    TV=30 → sent
    ======================  ==========  ===============  ===============
    $148  (today)                    1               6                6
    $5,000  (anchor)                 1               6                6
    $15,000  (+$10K)                 2              12               12
    $25,000  (+$20K)                 3              18               18
    $45,000  (+$40K)                 5              30               30
    ======================  ==========  ===============  ===============

    Args:
        tv_contracts: the contract count from TradingView's payload.
        live_equity: the caller's best estimate of REAL account equity.
            Typically from ``broker.get_live_equity()``. Callers should
            fall back to their paper-side tracking when the broker is
            unavailable or returns 0 — don't pass 0 as "account is
            empty", pass the paper equity as "I don't know the real
            number".
        initial_equity: the strategy's anchor equity — equity level at
            which ``scale_mult=1`` (i.e., you start at base_contracts).
            This should match the Pine strategy's ``base_capital`` input.
        base_contracts: how many contracts to trade per unit of
            scale_mult. Must be >= 1.
        scale_per_profit: how many dollars of profit above
            ``initial_equity`` buys one additional unit of scale_mult.
            E.g. $10000 means every +$10K in equity grows the position
            by ``base_contracts``.

    Returns:
        Tuple ``(final_contracts, max_contracts, scale_mult)``:
        - ``final_contracts``: the capped contract count to actually
          send to the broker. Always ``min(tv_contracts, max_contracts)``.
        - ``max_contracts``: the cap computed from live_equity.
        - ``scale_mult``: the scale multiplier for logging / Discord.

    Notes:
        - Uses ``int()`` truncation (Python) which matches Pine's
          ``math.floor()`` for positive inputs. The ``max(0, ...)``
          wrapper handles negative-profit cases cleanly.
        - If ``scale_per_profit <= 0`` the function falls back to
          ``scale_mult = 1`` rather than dividing by zero — this
          mirrors the intent of "no scale-up configured".
        - NaN/inf live_equity is treated as "unknown" and falls back
          to ``scale_mult = 1``. Without this guard, ``int(inf)``
          raises ``OverflowError`` and ``int(nan)`` raises
          ``ValueError``, which would crash the entry handler
          mid-flight after we'd already decided not to reject. A
          silent floor-to-base is safer than a 500 in the webhook.
        - The function is deliberately pure (no I/O, no state). All
          caching / fallbacks / logging happen at the call site so
          unit tests can exercise the math in isolation.
    """
    if base_contracts < 1:
        base_contracts = 1
    # NaN/inf guard — see docstring. math.isfinite() is False for
    # both NaN and ±inf. A bogus live_equity floors to scale_mult=1
    # rather than crashing int() downstream.
    if not math.isfinite(live_equity):
        scale_mult = 1
    elif scale_per_profit <= 0:
        scale_mult = 1
    else:
        scale_mult = 1 + max(
            0,
            int((live_equity - initial_equity) / scale_per_profit),
        )
    max_contracts = base_contracts * scale_mult
    final = min(max(0, tv_contracts), max_contracts)
    return final, max_contracts, scale_mult


PT = ZoneInfo("America/Los_Angeles")


@dataclass
class WebhookPosition:
    """Tracks an open position initiated by TradingView signal."""
    direction: str              # "LONG" or "SHORT"
    entry_price: float
    entry_time: datetime
    total_contracts: int        # ACTUAL contracts sent to broker (after cap)
    remaining_contracts: int
    tp_hits: List[int] = field(default_factory=list)  # [1, 2, ...] as TPs fire
    realized_pnl: float = 0.0
    # Broker fill tracking — set by _handle_entry when a live broker is
    # attached. broker_order_id lets the reconciler find this entry on
    # Webull. fill_status flags whether entry_price is verified ("FILLED")
    # or estimated from TV price ("SUBMITTED" / "UNKNOWN") pending
    # reconciler resolution.
    broker_order_id: Optional[str] = None
    fill_status: str = "FILLED"
    # === Cap-ratio tracking (fixes proportional exit bug, 2026-04-17) ===
    # When compute_contract_cap reduces TV's requested entry (e.g. TV asks
    # for 18, real account only supports 6 → ratio 6/18 = 1/3), every
    # subsequent exit signal must be scaled by the same ratio. Otherwise
    # TV's "TP1: sell 3 of 18" gets blindly applied as "sell 3 of 6" =
    # 50% close instead of the intended 1/6 = 1 contract.
    #
    # tv_total_contracts: what TV originally requested (e.g. 18)
    # tv_contracts_closed: cumulative contracts TV thinks have closed
    #
    # Cumulative tracking (vs per-leg multiplication) avoids rounding drift:
    # we always reconcile actual_closed = round(tv_closed * actual / tv).
    tv_total_contracts: int = 0
    tv_contracts_closed: int = 0


@dataclass
class WebhookSignal:
    """Parsed TradingView webhook signal."""
    action: str                 # ENTRY, TP_HIT, EXIT, SESSION_CLOSE
    direction: Optional[str]    # LONG, SHORT (for ENTRY)
    price: float
    contracts: int
    tp_level: Optional[int]     # 1-4 (for TP_HIT)
    reason: Optional[str]       # exit reason (for EXIT)
    raw: Dict                   # original JSON payload


class WebhookExecutor:
    """Routes TradingView signals to broker with risk controls.

    TradingView信号执行器：
    - 接收解析后的webhook信号
    - 检查风险控制（每日亏损限额、冷却期、最大仓位）
    - 通过broker下单
    - 跟踪仓位状态和每日盈亏
    - 同步状态到GitHub Gist供dashboard显示
    """

    def __init__(
        self,
        broker,  # optional live broker (e.g., WebullMGCBroker) — if set,
                 # entries/TP hits/exits are mirrored as real orders.
                 # When None, executor runs in pure paper mode.
        config: GoldScalperConfig,
        alerts: Optional[ScalperAlertEngine] = None,
        initial_equity: float = 5000.0,
        live_required: bool = False,  # 2026-10-14 (DNS boot race): True when
                 # the operator requested live trading (TRADOVATE_LIVE=1) —
                 # entries are REFUSED instead of paper-filled if broker is
                 # None, so a broker init failure can never silently downgrade
                 # a live system to paper.
    ):
        self.broker = broker
        self.config = config
        self.alerts = alerts
        self.live_required = live_required

        # State
        self.position: Optional[WebhookPosition] = None
        self.daily_pnl: float = 0.0
        self.daily_trades: int = 0
        self.daily_wins: int = 0
        self.daily_losses: int = 0
        self._last_day: Optional[int] = None
        self._circuit_breaker_fired: bool = False
        # 2026-10-14: broker equity at the first alert of the trading day —
        # lets the -$200 breaker see losses booked outside this process
        # (e.g. the 10-12 broker auto-liquidation that paper never saw).
        self._day_start_equity: Optional[float] = None
        self._cooldown_until: Optional[datetime] = None
        self._last_entry_attempt: Optional[datetime] = None  # dedup guard
        self._entry_dedup_seconds: int = 30  # reject duplicate entries within this window

        # Serialize concurrent webhook calls — FastAPI runs sync handlers in a
        # threadpool, so two simultaneous TradingView alerts can otherwise race
        # on self.position / self.daily_pnl mutations.
        self._exec_lock = threading.Lock()

        # Equity tracking for dashboard
        self._initial_equity: float = initial_equity
        self._equity: float = initial_equity
        self._trade_log: List[Dict] = []

        # Price comparison (TV vs Yahoo)
        self._price_comparisons: List[Dict] = []

        # State persistence — separate folder from signal-mode bot
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))))
        state_dir = os.path.join(project_root, "outputs", "paper_state", "webhook")
        os.makedirs(state_dir, exist_ok=True)
        self._price_cmp_file = os.path.join(state_dir, "price_comparison.json")
        self._state_file = os.path.join(state_dir, "webhook_executor_state.json")
        self._log_file = os.path.join(state_dir, "webhook_trade_log.json")
        self._load_state()
        # Crash-safety: reconcile loaded paper against broker truth.
        self._reconcile_on_startup()

    # ------------------------------------------------------------------
    # Live equity sourcing — used by the Plan A contract cap
    # ------------------------------------------------------------------
    def _get_live_equity(self) -> float:
        """Return real Webull equity if the broker exposes it, else paper.

        The contract cap in ``handle_entry`` needs a dollar figure to
        compute ``scale_mult`` from. Two sources, in priority order:

        1. ``broker.get_live_equity()`` — the real Webull buying power.
           This is what we WANT to use because it's grounded in the
           actual account. Cached for 5 min inside the broker so
           calling it on every webhook is cheap.
        2. ``self._equity`` — paper-side compounding equity. Used when
           the broker is None (pure-paper mode), when broker doesn't
           implement ``get_live_equity`` (e.g., a future broker
           subclass), or when the Webull fetch fails and returns 0.

        Returning 0 is not useful — the cap formula would compute
        ``scale_mult`` assuming the account is empty, which both
        mis-sizes AND makes paper mode unusable. So we treat "broker
        returned 0" as "broker doesn't know" and fall back to paper.
        """
        if self.broker is not None and hasattr(self.broker, "get_live_equity"):
            try:
                live = float(self.broker.get_live_equity())
                # Reject NaN/inf — a broken broker that returns inf
                # would sail past ``live > 0`` and crash the cap's
                # int() downstream. Treat it as "unknown" and fall
                # back to paper tracking.
                if live > 0 and math.isfinite(live):
                    return live
            except Exception as exc:
                logger.warning(
                    "[WEBHOOK] broker.get_live_equity() raised, falling "
                    "back to paper equity: %s",
                    exc,
                )
        return self._equity

    # ------------------------------------------------------------------
    # Paper/broker position reconciliation — broker is truth
    # ------------------------------------------------------------------
    def _sync_position_from_broker(self, signal_price: float = 0.0) -> None:
        """Reconcile self.position to actual Webull position before deciding.

        Paper state is a local cache; Webull is the source of truth. Without
        this sync, drifted paper state causes wrong decisions:
          * TP/Exit alert arrives → paper says flat → server rejects
            "No position to close" while Webull bleeds an unhedged trade.
          * Entry alert arrives → paper says flat → server opens a NEW
            position on top of one Webull already has → double exposure.

        Called at the top of every ``_run_locked()`` so every handler below
        works off reality. Cost is one broker API call (~100-300ms); the
        broker caches where possible. If the broker query fails or returns
        unknown, we keep paper state rather than corrupting it — better to
        act on stale state than block a legitimate trade.

        ``signal_price`` is used as a placeholder ``entry_price`` when we
        discover a broker position paper didn't know about. Using 0.0 would
        make downstream PnL math nonsensical; using the current alert price
        yields approximately 0 realized PnL for this one reconciled trade,
        which is a safer failure mode.
        """
        if self.broker is None:
            return  # paper-only mode — nothing to reconcile against
        if not getattr(self.broker, "_live_orders", False):
            return  # broker in dry-run — its get_position_qty returns None

        try:
            actual_qty = self.broker.get_position_qty()
        except Exception as exc:
            logger.warning(
                "[WEBHOOK] broker.get_position_qty() raised: %s — "
                "keeping paper state as-is", exc,
            )
            return

        if actual_qty is None:
            # Query failed or unknown — never treat None as flat.
            logger.warning(
                "[WEBHOOK] broker.get_position_qty() returned None — "
                "keeping paper state as-is",
            )
            return

        # Skip sync if paper position is awaiting broker fill confirmation.
        # A freshly placed order has fill_status="SUBMITTED"/"UNKNOWN" for
        # a few seconds before it shows up on Webull. During that window
        # get_position_qty() legitimately returns the *pre-fill* qty (often
        # 0), which would make sync clear a valid pending position. The
        # reconciler runs every 60s and has richer logic to resolve these
        # pending states, so defer to it rather than risk wiping paper.
        if self.position and self.position.fill_status in ("SUBMITTED", "UNKNOWN"):
            return

        # Signed paper qty: LONG +N, SHORT -N, flat 0.
        if self.position and self.position.remaining_contracts > 0:
            paper_qty = (
                self.position.remaining_contracts
                if self.position.direction == "LONG"
                else -self.position.remaining_contracts
            )
        else:
            paper_qty = 0

        if paper_qty == actual_qty:
            return  # already in sync — common case, no-op

        logger.warning(
            "[WEBHOOK] PAPER/BROKER DRIFT: paper_qty=%d, broker_qty=%d — "
            "reconciling paper to broker truth", paper_qty, actual_qty,
        )

        if actual_qty == 0:
            # Webull is flat → clear any phantom paper position.
            logger.info(
                "[WEBHOOK] Clearing phantom paper position (broker is flat)",
            )
            self.position = None
            return

        # Webull has a position — rebuild paper to match. entry_price is
        # unknown without an avg-cost query, so use signal_price as a
        # safe placeholder (near-zero PnL on close vs phantom $thousands).
        direction = "LONG" if actual_qty > 0 else "SHORT"
        qty = abs(actual_qty)
        placeholder_entry = signal_price if signal_price > 0 else 0.0
        self.position = WebhookPosition(
            direction=direction,
            entry_price=placeholder_entry,
            entry_time=datetime.now(PT),
            total_contracts=qty,
            remaining_contracts=qty,
            tv_total_contracts=qty,   # unknown — mirror broker qty
            tv_contracts_closed=0,
        )
        self.position.fill_status = "RECONCILED_FROM_BROKER"
        logger.info(
            "[WEBHOOK] Rebuilt paper position from broker: %s %d ct "
            "(entry_price placeholder=$%.2f, fill_status=RECONCILED_FROM_BROKER)",
            direction, qty, placeholder_entry,
        )

    def parse_signal(self, payload: Dict) -> WebhookSignal:
        """Parse raw webhook JSON into a WebhookSignal.

        Handles two formats:
        1. Native: {"action": "ENTRY", "direction": "SHORT", ...}
        2. TradingView: {"action": "buy/sell", "comment": "Short Entry", ...}
           - comment determines signal type (Entry, TP1-4, Hard Stop, Session Close)
           - action (buy/sell) + comment determine direction
        """
        raw_action = payload.get("action", "").strip().lower()
        comment = payload.get("comment", "").strip()
        comment_upper = comment.upper()

        # Defensive type conversion — malformed TV payloads (e.g. price="NaN"
        # or contracts="abc") would otherwise raise ValueError up through the
        # FastAPI handler and return a 500. Coerce to 0 so downstream
        # validators can reject cleanly with a 200 + rejection message.
        try:
            price = float(payload.get("price", 0))
        except (TypeError, ValueError):
            logger.warning("[WEBHOOK] Non-numeric price in payload: %r", payload.get("price"))
            price = 0.0
        # JSON allows NaN/Infinity as Python extensions via parse_constant;
        # both would slip past "price <= 0" checks downstream and pollute
        # paper state / PnL math. Normalize to 0 so the regular validation
        # rejects it.
        if not math.isfinite(price):
            logger.warning("[WEBHOOK] Non-finite price in payload: %r", payload.get("price"))
            price = 0.0
        try:
            contracts = int(payload.get("contracts", 0))
        except (TypeError, ValueError):
            logger.warning("[WEBHOOK] Non-numeric contracts in payload: %r", payload.get("contracts"))
            contracts = 0

        # --- Native format (action is already ENTRY/TP_HIT/EXIT/SESSION_CLOSE) ---
        if raw_action in ("entry", "tp_hit", "exit", "session_close"):
            try:
                tp_level_raw = int(payload.get("tp_level", 0)) or None
            except (TypeError, ValueError):
                tp_level_raw = None
            return WebhookSignal(
                action=raw_action.upper(),
                direction=payload.get("direction", "").upper() or None,
                price=price,
                contracts=contracts,
                tp_level=tp_level_raw,
                reason=payload.get("reason"),
                raw=payload,
            )

        # --- TradingView format (action is "buy" or "sell") ---
        is_buy = raw_action == "buy"

        # Determine signal type from comment
        action = "ENTRY"
        direction = None
        tp_level = None
        reason = None

        if "ENTRY" in comment_upper or "ENTER" in comment_upper:
            action = "ENTRY"
            # "Short Entry" → sell-to-open → SHORT
            # "Long Entry" → buy-to-open → LONG
            if "SHORT" in comment_upper:
                direction = "SHORT"
            elif "LONG" in comment_upper:
                direction = "LONG"
            else:
                # Fallback: buy=LONG, sell=SHORT
                direction = "LONG" if is_buy else "SHORT"

        elif "TP4" in comment_upper or "TP 4" in comment_upper:
            action = "TP_HIT"
            tp_level = 4
        elif "TP3" in comment_upper or "TP 3" in comment_upper:
            action = "TP_HIT"
            tp_level = 3
        elif "TP2" in comment_upper or "TP 2" in comment_upper:
            action = "TP_HIT"
            tp_level = 2
        elif "TP1" in comment_upper or "TP 1" in comment_upper:
            action = "TP_HIT"
            tp_level = 1
        elif "RUNNER" in comment_upper:
            action = "EXIT"
            reason = "Runner Exit"
        elif "STOP" in comment_upper:
            action = "EXIT"
            reason = "Hard Stop Hit"
        elif "SESSION" in comment_upper or "CLOSE ALL" in comment_upper:
            action = "SESSION_CLOSE"
        elif "EXIT" in comment_upper or "CLOSE" in comment_upper:
            action = "EXIT"
            reason = comment or "TradingView exit"
        elif (comment_upper.startswith("TP") or "RATCHET" in comment_upper
              or "LOCK" in comment_upper or "TRAIL" in comment_upper):
            # 2026-10-07 (codex review): MNQ SMC v2 exit comments that carry
            # no TPn/STOP/EXIT keyword — "TP +120 pips", "Sig Ratchet",
            # "Deep Lock", "EQ Trail". Previously these fell to the unknown
            # fallback and, if paper happened to be flat, opened a REVERSE
            # position. Always an exit.
            action = "EXIT"
            reason = comment
        elif ("LONG" in comment_upper or "SHORT" in comment_upper
              or "ADD" in comment_upper):
            # 2026-10-07: MNQ SMC v2 masks entry comments as "Profiteer Long" /
            # "Profiteer Short" and pyramids add-ons with the same comment.
            # Without this branch an add-on fill while in a position fell to
            # the unknown-comment fallback and was treated as an EXIT. Treat
            # it as an ENTRY; handle_entry rejects it as a duplicate when a
            # position is already open.
            action = "ENTRY"
            if "SHORT" in comment_upper:
                direction = "SHORT"
            elif "LONG" in comment_upper:
                direction = "LONG"
            else:
                direction = "LONG" if is_buy else "SHORT"
        else:
            # Unknown comment — REJECT. 2026-10-07 (codex review): the old
            # state-dependent guess (entry if flat, exit if in position) could
            # open a reverse position from a late exit alert or flatten the
            # broker from a duplicate entry. The dispatcher returns an error
            # for action "UNKNOWN". Note: MNQ SMC v2 with mask_comments=OFF
            # emits raw entry tags like "iCHoCH+iOB d0 r75 m50" which land
            # here and are dropped — keep mask_comments=ON in the strategy.
            logger.warning(
                "[WEBHOOK] Unknown comment '%s' with action '%s' — REJECTED "
                "(no state-dependent guessing)",
                comment, raw_action,
            )
            action = "UNKNOWN"
            reason = comment or "Unknown signal"

        logger.info(
            "[WEBHOOK] Parsed TV signal: action=%s dir=%s tp=%s reason=%s "
            "(raw: action=%s comment='%s')",
            action, direction, tp_level, reason, raw_action, comment,
        )

        return WebhookSignal(
            action=action,
            direction=direction,
            price=price,
            contracts=contracts,
            tp_level=tp_level,
            reason=reason,
            raw=payload,
        )

    def execute(self, signal: WebhookSignal) -> Dict:
        """Execute a parsed signal. Returns result dict.

        Thread-safe entrypoint. FastAPI runs sync handlers in a threadpool, so
        two simultaneous TradingView alerts can enter this method concurrently.
        We serialize the body under self._exec_lock so self.position and
        self.daily_pnl mutations are never raced.

        执行解析后的信号：
        1. 每日重置检查
        2. 根据信号类型分发到对应处理器
        3. 返回执行结果
        """
        with self._exec_lock:
            return self._run_locked(signal)

    def _run_locked(self, signal: WebhookSignal) -> Dict:
        """Serialized body of execute() — never call from outside this class."""
        now = datetime.now(PT)
        self._reset_daily_if_needed(now)

        # Broker-truth sync — reconcile self.position to actual Webull
        # position BEFORE dispatching to handlers. Addresses the class of
        # bugs where paper state drifts (missed entry webhook, manual
        # broker intervention, prior reconciler failure) and handlers
        # reject or double-execute based on fiction. See
        # _sync_position_from_broker() docstring for rationale.
        self._sync_position_from_broker(signal.price)

        # Price comparison: fetch Yahoo in background, don't block the trade
        if signal.price > 0:
            threading.Thread(
                target=self._record_price_comparison,
                args=(signal.price, signal.action, now),
                daemon=True,
            ).start()

        result = {"status": "error", "message": "Unknown action"}

        if signal.action == "ENTRY":
            result = self._handle_entry(signal, now)
        elif signal.action == "TP_HIT":
            result = self._handle_tp_hit(signal, now)
        elif signal.action == "EXIT":
            result = self._handle_exit(signal, now)
        elif signal.action == "SESSION_CLOSE":
            result = self._handle_session_close(signal, now)
        else:
            result = {"status": "error", "message": f"Unknown action: {signal.action}"}

        self._save_state()
        # Sync to Gist in background — don't block TradingView's 5s timeout
        threading.Thread(target=self._sync_state_to_gist, daemon=True).start()
        return result

    def _handle_entry(self, signal: WebhookSignal, now: datetime) -> Dict:
        """Handle ENTRY signal from TradingView."""

        # Payload validation — fail fast on malformed signals. Without these
        # checks a paper-only run would record garbage state (e.g. entry=0 →
        # phantom P&L of $48,000 on the first tick move).
        if signal.direction not in ("LONG", "SHORT"):
            msg = f"Invalid/missing direction: {signal.direction!r}"
            logger.warning("[WEBHOOK] %s — rejecting entry", msg)
            return {"status": "rejected", "message": msg}
        if signal.price <= 0:
            msg = f"Invalid entry price: {signal.price}"
            logger.warning("[WEBHOOK] %s — rejecting entry", msg)
            return {"status": "rejected", "message": msg}

        # 2026-10-14 (DNS boot race): live trading was requested but no broker
        # is attached (init crashed / credentials missing). Refuse the entry
        # outright — a paper fill here looks healthy in /status while real
        # signals go nowhere (10-14 incident: boot-time DNS failure left the
        # service paper-filling for 8 hours with TRADOVATE_LIVE=1).
        if self.live_required and self.broker is None:
            msg = ("Live trading requested but no broker attached — "
                   "REFUSING entry (fix broker init and restart)")
            logger.error("[WEBHOOK] %s", msg)
            self._notify(f"🛑 ENTRY REFUSED: {msg}")
            return {"status": "rejected", "message": msg}

        # Dedup guard — TradingView fires the same alert 2-4x at ~5s intervals
        if self._last_entry_attempt:
            elapsed = (now - self._last_entry_attempt).total_seconds()
            if elapsed < self._entry_dedup_seconds:
                msg = f"Duplicate entry rejected ({elapsed:.0f}s since last attempt)"
                logger.warning("[WEBHOOK] %s", msg)
                return {"status": "rejected", "message": msg}
        self._last_entry_attempt = now

        # Risk checks
        if self._circuit_breaker_fired:
            msg = f"Circuit breaker active (daily PnL ${self.daily_pnl:.2f})"
            logger.warning("[WEBHOOK] %s — rejecting entry", msg)
            return {"status": "rejected", "message": msg}

        if self.daily_pnl <= self.config.daily_loss_limit:
            self._circuit_breaker_fired = True
            msg = f"Daily loss limit hit: ${self.daily_pnl:.2f}"
            logger.warning("[WEBHOOK] %s", msg)
            return {"status": "rejected", "message": msg}

        # 2026-10-14: broker-truth breaker — paper daily_pnl misses losses
        # booked outside this process (10-12: broker auto-liquidation closed
        # the short, paper never saw -$124.76, breaker never tripped). Live
        # equity delta since day start catches those. None/unavailable -> skip.
        if (self._day_start_equity is not None
                and self.broker is not None
                and hasattr(self.broker, "get_live_equity")):
            try:
                eq_now = self.broker.get_live_equity()
                if eq_now is not None and float(eq_now) > 0:
                    live_day_pnl = float(eq_now) - self._day_start_equity
                    if live_day_pnl <= self.config.daily_loss_limit:
                        self._circuit_breaker_fired = True
                        msg = (f"Daily loss limit hit on BROKER equity: "
                               f"${live_day_pnl:.2f} (day start "
                               f"${self._day_start_equity:.2f} -> ${float(eq_now):.2f})")
                        logger.warning("[WEBHOOK] %s", msg)
                        self._notify(f"🛑 Circuit breaker (broker equity): {msg}")
                        return {"status": "rejected", "message": msg}
            except Exception as exc:
                logger.warning("[WEBHOOK] Broker-equity breaker check failed: %s", exc)

        if self._cooldown_until and now < self._cooldown_until:
            msg = f"Cooldown until {self._cooldown_until.strftime('%H:%M:%S')}"
            logger.info("[WEBHOOK] %s — rejecting entry", msg)
            return {"status": "rejected", "message": msg}

        if self.position and self.position.remaining_contracts > 0:
            msg = "Already in position — rejecting duplicate entry"
            logger.warning("[WEBHOOK] %s", msg)
            return {"status": "rejected", "message": msg}

        # 2026-10-07 (codex review #2): per-alert broker sync is disabled for
        # Tradovate (no fill-latency grace), so paper can be flat while the
        # broker still holds an orphaned position. Never stack a new entry
        # on top of that — ask the broker first and refuse if it is not flat.
        # A None / exception answer keeps the old behaviour (proceed) so a
        # transient query failure does not block legitimate trades.
        if (self.broker is not None
                and (getattr(self.broker, "live_orders", False)
                     or getattr(self.broker, "_live_orders", False))
                and hasattr(self.broker, "get_position_qty")):
            try:
                broker_qty = self.broker.get_position_qty()
            except Exception as exc:
                logger.warning(
                    "[WEBHOOK] Pre-entry broker check raised: %s — proceeding", exc,
                )
                broker_qty = None
            if broker_qty:
                msg = (
                    f"Broker already holds {broker_qty:+d} contracts while paper "
                    f"is flat — REFUSING entry (manual review: flatten or "
                    f"restart to reconcile)"
                )
                logger.error("[WEBHOOK] %s", msg)
                return {"status": "rejected", "message": msg}

        # Plan A contract cap — TV may send "huge" numbers because Pine
        # scales off ``strategy.equity`` (TradingView's simulated backtest
        # equity, which compounds through historical trades) rather than
        # real account size. Before this cap, a strategy with $45K+ of
        # simulated P&L was sending 30-contract alerts into a $148 real
        # account — either rejected at Webull margin or, worse,
        # blowing up a funded account.
        #
        # The cap mirrors Pine's intent exactly so scale-up still works
        # (see ``compute_contract_cap`` docstring for the table), but
        # anchors on REAL equity from the broker, not Pine's fiction:
        #
        #     scale_mult = 1 + floor(max(0, (live_eq - init_eq) / step))
        #     max_contracts = base_contracts × scale_mult
        #     contracts = min(tv_contracts, max_contracts)
        tv_contracts = signal.contracts
        if tv_contracts < 1:
            logger.warning(
                "[WEBHOOK] Rejecting entry: TV sent contracts=%d (must be >=1)",
                tv_contracts,
            )
            return {"status": "rejected", "message": "Zero contracts from TV"}

        live_equity = self._get_live_equity()
        contracts, max_contracts, scale_mult = compute_contract_cap(
            tv_contracts=tv_contracts,
            live_equity=live_equity,
            initial_equity=self._initial_equity,
            base_contracts=self.config.base_contracts,
            scale_per_profit=self.config.scale_per_profit,
        )
        if contracts < tv_contracts:
            logger.warning(
                "[WEBHOOK] CAP FIRED: tv=%d → sending=%d "
                "(live_equity=$%.2f, initial=$%.2f, scale_mult=%d, max=%d)",
                tv_contracts, contracts, live_equity, self._initial_equity,
                scale_mult, max_contracts,
            )
        else:
            logger.info(
                "[WEBHOOK] Sizing: tv=%d within cap=%d "
                "(live_equity=$%.2f, scale_mult=%d)",
                tv_contracts, max_contracts, live_equity, scale_mult,
            )

        # Crash-safety STEP 1: pre-write position with PENDING_OPEN intent
        # and persist BEFORE calling broker. If the process crashes between
        # broker.place_order() and disk commit, startup reconcile sees
        # PENDING_OPEN + broker has position -> upgrades to FILLED. Without
        # this, a crash during _verify_fill loses the trade from paper state
        # even though Webull filled (MGC TP3 incident, 2026-04-17).
        broker_order_id: Optional[str] = None
        actual_entry_price = signal.price       # default to TV price
        fill_status = "FILLED"                  # default for paper-only mode
        self.position = WebhookPosition(
            direction=signal.direction,
            entry_price=signal.price,           # estimate; upgraded after fill
            entry_time=now,
            total_contracts=contracts,
            remaining_contracts=contracts,
            tv_total_contracts=tv_contracts,
            tv_contracts_closed=0,
        )
        self.position.broker_order_id = None
        self.position.fill_status = "PENDING_OPEN" if self.broker is not None else "FILLED"
        self._save_state()

        if self.broker is not None:
            broker_side = "BUY" if signal.direction == "LONG" else "SELL"
            broker_result = self.broker.place_order(
                side=broker_side,
                quantity=contracts,
                limit_price=signal.price,
                comment=f"{signal.direction} Entry",
            )
            if not broker_result.get("success"):
                err = broker_result.get("error", "unknown")
                logger.error("[WEBHOOK] Broker REJECTED entry: %s -- clearing PENDING_OPEN paper", err)
                self._notify(f"🛑 Broker REJECTED entry: {err}")
                self.position = None
                self._save_state()
                return {
                    "status": "error",
                    "message": f"Broker rejected entry: {err}",
                }
            broker_order_id = broker_result.get("order_id")
            logger.info(
                "[WEBHOOK] Broker accepted entry: %s (dry_run=%s)",
                broker_order_id, broker_result.get("dry_run", False),
            )

            # Verify Webull's actual fill price BEFORE upgrading position state.
            actual_entry_price, fill_status = self._verify_fill(
                broker_order_id, signal.price, context=f"ENTRY/{signal.direction}",
            )
            # CANCELLED/REJECTED -- clear PENDING_OPEN paper to match Webull reality.
            if fill_status in ("CANCELLED", "REJECTED"):
                logger.error(
                    "[WEBHOOK] Entry order %s on Webull (id=%s) -- clearing PENDING_OPEN paper",
                    fill_status, broker_order_id,
                )
                self.position = None
                self._save_state()
                return {
                    "status": "error",
                    "message": f"Entry order {fill_status} on Webull",
                    "order_id": broker_order_id,
                }
            # SUBMITTED (timed out) or UNKNOWN: upgrade paper with TV estimate;
            # background reconciler corrects entry_price when the fill resolves.

        # Paper-side order id -- kept for logs + dashboard even with live broker.
        self._order_counter = getattr(self, "_order_counter", 0) + 1
        order_id = broker_order_id or f"TV-{self._order_counter:06d}"

        # STEP 4: upgrade PENDING_OPEN pre-write with ACTUAL fill details.
        self.position.entry_price = actual_entry_price
        self.position.broker_order_id = broker_order_id
        self.position.fill_status = fill_status

        logger.info(
            "[WEBHOOK] ENTRY %s %d ct @ $%.2f (fill_status=%s, tv=%.2f) — order %s",
            signal.direction, contracts, actual_entry_price, fill_status,
            signal.price, order_id,
        )

        # Discord alert (background — don't block TradingView response)
        if self.alerts:
            threading.Thread(
                target=self.alerts.on_entry,
                kwargs=dict(
                    direction=signal.direction,
                    symbol=self.config.symbol,
                    price=signal.price,
                    contracts=contracts,
                    scale_mult=contracts // self.config.base_contracts,
                    hard_stop_pips=self.config.hard_stop_pips,
                    tp_pips=[tp[0] for tp in self.config.tp_levels],
                    tp_contracts=[tp[1] for tp in self.config.tp_levels],
                    runner_qty=self.config.runner_contracts,
                    bias_summary="TradingView signal",
                    rsi=0,
                ),
                daemon=True,
            ).start()

        return {
            "status": "ok",
            "message": f"{signal.direction} {contracts}ct @ ${signal.price:.2f}",
            "order_id": order_id,
        }

    def _handle_tp_hit(self, signal: WebhookSignal, now: datetime) -> Dict:
        """Handle TP_HIT signal — partial close."""
        if not self.position or self.position.remaining_contracts <= 0:
            logger.info(
                "[WEBHOOK] Rejected TP%s @ $%.2f — no position to close "
                "(paper flat after broker sync)",
                signal.tp_level, signal.price,
            )
            return {"status": "rejected", "message": "No position to close"}

        # Payload validation — reject bad prices so we never fire a broker
        # order at $0 (which would open a new max-short on a netting account).
        if signal.price <= 0:
            msg = f"Invalid TP price: {signal.price}"
            logger.warning("[WEBHOOK] %s — rejecting TP hit", msg)
            return {"status": "rejected", "message": msg}

        # TP dedup — TradingView repeats the same TP alert 2-4x at ~5s
        # intervals. Without this guard, a 1-contract TP1 would close up to
        # 4 contracts and potentially flip the position on Webull.
        if signal.tp_level and signal.tp_level in self.position.tp_hits:
            msg = f"TP{signal.tp_level} already hit — rejecting duplicate"
            logger.warning("[WEBHOOK] %s", msg)
            return {"status": "rejected", "message": msg}

        # Reject malformed signals BEFORE touching scaling state. A 0/negative
        # signal_contracts shouldn't bump tv_contracts_closed or mark TPs as
        # hit (which would block legitimate retries via dedup).
        if signal.contracts <= 0:
            msg = f"Invalid contracts in signal: {signal.contracts}"
            logger.warning("[WEBHOOK] %s -- rejecting TP hit", msg)
            return {"status": "rejected", "message": msg}

        # Scale TV's signal qty by entry-cap ratio. CRITICAL: TV says "TP1
        # = 3 of 18" but our actual position is 6 (capped). Without scaling
        # we'd close 3/6 = 50%, not 1/6 = 1. See _scaled_exit_qty docstring.
        contracts = self._scaled_exit_qty(signal.contracts)
        contracts = min(contracts, self.position.remaining_contracts)
        if contracts < 1:
            # Scaled to zero -- TV's TP-tier is too small relative to our cap.
            # Mark TP as hit anyway so future TP dedup works, but skip broker.
            if signal.tp_level:
                self.position.tp_hits.append(signal.tp_level)
            return {
                "status": "skipped",
                "message": (
                    f"TP{signal.tp_level}: scaled qty=0 "
                    f"(TV={signal.contracts}/{self.position.tv_total_contracts}, "
                    f"actual_total={self.position.total_contracts}) -- no broker order"
                ),
            }

        # Fire offsetting broker order BEFORE mutating paper state.
        # Long position closes via SELL, short position closes via BUY.
        #
        # CRITICAL: place_order returns success when Webull *accepts* the
        # order — NOT when it fills. A SELL LIMIT placed above market sits
        # on the book unfilled. Without verification we'd write the trade
        # log with TV's TP price as the exit, even though no contracts
        # actually changed hands. That's how we logged a fictional +$1,380
        # TP3 win on 2026-04-17 while Webull actually took -$197 from a
        # later stop-loss fill. See verify_fill below.
        broker_order_id: Optional[str] = None
        actual_exit_price = signal.price       # default to TV price
        fill_status = "FILLED"                 # default for paper-only mode
        prev_fill_status = self.position.fill_status
        if self.broker is not None:
            # Crash-safety: mark PENDING_CLOSE before broker call so startup
            # reconcile can detect "close fired but paper never committed".
            self.position.fill_status = "PENDING_CLOSE"
            self._save_state()
            broker_side = "SELL" if self.position.direction == "LONG" else "BUY"
            broker_result = self.broker.place_order(
                side=broker_side,
                quantity=contracts,
                limit_price=signal.price,
                comment=f"TP{signal.tp_level or '?'} Close",
            )
            if not broker_result.get("success"):
                err = broker_result.get("error", "unknown")
                logger.error(
                    "[WEBHOOK] Broker REJECTED TP%s close: %s -- reverting paper fill_status",
                    signal.tp_level, err,
                )
                self.position.fill_status = prev_fill_status
                self._save_state()
                return {
                    "status": "error",
                    "message": f"Broker rejected TP close: {err}",
                }
            broker_order_id = broker_result.get("order_id")

            # === Verify fill before recording trade ===
            actual_exit_price, fill_status = self._verify_fill(
                broker_order_id, signal.price,
                context=f"TP{signal.tp_level or '?'}",
            )
            # CANCELLED/REJECTED -- revert PENDING_CLOSE, surface error.
            if fill_status in ("CANCELLED", "REJECTED"):
                logger.error(
                    "[WEBHOOK] TP%s order %s on Webull (id=%s) -- reverting paper fill_status",
                    signal.tp_level, fill_status, broker_order_id,
                )
                self.position.fill_status = prev_fill_status
                self._save_state()
                return {
                    "status": "error",
                    "message": f"TP{signal.tp_level} order {fill_status} on Webull",
                    "order_id": broker_order_id,
                }
            # Close accepted -- upgrade fill_status back to real state.
            self.position.fill_status = fill_status
            # SUBMITTED (timed out) or UNKNOWN — flag as pending. We still
            # update paper state because TV's strategy *thinks* it filled,
            # but the background reconciler will fix the trade log when
            # the real fill resolves.

        self._order_counter = getattr(self, "_order_counter", 0) + 1
        order_id = broker_order_id or f"TV-{self._order_counter:06d}"

        self.position.remaining_contracts -= contracts
        if signal.tp_level:
            self.position.tp_hits.append(signal.tp_level)

        # Calculate P&L using ACTUAL fill price from Webull (or TV price for paper)
        pip_val = self.config.pip_value
        if self.position.direction == "LONG":
            pips = (actual_exit_price - self.position.entry_price) / pip_val
        else:
            pips = (self.position.entry_price - actual_exit_price) / pip_val
        partial_pnl = pips * contracts
        self.position.realized_pnl += partial_pnl

        logger.info(
            "[WEBHOOK] TP%s: closed %d ct @ $%.2f (fill_status=%s, tv=%.2f) — PnL $%.0f — remaining %d ct",
            signal.tp_level, contracts, actual_exit_price, fill_status, signal.price,
            partial_pnl, self.position.remaining_contracts,
        )

        # Check if position fully closed
        if self.position.remaining_contracts <= 0:
            self._on_position_closed(now, exit_price=actual_exit_price,
                                     exit_reason=f"TP{signal.tp_level}",
                                     fill_status=fill_status,
                                     broker_order_id=broker_order_id)

        return {
            "status": "ok",
            "message": f"TP{signal.tp_level}: {contracts}ct closed @ ${actual_exit_price:.2f} ({fill_status})",
            "order_id": order_id,
            "fill_status": fill_status,
        }

    def _handle_exit(self, signal: WebhookSignal, now: datetime) -> Dict:
        """Handle EXIT signal — close all remaining."""
        if not self.position or self.position.remaining_contracts <= 0:
            logger.info(
                "[WEBHOOK] Rejected EXIT (%s) @ $%.2f — no position to close "
                "(paper flat after broker sync)",
                signal.reason or "unspecified", signal.price,
            )
            return {"status": "rejected", "message": "No position to close"}

        # Payload validation — same reason as _handle_tp_hit: never fire a
        # broker close order at $0.
        if signal.price <= 0:
            msg = f"Invalid exit price: {signal.price}"
            logger.warning("[WEBHOOK] %s — rejecting exit", msg)
            return {"status": "rejected", "message": msg}

        reason = signal.reason or "TradingView exit"
        # _handle_exit closes ALL remaining (not a TP-tier partial), so we
        # don't scale via _scaled_exit_qty here — just close everything left
        # in OUR position. But mark TV-side as fully closed so any later
        # straggling alert with non-zero qty doesn't double-account.
        contracts = self.position.remaining_contracts
        if self.position.tv_total_contracts > 0:
            self.position.tv_contracts_closed = self.position.tv_total_contracts

        # Fire offsetting broker order BEFORE mutating paper state.
        # Same fill-verification logic as _handle_tp_hit (see comment there).
        broker_order_id: Optional[str] = None
        actual_exit_price = signal.price
        fill_status = "FILLED"
        prev_fill_status = self.position.fill_status
        if self.broker is not None:
            # Crash-safety: mark PENDING_CLOSE before broker call (same
            # rationale as _handle_tp_hit).
            self.position.fill_status = "PENDING_CLOSE"
            self._save_state()
            broker_side = "SELL" if self.position.direction == "LONG" else "BUY"
            broker_result = self.broker.place_order(
                side=broker_side,
                quantity=contracts,
                limit_price=signal.price,
                comment=f"EXIT: {reason}",
            )
            if not broker_result.get("success"):
                err = broker_result.get("error", "unknown")
                logger.error(
                    "[WEBHOOK] Broker REJECTED exit (%s): %s -- reverting paper fill_status",
                    reason, err,
                )
                self.position.fill_status = prev_fill_status
                self._save_state()
                return {
                    "status": "error",
                    "message": f"Broker rejected exit: {err}",
                }
            broker_order_id = broker_result.get("order_id")

            # Verify fill before recording trade
            actual_exit_price, fill_status = self._verify_fill(
                broker_order_id, signal.price, context=f"EXIT/{reason}",
            )
            if fill_status in ("CANCELLED", "REJECTED"):
                logger.error(
                    "[WEBHOOK] EXIT order %s on Webull (id=%s) -- reverting paper fill_status",
                    fill_status, broker_order_id,
                )
                self.position.fill_status = prev_fill_status
                self._save_state()
                return {
                    "status": "error",
                    "message": f"EXIT order {fill_status} on Webull",
                    "order_id": broker_order_id,
                }

        self._order_counter = getattr(self, "_order_counter", 0) + 1
        order_id = broker_order_id or f"TV-{self._order_counter:06d}"

        # Calculate P&L using ACTUAL fill price
        pip_val = self.config.pip_value
        if self.position.direction == "LONG":
            pips = (actual_exit_price - self.position.entry_price) / pip_val
        else:
            pips = (self.position.entry_price - actual_exit_price) / pip_val
        exit_pnl = pips * contracts
        self.position.realized_pnl += exit_pnl
        self.position.remaining_contracts = 0

        logger.info(
            "[WEBHOOK] EXIT: %s — %d ct @ $%.2f (fill_status=%s, tv=%.2f) — PnL $%.0f",
            reason, contracts, actual_exit_price, fill_status, signal.price, exit_pnl,
        )

        self._on_position_closed(now, exit_price=actual_exit_price,
                                 exit_reason=reason,
                                 fill_status=fill_status,
                                 broker_order_id=broker_order_id)

        return {
            "status": "ok",
            "message": f"Closed {contracts}ct @ ${actual_exit_price:.2f} — {reason} ({fill_status})",
            "order_id": order_id,
            "fill_status": fill_status,
        }

    def _handle_session_close(self, signal: WebhookSignal, now: datetime) -> Dict:
        """Handle SESSION_CLOSE — close everything."""
        if not self.position or self.position.remaining_contracts <= 0:
            return {"status": "ok", "message": "No position — session close ignored"}

        signal.reason = "Session Close (TradingView)"
        return self._handle_exit(signal, now)

    def _scaled_exit_qty(self, signal_contracts: int) -> int:
        """Scale a TV exit-signal contract count down to our actual position.

        Pine sends exit qty as a fraction of TV's TOTAL position (e.g. TP1 = 3
        contracts when TV's total is 18). When compute_contract_cap clipped
        the entry from 18 -> 6 (because real account is small), TV's "3" is
        no longer 1/6 of our actual position — it's 50%! Scaling fixes that.

        Cumulative tracking (target_actual = round(tv_closed * ratio)) avoids
        rounding drift across a multi-leg sequence:

            TV plan:  TP1=3, TP2=6, TP3=6, Runner=3   (sum 18)
            Ratio:    actual=6 / tv=18 = 1/3
            Per-leg:  TP1: 3*1/3=1.0  -> close 1
                      TP2: prev=1, target=round(9*1/3)=3  -> close 3-1=2
                      TP3: prev=3, target=round(15*1/3)=5 -> close 5-3=2
                      Run: prev=5, target=round(18*1/3)=6 -> close 6-5=1
            Sums:     1+2+2+1 = 6  (matches actual entry)

        If no cap fired (tv_total == actual_total), returns signal_contracts
        unchanged.

        Returns 0 if scaled qty rounds to 0 (caller decides what to do).
        """
        if not self.position:
            return 0
        tv_total = self.position.tv_total_contracts
        actual_total = self.position.total_contracts
        if tv_total <= 0 or actual_total <= 0 or tv_total == actual_total:
            # No cap was applied (or position from older code without tracking).
            # Pass through. Cap on remaining_contracts happens at call site.
            return signal_contracts

        ratio = actual_total / tv_total
        new_tv_closed = self.position.tv_contracts_closed + signal_contracts
        target_actual_closed = round(new_tv_closed * ratio)
        prev_actual_closed = round(self.position.tv_contracts_closed * ratio)
        this_close = target_actual_closed - prev_actual_closed
        # Belt-and-suspenders: never exceed remaining
        this_close = min(this_close, self.position.remaining_contracts)
        # Update TV-side counter so next exit's math works
        self.position.tv_contracts_closed = new_tv_closed
        if this_close != signal_contracts:
            logger.info(
                "[WEBHOOK] Scaled exit qty: TV asked %d (of %d), "
                "actual %d (of %d), ratio=%.3f -> closing %d",
                signal_contracts, tv_total, actual_total, actual_total,
                ratio, this_close,
            )
        return max(0, this_close)

    def _verify_fill(
        self,
        broker_order_id: Optional[str],
        tv_price: float,
        context: str = "",
        timeout_seconds: float = 5.0,
    ) -> "tuple[float, str]":
        """Wait for the broker order to fill, return (actual_price, status).

        Returns:
            actual_price: avg fill price from Webull if FILLED, else tv_price.
            status: "FILLED" / "SUBMITTED" / "CANCELLED" / "REJECTED" / "UNKNOWN".

        For paper-only mode (no broker), returns (tv_price, "FILLED") so
        existing paper-only behavior is unchanged.
        """
        if self.broker is None or not broker_order_id:
            return (tv_price, "FILLED")

        # broker.wait_for_fill handles dry-run too (returns synthetic FILLED)
        if not hasattr(self.broker, "wait_for_fill"):
            # Older brokers without verification support — fall back to TV price.
            # This keeps the change backwards-compatible with PaperGoldBroker.
            return (tv_price, "FILLED")

        result = self.broker.wait_for_fill(
            broker_order_id, timeout_seconds=timeout_seconds,
        )
        status = result.get("status", "UNKNOWN")
        filled_price = result.get("filled_price", 0.0) or 0.0

        # Dry-run broker — wait_for_fill returns synthetic FILLED with
        # filled_price=0. Don't log/return a fake $0 price; just pass
        # through TV's price as the "fill".
        if result.get("dry_run"):
            return (tv_price, "FILLED")

        if status == "FILLED" and filled_price > 0:
            logger.info(
                "[WEBHOOK] %s verified FILLED on Webull: id=%s, "
                "tv_price=$%.2f, actual=$%.2f, slippage=$%.2f",
                context, broker_order_id, tv_price, filled_price,
                filled_price - tv_price,
            )
            return (filled_price, status)

        if status == "SUBMITTED":
            logger.warning(
                "[WEBHOOK] %s order STILL SUBMITTED after %.1fs on Webull "
                "(id=%s) — recording trade with TV price as estimate; "
                "background reconciler should resolve",
                context, timeout_seconds, broker_order_id,
            )
            return (tv_price, status)

        # CANCELLED / REJECTED / UNKNOWN — caller decides; we just report.
        logger.error(
            "[WEBHOOK] %s order on Webull is %s (id=%s)",
            context, status, broker_order_id,
        )
        return (tv_price, status)

    def _on_position_closed(self, now: datetime, exit_price: float = 0,
                            exit_reason: str = "",
                            fill_status: str = "FILLED",
                            broker_order_id: Optional[str] = None) -> None:
        """Update stats after position fully closed."""
        if not self.position:
            return

        total_pnl = self.position.realized_pnl
        self.daily_pnl += total_pnl
        self.daily_trades += 1
        self._equity += total_pnl

        if total_pnl >= 0:
            self.daily_wins += 1
        else:
            self.daily_losses += 1

        duration = (now - self.position.entry_time).total_seconds() / 60.0

        logger.info(
            "[WEBHOOK] Position closed: %s — PnL $%.2f — Duration %.0f min — "
            "Daily: $%.2f (%dW/%dL) — Equity: $%.2f — fill=%s",
            self.position.direction, total_pnl, duration,
            self.daily_pnl, self.daily_wins, self.daily_losses,
            self._equity, fill_status,
        )

        # Append to trade log. Include fill_status + broker_order_id so the
        # reconciler can find PENDING entries later, and so the dashboard can
        # mark unverified rows. Existing rows without these keys default to
        # FILLED on read.
        self._trade_log.append({
            "time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": self.config.symbol.replace("=F", ""),
            "direction": self.position.direction,
            "contracts": self.position.total_contracts,
            "entry": self.position.entry_price,
            "exit": exit_price or self.position.entry_price,
            "pnl": round(total_pnl, 2),
            "reason": exit_reason or f"TPs hit: {self.position.tp_hits}",
            "fill_status": fill_status,
            "broker_order_id": broker_order_id,
        })

        # Send trade summary alert (background — don't block TradingView response)
        if self.alerts:
            threading.Thread(
                target=self.alerts.on_trade_summary,
                kwargs=dict(
                    direction=self.position.direction,
                    entry_price=self.position.entry_price,
                    exit_price=exit_price,
                    total_pnl=total_pnl,
                    duration_minutes=duration,
                    exit_reason=exit_reason or f"TPs hit: {self.position.tp_hits}",
                    tp_log=[],
                    equity=self._equity,
                    daily_wins=self.daily_wins,
                    daily_losses=self.daily_losses,
                    daily_trades=self.daily_trades,
                ),
                daemon=True,
            ).start()

        # Cooldown
        if total_pnl <= 0:
            self._cooldown_until = now + timedelta(minutes=10)
            logger.info("[WEBHOOK] Cooldown 10min after loss (until %s)",
                        self._cooldown_until.strftime("%H:%M:%S"))
        else:
            self._cooldown_until = now + timedelta(minutes=2)

        self.position = None

    def _record_price_comparison(
        self, tv_price: float, action: str, now: datetime,
    ) -> None:
        """Fetch Yahoo price and record comparison (runs in background thread)."""
        yahoo_price = _fetch_yahoo_price(self.config.symbol)
        if yahoo_price is None:
            return

        # Sanity check: reject if TV price diverges >5% from Yahoo
        if yahoo_price > 0 and abs(tv_price - yahoo_price) / yahoo_price > 0.05:
            logger.warning(
                "[PRICE_CMP] REJECTED — TV=$%.2f vs Yahoo=$%.2f (>5%% divergence) [%s]",
                tv_price, yahoo_price, action,
            )
            return

        diff = tv_price - yahoo_price
        diff_pips = diff / self.config.pip_value

        entry = {
            "time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "action": action,
            "tv_price": round(tv_price, 2),
            "yahoo_price": round(yahoo_price, 2),
            "diff": round(diff, 2),
            "diff_pips": round(diff_pips, 1),
        }

        self._price_comparisons.append(entry)
        # Keep last 500
        if len(self._price_comparisons) > 500:
            self._price_comparisons = self._price_comparisons[-500:]

        logger.info(
            "[PRICE_CMP] TV=$%.2f  Yahoo=$%.2f  diff=$%.2f (%+.0f pips)  [%s]",
            tv_price, yahoo_price, diff, diff_pips, action,
        )

        # Save locally
        try:
            tmp = self._price_cmp_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self._price_comparisons, f, indent=2)
            _atomic_replace_with_retry(tmp, self._price_cmp_file)
        except OSError as e:
            logger.debug("[PRICE_CMP] Save failed: %s", e)

        # Sync to Gist
        self._sync_price_comparison_to_gist()

    def _sync_price_comparison_to_gist(self) -> None:
        """Upload price comparison data to Gist for dashboard."""
        gist_id, gh_token = self._get_gist_creds()
        if not gist_id or not gh_token:
            return
        try:
            import requests
            requests.patch(
                f"https://api.github.com/gists/{gist_id}",
                headers={
                    "Authorization": f"token {gh_token}",
                    "Accept": "application/vnd.github.v3+json",
                },
                json={"files": {
                    "price_comparison.json": {
                        "content": json.dumps(
                            self._price_comparisons[-100:], indent=2
                        ),
                    },
                }},
                timeout=10,
            )
        except Exception as exc:
            logger.debug("[PRICE_CMP] Gist sync error: %s", exc)

    def _get_gist_creds(self) -> tuple:
        """Get Gist ID and GitHub token from env."""
        gist_id = os.environ.get("KRAKEN_STATE_GIST_ID", "")
        gh_token = os.environ.get("GITHUB_TOKEN", "")
        if not gist_id or not gh_token:
            env_file = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__)))),
                "secrets", ".env",
            )
            if os.path.exists(env_file):
                with open(env_file) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, _, v = line.partition("=")
                            v = v.strip().strip('"').strip("'")
                            if k.strip() == "KRAKEN_STATE_GIST_ID" and not gist_id:
                                gist_id = v
                            elif k.strip() == "GITHUB_TOKEN" and not gh_token:
                                gh_token = v
        return gist_id, gh_token

    def _reset_daily_if_needed(self, now: datetime) -> None:
        """Reset daily stats on new calendar day."""
        day = now.date().toordinal()
        if self._last_day != day:
            if self._last_day is not None:
                logger.info(
                    "[WEBHOOK] New day — yesterday: PnL=$%.2f, W/L=%d/%d",
                    self.daily_pnl, self.daily_wins, self.daily_losses,
                )
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.daily_wins = 0
            self.daily_losses = 0
            self._circuit_breaker_fired = False
            self._last_day = day
            # 2026-10-14: snapshot real broker equity at day start so the
            # breaker can compare against broker truth, not just paper.
            self._day_start_equity = None
            if (self.broker is not None
                    and hasattr(self.broker, "get_live_equity")):
                try:
                    try:
                        eq = self.broker.get_live_equity(force_refresh=True)
                    except TypeError:
                        eq = self.broker.get_live_equity()
                    if eq is not None and float(eq) > 0:
                        self._day_start_equity = float(eq)
                        logger.info(
                            "[WEBHOOK] Day-start broker equity: $%.2f",
                            self._day_start_equity,
                        )
                except Exception as exc:
                    logger.warning(
                        "[WEBHOOK] Day-start equity fetch failed: %s", exc)

    # ------------------------------------------------------------------
    # 2026-10-14 operational additions: Slack notify
    # ------------------------------------------------------------------
    def _notify(self, text: str) -> None:
        """Operational Slack alert; never raises into trading code."""
        if self.alerts is not None and hasattr(self.alerts, "notify"):
            try:
                self.alerts.notify(text)
            except Exception as exc:
                logger.warning("[WEBHOOK] notify failed: %s", exc)

    def force_flatten(self, reason: str) -> None:
        """Clear paper position and persist — no broker call, no PnL accounting.

        Intended for the reconciler's drift-correction path when Webull reports
        flat but paper still holds a phantom position. We intentionally do NOT
        book PnL here because the matching broker close already happened (or
        never existed), and our paper's entry_price may be stale.
        """
        if self.position is None:
            return
        # Skip for PENDING_OPEN paper -- broker order is still in-flight.
        # Killing paper here races with the entry handler's commit/clear.
        if self.position.fill_status == "PENDING_OPEN":
            logger.info(
                "[WEBHOOK] force_flatten skipped -- position is PENDING_OPEN (%s)",
                reason,
            )
            return
        logger.warning(
            "[WEBHOOK] force_flatten: dropping phantom %s %d ct @ $%.2f -- %s",
            self.position.direction,
            self.position.remaining_contracts,
            self.position.entry_price,
            reason,
        )
        # 2026-10-14: broker closed the position externally (auto-liquidation
        # or manual) — surface it to Slack so it is never silent again.
        self._notify(f"⚠️ Phantom position cleared ({reason}) — broker closed "
                     f"externally; check Tradovate order history")
        self.position = None
        self._save_state()

    def _save_state(self) -> None:
        """Persist state to JSON and sync to GitHub Gist for dashboard."""
        # Dashboard-compatible state (matches PaperGoldBroker format)
        pos_for_dashboard = None
        if self.position and self.position.remaining_contracts > 0:
            pos_for_dashboard = {
                "direction": self.position.direction,
                "entry_price": self.position.entry_price,
                "contracts": self.position.remaining_contracts,
            }

        gist_state = {
            "equity": self._equity,
            "initial_balance": self._initial_equity,
            "trade_count": len(self._trade_log),
            "position": pos_for_dashboard,
        }

        # Local state (includes executor-specific fields for restart)
        local_state = {
            **gist_state,
            "daily_pnl": self.daily_pnl,
            "daily_trades": self.daily_trades,
            "daily_wins": self.daily_wins,
            "daily_losses": self.daily_losses,
            "circuit_breaker": self._circuit_breaker_fired,
            # 2026-10-07 (codex review): persist the PT trade date so a
            # same-day restart does not reset daily_pnl / clear the breaker.
            "last_day": self._last_day,
            # Keep detailed position for executor restore
            "position_detail": None,
        }
        if self.position:
            local_state["position_detail"] = {
                "direction": self.position.direction,
                "entry_price": self.position.entry_price,
                "total_contracts": self.position.total_contracts,
                "remaining_contracts": self.position.remaining_contracts,
                "tp_hits": self.position.tp_hits,
                "realized_pnl": self.position.realized_pnl,
                "fill_status": self.position.fill_status,
                "broker_order_id": self.position.broker_order_id,
                "tv_total_contracts": self.position.tv_total_contracts,
                "tv_contracts_closed": self.position.tv_contracts_closed,
            }
        # 2026-10-14: persist day-start equity so the live-equity breaker
        # survives a same-day restart (same rationale as last_day).
        local_state["day_start_equity"] = self._day_start_equity

        # Save local state
        try:
            tmp = self._state_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(local_state, f, indent=2)
            _atomic_replace_with_retry(tmp, self._state_file)
        except OSError as e:
            logger.error("[WEBHOOK] State save failed: %s", e)

        # Save local trade log
        try:
            tmp = self._log_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self._trade_log, f, indent=2)
            _atomic_replace_with_retry(tmp, self._log_file)
        except OSError as e:
            logger.error("[WEBHOOK] Trade log save failed: %s", e)

        # Gist sync now triggered from execute() in background thread
        self._pending_gist_state = gist_state

    def _sync_state_to_gist(self) -> None:
        """Background thread: push latest state to Gist (non-blocking)."""
        state = getattr(self, "_pending_gist_state", None)
        if state:
            self._sync_to_gist(state)

    def _sync_to_gist(self, state: dict) -> None:
        """Upload state + trade log to GitHub Gist for the dashboard."""
        gist_id, gh_token = self._get_gist_creds()
        if not gist_id or not gh_token:
            return

        try:
            import requests
            files = {
                "gold_scalper_state.json": {
                    "content": json.dumps(state, indent=2),
                },
            }
            if self._trade_log:
                files["gold_scalper_trade_log.json"] = {
                    "content": json.dumps(self._trade_log[-200:], indent=2),
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
            logger.debug("[WEBHOOK] Gist sync error: %s", exc)

    def _reconcile_on_startup(self) -> None:
        """Reconcile loaded paper state against live broker at startup.

        Closes the crash-unsafety window between Webull fill and paper disk
        commit. Runs once from __init__ after _load_state. No-op in paper-only
        mode or when the broker is in dry-run. Handles the 6 paper x broker
        combinations that matter after a crash:

          PENDING_OPEN + broker has position   -> upgrade to FILLED
          PENDING_OPEN + broker flat           -> order never filled, clear paper
          PENDING_CLOSE + broker flat          -> close completed, book reconciled trade
          PENDING_CLOSE + broker still holds   -> close failed, revert to FILLED
          FILLED + broker flat                 -> broker closed externally, clear paper
          FILLED + qty mismatch                -> rebuild paper from broker truth
        """
        if self.broker is None:
            return
        # 2026-10-07 (codex review): TradovateExecutor exposes ``live_orders``
        # (public), Webull exposed ``_live_orders``. Accept either so the
        # crash-recovery reconcile actually runs in live Tradovate mode.
        if not (getattr(self.broker, "_live_orders", False)
                or getattr(self.broker, "live_orders", False)):
            return

        # Retry on None -- Webull's cold-start throttle 429s rapid restarts.
        broker_qty = None
        for attempt in range(3):
            broker_qty = self.broker.get_position_qty()
            if broker_qty is not None:
                break
            if attempt < 2:
                delay = 2.0 * (2 ** attempt)
                logger.warning(
                    "[WEBHOOK] Startup reconcile: broker returned None -- retry %d/3 in %.0fs",
                    attempt + 1, delay,
                )
                time.sleep(delay)
        if broker_qty is None:
            logger.warning("[WEBHOOK] Startup reconcile skipped: broker unreachable after 3 attempts")
            return

        # 2026-10-07 (codex review #2): one snapshot is not enough — Tradovate's
        # position snapshot can lag a fill by seconds, and every branch below
        # that clears or rebuilds paper is destructive. Require a second,
        # identical read a few seconds later; otherwise skip and let the 60s
        # reconciler (2-cycle grace) handle it.
        time.sleep(3.0)
        try:
            broker_qty_2 = self.broker.get_position_qty()
        except Exception as exc:
            logger.warning("[WEBHOOK] Startup reconcile skipped: second read raised %s", exc)
            return
        if broker_qty_2 != broker_qty:
            logger.warning(
                "[WEBHOOK] Startup reconcile skipped: broker snapshot unstable "
                "(%s then %s) — deferring to reconciler", broker_qty, broker_qty_2,
            )
            return

        if self.position and self.position.remaining_contracts > 0:
            paper_qty = (
                self.position.remaining_contracts
                if self.position.direction == "LONG"
                else -self.position.remaining_contracts
            )
            paper_fill_status = self.position.fill_status
        else:
            paper_qty = 0
            paper_fill_status = "NONE"

        logger.info(
            "[WEBHOOK] Startup reconcile: paper=%+d (%s) broker=%+d",
            paper_qty, paper_fill_status, broker_qty,
        )

        # Both flat -- noop.
        if paper_qty == 0 and broker_qty == 0:
            return

        # Match qty + FILLED -- already in sync.
        if paper_qty == broker_qty and paper_fill_status == "FILLED":
            return

        # PENDING_OPEN + broker has matching position -> upgrade to FILLED.
        if paper_fill_status == "PENDING_OPEN" and paper_qty == broker_qty and broker_qty != 0:
            logger.warning(
                "[WEBHOOK] Startup reconcile: upgrading PENDING_OPEN to FILLED (paper=broker=%+d)",
                broker_qty,
            )
            self.position.fill_status = "FILLED"
            self._save_state()
            return

        # PENDING_OPEN + broker flat -> order never filled.
        if paper_fill_status == "PENDING_OPEN" and broker_qty == 0:
            logger.warning(
                "[WEBHOOK] Startup reconcile: PENDING_OPEN + broker flat -- order never filled, clearing paper",
            )
            self.position = None
            self._save_state()
            return

        # PENDING_CLOSE + broker flat -> close completed, book reconciled trade.
        if paper_fill_status == "PENDING_CLOSE" and broker_qty == 0:
            logger.warning(
                "[WEBHOOK] Startup reconcile: PENDING_CLOSE + broker flat -- close completed, booking reconciled trade",
            )
            if self.position:
                self._trade_log.append({
                    "time": datetime.now(PT).strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": self.config.symbol.replace("=F", ""),
                    "direction": self.position.direction,
                    "contracts": self.position.total_contracts,
                    "entry": self.position.entry_price,
                    "exit": self.position.entry_price,  # unknown; zero-PnL placeholder
                    "pnl": 0.0,
                    "reason": "RECONCILED_ON_STARTUP (PENDING_CLOSE + broker flat)",
                    "fill_status": "RECONCILED",
                    "broker_order_id": self.position.broker_order_id,
                })
            self.position = None
            self._save_state()
            return

        # PENDING_CLOSE + broker still holds matching qty -> close failed.
        if paper_fill_status == "PENDING_CLOSE" and broker_qty == paper_qty:
            logger.warning(
                "[WEBHOOK] Startup reconcile: PENDING_CLOSE + broker still holds (paper=broker=%+d) -- reverting to FILLED",
                broker_qty,
            )
            self.position.fill_status = "FILLED"
            self._save_state()
            return

        # Any other mismatch -> rebuild paper from broker truth.
        if broker_qty == 0:
            logger.warning(
                "[WEBHOOK] Startup reconcile: paper=%+d broker=0 -- clearing phantom paper",
                paper_qty,
            )
            self.position = None
            self._save_state()
            return

        direction = "LONG" if broker_qty > 0 else "SHORT"
        qty = abs(broker_qty)
        placeholder_entry = (
            self.position.entry_price if self.position and self.position.entry_price > 0 else 0.0
        )
        logger.warning(
            "[WEBHOOK] Startup reconcile: rebuilding paper from broker (%s %d ct @ placeholder $%.2f)",
            direction, qty, placeholder_entry,
        )
        self.position = WebhookPosition(
            direction=direction,
            entry_price=placeholder_entry,
            entry_time=datetime.now(PT),
            total_contracts=qty,
            remaining_contracts=qty,
            tv_total_contracts=qty,
            tv_contracts_closed=0,
        )
        self.position.fill_status = "RECONCILED_FROM_BROKER"
        self._save_state()

    def _load_state(self) -> None:
        """Load state from JSON if exists."""
        # Load trade log
        if os.path.exists(self._log_file):
            try:
                with open(self._log_file) as f:
                    self._trade_log = json.load(f)
            except Exception as e:
                logger.warning("[WEBHOOK] Trade log load failed: %s", e)

        if os.path.exists(self._price_cmp_file):
            try:
                with open(self._price_cmp_file) as f:
                    raw = json.load(f)
                # Filter out bogus entries (TV price >5% from Yahoo)
                clean = []
                for e in raw:
                    yp = e.get("yahoo_price", 0)
                    tp = e.get("tv_price", 0)
                    if yp > 0 and abs(tp - yp) / yp <= 0.05:
                        clean.append(e)
                dropped = len(raw) - len(clean)
                if dropped:
                    logger.info("[PRICE_CMP] Filtered %d bad entries on load", dropped)
                self._price_comparisons = clean
            except Exception:
                pass

        if not os.path.exists(self._state_file):
            return
        try:
            with open(self._state_file) as f:
                state = json.load(f)
            self.daily_pnl = state.get("daily_pnl", 0.0)
            self.daily_trades = state.get("daily_trades", 0)
            self.daily_wins = state.get("daily_wins", 0)
            self.daily_losses = state.get("daily_losses", 0)
            self._circuit_breaker_fired = state.get("circuit_breaker", False)
            self._last_day = state.get("last_day")
            self._day_start_equity = state.get("day_start_equity")
            self._equity = state.get("equity", self._initial_equity)
            self._initial_equity = state.get("initial_balance", self._initial_equity)

            pos = state.get("position_detail") or state.get("position")
            if pos and pos.get("remaining_contracts", pos.get("contracts", 0)) > 0:
                total_ct = pos.get("total_contracts", pos.get("contracts", 0))
                self.position = WebhookPosition(
                    direction=pos["direction"],
                    entry_price=pos["entry_price"],
                    entry_time=datetime.now(PT),
                    total_contracts=total_ct,
                    remaining_contracts=pos.get("remaining_contracts", total_ct),
                    tp_hits=pos.get("tp_hits", []),
                    realized_pnl=pos.get("realized_pnl", 0.0),
                    tv_total_contracts=pos.get("tv_total_contracts", total_ct),
                    tv_contracts_closed=pos.get("tv_contracts_closed", 0),
                )
                self.position.fill_status = pos.get("fill_status", "FILLED")
                self.position.broker_order_id = pos.get("broker_order_id")
                logger.info(
                    "[WEBHOOK] Restored position: %s %d ct @ $%.2f (fill_status=%s)",
                    self.position.direction,
                    self.position.remaining_contracts,
                    self.position.entry_price,
                    self.position.fill_status,
                )
        except Exception as e:
            logger.warning("[WEBHOOK] State load failed: %s", e)
