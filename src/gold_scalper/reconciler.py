"""Background reconciler -- corrects PENDING/SUBMITTED trade-log entries.

Why this exists
---------------
Even with verify_fill at order-placement time, an order can be SUBMITTED
when the 5-second wait timer expires (Webull slow, market frozen,
network hiccup). When that happens we record the trade with TV's price
as an estimate and ``fill_status: "SUBMITTED"``. This module periodically
re-checks those orders on Webull and:

  * If now FILLED   -> rewrites the trade-log entry with the real price
                      and adjusts equity / daily PnL by the delta.
  * If now CANCELLED -> marks trade as cancelled and rolls back the paper
                       equity / PnL impact.
  * If still SUBMITTED -> leaves alone, retries next cycle.
  * If REJECTED     -> marks rejected, rolls back.

It also detects position-state drift between the executor's paper view
and Webull reality (e.g., a manual close on Webull) and logs a warning.
We do NOT auto-fix position state -- that's risky on a live account; a
human should look at the warning and decide.

Design notes
------------
* Stateless -- reads trade log + queries Webull each cycle. No DB needed.
* Atomic writes -- uses tmpfile + os.replace pattern from rest of codebase.
* Idempotent -- running it twice on the same trade has no effect.
* Bounded -- only checks entries from the last ``recheck_window_seconds``
  to keep API usage reasonable on long-running trade logs.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# How far back to look for PENDING entries. 24h covers any normal trading
# day; older entries are assumed dead (unfilled DAY orders expire EOD).
_RECHECK_WINDOW_SECONDS = 24 * 3600

# Minimum gap between reconciler cycles. 60s is a balance between
# responsiveness and Webull API quota. The MGC TP3 incident on 2026-04-17
# would have been caught within 1 minute of the limit cancelling.
_DEFAULT_INTERVAL_SECONDS = 60.0


class TradeLogReconciler:
    """Background task that corrects PENDING trades against Webull truth.

    Wires into a single executor (gold or TQQQ). For both at once,
    instantiate twice with different log paths + status fetchers.
    """

    def __init__(
        self,
        name: str,
        log_path: str,
        get_order_status,           # Callable[[str], dict]
        get_position_qty,            # Callable[[], int | None]
        on_trade_corrected=None,     # Callable[[dict, dict], None]
        get_broker_position_qty=None,  # Callable[[], int | None]
        flatten_paper=None,            # Callable[[str], None]
        interval_seconds: float = _DEFAULT_INTERVAL_SECONDS,
        recheck_window_seconds: float = _RECHECK_WINDOW_SECONDS,
    ):
        """
        Args:
            name: short label for logs ("gold" / "tqqq").
            log_path: path to the trade log JSON array file.
            get_order_status: callable taking client_order_id -> status dict
                with keys: status, filled_price, filled_quantity. Same
                shape as WebullMGCBroker.get_order_status.
            get_position_qty: callable returning the executor's current paper
                position quantity as a SIGNED int (+N = long N, -N = short N,
                0 = flat). Used for drift detection. Pass lambda: None to
                disable all drift handling.
            on_trade_corrected: optional callback fired when a trade entry
                is corrected. Signature (old_entry, new_entry) -- useful for
                hooking equity/PnL adjustments back into the executor.
            get_broker_position_qty: callable returning the SIGNED position
                reported by the broker (Webull) for the executor's instrument.
                Returns None if the query fails or the broker is dry-run --
                caller treats None as "unknown" and skips drift for this cycle.
                When None, drift detection is disabled entirely.
            flatten_paper: callable(reason: str) that clears the executor's
                paper position (no PnL booking, no broker call). Called ONLY
                when the broker confirms flat but paper holds a phantom --
                the safe direction of drift. When None, drift is log-only.
            interval_seconds: time between reconciler cycles.
            recheck_window_seconds: only check entries from the last N seconds.
        """
        self._name = name
        self._log_path = log_path
        self._get_order_status = get_order_status
        self._get_position_qty = get_position_qty
        self._on_trade_corrected = on_trade_corrected
        self._get_broker_position_qty = get_broker_position_qty
        self._flatten_paper = flatten_paper
        self._interval = interval_seconds
        self._recheck_window = recheck_window_seconds

        self._stop_flag = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Per-cycle stats
        self._cycle_count = 0
        self._corrections_made = 0

        # Phantom-paper grace-period state. The auto-clear path used to
        # fire on a single broker=FLAT + paper!=0 reading, which races with
        # fresh order placement — Tradovate's position snapshot lags the
        # fill by ~1-2s. Real-world impact: 2026-05-19 00:40:02 a SHORT
        # entry was placed, the reconciler queried Tradovate in the same
        # second, saw broker=0, force-cleared paper, and then every Pine
        # exit for that position was rejected as "no position to close" —
        # leaving 4 short open and unmanaged for 4+ hours.
        #
        # Fix: track the most recent (paper_qty, broker_qty) drift state
        # and require it to persist across at least 2 consecutive cycles
        # before auto-clearing. Any state change (or in-sync return)
        # resets the counter. Two cycles at 60s interval = ~60-120s of
        # confirmation, plenty of time for any broker to reflect a fill.
        self._last_drift_state: Optional[tuple] = None
        self._drift_consecutive_count = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Spawn the background loop thread (idempotent)."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_flag.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name=f"reconciler-{self._name}", daemon=True,
        )
        self._thread.start()
        logger.info(
            "[RECON-%s] Started: log=%s interval=%.0fs",
            self._name, self._log_path, self._interval,
        )

    def stop(self, join_timeout: float = 5.0) -> None:
        """Signal stop and wait briefly for the thread to exit."""
        self._stop_flag.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=join_timeout)

    def status(self) -> dict:
        """Reconciler status for /status endpoint."""
        return {
            "name": self._name,
            "running": bool(self._thread and self._thread.is_alive()),
            "interval_seconds": self._interval,
            "cycles_completed": self._cycle_count,
            "corrections_made": self._corrections_made,
            "log_path": self._log_path,
        }

    # ------------------------------------------------------------------
    # Run loop
    # ------------------------------------------------------------------
    def _run_loop(self) -> None:
        # Skip the first immediate run so the executor has time to write
        # state on startup. Otherwise we'd race on a fresh log file.
        if self._stop_flag.wait(self._interval):
            return
        while not self._stop_flag.is_set():
            try:
                self._reconcile_once()
            except Exception as exc:
                logger.error("[RECON-%s] Cycle failed: %s", self._name, exc,
                             exc_info=True)
            self._cycle_count += 1
            # Use Event.wait(timeout) instead of time.sleep so stop() is responsive
            if self._stop_flag.wait(self._interval):
                return

    # ------------------------------------------------------------------
    # Core reconciliation
    # ------------------------------------------------------------------
    def _reconcile_once(self) -> None:
        """One pass over the trade log + position drift check."""
        if not os.path.exists(self._log_path):
            return  # Nothing to reconcile

        try:
            with open(self._log_path, "r", encoding="utf-8") as f:
                trades = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("[RECON-%s] Cannot read log: %s", self._name, exc)
            return

        if not isinstance(trades, list) or not trades:
            return

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._recheck_window)
        modified = False

        for i, trade in enumerate(trades):
            if not isinstance(trade, dict):
                continue

            # Only act on entries that are explicitly PENDING/SUBMITTED.
            # Entries written before this PR don't have fill_status -- leave alone.
            status_now = (trade.get("fill_status") or "").upper()
            if status_now not in ("SUBMITTED", "UNKNOWN", "PENDING"):
                continue

            order_id = trade.get("broker_order_id")
            if not order_id:
                continue

            # Bounded by recheck window so old logs don't blow up API usage
            trade_time = self._parse_trade_time(trade.get("time"))
            if trade_time and trade_time < cutoff:
                continue

            # Hit Webull
            try:
                resolved = self._get_order_status(order_id)
            except Exception as exc:
                logger.warning(
                    "[RECON-%s] get_order_status failed for %s: %s",
                    self._name, order_id, exc,
                )
                continue

            new_status = (resolved.get("status") or "UNKNOWN").upper()
            if new_status in ("SUBMITTED", "UNKNOWN"):
                continue  # Still pending or transient; try next cycle.

            # Terminal -- apply the correction
            old_entry = dict(trade)
            if new_status == "FILLED":
                actual_price = resolved.get("filled_price", 0) or 0
                if actual_price > 0:
                    trade["exit"] = round(actual_price, 4) if "exit" in trade else trade.get("exit")
                    # MGC executor uses "exit"; TQQQ uses "exit_price". Update both.
                    if "exit_price" in trade:
                        trade["exit_price"] = round(actual_price, 4)
                    trade["fill_status"] = "FILLED"
                    trade["reconciled_at"] = datetime.now(timezone.utc).isoformat()
                    logger.info(
                        "[RECON-%s] Trade %d: SUBMITTED -> FILLED at $%.2f "
                        "(was estimated $%.2f)",
                        self._name, i, actual_price, old_entry.get("exit") or 0,
                    )
                    modified = True
                    self._corrections_made += 1
                    if self._on_trade_corrected:
                        try:
                            self._on_trade_corrected(old_entry, trade)
                        except Exception as exc:
                            logger.error(
                                "[RECON-%s] on_trade_corrected callback failed: %s",
                                self._name, exc,
                            )
            elif new_status in ("CANCELLED", "REJECTED"):
                trade["fill_status"] = new_status
                trade["reconciled_at"] = datetime.now(timezone.utc).isoformat()
                trade["pnl"] = 0.0       # MGC log key
                trade["pnl_usd"] = 0.0   # TQQQ log key
                trade["reason"] = (trade.get("reason", "") + f" [RECON: {new_status}]").strip()
                logger.warning(
                    "[RECON-%s] Trade %d: was SUBMITTED, now %s on Webull -- "
                    "marking cancelled, PnL zeroed",
                    self._name, i, new_status,
                )
                modified = True
                self._corrections_made += 1
                if self._on_trade_corrected:
                    try:
                        self._on_trade_corrected(old_entry, trade)
                    except Exception as exc:
                        logger.error(
                            "[RECON-%s] on_trade_corrected callback failed: %s",
                            self._name, exc,
                        )

        if modified:
            # Re-read before write to avoid clobbering concurrent appends
            # from the executor. The reconciler runs every 60s but the
            # executor can append to the trade log at any moment when a
            # position closes. Without this re-read, a position close
            # landing in the brief read-modify-write window would be lost
            # when we write back our (now stale) snapshot.
            #
            # Strategy: re-read latest trades, copy our corrections by
            # broker_order_id (immutable per trade), preserve any new
            # entries that appeared since our initial read.
            self._safe_merge_write(trades)

        # Position drift detection — compare paper vs broker reality.
        # Auto-correction is limited to the SAFE direction (broker flat +
        # paper has position = phantom). Other drift patterns (broker has,
        # paper missing; opposite directions; quantity mismatch) are
        # log-only because auto-creating a paper position without a known
        # entry_price would poison PnL.
        if self._get_broker_position_qty is None:
            return
        try:
            paper_qty = self._get_position_qty()
        except Exception as exc:
            logger.warning("[RECON-%s] paper qty fetch failed: %s",
                           self._name, exc)
            return
        try:
            broker_qty = self._get_broker_position_qty()
        except Exception as exc:
            logger.warning("[RECON-%s] broker qty fetch failed: %s",
                           self._name, exc)
            return
        if broker_qty is None:
            return  # broker query unavailable — skip this cycle
        if paper_qty is None:
            paper_qty = 0
        if paper_qty == broker_qty:
            # In sync — clear any pending grace state.
            self._last_drift_state = None
            self._drift_consecutive_count = 0
            return

        # Track how many consecutive cycles the exact same drift state has
        # persisted. Any change in either qty resets the counter to 1.
        current_state = (paper_qty, broker_qty)
        if self._last_drift_state == current_state:
            self._drift_consecutive_count += 1
        else:
            self._last_drift_state = current_state
            self._drift_consecutive_count = 1

        # Drift detected — decide which bucket and act.
        if broker_qty == 0 and paper_qty != 0:
            # Phantom on paper (e.g., manual close on broker, or a close
            # order whose broker_reject left paper untouched). SAFE to
            # auto-clear — the matching close already happened on the
            # broker, so we just drop the paper ghost.
            #
            # BUT require >=2 consecutive cycles of the same state before
            # acting, to avoid the order-placement race where Tradovate's
            # position snapshot lags a fresh fill by 1-2 seconds. With
            # 60s interval, this means ~60-120s of confirmation before
            # auto-clearing, which is plenty for any broker to catch up.
            if self._drift_consecutive_count < 2:
                logger.warning(
                    "[RECON-%s] POSITION DRIFT (phantom paper, cycle %d/2 — "
                    "in grace): paper=%+d broker=FLAT — will auto-clear if "
                    "drift persists next cycle (guards against fresh-order race)",
                    self._name, self._drift_consecutive_count, paper_qty,
                )
                return

            logger.warning(
                "[RECON-%s] POSITION DRIFT (phantom paper, confirmed across "
                "%d cycles): paper=%+d broker=FLAT — auto-clearing paper",
                self._name, self._drift_consecutive_count, paper_qty,
            )
            if self._flatten_paper is not None:
                try:
                    self._flatten_paper(
                        f"reconciler drift: broker flat, paper had {paper_qty:+d}"
                    )
                    self._corrections_made += 1
                except Exception as exc:
                    logger.error(
                        "[RECON-%s] flatten_paper failed: %s",
                        self._name, exc,
                    )
            # Reset state — we've acted; future cycles start fresh.
            self._last_drift_state = None
            self._drift_consecutive_count = 0
        else:
            # All other mismatches: direction flip, qty mismatch, or
            # broker has position that paper doesn't. Log only — human
            # must reconcile manually because entry_price cannot be
            # safely inferred from a position snapshot.
            logger.warning(
                "[RECON-%s] POSITION DRIFT (manual review): paper=%+d "
                "broker=%+d — not auto-correcting",
                self._name, paper_qty, broker_qty,
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_trade_time(time_str) -> Optional[datetime]:
        """Parse trade time strings into UTC-aware datetime.

        Trade logs use either "YYYY-MM-DD HH:MM:SS" (gold executor naive
        local) or ISO-8601 with TZ (TQQQ executor). Treat naive as UTC for
        cutoff comparison -- we accept some skew rather than crash.
        """
        if not isinstance(time_str, str):
            return None
        try:
            if "T" in time_str:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None

    def _safe_merge_write(self, trades_with_corrections: list) -> None:
        """Re-read disk, apply our corrections by order_id, then write.

        Resolves the race where the executor appends a new trade between
        our initial read and our write. Indexing by broker_order_id is safe
        because order_ids are UUIDs we mint per-order — globally unique
        and stable for the trade's lifetime.
        """
        # Build correction map from our in-memory snapshot
        corrections_by_id = {}
        for t in trades_with_corrections:
            if not isinstance(t, dict):
                continue
            oid = t.get("broker_order_id")
            if oid and t.get("reconciled_at"):
                corrections_by_id[oid] = t

        if not corrections_by_id:
            return  # Nothing to merge — caller should not have called us

        # Re-read latest disk state
        try:
            with open(self._log_path, "r", encoding="utf-8") as f:
                latest = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("[RECON-%s] Re-read for merge failed: %s — using snapshot",
                           self._name, exc)
            self._atomic_write(trades_with_corrections)
            return

        if not isinstance(latest, list):
            self._atomic_write(trades_with_corrections)
            return

        # Apply corrections to whichever entries on disk match by order_id
        applied = 0
        for entry in latest:
            if not isinstance(entry, dict):
                continue
            oid = entry.get("broker_order_id")
            if oid and oid in corrections_by_id:
                entry.update(corrections_by_id[oid])
                applied += 1

        if applied < len(corrections_by_id):
            logger.warning(
                "[RECON-%s] Merge: %d/%d corrections applied (%d entries vanished from disk)",
                self._name, applied, len(corrections_by_id),
                len(corrections_by_id) - applied,
            )
        self._atomic_write(latest)

    def _atomic_write(self, trades: list) -> None:
        """Write trades back atomically (tmpfile + os.replace).

        Same pattern used elsewhere in the codebase. Prevents truncated
        log files if the process dies mid-write.
        """
        directory = os.path.dirname(self._log_path)
        try:
            os.makedirs(directory, exist_ok=True)
        except OSError:
            pass
        tmp_fd, tmp_path = tempfile.mkstemp(
            prefix=".reconcile_", suffix=".json", dir=directory or None,
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(trades, f, indent=2)
            os.replace(tmp_path, self._log_path)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise
