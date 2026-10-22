"""Webhook server — receives TradingView alerts and routes to executors.

Multi-instrument webhook router:
- POST /webhook receives ALL TradingView alerts
- Routes by `ticker` field: BTCUSD → BTC executor, everything else → Gold executor
- GET /status returns position status for all executors

Usage:
    python main.py gold-scalper
    # Then set TradingView alert webhook URL to: https://<your-cloudflare-url>/webhook

Gold signal format (Pine Script):
    {"action": "ENTRY", "direction": "SHORT", "price": 4500.5, "contracts": 6}
    {"action": "TP_HIT", "tp_level": 1, "price": 4494.5, "contracts": 1}

BTC UT Bot format:
    {"action": "buy",  "contracts": 2, "ticker": "BTCUSD", "position_size":  1, "price": 68394}
    {"action": "sell", "contracts": 2, "ticker": "BTCUSD", "position_size": -1, "price": 68394}
"""

from __future__ import annotations

# SECURITY — install Webull SDK credential redaction BEFORE any webull SDK
# module gets imported, directly or transitively. The webull SDK's default
# ApiClient constructor installs stdout + file log handlers that dump
# x-app-key / x-app-secret / x-signature / x-signature-nonce headers on
# every API call. This import + install_log_redaction() call neutralizes
# the webull.core logger and attaches a RedactingFilter to logging.lastResort
# so any credential bytes that slip past the primary defense are stripped
# before reaching stderr. See src/gold_scalper/sdk_redaction.py for details.
#
# The leak was discovered on 2026-04-15 when webull_trade_sdk.log was found
# in the project root containing ~30 header dumps from the live TQQQ
# executor's first 15 hours of runtime. This redaction layer + the per-
# ApiClient disable_sdk_default_logger() calls in webull_tqqq_executor.py
# and webull_mgc_broker.py together close that leak.
from src.gold_scalper.sdk_redaction import install_log_redaction
install_log_redaction()

import logging
import sys

# Install a stdout StreamHandler on the root logger so that INFO-level
# records from `src.gold_scalper.*` loggers reach the webhook_stdout_*.log
# file (run_webhook.ps1 redirects python stdout via `>> log 2>&1`).
# Without this, uvicorn configures only its own `uvicorn.*` loggers and
# all of our `logger.info(...)` calls in webhook_server / webhook_executor
# fall through to `logging.lastResort` which drops anything below WARNING.
#
# Redaction: install_log_redaction() above attached a RedactingFilter to
# the root logger, but logger-level filters do NOT apply to records that
# propagate up from child loggers -- only handler-level filters do. So
# we must copy the root's filters onto this new handler, otherwise
# credential bytes from child loggers could reach the log unsanitized.
_root_logger = logging.getLogger()
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(logging.Formatter(
    "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
))
for _filt in _root_logger.filters:
    _console_handler.addFilter(_filt)
_root_logger.addHandler(_console_handler)
_root_logger.setLevel(logging.INFO)

import os
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import asyncio
import queue
import threading
import time

import nest_asyncio               # allow ib_insync event loop inside uvicorn's

import uvicorn  # import early — before src/selectors shadows stdlib selectors

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.gold_scalper.alerts import ScalperAlertEngine
from src.gold_scalper.config import GoldScalperConfig, load_config
from src.gold_scalper.webhook_executor import WebhookExecutor

logger = logging.getLogger(__name__)

PT = ZoneInfo("America/Los_Angeles")

# Global executors (initialized by start_server)
_gold_executor: Optional[WebhookExecutor] = None
_btc_executor = None  # BTCWebhookExecutor, lazy import
_webull_executor = None  # WebullExecutor, lazy import
_qqq_executor = None  # QQQOptionsExecutor, lazy import

# Background reconcilers — re-check SUBMITTED orders against Webull truth
# every 60s and correct trade log entries (FILLED price / CANCELLED rollback).
# See src/gold_scalper/reconciler.py for design.
_gold_reconciler = None
_tqqq_reconciler = None


# Per-executor worker queues — decouple webhook handler latency from broker
# round-trip latency. The request thread enqueues + returns 200 in <50ms; a
# daemon thread drains the queue and runs the actual executor.execute() work.
# This fixes TradingView's "Webhook delivery failed -- request took too long
# and timed out" notification, which fired on 2026-04-28 06:40 even though
# the underlying TQQQ trade actually executed (TV's ~3-5s timeout vs our
# observed 4s synchronous handler latency under Webull 429 rate-limit).
class ExecutorWorker:
    """Single-consumer FIFO worker queue for one executor.

    submit(body) -> True if accepted, False if queue full (drops + logs).
    The consumer thread runs ``self._fn(body)`` and logs the result. Errors
    are caught and logged but do not kill the worker.

    Bounded queue (default 100) prevents unbounded growth under runaway-curl
    or stuck-broker scenarios. Realistic peak load is <10 alerts/min, so 100
    is generous headroom; if we ever hit it that signals a real problem
    (broker hung, infinite loop) and dropping is the safe default.
    """

    def __init__(self, name: str, fn, maxsize: int = 100):
        self._name = name
        self._fn = fn
        self._q: queue.Queue = queue.Queue(maxsize=maxsize)
        t = threading.Thread(
            target=self._run, daemon=True, name=f"worker-{name}",
        )
        t.start()

    def submit(self, body: dict) -> bool:
        try:
            self._q.put_nowait(body)
            return True
        except queue.Full:
            logger.error(
                "[WORKER-%s] Queue full (max=%d) -- DROPPING alert: %s",
                self._name, self._q.maxsize,
                str({k: v for k, v in body.items() if k != "passphrase"})[:200],
            )
            return False

    def depth(self) -> int:
        return self._q.qsize()

    def _run(self):
        while True:
            body = self._q.get()
            try:
                result = self._fn(body)
                logger.info(
                    "[WORKER-%s] Processed: %s",
                    self._name, str(result)[:200],
                )
            except Exception:
                logger.exception("[WORKER-%s] Handler raised", self._name)
            finally:
                self._q.task_done()


# Worker globals (initialized in start_server after each executor is built)
_gold_worker: Optional[ExecutorWorker] = None
_btc_worker: Optional[ExecutorWorker] = None
_qqq_worker: Optional[ExecutorWorker] = None
_webull_worker: Optional[ExecutorWorker] = None


# Tickers that route to BTC executor (Alpaca)
_BTC_TICKERS = {"BTCUSD", "BTC/USD", "BTC-USD", "XBTUSD"}

# Tickers that route to QQQ options executor
_QQQ_TICKERS = {"QQQ", "QQQO"}

# Tickers that route to Webull TQQQ executor
_WEBULL_TICKERS = {"TQQQ"}

app = FastAPI(title="Trading Webhook Server", version="2.0")

# Shared-secret passphrase — set via WEBHOOK_SECRET env var.
# TradingView alerts must include {"passphrase": "<secret>", ...} in JSON body.
# If WEBHOOK_SECRET is empty/unset, auth is BYPASSED (dev mode) with a WARNING.
_WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
if not _WEBHOOK_SECRET:
    logger.warning(
        "[WEBHOOK] WEBHOOK_SECRET not set — /webhook is UNAUTHENTICATED. "
        "Set WEBHOOK_SECRET in secrets/.env to enable shared-secret auth."
    )


def _check_auth(body: dict) -> Optional[JSONResponse]:
    """Validate passphrase. Returns error response if invalid, None if OK."""
    if not _WEBHOOK_SECRET:
        return None  # dev-mode bypass, warned at startup
    supplied = str(body.get("passphrase", "")).strip()
    if not supplied:
        logger.warning("[WEBHOOK] Rejected: missing passphrase")
        return JSONResponse(status_code=401, content={"error": "missing passphrase"})
    # Constant-time comparison to resist timing attacks
    import hmac
    if not hmac.compare_digest(supplied, _WEBHOOK_SECRET):
        logger.warning("[WEBHOOK] Rejected: invalid passphrase")
        return JSONResponse(status_code=401, content={"error": "invalid passphrase"})
    return None


@app.post("/webhook")
async def webhook(request: Request):
    """Receive TradingView alert webhook and route by ticker.

    Routes:
    - ticker in BTC_TICKERS → BTC executor (Alpaca crypto paper)
    - everything else → Gold executor (paper)
    """
    try:
        body = await request.json()
    except Exception:
        raw = await request.body()
        # 2026-10-07 (codex review #2): a malformed body may still contain the
        # passphrase — log its size only and never echo it back.
        logger.warning("[WEBHOOK] Non-JSON body received (%d bytes)", len(raw))
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid JSON"},
        )

    # Shared-secret authentication (skip in dev mode if WEBHOOK_SECRET unset)
    auth_err = _check_auth(body)
    if auth_err is not None:
        return auth_err

    now = datetime.now(PT)
    ticker = body.get("ticker", "").strip().upper()

    # 2026-10-07 (codex review): never log the shared secret.
    logger.info(
        "[WEBHOOK] %s Received (ticker=%s): %s",
        now.strftime("%H:%M:%S"), ticker or "none",
        str({k: v for k, v in body.items() if k != "passphrase"})[:300],
    )

    # Route by ticker
    # Priority: Webull (if configured) → QQQ Options → BTC (Alpaca) → Gold (default)
    norm_ticker = ticker.replace("/", "").replace("-", "")
    if _webull_executor and (norm_ticker in _WEBULL_TICKERS or ticker in _WEBULL_TICKERS):
        return _handle_webull(body)
    elif norm_ticker in _QQQ_TICKERS or ticker in _QQQ_TICKERS:
        return _handle_qqq(body)
    elif ticker in _BTC_TICKERS:
        return _handle_btc(body)
    else:
        # 2026-10-07 (codex review): the default route used to accept ANY
        # ticker. Require the alert's ticker root to match the configured
        # futures contract (TRADOVATE_SYMBOL=MNQZ6 -> root "MNQ") so a stale
        # MGC alert or a typo cannot trade the Tradovate account.
        root = os.environ.get("TRADOVATE_SYMBOL", "").strip().upper()[:3]
        if root and not ticker.startswith(root):
            logger.warning(
                "[WEBHOOK] Rejected: ticker %r does not match configured "
                "contract root %r", ticker, root,
            )
            return JSONResponse(
                status_code=400,
                content={"error": f"ticker {ticker!r} not routed (expected {root}*)"},
            )
        return _handle_gold(body)


# Each _handle_* below enqueues onto the executor's worker queue and returns
# 200 in <50ms. The actual broker work happens asynchronously in the worker
# thread. Errors during execution are logged by the worker (see
# ExecutorWorker._run); they no longer surface in the HTTP response, so check
# the webhook log for execution outcomes (TV's alert log only shows ack/no-ack).
def _handle_btc(body: dict) -> JSONResponse:
    """Enqueue BTC alert to worker queue."""
    if _btc_executor is None or _btc_worker is None:
        logger.error("[WEBHOOK] BTC executor not initialized")
        return JSONResponse(
            status_code=503,
            content={"error": "BTC executor not initialized"},
        )

    if not _btc_worker.submit(body):
        return JSONResponse(
            status_code=503,
            content={"error": "BTC queue full -- alert dropped"},
        )

    return JSONResponse(content={"status": "accepted", "executor": "btc"})


def _handle_qqq(body: dict) -> JSONResponse:
    """Enqueue QQQ options alert to worker queue."""
    if _qqq_executor is None or _qqq_worker is None:
        logger.error("[WEBHOOK] QQQ options executor not initialized")
        return JSONResponse(
            status_code=503,
            content={"error": "QQQ options executor not initialized"},
        )

    if not _qqq_worker.submit(body):
        return JSONResponse(
            status_code=503,
            content={"error": "QQQ options queue full -- alert dropped"},
        )

    return JSONResponse(content={"status": "accepted", "executor": "qqq"})


def _handle_webull(body: dict) -> JSONResponse:
    """Enqueue Webull TQQQ alert to worker queue. (MGC routes to gold.)"""
    if _webull_executor is None or _webull_worker is None:
        logger.error("[WEBHOOK] Webull executor not initialized")
        return JSONResponse(
            status_code=503,
            content={"error": "Webull executor not initialized"},
        )

    if not _webull_worker.submit(body):
        return JSONResponse(
            status_code=503,
            content={"error": "Webull queue full -- alert dropped"},
        )

    return JSONResponse(content={"status": "accepted", "executor": "webull"})


def _handle_gold(body: dict) -> JSONResponse:
    """Enqueue Gold alert to worker queue.

    Note: parse_signal stays synchronous in the worker (not the request
    thread) -- it's fast but keeps the request hot path free of any
    executor-internal logic.
    """
    if _gold_executor is None or _gold_worker is None:
        logger.error("[WEBHOOK] Gold executor not initialized")
        return JSONResponse(
            status_code=503,
            content={"error": "Gold executor not initialized"},
        )

    if not _gold_worker.submit(body):
        return JSONResponse(
            status_code=503,
            content={"error": "Gold queue full -- alert dropped"},
        )

    return JSONResponse(content={"status": "accepted", "executor": "gold"})


@app.get("/status")
async def status():
    """Position status for all executors."""
    result = {"status": "running"}

    # Gold status
    if _gold_executor:
        gold_pos = None
        if _gold_executor.position:
            p = _gold_executor.position
            gold_pos = {
                "direction": p.direction,
                "entry_price": p.entry_price,
                "remaining_contracts": p.remaining_contracts,
                "tp_hits": p.tp_hits,
                "realized_pnl": p.realized_pnl,
            }
        gold_status = {
            "position": gold_pos,
            "daily_pnl": _gold_executor.daily_pnl,
            "daily_trades": _gold_executor.daily_trades,
            "daily_wl": f"{_gold_executor.daily_wins}W/{_gold_executor.daily_losses}L",
            "circuit_breaker": _gold_executor._circuit_breaker_fired,
        }
        # Surface live broker status (Webull MGC) when attached
        gold_broker = getattr(_gold_executor, "broker", None)
        if gold_broker is not None and hasattr(gold_broker, "get_status"):
            try:
                gold_status["broker"] = gold_broker.get_status()
            except Exception as exc:
                gold_status["broker"] = {"error": str(exc)}
        result["gold"] = gold_status

    # BTC status
    if _btc_executor:
        btc_pos = None
        if _btc_executor.position:
            p = _btc_executor.position
            btc_pos = {
                "direction": p.direction,
                "entry_price": p.entry_price,
                "qty": p.qty,
            }
        result["btc"] = {
            "position": btc_pos,
            "daily_pnl": _btc_executor.daily_pnl,
            "daily_trades": _btc_executor.daily_trades,
            "daily_wl": f"{_btc_executor.daily_wins}W/{_btc_executor.daily_losses}L",
            "circuit_breaker": _btc_executor._circuit_breaker_fired,
        }

    # QQQ options status
    if _qqq_executor:
        result["qqq_options"] = _qqq_executor.get_status()

    # Webull TQQQ status
    if _webull_executor:
        result["webull_tqqq"] = _webull_executor.get_status()

    # Reconciler status — visibility into background trade-log corrections
    reconcilers = {}
    if _gold_reconciler:
        reconcilers["gold"] = _gold_reconciler.status()
    if _tqqq_reconciler:
        reconcilers["tqqq"] = _tqqq_reconciler.status()
    if reconcilers:
        result["reconcilers"] = reconcilers

    # Worker queue depths -- non-zero depth means alerts are pending broker
    # round-trip. Sustained non-zero depth indicates broker is slow or stuck.
    workers = {}
    if _gold_worker:
        workers["gold"] = {"depth": _gold_worker.depth()}
    if _btc_worker:
        workers["btc"] = {"depth": _btc_worker.depth()}
    if _qqq_worker:
        workers["qqq"] = {"depth": _qqq_worker.depth()}
    if _webull_worker:
        workers["webull"] = {"depth": _webull_worker.depth()}
    if workers:
        result["workers"] = workers

    return result


@app.get("/health")
async def health():
    """Simple health check for monitoring."""
    return {"ok": True}


def _load_env():
    """Load secrets from .env file."""
    env = {}
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))
    env_file = os.path.join(project_root, "secrets", ".env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def start_server(
    broker,
    config: GoldScalperConfig,
    host: str = "0.0.0.0",
    port: int = 8000,
    webhook_url: str = "",
):
    """Start the webhook server with Gold + BTC executors.

    Args:
        broker: optional pre-constructed broker for the Gold executor.
                If None, we attempt to build a WebullMGCBroker from env vars
                (WEBULL_APP_KEY/SECRET + WEBULL_MGC_* — see
                webull_mgc_broker.build_from_env). When credentials are
                missing the gold executor runs in pure paper mode.
        config: Gold scalper config
        host: Bind address (0.0.0.0 for external access)
        port: Port number
        webhook_url: Discord webhook URL for alerts
    """
    global _gold_executor, _btc_executor, _webull_executor

    alerts = ScalperAlertEngine(webhook_url) if webhook_url else None

    # Load env first so build_from_env sees WEBULL_* keys
    env = _load_env()
    for key, value in env.items():
        os.environ.setdefault(key, value)

    # MGC broker = Tradovate REST API (R-hard, 2026-05-11).
    # Webull MGC has been discarded as an MGC broker per iii directive — the
    # build path is gone entirely. Webull TQQQ (separate executor below) is
    # unaffected.
    #
    # State machine:
    #   - Tradovate not in .env       -> broker stays None -> MGC paper mode
    #   - Tradovate configured, LIVE=0 -> Tradovate primary in DRY-RUN (paper
    #                                    trades through the executor; no real
    #                                    orders hit the network)
    #   - Tradovate configured, LIVE=1, auth FAILS -> broker stays None,
    #                                    MGC paper mode, LOUD error logged
    #                                    (user wanted live but auth broke)
    #   - Tradovate configured, LIVE=1, auth OK    -> Tradovate primary LIVE
    if broker is None:
        try:
            from src.gold_scalper.tradovate_executor import TradovateExecutor
            td_broker = TradovateExecutor.build_from_env()
            if td_broker is None:
                logger.info(
                    "[WEBHOOK] Tradovate not configured — MGC will run in "
                    "paper mode (no live broker)."
                )
            elif not td_broker.live_orders:
                # LIVE=0: Tradovate as primary in dry-run. Paper trades
                # still flow through the Tradovate executor's place_order
                # so the path mirrors the eventual live path.
                broker = td_broker
                logger.info(
                    "[WEBHOOK] Gold executor will use TradovateExecutor "
                    "in DRY-RUN (TRADOVATE_LIVE=0). symbol=%s",
                    td_broker.symbol,
                )
            else:
                # LIVE=1: must succeed at auth before becoming primary.
                td_broker.start()
                if td_broker._connected:
                    broker = td_broker
                    logger.info(
                        "[WEBHOOK] Gold executor will use TradovateExecutor "
                        "LIVE: symbol=%s account_id=%s",
                        td_broker.symbol, td_broker.account_id,
                    )
                    # Reconciler hooks (get_order_status, get_position_qty)
                    # were implemented 2026-05-12. The reconciler init block
                    # further below tests hasattr() and starts cleanly.
                else:
                    # 2026-10-14 (DNS boot race): keep the broker attached.
                    # place_order calls _ensure_token() and re-authenticates
                    # on the next signal; until then orders FAIL (entry is
                    # rejected + paper rolled back), never silently paper-
                    # filled. Reconciler hooks all return None while unauth,
                    # so startup reconcile + 60s reconciler degrade safely.
                    broker = td_broker
                    logger.error(
                        "[WEBHOOK] Tradovate LIVE requested but initial auth "
                        "FAILED: %s. Broker stays attached — auth retries on "
                        "each signal; entries fail (no paper fill) until it "
                        "succeeds.",
                        td_broker._last_auth_error,
                    )
                    if alerts is not None:
                        try:
                            alerts.notify(
                                "⚠️ MNQ webhook started with Tradovate auth "
                                f"FAILED ({td_broker._last_auth_error}); will "
                                "retry per signal — watch the first entry")
                        except Exception:
                            pass
        except Exception as exc:
            logger.error(
                "[WEBHOOK] TradovateExecutor init crashed: %s — "
                "MGC paper mode this run", exc,
            )

    # NT mirror — optional parallel live broker via the PythonBridge
    # NinjaScript AddOn (TCP, bidirectional). If NT_ACCOUNT_NAME +
    # NT_INSTRUMENT are set, wrap the (possibly-None) primary with
    # MultiBrokerFanout so each place_order is mirrored. Webull stays
    # source of truth; NT failures log + flip a desync flag but never
    # block Webull execution.
    try:
        from src.gold_scalper.ninjatrader_executor import NinjaTraderExecutor
        nt_broker = NinjaTraderExecutor.build_from_env()
        if nt_broker is not None:
            # Start the TCP client. Non-fatal if the AddOn isn't running yet
            # (background reconnect loop will keep trying).
            nt_broker.start()
            if broker is not None:
                from src.gold_scalper.multi_broker_fanout import MultiBrokerFanout
                broker = MultiBrokerFanout(primary=broker, secondary=nt_broker)
                logger.info(
                    "[WEBHOOK] Gold broker wrapped: primary=Webull, "
                    "mirror=NT (live=%s)", nt_broker.live_orders,
                )
            else:
                # No Webull primary: NT becomes the only broker. Fill
                # verification + reconciler assume Webull truth, so this
                # path runs without those checks. Allowed but warned.
                broker = nt_broker
                logger.warning(
                    "[WEBHOOK] No Webull broker — NT is the ONLY broker. "
                    "Reconciler is Webull-shaped; rely on NT events + NT UI."
                )
    except Exception as exc:
        logger.error(
            "[WEBHOOK] NT mirror init failed: %s — Webull continues alone", exc,
        )

    # Initialize Gold executor (broker is None for paper, WebullMGCBroker for live)
    # 2026-10-14: live_required tells the executor that paper-filling is not
    # acceptable — if broker ended up None despite TRADOVATE_LIVE=1 (init
    # crash, missing credentials), entries are refused instead of simulated.
    _td_live_requested = (
        os.environ.get("TRADOVATE_LIVE", "").strip().lower()
        in ("1", "true", "yes")
    )
    _gold_executor = WebhookExecutor(
        broker, config, alerts, live_required=_td_live_requested,
    )
    logger.info("[WEBHOOK] Gold executor initialized")

    global _gold_worker
    _gold_worker = ExecutorWorker(
        name="gold",
        fn=lambda b: _gold_executor.execute(_gold_executor.parse_signal(b)),
    )

    # Initialize BTC executor (Alpaca crypto paper) — env already loaded above
    btc_key = os.environ.get("ALPACA_CRYPTO_KEY", "")
    btc_secret = os.environ.get("ALPACA_CRYPTO_SECRET", "")

    if btc_key and btc_secret:
        from src.gold_scalper.btc_executor import BTCWebhookExecutor
        _btc_executor = BTCWebhookExecutor(
            api_key=btc_key,
            api_secret=btc_secret,
            alerts=alerts,
        )
        logger.info("[WEBHOOK] BTC executor initialized (Alpaca crypto paper)")
        global _btc_worker
        _btc_worker = ExecutorWorker(
            name="btc",
            fn=lambda b: _btc_executor.execute(b),
        )
    else:
        logger.warning("[WEBHOOK] No ALPACA_CRYPTO_KEY — BTC executor disabled")

    # Initialize QQQ options executor (uses same Alpaca account as BTC — third account)
    if btc_key and btc_secret:
        from src.gold_scalper.qqq_options_executor import QQQOptionsExecutor
        _qqq_executor = QQQOptionsExecutor(
            api_key=btc_key,
            api_secret=btc_secret,
        )
        logger.info("[WEBHOOK] QQQ options executor initialized (Alpaca paper, options level 3)")
        global _qqq_worker
        _qqq_worker = ExecutorWorker(
            name="qqq",
            fn=lambda b: _qqq_executor.execute(b),
        )
    else:
        logger.warning("[WEBHOOK] No ALPACA_CRYPTO_KEY — QQQ options executor disabled")

    # Webull TQQQ executor — env already loaded via setdefault above
    webull_key = os.environ.get("WEBULL_APP_KEY", "")
    webull_secret = os.environ.get("WEBULL_APP_SECRET", "")

    if webull_key and webull_secret:
        from src.gold_scalper.webull_tqqq_executor import WebullTQQQExecutor
        # Safe-by-default — real orders require explicit WEBULL_TQQQ_LIVE=1
        # (matches WEBULL_MGC_LIVE pattern). Without this flag a funded
        # Webull account would start firing real TQQQ orders the moment
        # the server starts, even if the operator only intended paper.
        tqqq_live_flag = os.environ.get("WEBULL_TQQQ_LIVE", "").strip().lower()
        tqqq_live = tqqq_live_flag in ("1", "true", "yes", "on")
        _webull_executor = WebullTQQQExecutor(
            app_key=webull_key,
            app_secret=webull_secret,
            live_orders=tqqq_live,
        )
        logger.info(
            "[WEBHOOK] Webull TQQQ executor initialized (live_orders=%s)",
            tqqq_live,
        )
        global _webull_worker
        _webull_worker = ExecutorWorker(
            name="webull",
            fn=lambda b: _webull_executor.execute(b),
        )
    else:
        logger.warning("[WEBHOOK] No WEBULL_APP_KEY — Webull TQQQ executor disabled")

    logger.info(
        "[WEBHOOK] Starting server on %s:%d — "
        "waiting for TradingView signals...",
        host, port,
    )
    # Describe gold route — Tradovate, Webull, or pure paper
    if _gold_executor and getattr(_gold_executor, "broker", None) is not None:
        gold_broker = _gold_executor.broker
        cls_name = type(gold_broker).__name__
        if "Tradovate" in cls_name:
            broker_label = "Tradovate"
        elif "Webull" in cls_name:
            broker_label = "Webull"
        elif "Fanout" in cls_name:
            primary = getattr(gold_broker, "primary", None)
            broker_label = type(primary).__name__ if primary else "Fanout"
        else:
            broker_label = cls_name
        suffix = "LIVE" if getattr(gold_broker, "live_orders", False) else "DRYRUN"
        gold_route = f"Gold/{broker_label}-{suffix} (symbol={gold_broker.symbol})"
    else:
        gold_route = "Gold (paper)"

    routes = [gold_route]
    if _btc_executor:
        routes.append("BTC/Alpaca (ticker=BTCUSD)")
    if _qqq_executor:
        routes.append("QQQ Options (ticker=QQQ)")
    if _webull_executor:
        routes.append("TQQQ/Webull (ticker=TQQQ)")
    logger.info("[WEBHOOK] Routes: %s", " + ".join(routes))

    # === Background reconcilers ===
    # For each LIVE executor with a Webull broker, spin up a reconciler that
    # re-checks SUBMITTED trade-log entries every 60s. This catches the case
    # where verify_fill timed out at order placement (5s wait) but the order
    # eventually filled or got cancelled. Without the reconciler, the trade
    # log would carry "SUBMITTED" forever with TV's estimated price.
    global _gold_reconciler, _tqqq_reconciler
    from src.gold_scalper.reconciler import TradeLogReconciler

    # Capture position once per call to avoid a read-twice race with the
    # executor thread nulling out .position between the guard and the
    # attribute access.
    def _gold_paper_qty():
        p = _gold_executor.position if _gold_executor else None
        if p is None:
            return 0
        return p.remaining_contracts if p.direction == "LONG" else -p.remaining_contracts

    def _tqqq_paper_qty():
        p = _webull_executor.position if _webull_executor else None
        if p is None:
            return 0
        return p.qty if p.direction == "LONG" else -p.qty

    # Gold MGC reconciler — only when broker is live AND implements the
    # Webull-shaped hooks (get_order_status, get_position_qty). Webull
    # broker has them; TradovateExecutor does not (yet). The hasattr
    # checks work through MultiBrokerFanout's __getattr__ proxy.
    if (_gold_executor and _gold_executor.broker is not None
            and getattr(_gold_executor.broker, "live_orders", False)
            and hasattr(_gold_executor.broker, "get_order_status")
            and hasattr(_gold_executor.broker, "get_position_qty")):
        gold_log = getattr(_gold_executor, "_log_file", None)
        if gold_log:
            _gold_reconciler = TradeLogReconciler(
                name="gold",
                log_path=gold_log,
                get_order_status=_gold_executor.broker.get_order_status,
                get_position_qty=_gold_paper_qty,
                get_broker_position_qty=_gold_executor.broker.get_position_qty,
                flatten_paper=_gold_executor.force_flatten,
            )
            _gold_reconciler.start()

    # TQQQ reconciler — only when live
    if _webull_executor and getattr(_webull_executor, "_live_orders", False):
        from src.gold_scalper.webull_tqqq_executor import _TRADE_LOG as TQQQ_LOG
        _tqqq_reconciler = TradeLogReconciler(
            name="tqqq",
            log_path=TQQQ_LOG,
            get_order_status=_webull_executor._get_order_status,
            get_position_qty=_tqqq_paper_qty,
            get_broker_position_qty=_webull_executor.get_broker_position_qty,
            flatten_paper=_webull_executor.force_flatten,
        )
        _tqqq_reconciler.start()

    logger.info(
        "[WEBHOOK] Set TradingView alert webhook URL to: "
        "https://<your-cloudflare-url>/webhook"
    )

    # Run uvicorn
    loop = asyncio.get_event_loop()
    nest_asyncio.apply(loop)

    uvi_config = uvicorn.Config(
        app, host=host, port=port,
        log_level="info", access_log=True,
    )
    server = uvicorn.Server(uvi_config)
    loop.run_until_complete(server.serve())
