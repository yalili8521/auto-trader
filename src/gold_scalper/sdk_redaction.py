"""Webull SDK credential redaction — import before any webull SDK use.

This module is the production-side twin of the redaction layer in
``scripts/webull_mgc_roundtrip_test.py``. It exists because the Webull
Python SDK, by default, does two things that leak API credentials:

1. ``ApiClient.__init__`` calls ``set_stream_logger`` and
   ``set_file_logger`` (gated on ``_stream_logger_set`` and
   ``_file_logger_set`` instance flags), which install handlers on the
   ``webull.core`` logger that dump the full HTTP request dict —
   including ``x-app-key``, ``x-app-secret``, ``x-signature``,
   ``x-signature-nonce``, ``x-timestamp`` headers — on every API call.
   The stream handler writes to stdout; the file handler writes to
   ``webull_trade_sdk.log`` in the process CWD.

2. SDK exceptions (``ServerException`` and friends) stringify into the
   same full request dict. If any code path prints ``str(exc)`` or
   passes an SDK exception to ``logger.error(..., exc)``, the headers
   end up on stderr or in a log file.

We defend against both with three layers:

A. Filter on ``logging.lastResort``. In Python's logging design,
   parent-logger filters are NOT applied to records that propagate up
   the hierarchy — only handler-level filters are. When no handler is
   attached to the root logger, records from child loggers fall
   through to ``logging.lastResort`` (the default WARNING-level stderr
   handler). Installing our ``RedactingFilter`` on ``lastResort`` is
   the only way to sanitize those propagated records.

B. ``webull.core`` logger neutralized: existing handlers removed,
   NullHandler added, level set to CRITICAL, ``propagate=False``,
   ``RedactingFilter`` attached. If the SDK ever adds its own handler
   post-install, the logger is a dead end.

C. Per-ApiClient disable: ``disable_sdk_default_logger(api_client)``
   sets ``_stream_logger_set = True`` and ``_file_logger_set = True``
   BEFORE ``TradeClient(api_client)`` / ``DataClient(api_client)``
   construction, so the SDK skips its own ``set_stream_logger`` /
   ``set_file_logger`` handler install entirely — no file, no stream.

Usage
-----
At the top of any entry-point module that (directly or transitively)
imports webull SDK classes::

    from src.gold_scalper.sdk_redaction import install_log_redaction
    install_log_redaction()

At every ``ApiClient(...)`` construction site::

    from src.gold_scalper.sdk_redaction import disable_sdk_default_logger
    api_client = ApiClient(app_key=..., app_secret=..., region_id=...)
    disable_sdk_default_logger(api_client)
    trade_client = TradeClient(api_client)  # SDK skips logger install

The ``redact(text)`` helper is also exported for ad-hoc use when
printing broker responses or exception messages.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Any

# Regex patterns that match credential values in common SDK dump formats.
# Order matters — header-prefix patterns run first so their replacements
# don't get double-replaced by the catch-all hex/base64 rules.
_SENSITIVE_PATTERNS = [
    # Dict-style dumps: "x-app-key": "<value>" or 'x-app-key': '<value>'
    (re.compile(r'(x-app-key["\']?\s*[:=]\s*["\']?)[^"\',\s}]+'),
     r'\1<REDACTED>'),
    (re.compile(r'(x-app-secret["\']?\s*[:=]\s*["\']?)[^"\',\s}]+'),
     r'\1<REDACTED>'),
    (re.compile(r'(x-signature["\']?\s*[:=]\s*["\']?)[^"\',\s}]+'),
     r'\1<REDACTED>'),
    (re.compile(r'(x-signature-nonce["\']?\s*[:=]\s*["\']?)[^"\',\s}]+'),
     r'\1<REDACTED>'),
    (re.compile(r'(x-timestamp["\']?\s*[:=]\s*["\']?)[^"\',\s}]+'),
     r'\1<REDACTED>'),
    # TradingView webhook passphrase -- present in raw POST body which
    # webhook_server.py logs at INFO after successful auth. Shares the
    # same leak surface as Webull headers once a root StreamHandler is
    # wired up.
    (re.compile(r'(passphrase["\']?\s*[:=]\s*["\']?)[^"\',\s}]+'),
     r'\1<REDACTED>'),
    # Catch-all: 32+ char hex string (typical API key / signature format)
    (re.compile(r'\b[0-9a-fA-F]{32,}\b'), '<REDACTED_HEX>'),
    # Long base64-ish string (signatures often look like this)
    (re.compile(r'\b[A-Za-z0-9+/]{28,}={0,2}\b'), '<REDACTED_B64>'),
]


def redact(text: Any) -> str:
    """Strip secrets from arbitrary input. Always safe to call.

    Converts non-strings via ``str(text)``. On any conversion or regex
    failure returns a placeholder rather than raising — redaction must
    never itself become a failure point.
    """
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception:
            return "<unstringifiable>"
    try:
        for pat, repl in _SENSITIVE_PATTERNS:
            text = pat.sub(repl, text)
    except Exception:
        return "<redact-failed>"
    return text


class RedactingFilter(logging.Filter):
    """Sanitize every log record that flows through a handler with this filter.

    Rewrites both ``record.msg`` and any string items in ``record.args``.
    Non-string args (ints, dicts, exception objects) pass through
    untouched — but be careful: if a caller does
    ``logger.error("%s", exc)``, the exception's ``__str__`` result gets
    formatted at emit time and bypasses this filter's args-level redact.
    The lastResort handler formatter sees the final formatted message
    as ``record.getMessage()``, so wrapping format-level redaction would
    require a custom Formatter. In practice the three defense layers
    plus the ``_stream_logger_set`` flag make the args-level gap moot
    for the production webhook path.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            if isinstance(record.msg, str):
                record.msg = redact(record.msg)
            if record.args:
                # Python's LogRecord.__init__ unwraps args=(mapping,) into
                # args=mapping, so record.args may be a Mapping (dict) rather
                # than a tuple. Iterating a Mapping yields keys, which would
                # replace the original dict with a tuple of its keys -- then
                # %-formatting sees ('key1', 'key2') instead of {...} and
                # raises TypeError. Handle the two arg-shapes separately.
                if isinstance(record.args, Mapping):
                    record.args = {
                        k: (redact(v) if isinstance(v, str) else v)
                        for k, v in record.args.items()
                    }
                else:
                    new_args = []
                    for arg in record.args:
                        if isinstance(arg, str):
                            new_args.append(redact(arg))
                        else:
                            new_args.append(arg)
                    record.args = tuple(new_args)
        except Exception:
            # Never let a redaction bug block logging — silently drop
            # the modification and let the original record through.
            pass
        return True


_INSTALLED = False


def install_log_redaction() -> None:
    """Silence + sanitize Webull SDK logs. Idempotent.

    Call this once at the top of any entry-point module that (directly
    or transitively) will import webull SDK classes. Must run BEFORE
    the first ``ApiClient(...)`` construction.

    After this call:
    - ``logging.lastResort`` has a ``RedactingFilter`` attached, so any
      un-handled log record from any child logger gets sanitized before
      reaching stderr.
    - The root logger has a ``RedactingFilter`` attached (covers the
      edge case of records created directly on root).
    - The ``webull.core`` logger is neutralized: existing handlers
      removed, NullHandler added, level CRITICAL, propagate=False,
      filter attached.
    """
    global _INSTALLED
    if _INSTALLED:
        return

    filt = RedactingFilter()

    # Layer A — lastResort handler (the critical one).
    if logging.lastResort is not None:
        logging.lastResort.addFilter(filt)

    # Layer B — root logger + any existing handlers.
    root = logging.getLogger()
    root.addFilter(filt)
    for h in root.handlers:
        h.addFilter(filt)

    # Layer C — neutralize the SDK's own logger.
    webull_log = logging.getLogger("webull.core")
    for h in list(webull_log.handlers):
        webull_log.removeHandler(h)
    webull_log.addHandler(logging.NullHandler())
    webull_log.addFilter(filt)
    webull_log.setLevel(logging.CRITICAL)
    webull_log.propagate = False

    _INSTALLED = True


def disable_sdk_default_logger(api_client: Any) -> None:
    """Tell a Webull ``ApiClient`` not to install its default log handlers.

    Must be called BEFORE ``TradeClient(api_client)`` or
    ``DataClient(api_client)`` construction. Those constructors call
    ``api_client.set_stream_logger`` / ``set_file_logger`` which are
    guarded by these two instance flags — setting them True makes the
    constructor skip the handler install entirely, so no
    ``webull_trade_sdk.log`` file is created and nothing is written to
    stdout.

    Never raises: if the ApiClient instance doesn't expose these
    attributes (e.g. a future SDK version that renames them), we log a
    single WARNING so an SDK-upgrade regression is loud instead of
    silent. Layers A and C in ``install_log_redaction`` still protect
    the logger itself, so a broken per-ApiClient gate is a degraded
    state — not a credential leak — but we want to know about it.
    """
    any_set = False
    for attr in ("_stream_logger_set", "_file_logger_set"):
        try:
            if hasattr(api_client, attr):
                setattr(api_client, attr, True)
                any_set = True
        except Exception:
            pass
    if not any_set:
        # Loud regression signal. Use a module-scoped logger so the
        # RedactingFilter attached to root/lastResort still sanitizes
        # the warning (in case the api_client repr leaks anything).
        logging.getLogger(__name__).warning(
            "[SDK_REDACTION] disable_sdk_default_logger: neither "
            "_stream_logger_set nor _file_logger_set present on "
            "ApiClient — SDK may have renamed these flags. Layer C "
            "(webull.core logger neutralized) still protects credentials, "
            "but per-ApiClient gate is no longer active."
        )
