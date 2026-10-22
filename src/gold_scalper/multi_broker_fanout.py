"""MultiBrokerFanout -- fan place_order calls to a primary + secondary broker.

Wraps two brokers so WebhookExecutor sees one. The PRIMARY (Webull MGC) is
source of truth for fill tracking, paper state, reconciler, and live
equity. The SECONDARY (NinjaTraderExecutor) runs as an independent mirror
for parallel-live exposure on a second platform.

Rules:
- Primary's place_order result is what gets returned. WebhookExecutor's
  state machine follows primary semantics (PENDING_OPEN, fill_status,
  rollback on rejection, etc.).
- Secondary place_order errors are logged but never propagate. A secondary
  rejection or exception does not block primary execution.
- Methods other than place_order proxy to primary unchanged via __getattr__
  (this covers get_live_equity, symbol, _verify_fill, etc.).

This is intentionally NOT a generic 'broker pool' or 'order router'. It's
a thin fan-out for one specific use case: Webull primary + NT mirror.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class MultiBrokerFanout:
    def __init__(self, primary, secondary):
        self.primary = primary
        self.secondary = secondary

    def place_order(self, **kwargs) -> dict:
        primary_result = self.primary.place_order(**kwargs)

        try:
            secondary_result = self.secondary.place_order(**kwargs)
            if not secondary_result.get("success"):
                logger.warning(
                    "[FANOUT] Secondary REJECTED order (primary continues): %s",
                    secondary_result.get("error", "unknown"),
                )
        except Exception as exc:
            logger.exception("[FANOUT] Secondary place_order raised: %s", exc)

        return primary_result

    def get_status(self) -> dict:
        primary_status: dict = {}
        secondary_status: dict = {}
        if hasattr(self.primary, "get_status"):
            try:
                primary_status = self.primary.get_status()
            except Exception as exc:
                primary_status = {"error": str(exc)}
        if hasattr(self.secondary, "get_status"):
            try:
                secondary_status = self.secondary.get_status()
            except Exception as exc:
                secondary_status = {"error": str(exc)}
        return {"primary": primary_status, "secondary": secondary_status}

    def __getattr__(self, name: str) -> Any:
        """Proxy any other attribute access to primary.

        Covers get_live_equity, symbol, _verify_fill, and any future method
        WebhookExecutor calls that we did not anticipate. Note: __getattr__
        is only invoked when normal lookup fails, so place_order and
        get_status (defined above) are NOT proxied -- they hit our overrides.
        """
        return getattr(self.primary, name)
