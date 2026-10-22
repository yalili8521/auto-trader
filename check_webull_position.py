"""One-shot: print actual Webull positions + balance for the MGC/TQQQ account.

Read-only. No orders placed. Loads creds from secrets/.env.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def main() -> int:
    _load_env(Path(__file__).parent / "secrets" / ".env")

    app_key = os.environ.get("WEBULL_APP_KEY", "")
    app_secret = os.environ.get("WEBULL_APP_SECRET", "")
    explicit_account_id = os.environ.get("WEBULL_MGC_ACCOUNT_ID", "").strip()

    if not app_key or not app_secret:
        print("ERROR: WEBULL_APP_KEY / WEBULL_APP_SECRET not set", file=sys.stderr)
        return 2

    from webull.core.client import ApiClient
    from webull.core.common.region import Region
    from webull.trade.trade_client import TradeClient
    from src.gold_scalper.sdk_redaction import disable_sdk_default_logger

    api = ApiClient(app_key=app_key, app_secret=app_secret, region_id=Region.US.value)
    disable_sdk_default_logger(api)
    tc = TradeClient(api)

    if explicit_account_id:
        account_id = explicit_account_id
    else:
        resp = tc.account_v2.get_account_list()
        accounts = resp.json() if hasattr(resp, "json") else resp
        if isinstance(accounts, dict):
            accounts = accounts.get("data", accounts.get("accounts", []))
        if not accounts:
            print("ERROR: no accounts returned", file=sys.stderr)
            return 3
        first = accounts[0]
        account_id = str(first.get("account_id", first.get("accountId", "")))

    print(f"account_id: {account_id}")

    bal = tc.account_v2.get_account_balance(account_id)
    bal = bal.json() if hasattr(bal, "json") else bal
    print("\n=== BALANCE ===")
    print(json.dumps(bal, indent=2, default=str))

    pos = tc.account_v2.get_account_position(account_id)
    pos = pos.json() if hasattr(pos, "json") else pos
    print("\n=== POSITIONS ===")
    print(json.dumps(pos, indent=2, default=str))

    return 0


if __name__ == "__main__":
    sys.exit(main())
