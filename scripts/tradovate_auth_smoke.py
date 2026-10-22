"""One-off Tradovate auth smoke test.

Loads secrets/.env, builds TradovateExecutor, calls _authenticate() +
(optionally) _resolve_account_id() against TRADOVATE_API_URL. Prints a
short PASS/FAIL summary. Places NO orders. Safe to run repeatedly.

Run from the auto-trader root:

    .venv\\Scripts\\python.exe scripts\\tradovate_auth_smoke.py

Exits 0 on PASS, non-zero on FAIL (so it can be used in CI / health
checks later if useful).
"""

from __future__ import annotations

import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Minimal .env loader — avoid coupling to webhook_server internals.
ENV_PATH = os.path.join(PROJECT_ROOT, "secrets", ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and v:
                os.environ.setdefault(k, v)

from src.gold_scalper.tradovate_executor import TradovateExecutor

td = TradovateExecutor.build_from_env()
if td is None:
    print("FAIL: TradovateExecutor.build_from_env() returned None — "
          "required env vars missing. Check secrets/.env.")
    sys.exit(1)

print(f"Target API: {td.api_url}")
print(f"User:       {td.username}")
print(f"Symbol:     {td.symbol}")
print(f"Account ID: {td.account_id if td.account_id else '(will resolve)'}")
print(f"App ID:     {td.app_id}")
print()

print("--> POST /auth/accesstokenrequest ...")
try:
    td._authenticate()
except Exception as exc:
    print(f"FAIL: auth raised: {exc}")
    sys.exit(2)

if not td._access_token:
    print(f"FAIL: no access token. last_auth_error={td._last_auth_error}")
    sys.exit(3)

remaining = int(td._token_expiry_ts - time.time())
print(f"    OK -- token good for {remaining}s")
print()

if td.account_id is None:
    print("--> GET /account/list ...")
    try:
        td._resolve_account_id()
    except Exception as exc:
        print(f"FAIL: account resolution: {exc}")
        sys.exit(4)
    print(f"    OK -- account_id={td.account_id}")
else:
    print("--> /account/list skipped (TRADOVATE_ACCOUNT_ID preset in env)")

print()
print("PASS: auth + account ready. No orders placed.")
print(f"      url={td.api_url}  symbol={td.symbol}  account={td.account_id}")
sys.exit(0)
