"""Verbose Tradovate auth debug — raw HTTP response dump.

Bypasses TradovateExecutor and calls /auth/accesstokenrequest with
requests.post directly so we can see everything Tradovate sends back:
status code, response headers, full body. Useful when the executor's
parsed error ("incorrect username or password") doesn't tell us why.

Password is read from .env but NEVER printed.

Run from auto-trader root:
    .venv\\Scripts\\python.exe scripts\\tradovate_auth_debug.py
"""

from __future__ import annotations

import json
import os
import sys

import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(PROJECT_ROOT, "secrets", ".env")

env = {}
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k:
                env[k] = v

def _need(key: str) -> str:
    val = env.get(key, "")
    if not val:
        print(f"FAIL: .env missing {key}")
        sys.exit(1)
    return val

username     = _need("TRADOVATE_USERNAME")
password     = _need("TRADOVATE_PASSWORD")
cid_str      = _need("TRADOVATE_CID")
secret       = _need("TRADOVATE_SECRET")
app_id       = env.get("TRADOVATE_APP_ID", "iii-gold-scalper")
app_version  = env.get("TRADOVATE_APP_VERSION", "1.0")
device_id    = _need("TRADOVATE_DEVICE_ID")
api_url      = env.get("TRADOVATE_API_URL", "https://live.tradovateapi.com/v1").rstrip("/")

body = {
    "name": username,
    "password": password,
    "appId": app_id,
    "appVersion": app_version,
    "cid": int(cid_str) if cid_str.isdigit() else cid_str,
    "sec": secret,
    "deviceId": device_id,
}

# Length-only redaction. NEVER prints any characters of a secret.
# Earlier version printed first/last 3 chars; that leaks too much.
def _redact(s: str, keep: int = 0) -> str:
    if not s:
        return "<blank>"
    return f"<{len(s)} chars>"

print("=== Request (values redacted) ===")
print(f"  URL:       {api_url}/auth/accesstokenrequest")
print(f"  name:      {username}")
print(f"  password:  {_redact(password)}")
print(f"  appId:     {app_id}")
print(f"  appVersion:{app_version}")
print(f"  cid:       {body['cid']}  (type={type(body['cid']).__name__})")
print(f"  sec:       {_redact(secret, 6)}")
print(f"  deviceId:  {_redact(device_id, 6)}")
print()

print("=== Sending POST ===")
try:
    r = requests.post(
        f"{api_url}/auth/accesstokenrequest",
        json=body,
        timeout=10.0,
    )
except requests.RequestException as exc:
    print(f"NETWORK ERROR: {exc}")
    sys.exit(2)

print(f"HTTP {r.status_code} {r.reason}")
print()
print("=== Response headers ===")
for k, v in r.headers.items():
    print(f"  {k}: {v}")
print()
print("=== Response body (raw text) ===")
print(r.text)
print()
print("=== Response body (parsed JSON, if any) ===")
try:
    parsed = r.json()
    # Redact tokens if present (they're sensitive even if auth fails).
    if isinstance(parsed, dict):
        for k in ("accessToken", "mdAccessToken"):
            if parsed.get(k):
                parsed[k] = _redact(parsed[k], 8)
    print(json.dumps(parsed, indent=2))
except Exception as exc:
    print(f"(not JSON: {exc})")
