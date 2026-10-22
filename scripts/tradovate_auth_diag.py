"""Tradovate auth diagnostic — try multiple body shapes to narrow down the failure.

Tests in order:
  1. Body MATCHING Tradovate's official example (no deviceId)
  2. Body with deviceId (current executor shape)
  3. Body with cid as string instead of int
  4. Body with name = email (user@example.com)
  5. Body with NO password (sanity check — should fail differently)

Each test prints status code + truncated response body. Secrets are NEVER printed.
"""

from __future__ import annotations

import json
import os
import sys

import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(PROJECT_ROOT, "secrets", ".env")

env = {}
with open(ENV_PATH, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")

username    = env["TRADOVATE_USERNAME"]
password    = env["TRADOVATE_PASSWORD"]
cid_str     = env["TRADOVATE_CID"]
secret      = env["TRADOVATE_SECRET"]
app_id      = env.get("TRADOVATE_APP_ID", "auto-trader")
app_version = env.get("TRADOVATE_APP_VERSION", "1.0")
device_id   = env["TRADOVATE_DEVICE_ID"]
api_url     = env.get("TRADOVATE_API_URL", "https://live.tradovateapi.com/v1").rstrip("/")
url         = f"{api_url}/auth/accesstokenrequest"

# The .env username appears to be a handle, not an email. Email guess.
email_guess = "user@example.com"

print(f"Target: {url}")
print(f"Username length: {len(username)}, Password length: {len(password)}, "
      f"CID: {cid_str}, Secret length: {len(secret)}, "
      f"DeviceId length: {len(device_id)}")
print()

def try_auth(label: str, body: dict) -> None:
    print(f"--- {label} ---")
    # Build redacted summary of fields used (key names only)
    print("  Fields:", list(body.keys()))
    try:
        r = requests.post(url, json=body, timeout=10.0)
        print(f"  HTTP {r.status_code}")
        try:
            j = r.json()
            # Print response keys + non-secret values, redact tokens
            for k, v in (j.items() if isinstance(j, dict) else []):
                if k in ("accessToken", "mdAccessToken"):
                    print(f"    {k}: <{len(str(v))} chars>")
                else:
                    print(f"    {k}: {v}")
        except Exception:
            print(f"  body (truncated): {r.text[:200]}")
    except Exception as exc:
        print(f"  ERROR: {exc}")
    print()


# Test 1: matches Tradovate's official example exactly
try_auth("Test 1: official example shape (NO deviceId)", {
    "name": username,
    "password": password,
    "appId": app_id,
    "appVersion": app_version,
    "cid": int(cid_str),
    "sec": secret,
})

# Test 2: current executor shape (with deviceId)
try_auth("Test 2: current executor (WITH deviceId)", {
    "name": username,
    "password": password,
    "appId": app_id,
    "appVersion": app_version,
    "cid": int(cid_str),
    "sec": secret,
    "deviceId": device_id,
})

# Test 3: cid as string
try_auth("Test 3: cid as string instead of int", {
    "name": username,
    "password": password,
    "appId": app_id,
    "appVersion": app_version,
    "cid": cid_str,  # string, not int
    "sec": secret,
})

# Test 4: name = email
try_auth(f"Test 4: name = email ({email_guess})", {
    "name": email_guess,
    "password": password,
    "appId": app_id,
    "appVersion": app_version,
    "cid": int(cid_str),
    "sec": secret,
})

# Test 5: no password — should fail with different error (control)
try_auth("Test 5: no password (control / sanity check)", {
    "name": username,
    "password": "",
    "appId": app_id,
    "appVersion": app_version,
    "cid": int(cid_str),
    "sec": secret,
})

print("Done. Compare error messages across tests — different messages = different failure modes.")
