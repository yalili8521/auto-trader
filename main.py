"""Auto-Trader — standalone live webhook pipeline.

Receives TradingView alerts, executes on Webull (MGC, TQQQ) / Alpaca (BTC, QQQ).

Runs independently of the quantitivestocks dev folder. Has its own venv,
secrets, state, and trade log. Changes here don't affect the ETF paper
trader, and changes there don't affect this live pipeline.

Usage
-----
    .venv/Scripts/python.exe main.py webhook          # start server (default port 8000)
    .venv/Scripts/python.exe main.py webhook --port 8000
    .venv/Scripts/python.exe main.py status           # curl /status
"""

from __future__ import annotations

import argparse
import os
import sys


def cmd_webhook(args: argparse.Namespace) -> None:
    """Start the gold-scalper webhook server (MGC + TQQQ + BTC + QQQ)."""
    # Load secrets before importing gold_scalper so env vars are visible
    _load_env_file()

    from src.gold_scalper.webhook_server import start_server
    from src.gold_scalper.config import load_config

    config = load_config()
    webhook_url = os.environ.get("ALERT_WEBHOOK_URL", "")
    start_server(broker=None, config=config, host="0.0.0.0",
                 port=args.port, webhook_url=webhook_url)


def cmd_status(args: argparse.Namespace) -> None:
    """Print local /status endpoint as JSON (convenience wrapper)."""
    import json
    import urllib.request
    url = f"http://localhost:{args.port}/status"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
        print(json.dumps(data, indent=2))
    except Exception as exc:
        print(f"ERROR: Cannot reach {url}: {exc}", file=sys.stderr)
        sys.exit(1)


def _load_env_file() -> None:
    """Populate os.environ from secrets/.env (setdefault — shell wins)."""
    project_root = os.path.dirname(os.path.abspath(__file__))
    env_file = os.path.join(project_root, "secrets", ".env")
    if not os.path.exists(env_file):
        print(f"WARNING: {env_file} not found — env vars not loaded from file",
              file=sys.stderr)
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main() -> None:
    parser = argparse.ArgumentParser(prog="auto-trader")
    sub = parser.add_subparsers(dest="command", required=True)

    p_webhook = sub.add_parser("webhook", help="Start webhook server")
    p_webhook.add_argument("--port", type=int, default=8000,
                           help="Port to bind (default 8000)")
    p_webhook.set_defaults(func=cmd_webhook)

    p_status = sub.add_parser("status", help="Print /status JSON")
    p_status.add_argument("--port", type=int, default=8000)
    p_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
