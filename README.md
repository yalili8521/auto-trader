# auto-trader

A production webhook pipeline that turns TradingView strategy alerts into live futures
orders, and keeps running unattended. Python 3.12, FastAPI, one broker REST API, two
Windows services, a reconciler, and a test suite that runs without any broker account.

This repository is the **execution layer**. The trading strategies themselves (Pine
Script) are not published; the server treats them as an alert source.

```
 TradingView strategy --alert JSON--> Cloudflare tunnel --> FastAPI /webhook
                                                               |
                        passphrase check, parse, route by ticker
                                                               |
                      WebhookExecutor (paper position tracker, TP ladder, P&L,
                      daily loss breaker, sizing from live equity)
                                                               |
                      MultiBrokerFanout --> primary broker (Tradovate REST)
                                        +-> optional mirror (NinjaTrader 8 TCP bridge)
                                                               |
                      Reconciler (60 s): broker position vs paper state --> Slack alerts
```

## What it does

- **Receives** TradingView alerts over HTTPS through a Cloudflare tunnel, verifies a
  shared passphrase, and routes by ticker to the right executor (futures, equity,
  crypto, options; the last three are earlier Webull and Alpaca paths kept for history).
- **Tracks** the position in a local paper ledger (entry, four take-profit levels,
  runner, stop), books P&L, and refuses new entries when the day's realized loss hits
  the limit. The daily breaker reads **real account equity** from the broker, so losses
  booked outside this process (a broker-side liquidation) still trip it.
- **Executes** on Tradovate through its REST API, with a per-order size cap set in the
  environment, and can fan out the same order to a NinjaTrader 8 account through a
  small NinjaScript AddOn that speaks newline-delimited JSON over TCP
  (`ninjatrader_addon/PythonBridge.cs`, `ninjatrader_executor.py`).
- **Reconciles** every 60 seconds: compares the broker's position snapshot with the
  paper ledger, auto-clears phantom paper positions, logs anything it will not fix
  on its own, and posts to Slack.
- **Runs as services**: the webhook server and the tunnel are NSSM-wrapped Windows
  services that start at boot and restart on crash (`ops/install_services.ps1`).

## Incidents that shaped the code

Each of these is a real outage; the fix and its test are in the tree.

| Date | What happened | Fix |
|---|---|---|
| 2026-05-19 | Reconciler read a stale broker snapshot one second after an entry, concluded the paper position was a phantom, and wiped it without closing the broker position. The real position sat unmanaged for 4.5 hours. | Phantom auto-clear now requires the same `(paper_qty, broker_qty)` drift to persist across two consecutive cycles (`reconciler.py`). Regression test covers race, real phantom, state flux, and manual-review paths. |
| 2026-05-19 to 21 | Launcher tied to the interactive logon died on sleep and window close. Three outages in three days. | Moved to NSSM services in Session 0 (`ops/install_services.ps1`), with `PYTHONUTF8=1` because the LocalSystem code page broke the `.env` loader. |
| 2026-10-14 | Machine rebooted before DNS was up; broker auth failed once and the server silently fell back to paper mode for eight hours with live trading enabled. One entry was missed. | Broker handle is kept and re-authenticated lazily on the next order; when live trading is requested and no broker is attached, entries are **refused** and an alert is sent instead of a silent downgrade (`webhook_server.py`, `webhook_executor.py`). |
| 2026-10-12 | A loss was booked by the broker outside this process, so the in-process daily breaker never saw it. | Breaker now snapshots day-start equity from the broker and compares against live net liquidation (`get_live_equity`). |

`KNOWN_ISSUE_chart_vs_alert_position_guard.md` documents a divergence between the
TradingView simulated position state and the live one that no code change here can
fix; it is kept because it explains several "the chart shows a trade the server never
took" questions.

## Credential hygiene

- All secrets live in `secrets/.env` (gitignored; `secrets/.env.example` lists the keys).
  Nothing in the source tree contains a key, an account id, or a hostname.
- `sdk_redaction.py` is imported before the Webull SDK because that SDK logs request
  signatures and credentials by default.
- Diagnostic scripts print lengths of secrets, never values.

## Tests

No broker account or network needed; brokers are mocked.

```
.venv\Scripts\python.exe -m unittest test_tradovate_executor   # 49 tests: auth, order placement, position snapshot, size cap, reconcile paths
.venv\Scripts\python.exe -m unittest test_nt_executor          # 16 tests: NinjaTrader TCP bridge protocol, ACK/REJECT, reconnect
.venv\Scripts\python.exe -m unittest test_alert_to_executor    # 11 tests: alert payload -> router -> broker, run synchronously against the same executor call the worker queue makes
```

## Running it

```
uv venv && uv pip install -e .
cp secrets/.env.example secrets/.env      # fill in broker credentials
.venv\Scripts\python.exe main.py webhook  # serves on :8000
.venv\Scripts\python.exe main.py status   # GET /status as JSON
```

For unattended operation on Windows, edit the paths at the top of
`ops/install_services.ps1` and run it from an elevated PowerShell. `ops/run_webhook.ps1`
is the attached, single-window launcher.

Required environment (see `main.py` and `config.py` for the full list): `WEBHOOK_SECRET`,
`TRADOVATE_USERNAME`, `TRADOVATE_PASSWORD`, `TRADOVATE_CID`, `TRADOVATE_SECRET`,
`TRADOVATE_APP_ID`, `TRADOVATE_DEVICE_ID`, `TRADOVATE_SYMBOL`,
`TRADOVATE_MAX_POSITION_QTY`, `TRADOVATE_LIVE` (0 = dry run), `ALERT_WEBHOOK_URL` (Slack).

## Layout

```
main.py                       CLI: webhook | status; loads secrets/.env
src/gold_scalper/
  webhook_server.py           FastAPI receiver, passphrase check, routing, /status, startup reconcile
  webhook_executor.py         paper position tracker, TP ladder, P&L, daily breaker, sizing
  tradovate_executor.py       Tradovate REST: auth, orders, positions, live equity
  reconciler.py               60 s broker-vs-paper reconciliation with 2-cycle grace
  multi_broker_fanout.py      primary + mirror broker behind one interface
  ninjatrader_executor.py     TCP client for the NinjaTrader AddOn
  webull_mgc_broker.py, webull_tqqq_executor.py, btc_executor.py, qqq_options_executor.py
                              earlier broker paths (Webull futures/equity, Alpaca crypto/options)
  alerts.py                   Slack / Discord notifications
  sdk_redaction.py            credential redaction for the Webull SDK logger
  config.py                   strategy parameters (config/gold_scalper.json)
ninjatrader_addon/PythonBridge.cs   NinjaScript AddOn: TCP JSON order bridge
ops/                          NSSM service installer and attached launcher
scripts/                      backtest ports, walk-forward and Monte Carlo risk analysis, auth diagnostics
test_*.py                     unit tests (unittest + mocks)
```

## Status

Live on a small Tradovate account since April 2026 (gold micro futures), repointed to
Nasdaq micro futures in October 2026. Single position at a time, market orders only; the
stop and take-profit alerts of the strategy itself manage exits. Not financial advice; the
interesting part of this repository is the plumbing, not the P&L.

## License

MIT
