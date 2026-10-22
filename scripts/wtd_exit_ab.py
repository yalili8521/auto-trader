"""WTD exit-layer A/B: does trailing the runner beat dumping it at a fixed TP2?

Reuses the validated v3.2 port (backtest_v32_variants) for DATA, INDICATORS,
ENTRIES, STOP, SIZING, SESSIONS — all held IDENTICAL. The ONLY thing that
varies is the exit rule for the 2nd lot after TP1:

  BASE        : take 1 lot at TP1, dump the 2nd at fixed TP2 (220 pips). [as-is]
  TP2_FAR     : same, but push TP2 to 400 pips (partial de-truncation).
  TRAIL       : take 1 lot at TP1, then NO upper target — 2nd lot rides the
                8-bar swing-trail (15-pip offset) to SL / session / timeout.
  TRAIL_ATR   : same trail, but offset = max(1.0*ATR, 20 pips) — vol-adaptive,
                mirrors today's LTM band-trail finding on gold.

Entry model, slippage, commission, and the conservative "SL-first on a
both-touched bar" rule are inherited verbatim. This is a differential test on
the SAME bars, not an absolute-PF claim (Yahoo MGC=F 60d 5m, Apr-23 snapshot).

Also reports MFE-after-TP1: how far price actually travels past entry once
TP1 is in hand. If the median MFE >> 220 pips, TP2 is truncating the tail and
the trail should win; if not, the ladder is already capturing the move.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backtest_v32_variants as base

# Inherit every param verbatim
PIP = base.PIP_SIZE
TICKV = base.TICK_VALUE
COMM = base.COMMISSION
SLIP = base.SLIPPAGE_PIP * PIP
NUM = base.NUM_CONTRACTS
TP1_PIPS = base.TP1_PIPS
TP2_PIPS = base.TP2_PIPS
SL_AFTER_TP1 = base.SL_AFTER_TP1
TRAIL_BARS = base.TRAIL_BARS
TRAIL_OFFSET = base.TRAIL_OFFSET
TIMEOUT_HOURS = base.TIMEOUT_HOURS
START = base.STARTING_EQUITY


def run(df, *, min_tf_agree, exit_mode, tp2_pips=TP2_PIPS,
        trail_offset_pips=TRAIL_OFFSET, trail_atr_mult=1.0, trail_floor_pips=20.0):
    o = df["o"].to_numpy(); h = df["h"].to_numpy(); l = df["l"].to_numpy(); c = df["c"].to_numpy()
    rsi_a = df["rsi"].to_numpy(); bull = df["bull_count"].to_numpy(); bear = df["bear_count"].to_numpy()
    beng = df["bull_eng"].to_numpy(); seng = df["bear_eng"].to_numpy()
    vol_ok = df["vol_ok"].fillna(False).to_numpy(); wt_ok = df["wt_long_ok"].fillna(False).to_numpy()
    ct = df["can_trade"].to_numpy(); sc = df["sess_close"].to_numpy()
    sl_d = df["sl_dist"].to_numpy(); atr_a = df["atr"].to_numpy()
    idx = df.index; n = len(df)

    tp1_d = TP1_PIPS * PIP; tp2_d = tp2_pips * PIP
    trades = []; pos = None; equity = START

    for i in range(3, n - 1):
        if pos is None:
            long_sig = (ct[i] and bull[i] >= min_tf_agree and not np.isnan(rsi_a[i])
                        and rsi_a[i] > base.RSI_LONG_MIN and beng[i] and vol_ok[i] and wt_ok[i])
            short_sig = (ct[i] and bear[i] >= min_tf_agree and not np.isnan(rsi_a[i])
                         and rsi_a[i] < base.RSI_SHORT_MAX and seng[i] and vol_ok[i])
            if long_sig or short_sig:
                d = 1 if long_sig else -1
                entry_px = o[i + 1] + (SLIP if d == 1 else -SLIP)
                pos = dict(entry_bar=i + 1, entry_time=idx[i + 1], dir=d, entry_px=entry_px,
                           contracts=NUM, rem=NUM, realized=0.0, tp1_hit=False, tp2_hit=False,
                           current_sl=entry_px - d * sl_d[i], mfe_pips=0.0, atr_at_entry=atr_a[i])
            continue

        if pos["entry_bar"] > i:
            continue
        mi = i; bh = h[mi]; bl = l[mi]; bc = c[mi]
        d = pos["dir"]; entry_px = pos["entry_px"]
        tp1_px = entry_px + d * tp1_d; tp2_px = entry_px + d * tp2_d; sl_px = pos["current_sl"]
        exit_reason = None; exit_px = None

        # Track MFE after TP1 is in hand (2nd-lot territory)
        if pos["tp1_hit"]:
            fav = d * ((bh if d == 1 else bl) - entry_px) / PIP
            pos["mfe_pips"] = max(pos["mfe_pips"], fav)

        if d == 1:
            sl_touched = bl <= sl_px
            tp1_touched = (not pos["tp1_hit"]) and bh >= tp1_px
            tp2_touched = (not pos["tp2_hit"]) and bh >= tp2_px
        else:
            sl_touched = bh >= sl_px
            tp1_touched = (not pos["tp1_hit"]) and bl <= tp1_px
            tp2_touched = (not pos["tp2_hit"]) and bl <= tp2_px

        # TP1 partial (conservative: SL wins the bar if both touched)
        if tp1_touched and not sl_touched:
            fill = tp1_px - d * SLIP
            pnl = d * (fill - entry_px) / PIP * TICKV * base.TP1_QTY
            pos["realized"] += pnl; pos["rem"] -= base.TP1_QTY; pos["tp1_hit"] = True
            pos["current_sl"] = entry_px - d * (SL_AFTER_TP1 * PIP)

        # TP2 target — only in ladder modes (BASE / TP2_FAR).
        # PROBE mode has no target at all: the 2nd lot exits only on hard SL or
        # session close, so its recorded MFE is the TRUE untruncated tail.
        if exit_mode in ("BASE", "TP2_FAR") and tp2_touched and not sl_touched and pos["tp1_hit"]:
            fill = tp2_px - d * SLIP
            pnl = d * (fill - entry_px) / PIP * TICKV * base.TP2_QTY
            pos["realized"] += pnl; pos["rem"] -= base.TP2_QTY; pos["tp2_hit"] = True
            if pos["rem"] <= 0:
                exit_reason = "TP2"; exit_px = fill

        # Stop for remaining lots
        if pos["rem"] > 0 and sl_touched:
            fill = sl_px - d * SLIP
            pnl = d * (fill - entry_px) / PIP * TICKV * pos["rem"]
            pos["realized"] += pnl
            exit_reason = ("SL_after_TP2" if pos["tp1_hit"] and pos["tp2_hit"]
                           else "SL_after_TP1" if pos["tp1_hit"] else "Hard_Stop")
            exit_px = fill; pos["rem"] = 0

        # Session close 12:45 PT
        if pos["rem"] > 0 and sc[mi]:
            fill = bc - d * SLIP
            pos["realized"] += d * (fill - entry_px) / PIP * TICKV * pos["rem"]
            exit_reason = "Session_Close"; exit_px = fill; pos["rem"] = 0

        # Trail after TP1 (defensive in BASE; the PRIMARY exit in TRAIL modes).
        # PROBE never trails — it rides to SL/session so we see the real tail.
        if exit_mode != "PROBE" and pos["rem"] > 0 and pos["tp1_hit"]:
            lo = l[max(0, mi - TRAIL_BARS + 1):mi + 1].min()
            hi = h[max(0, mi - TRAIL_BARS + 1):mi + 1].max()
            if exit_mode == "TRAIL_ATR":
                off = max(pos["atr_at_entry"] * trail_atr_mult, trail_floor_pips * PIP)
            else:
                off = trail_offset_pips * PIP
            if d == 1:
                pos["current_sl"] = max(pos["current_sl"], lo - off)
            else:
                pos["current_sl"] = min(pos["current_sl"], hi + off)

        # Timeout (only pre-TP2 in ladder; pre-any-target in trail so runner still protected).
        # PROBE ignores timeout too — pure SL/session ride for the tail measurement.
        bars_held = mi - pos["entry_bar"]
        if exit_mode != "PROBE" and pos["rem"] > 0 and not pos["tp2_hit"] and bars_held >= int(TIMEOUT_HOURS * 12):
            if d * (bc - entry_px) / PIP < -100:
                fill = bc - d * SLIP
                pos["realized"] += d * (fill - entry_px) / PIP * TICKV * pos["rem"]
                exit_reason = "Timeout_DeepLoss"; exit_px = fill; pos["rem"] = 0

        if pos["rem"] <= 0:
            comm = COMM * pos["contracts"] * 2
            trades.append(dict(entry_time=pos["entry_time"], exit_time=idx[mi],
                               dir="LONG" if d == 1 else "SHORT", entry=entry_px,
                               exit=exit_px if exit_px is not None else bc,
                               gross_pnl=pos["realized"], net_pnl=pos["realized"] - comm,
                               reason=exit_reason or "EOD", bars_held=bars_held,
                               mfe_after_tp1=pos["mfe_pips"] if pos["tp1_hit"] else np.nan))
            equity += pos["realized"] - comm; pos = None

    return trades


def stats(label, trades):
    if not trades:
        return dict(label=label, n=0)
    p = np.array([t["net_pnl"] for t in trades])
    w = p[p > 0]; ls = p[p < 0]
    gp = w.sum(); gl = abs(ls.sum())
    eq = START + np.cumsum(p); dd = (np.maximum.accumulate(eq) - eq).max()
    return dict(label=label, n=len(trades), net=p.sum(), win=100 * len(w) / len(trades),
                pf=gp / gl if gl else float("inf"), avg_win=w.mean() if len(w) else 0,
                avg_loss=ls.mean() if len(ls) else 0, max_dd=dd,
                best=p.max(), worst=p.min())


def main():
    d5, d15, d1h, d4h, d1d = base.load_data()
    df = base.build_frame(d5, d15, d1h, d4h, d1d)
    print(f"Bars: {len(df)} 5m  {df.index[0].date()} .. {df.index[-1].date()}  (Yahoo MGC=F, port-not-TV)\n")

    MTF = 3
    variants = [
        ("BASE (TP2 dump @220)", dict(exit_mode="BASE")),
        ("TP2_FAR (@400)", dict(exit_mode="TP2_FAR", tp2_pips=400.0)),
        ("TRAIL (8-bar swing, 15p)", dict(exit_mode="TRAIL")),
        ("TRAIL_ATR (max 1.0*ATR,20p)", dict(exit_mode="TRAIL_ATR")),
    ]

    rows = []; base_trades = None
    for label, kw in variants:
        tr = run(df, min_tf_agree=MTF, **kw)
        rows.append(stats(label, tr))
        if label.startswith("BASE"):
            base_trades = tr

    hdr = f"{'Variant':<30}{'N':>4}{'Net$':>8}{'Win%':>6}{'PF':>6}{'AvgW':>7}{'AvgL':>7}{'MaxDD':>7}{'Best':>7}"
    print(hdr); print("-" * len(hdr))
    for s in rows:
        print(f"{s['label']:<30}{s['n']:>4}{s['net']:>+8.0f}{s['win']:>5.0f}%{s['pf']:>6.2f}"
              f"{s['avg_win']:>+7.0f}{s['avg_loss']:>+7.0f}{s['max_dd']:>+7.0f}{s['best']:>+7.0f}")

    # The smoking gun, measured honestly: run a PROBE where the 2nd lot has NO
    # target and NO trail — it rides to hard SL / session close — and record how
    # far price actually travelled past entry after TP1. This is the true
    # untruncated tail, uncontaminated by any exit rule.
    probe = run(df, min_tf_agree=MTF, exit_mode="PROBE")
    mfe = np.array([t["mfe_after_tp1"] for t in probe if not np.isnan(t["mfe_after_tp1"])])
    if len(mfe):
        print(f"\nTrue untruncated MFE-after-TP1 (PROBE: ride to SL/session, n={len(mfe)}):")
        print(f"  median {np.median(mfe):.0f} | 75th {np.percentile(mfe,75):.0f} | "
              f"90th {np.percentile(mfe,90):.0f} | 95th {np.percentile(mfe,95):.0f} | max {mfe.max():.0f} pips")
        print(f"  Ran past TP2 (220p): {100*np.mean(mfe>220):.0f}%   past 300p: "
              f"{100*np.mean(mfe>300):.0f}%   past 400p: {100*np.mean(mfe>400):.0f}%")
        print(f"  -> If most winners peak BELOW 220, a fixed TP2 is not truncating much,")
        print(f"     and trailing just donates give-back on the median trade. Read the table above.")

    # Where does the money actually leak? Sweep entry-tightness (min_tf), hold
    # exit=BASE, and break the loss down by reason. The exit is fine; the
    # question is whether tightening entry kills the Hard_Stop / Timeout tail.
    from collections import defaultdict
    print("\nEntry-quality sweep (exit held at BASE ladder), loss composition:")
    print(f"{'min_tf':<8}{'N':>4}{'Net$':>8}{'PF':>6}{'Win%':>6}   loss buckets (net$ by reason)")
    for mtf in (2, 3, 4, 5):
        tr = run(df, min_tf_agree=mtf, exit_mode="BASE")
        if not tr:
            print(f"{mtf:<8}{0:>4}   no trades"); continue
        s = stats(f"mtf{mtf}", tr)
        buckets = defaultdict(float)
        for t in tr:
            buckets[t["reason"]] += t["net_pnl"]
        leaks = "  ".join(f"{k} {v:+.0f}" for k, v in sorted(buckets.items(), key=lambda x: x[1])
                          if v < 0)
        print(f"{mtf:<8}{s['n']:>4}{s['net']:>+8.0f}{s['pf']:>6.2f}{s['win']:>5.0f}%   {leaks}")


if __name__ == "__main__":
    main()
