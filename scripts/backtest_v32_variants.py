"""Python port of gold_multi_tf_nyse_scalper v3.2 / v3.2A / v3.2B.

Runs a side-by-side backtest on Yahoo MGC=F 5m data (60-day free window).
Faithful-but-simplified port of the Pine Script logic. Known gaps vs TV:

  - Entry fill model: executes at NEXT 5m bar's OPEN (Pine default is "on
    bar close, fill next bar's open" when calc_on_order_fills=false).
  - Intrabar fills: on bars where both SL and TP1 are touched, assumes SL
    fires first (conservative). Real TV uses intrabar path inference which
    isn't available from daily OHLC.
  - Slippage: 2 ticks ($0.20) adverse on every fill, matching Pine inputs.
  - request.security + lookahead_off → forward-filled HTF resample.
  - No equity-based scale ladder during backtest; holds BASE=2 throughout
    (starts at $3k, scale threshold is +$10k equity → hit probably never
    in 60-day window, and varying it confounds the A/B comparison anyway).
  - No NYSE auto-close Friday-weekend logic; 12:45 PT session close is
    implemented.

The goal is A/B/C differential, not absolute PF claim. Treat the baseline
v3.2 number as "what this port produces under these rules" and compare
v3.2A / v3.2B against THIS baseline, not against TV's number.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
PROJECT = Path(__file__).resolve().parent.parent
CACHE   = PROJECT / "outputs" / "yahoo_cache"
CACHE.mkdir(parents=True, exist_ok=True)

SYMBOL       = "MGC=F"
PIP_SIZE     = 0.10
TICK_VALUE   = 1.00   # $1 per pip per contract for MGC (10 oz * $0.10)
COMMISSION   = 1.95   # per contract per fill
SLIPPAGE_PIP = 2.0    # ticks = pips here

# Risk params (from v3.2 Pine)
NUM_CONTRACTS = 2
TP1_PIPS, TP1_QTY = 60.0,  1
TP2_PIPS, TP2_QTY = 220.0, 1
TP3_PIPS, TP3_QTY = 400.0, 0
TP4_PIPS, TP4_QTY = 600.0, 0
ATR_LENGTH    = 14
ATR_MULT      = 3.5
MIN_STOP_PIPS = 100.0
MAX_STOP_PIPS = 500.0
SL_AFTER_TP1  = -10.0   # negative = lock profit
TIMEOUT_HOURS = 2.0
TIMEOUT_MIN_PIPS = 30.0
TRAIL_BARS       = 8
TRAIL_OFFSET     = 15.0

# Indicator params
RSI_LEN        = 14
RSI_LONG_MIN   = 55
RSI_SHORT_MAX  = 35
BIAS_EMA_LEN   = 15
WT_N1, WT_N2   = 10, 21
WT_OB          = 53.0
VOL_FILTER_MULT = 2.5
VOL_MEDIAN_WINDOW = 50

# Starting state
STARTING_EQUITY = 3000.0

# ------------------------------------------------------------------
# Data loading
# ------------------------------------------------------------------
def fetch_cached(symbol: str, interval: str, period: str) -> pd.DataFrame:
    f = CACHE / f"{symbol.replace('=','_')}_{interval}_{period}.pkl"
    if f.exists():
        return pd.read_pickle(f)
    import yfinance as yf
    df = yf.Ticker(symbol).history(period=period, interval=interval)
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df.columns = ["o", "h", "l", "c", "v"]
    df.to_pickle(f)
    return df


def load_data():
    d5  = fetch_cached(SYMBOL, "5m",  "60d")
    d15 = fetch_cached(SYMBOL, "15m", "60d")
    d1h = fetch_cached(SYMBOL, "1h",  "730d")
    d1d = fetch_cached(SYMBOL, "1d",  "2y")
    # Resample 1h → 4h since Yahoo has no 4h endpoint.
    d4h = d1h.resample("4h").agg({"o":"first","h":"max","l":"min","c":"last","v":"sum"}).dropna()
    return d5, d15, d1h, d4h, d1d

# ------------------------------------------------------------------
# Indicators
# ------------------------------------------------------------------
def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def rma(s: pd.Series, n: int) -> pd.Series:
    """Pine's ta.rma / Wilder smoothing."""
    return s.ewm(alpha=1.0/n, adjust=False).mean()

def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    up   = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    rs = rma(up, n) / rma(down, n).replace(0, np.nan)
    return 100 - 100/(1+rs)

def atr(h: pd.Series, l: pd.Series, c: pd.Series, n: int = 14) -> pd.Series:
    prev_c = c.shift(1)
    tr = pd.concat([(h-l), (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
    return rma(tr, n)

def wavetrend(h, l, c, n1=10, n2=21):
    ap  = (h + l + c) / 3.0
    esa = ema(ap, n1)
    d   = ema((ap - esa).abs(), n1)
    ci  = (ap - esa) / (0.015 * d.replace(0, np.nan))
    w1  = ema(ci, n2)
    w2  = w1.rolling(4).mean()
    return w1, w2

def adx(h, l, c, n=14):
    """Pine's ta.dmi(diLength, adxSmoothing) returning [+DI, -DI, ADX]."""
    up_move = h.diff()
    dn_move = -l.diff()
    plus_dm  = np.where((up_move > dn_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((dn_move > up_move) & (dn_move > 0), dn_move, 0.0)
    plus_dm  = pd.Series(plus_dm,  index=h.index)
    minus_dm = pd.Series(minus_dm, index=h.index)
    tr = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr_n = rma(tr, n)
    plus_di  = 100 * rma(plus_dm,  n) / atr_n
    minus_di = 100 * rma(minus_dm, n) / atr_n
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) * 100
    adx_ = rma(dx, n)
    return plus_di, minus_di, adx_

# ------------------------------------------------------------------
# MTF bias — EMA(15) trend on 5m/15m/1h/4h/1d, forward-filled to 5m
# ------------------------------------------------------------------
def bias_series(htf: pd.DataFrame, n: int = 15) -> pd.Series:
    """+1 if close > EMA(15), else -1. Index = HTF bar close times."""
    return np.where(htf["c"] > ema(htf["c"], n), 1, -1)

def align_to_5m(idx_5m: pd.DatetimeIndex, htf: pd.DataFrame, bias_vals) -> pd.Series:
    s = pd.Series(bias_vals, index=htf.index)
    # The 5m bar at time T sees the HTF bar that CLOSED at or before T.
    # Pine's lookahead_off means we use the value of the bar whose close_time <= T.
    # reindex + forward-fill, shifted one HTF bar back to prevent lookahead.
    s = s.shift(1)  # use previous completed HTF bar only
    return s.reindex(idx_5m, method="ffill").astype("Int64")

# ------------------------------------------------------------------
# Session filters (Pine uses America/Los_Angeles; Yahoo is ET)
# ------------------------------------------------------------------
def session_flags(idx: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Returns (is_nyse, is_dead_zone, is_session_close) in PT terms."""
    # Convert ET index to PT
    pt = idx.tz_convert("America/Los_Angeles")
    hour = pt.hour
    minute = pt.minute
    dow = pt.dayofweek  # 0=Mon .. 6=Sun

    minute_of_day = hour * 60 + minute

    # NYSE: 06:30-13:00 PT, weekdays (Mon=0..Fri=4)
    is_nyse = (minute_of_day >= 6*60+30) & (minute_of_day < 13*60) & (dow <= 4)

    # Dead zone (from Pine v3.2):
    #   0030-0714 weekdays, 1244-2015 weekdays, 2015-2359 Sat (5),
    #   all day Sun (6), 0000-2014 Mon (0)
    dz_m = minute_of_day
    dz = np.zeros(len(pt), dtype=bool)
    dz |= ((dz_m >= 30) & (dz_m < 7*60+14) & (dow <= 4))
    dz |= ((dz_m >= 12*60+44) & (dz_m < 20*60+15) & (dow <= 4))
    dz |= ((dz_m >= 20*60+15) & (dz_m <= 23*60+59) & (dow == 5))
    dz |= (dow == 6)
    dz |= ((dz_m >= 0) & (dz_m < 20*60+14) & (dow == 0))

    # 12:45 PT session close (4-min window 12:45-12:49, weekdays only)
    sc = ((dz_m >= 12*60+45) & (dz_m < 12*60+49) & (dow <= 4))

    return (pd.Series(is_nyse, index=idx),
            pd.Series(dz,       index=idx),
            pd.Series(sc,       index=idx))

# ------------------------------------------------------------------
# Backtest engine (event-driven bar-by-bar, vectorized prep)
# ------------------------------------------------------------------
def build_frame(d5, d15, d1h, d4h, d1d):
    """Compute all indicator / filter columns on the 5m frame."""
    df = d5.copy()
    idx = df.index

    # Indicators on 5m
    df["rsi"] = rsi(df["c"], RSI_LEN)
    a = atr(df["h"], df["l"], df["c"], ATR_LENGTH)
    df["atr"] = a
    df["atr_median"] = a.rolling(VOL_MEDIAN_WINDOW).median()
    df["vol_ok"] = (df["atr_median"] > 0) & (df["atr"] < df["atr_median"] * VOL_FILTER_MULT)
    w1, w2 = wavetrend(df["h"], df["l"], df["c"], WT_N1, WT_N2)
    df["wt1"] = w1; df["wt2"] = w2
    _, _, adx_ = adx(df["h"], df["l"], df["c"], n=14)
    df["adx"] = adx_

    # MTF bias (5m: close>open; others: EMA(15) trend)
    df["b_5m"]  = np.where(df["c"] > df["o"], 1, -1)
    df["b_15m"] = align_to_5m(idx, d15, bias_series(d15))
    df["b_1h"]  = align_to_5m(idx, d1h, bias_series(d1h))
    df["b_4h"]  = align_to_5m(idx, d4h, bias_series(d4h))
    df["b_d"]   = align_to_5m(idx, d1d, bias_series(d1d))

    # Stack counts
    for col in ["b_5m","b_15m","b_1h","b_4h","b_d"]:
        df[col] = df[col].astype("Int64")
    df["bull_count"] = ((df["b_5m"]==1).fillna(False).astype(int)
                      + (df["b_15m"]==1).fillna(False).astype(int)
                      + (df["b_1h"]==1).fillna(False).astype(int)
                      + (df["b_4h"]==1).fillna(False).astype(int)
                      + (df["b_d"]==1).fillna(False).astype(int))
    df["bear_count"] = ((df["b_5m"]==-1).fillna(False).astype(int)
                      + (df["b_15m"]==-1).fillna(False).astype(int)
                      + (df["b_1h"]==-1).fillna(False).astype(int)
                      + (df["b_4h"]==-1).fillna(False).astype(int)
                      + (df["b_d"]==-1).fillna(False).astype(int))

    # Engulfing — Pine uses close[1]/open[1]/close[2]/open[2]: the PREVIOUS
    # bar engulfing the one before it. At bar T we look at T-1 vs T-2.
    c1 = df["c"].shift(1); o1 = df["o"].shift(1)
    c2 = df["c"].shift(2); o2 = df["o"].shift(2)
    df["bull_eng"] = (c1 > o1) & (o1 <= c2) & (c1 >= o2)
    df["bear_eng"] = (c1 < o1) & (o1 >= c2) & (c1 <= o2)

    # WT long filter: wt1 < OB AND wt1 >= wt2
    df["wt_long_ok"] = (df["wt1"] < WT_OB) & (df["wt1"] >= df["wt2"])

    # Sessions
    is_nyse, is_dz, sess_close = session_flags(idx)
    df["is_nyse"]      = is_nyse
    df["is_dead_zone"] = is_dz
    df["sess_close"]   = sess_close
    df["can_trade"]    = is_nyse & ~is_dz   # live defaults: use_nyse_filter=true
    df["sl_dist"]      = (df["atr"] / PIP_SIZE * ATR_MULT).clip(MIN_STOP_PIPS, MAX_STOP_PIPS) * PIP_SIZE

    return df


def run_backtest(df: pd.DataFrame, *, min_tf_agree: int, use_adx_filter: bool, adx_threshold: float, label: str) -> dict:
    """Event-driven loop through 5m bars. One position at a time (Pine's pyramid=0 default)."""
    trades = []
    n = len(df)
    pos = None   # dict when in a position, else None
    equity = STARTING_EQUITY

    # Arrays for speed
    o = df["o"].to_numpy()
    h = df["h"].to_numpy()
    l = df["l"].to_numpy()
    c = df["c"].to_numpy()
    rsi_a = df["rsi"].to_numpy()
    bull = df["bull_count"].to_numpy()
    bear = df["bear_count"].to_numpy()
    beng = df["bull_eng"].to_numpy()
    seng = df["bear_eng"].to_numpy()
    vol_ok = df["vol_ok"].fillna(False).to_numpy()
    wt_ok  = df["wt_long_ok"].fillna(False).to_numpy()
    adx_a  = df["adx"].to_numpy()
    ct     = df["can_trade"].to_numpy()
    sc     = df["sess_close"].to_numpy()
    sl_d   = df["sl_dist"].to_numpy()
    idx    = df.index

    # Pip offsets
    tp1_d = TP1_PIPS * PIP_SIZE
    tp2_d = TP2_PIPS * PIP_SIZE
    slip  = SLIPPAGE_PIP * PIP_SIZE

    for i in range(3, n-1):  # need 2-bar lookback on engulfing; entry fills on i+1 open
        # --- Entry evaluation on bar i (signal known at close of i) ---
        if pos is None:
            adx_pass = (not use_adx_filter) or (not np.isnan(adx_a[i]) and adx_a[i] >= adx_threshold)

            long_sig = (ct[i] and bull[i] >= min_tf_agree and not np.isnan(rsi_a[i])
                        and rsi_a[i] > RSI_LONG_MIN and beng[i]
                        and vol_ok[i] and wt_ok[i] and adx_pass)
            short_sig = (ct[i] and bear[i] >= min_tf_agree and not np.isnan(rsi_a[i])
                         and rsi_a[i] < RSI_SHORT_MAX and seng[i]
                         and vol_ok[i] and adx_pass)

            if long_sig or short_sig:
                # Fill at next bar's open + adverse slippage
                entry_px = o[i+1] + (slip if long_sig else -slip)
                direction = 1 if long_sig else -1
                sl_init = entry_px - direction * sl_d[i]
                pos = {
                    "entry_bar": i+1,
                    "entry_time": idx[i+1],
                    "dir": direction,
                    "entry_px": entry_px,
                    "contracts": NUM_CONTRACTS,
                    "tp1_qty_remaining": TP1_QTY,
                    "tp2_qty_remaining": TP2_QTY,
                    "contracts_remaining": NUM_CONTRACTS,
                    "realized_pnl": 0.0,   # from TP1/TP2 partial exits
                    "tp1_hit": False,
                    "tp2_hit": False,
                    "current_sl": sl_init,
                    "hit_log": [],
                }
                continue

        # --- Manage existing position ---
        # Entry fills at bar (entry_bar)'s open; manage from that bar onward.
        # We `continue` after entering, so this block runs on the NEXT outer-loop
        # iteration (i = entry_bar). Skip if somehow we're pre-entry.
        if pos is not None:
            if pos["entry_bar"] > i:
                continue  # not yet into the management bar

            mi = i  # management bar
            bar_h = h[mi]; bar_l = l[mi]; bar_o = o[mi]; bar_c = c[mi]
            direction = pos["dir"]
            entry_px  = pos["entry_px"]
            tp1_px = entry_px + direction * tp1_d
            tp2_px = entry_px + direction * tp2_d
            sl_px  = pos["current_sl"]

            # Check intrabar: for a LONG — low first tests SL, high tests TP.
            # Conservative: if both touched same bar, assume SL first.
            exit_reason = None
            exit_px = None

            if direction == 1:  # LONG
                sl_touched  = bar_l <= sl_px
                tp1_touched = (not pos["tp1_hit"]) and (bar_h >= tp1_px)
                tp2_touched = (not pos["tp2_hit"]) and (bar_h >= tp2_px)
            else:                # SHORT
                sl_touched  = bar_h >= sl_px
                tp1_touched = (not pos["tp1_hit"]) and (bar_l <= tp1_px)
                tp2_touched = (not pos["tp2_hit"]) and (bar_l <= tp2_px)

            # Process TP1 partial first (if SL not touched OR TP touched BEFORE SL -- conservative=after)
            if tp1_touched and not sl_touched:
                fill_px = tp1_px - direction * slip
                pnl_partial = direction * (fill_px - entry_px) / PIP_SIZE * TICK_VALUE * TP1_QTY
                pos["realized_pnl"] += pnl_partial
                pos["contracts_remaining"] -= TP1_QTY
                pos["tp1_qty_remaining"]    = 0
                pos["tp1_hit"] = True
                pos["hit_log"].append(("TP1", fill_px, pnl_partial))
                # Move SL to entry - SL_AFTER_TP1*pip for long (locks +$1 profit)
                pos["current_sl"] = entry_px - direction * (SL_AFTER_TP1 * PIP_SIZE)

            if tp2_touched and not sl_touched and pos["tp1_hit"]:
                # TP2 only triggers AFTER TP1 (both from entry, but we check ordering by price)
                fill_px = tp2_px - direction * slip
                pnl_partial = direction * (fill_px - entry_px) / PIP_SIZE * TICK_VALUE * TP2_QTY
                pos["realized_pnl"] += pnl_partial
                pos["contracts_remaining"] -= TP2_QTY
                pos["tp2_qty_remaining"]    = 0
                pos["tp2_hit"] = True
                pos["hit_log"].append(("TP2", fill_px, pnl_partial))
                if pos["contracts_remaining"] <= 0:
                    # TP2 only runs after TP1 (gated above), and only reaches here
                    # when SL wasn't touched this bar, so [-2] is always TP1.
                    exit_reason = "TP2"
                    exit_px = fill_px

            # After partial TPs — if any contracts remain, check SL
            if pos["contracts_remaining"] > 0 and sl_touched:
                fill_px = sl_px - direction * slip
                pnl_remaining = direction * (fill_px - entry_px) / PIP_SIZE * TICK_VALUE * pos["contracts_remaining"]
                pos["realized_pnl"] += pnl_remaining
                pos["hit_log"].append(("SL", fill_px, pnl_remaining))
                if pos["tp1_hit"] and pos["tp2_hit"]:
                    exit_reason = "SL_after_TP2"
                elif pos["tp1_hit"]:
                    exit_reason = "SL_after_TP1"
                else:
                    exit_reason = "Hard_Stop"
                exit_px = fill_px
                pos["contracts_remaining"] = 0

            # Session close (12:45 PT)
            if pos["contracts_remaining"] > 0 and sc[mi]:
                fill_px = bar_c - direction * slip
                pnl_remaining = direction * (fill_px - entry_px) / PIP_SIZE * TICK_VALUE * pos["contracts_remaining"]
                pos["realized_pnl"] += pnl_remaining
                pos["hit_log"].append(("SessClose", fill_px, pnl_remaining))
                exit_reason = "Session_Close"
                exit_px = fill_px
                pos["contracts_remaining"] = 0

            # Trailing SL after TP1 hit
            if pos["contracts_remaining"] > 0 and pos["tp1_hit"]:
                # Swing low/high over last TRAIL_BARS
                lo_slice = l[max(0, mi-TRAIL_BARS+1):mi+1].min()
                hi_slice = h[max(0, mi-TRAIL_BARS+1):mi+1].max()
                if direction == 1:
                    trail = lo_slice - TRAIL_OFFSET * PIP_SIZE
                    pos["current_sl"] = max(pos["current_sl"], trail)
                else:
                    trail = hi_slice + TRAIL_OFFSET * PIP_SIZE
                    pos["current_sl"] = min(pos["current_sl"], trail)

            # Timeout check: 2h = 24 bars of 5m
            bars_held = mi - pos["entry_bar"]
            if pos["contracts_remaining"] > 0 and not pos["tp2_hit"] and bars_held >= int(TIMEOUT_HOURS * 12):
                cur_pnl_pips = direction * (bar_c - entry_px) / PIP_SIZE
                if cur_pnl_pips < -100:
                    fill_px = bar_c - direction * slip
                    pnl_remaining = direction * (fill_px - entry_px) / PIP_SIZE * TICK_VALUE * pos["contracts_remaining"]
                    pos["realized_pnl"] += pnl_remaining
                    pos["hit_log"].append(("Timeout", fill_px, pnl_remaining))
                    exit_reason = "Timeout_DeepLoss"
                    exit_px = fill_px
                    pos["contracts_remaining"] = 0

            # Close out if flat
            if pos["contracts_remaining"] <= 0:
                commission = COMMISSION * pos["contracts"] * 2  # entry + all exit fills combined
                net = pos["realized_pnl"] - commission
                trades.append({
                    "entry_time": pos["entry_time"],
                    "exit_time": idx[mi],
                    "dir": "LONG" if direction == 1 else "SHORT",
                    "contracts": pos["contracts"],
                    "entry": pos["entry_px"],
                    "exit": exit_px if exit_px is not None else bar_c,
                    "gross_pnl": pos["realized_pnl"],
                    "commission": commission,
                    "net_pnl": net,
                    "reason": exit_reason or "EOD",
                    "bars_held": bars_held,
                })
                equity += net
                pos = None

    # End of data — force-close any open pos at last close
    if pos is not None:
        direction = pos["dir"]
        fill_px = c[-1] - direction * slip
        pnl_remaining = direction * (fill_px - pos["entry_px"]) / PIP_SIZE * TICK_VALUE * pos["contracts_remaining"]
        pos["realized_pnl"] += pnl_remaining
        commission = COMMISSION * pos["contracts"] * 2
        net = pos["realized_pnl"] - commission
        trades.append({
            "entry_time": pos["entry_time"], "exit_time": idx[-1],
            "dir": "LONG" if direction == 1 else "SHORT",
            "contracts": pos["contracts"], "entry": pos["entry_px"], "exit": fill_px,
            "gross_pnl": pos["realized_pnl"], "commission": commission, "net_pnl": net,
            "reason": "EOD_ForceClose", "bars_held": len(df) - 1 - pos["entry_bar"],
        })
        equity += net

    return {"label": label, "trades": trades, "final_equity": equity}

# ------------------------------------------------------------------
# Reporting
# ------------------------------------------------------------------
def summarize(result: dict) -> dict:
    trades = result["trades"]
    if not trades:
        return {"label": result["label"], "n": 0}
    pnls = np.array([t["net_pnl"] for t in trades])
    wins = pnls[pnls > 0]; losses = pnls[pnls < 0]
    gp = wins.sum(); gl = abs(losses.sum())
    pf = gp/gl if gl > 0 else float("inf")

    eq = STARTING_EQUITY + np.cumsum(pnls)
    peak = np.maximum.accumulate(eq)
    dd   = peak - eq
    max_dd = dd.max()

    longs  = [t for t in trades if t["dir"]=="LONG"]
    shorts = [t for t in trades if t["dir"]=="SHORT"]
    long_pnl  = sum(t["net_pnl"] for t in longs)
    short_pnl = sum(t["net_pnl"] for t in shorts)

    streak, longest = 0, 0
    for p in pnls:
        if p < 0:
            streak += 1; longest = max(longest, streak)
        else:
            streak = 0

    return {
        "label": result["label"],
        "n": len(trades),
        "net": pnls.sum(),
        "win%": 100*len(wins)/len(trades),
        "pf": pf,
        "avg_win": wins.mean() if len(wins) else 0,
        "avg_loss": losses.mean() if len(losses) else 0,
        "max_dd": max_dd,
        "longest_losing": longest,
        "longs_n": len(longs), "long_pnl": long_pnl,
        "shorts_n": len(shorts), "short_pnl": short_pnl,
        "final_eq": result["final_equity"],
    }


def main():
    print("Fetching / loading Yahoo data...")
    d5, d15, d1h, d4h, d1d = load_data()
    print(f"  5m:  {len(d5):>5d} bars, {d5.index[0]} to {d5.index[-1]}")
    print(f"  15m: {len(d15):>5d} bars")
    print(f"  1h:  {len(d1h):>5d} bars")
    print(f"  4h:  {len(d4h):>5d} bars (resampled from 1h)")
    print(f"  1d:  {len(d1d):>5d} bars")
    print()

    print("Building 5m frame with indicators + MTF bias + session flags...")
    df = build_frame(d5, d15, d1h, d4h, d1d)
    print(f"  frame ready: {len(df)} rows, {df.columns.tolist()}")
    print()

    configs = [
        dict(label="v3.2  (baseline, min_tf=3, no ADX)",     min_tf_agree=3, use_adx_filter=False, adx_threshold=25.0),
        dict(label="v3.2A (min_tf=4, no ADX)",                min_tf_agree=4, use_adx_filter=False, adx_threshold=25.0),
        dict(label="v3.2B (min_tf=3, ADX>=25)",               min_tf_agree=3, use_adx_filter=True,  adx_threshold=25.0),
    ]

    results = []
    for cfg in configs:
        print(f"Running {cfg['label']}...")
        r = run_backtest(df, **{k:v for k,v in cfg.items() if k != 'label'}, label=cfg["label"])
        s = summarize(r)
        results.append((r, s))
        print(f"  -> {s['n']} trades, net=${s.get('net',0):+.0f}, pf={s.get('pf',0):.2f}")

    print()
    print("=" * 100)
    print("SUMMARY — Yahoo MGC=F, 60d 5m bars, port-not-TV")
    print("=" * 100)
    hdr = f"{'Variant':<40s}  {'N':>3s} {'Net':>8s} {'Win%':>5s} {'PF':>5s} {'AvgWin':>7s} {'AvgLoss':>8s} {'MaxDD':>7s} {'LongSt':>7s}"
    print(hdr)
    print("-" * len(hdr))
    for _, s in results:
        if s["n"] == 0:
            print(f"{s['label']:<40s}  NO TRADES")
            continue
        print(f"{s['label']:<40s}  {s['n']:>3d} {s['net']:>+8.0f} {s['win%']:>4.1f}% {s['pf']:>5.2f} {s['avg_win']:>+7.0f} {s['avg_loss']:>+8.0f} {s['max_dd']:>+7.0f} {s['longest_losing']:>7d}")

    print()
    print("Long / Short split:")
    for _, s in results:
        if s["n"] == 0: continue
        print(f"  {s['label']:<40s} LONG: {s['longs_n']:>3d} (${s['long_pnl']:+.0f})   SHORT: {s['shorts_n']:>3d} (${s['short_pnl']:+.0f})")

    # Dump trade lists for each variant for audit
    for r, s in results:
        out = CACHE / f"trades_{s['label'].split()[0]}.csv"
        if s["n"] > 0:
            pd.DataFrame(r["trades"]).to_csv(out, index=False)

    print()
    print("Per-trade CSVs dumped to outputs/yahoo_cache/trades_v*.csv")
    return results


if __name__ == "__main__":
    main()
