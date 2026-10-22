"""Risk analysis for gold scalper variants on $3k starting capital.

Three analyses on existing TradingView backtest CSVs:
1. Quarterly walk-forward — regime consistency across 4 equal time windows
2. Monte Carlo risk-of-ruin — bootstrap 10k simulated years from $3k start
3. Fragile early stage — chronological first 50 trades equity path

The Monte Carlo normalizes each trade's P&L to a per-base-3-contract basis,
then re-applies the scale_mult ladder (+1x per $10k equity above start) at
each simulated step. Bootstrap is i.i.d. trade sampling — does NOT preserve
serial correlation, so streak-related tail risk may be underestimated.
"""
import numpy as np
import pandas as pd
import os
from pathlib import Path

DESKTOP = Path(os.environ.get("BACKTEST_CSV_DIR", "."))  # folder holding the TradingView export CSVs

FILES = {
    "V3.1-all":  DESKTOP / "Gold_Multi-TF_NYSE_Scalper_v3.1_COMEX_MINI_MGC1!_2026-04-20.csv",
    "V3.1-NYSE": DESKTOP / "Gold_Multi-TF_NYSE_Scalper_v3.1_COMEX_MINI_MGC1!_2026-04-21.csv",
    "V3.1-real": DESKTOP / "Gold_Multi-TF_NYSE_Scalper_v3.1_COMEX_MINI_MGC1!_2026-04-21 (1).csv",
}

# Account + sizing
START_CAPITAL  = 3000.0
SCALE_STEP     = 10000.0
BASE_CONTRACTS = 3
MARGIN_PER_CTR = 564.0
RUIN_THRESHOLD = BASE_CONTRACTS * MARGIN_PER_CTR  # $1,692 — can't margin base 3-lot

# Monte Carlo
N_SIMS = 10_000
SEED   = 42


def load_exits(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    exits = df[df["Type"].str.startswith("Exit", na=False)].copy()
    exits["dt"] = pd.to_datetime(exits["Date and time"])
    exits["size_abs"] = exits["Size (qty)"].abs()
    # Per-base (3-contract) P&L — removes the scale_mult baked into the CSV so
    # we can reapply our own scaling in simulation.
    exits["per_base_pnl"] = exits["Net P&L USD"] * (BASE_CONTRACTS / exits["size_abs"])
    return exits.sort_values("dt").reset_index(drop=True)


def monte_carlo(per_base_pnls: np.ndarray, n_trades: int) -> dict:
    """Bootstrap N_SIMS paths from $3k start; vectorized across sims per step."""
    rng = np.random.default_rng(SEED)
    draws = rng.choice(per_base_pnls, size=(N_SIMS, n_trades), replace=True)

    eqs = np.full(N_SIMS, START_CAPITAL, dtype=np.float64)
    min_eqs = eqs.copy()
    for t in range(n_trades):
        above = np.maximum(eqs - START_CAPITAL, 0.0)
        scale = (above // SCALE_STEP).astype(np.int64) + 1
        eqs = eqs + draws[:, t] * scale
        np.minimum(min_eqs, eqs, out=min_eqs)

    return {
        "median_final": float(np.median(eqs)),
        "p5_final":     float(np.percentile(eqs, 5)),
        "p95_final":    float(np.percentile(eqs, 95)),
        "p_ruin":       float((min_eqs < RUIN_THRESHOLD).mean()),
        "p_lose":       float((eqs < START_CAPITAL).mean()),
        "p_10x":        float((eqs > 10 * START_CAPITAL).mean()),
    }


def quarterly(exits: pd.DataFrame) -> list[dict]:
    """Split into 4 equal time windows and compute per-window Net, DD, Net/DD."""
    start, end = exits["dt"].min(), exits["dt"].max()
    window = (end - start) / 4
    out = []
    for i in range(4):
        w0 = start + i * window
        w1 = start + (i + 1) * window if i < 3 else end + pd.Timedelta(seconds=1)
        chunk = exits[(exits["dt"] >= w0) & (exits["dt"] < w1)]
        pnl = chunk["Net P&L USD"]
        if len(pnl) == 0:
            out.append({"period": f"W{i+1}", "start": w0.strftime("%Y-%m-%d"),
                        "end": w1.strftime("%Y-%m-%d"), "n": 0,
                        "net": 0.0, "max_dd": 0.0, "n_dd": 0.0})
            continue
        cum = pnl.cumsum()
        dd = (cum - cum.cummax()).min()
        max_dd = abs(dd) if not pd.isna(dd) else 0.0
        net = float(pnl.sum())
        out.append({
            "period": f"W{i+1}",
            "start":  w0.strftime("%Y-%m-%d"),
            "end":    w1.strftime("%Y-%m-%d"),
            "n":      len(pnl),
            "net":    net,
            "max_dd": float(max_dd),
            "n_dd":   net / max_dd if max_dd > 0 else 0.0,
        })
    return out


def first_n_sim(exits: pd.DataFrame, n: int = 50) -> dict:
    """Simulate chronological first-N trades from $3k with scale ladder."""
    eq = START_CAPITAL
    minimum = eq
    for pnl in exits.head(n)["per_base_pnl"]:
        scale = int((eq - START_CAPITAL) // SCALE_STEP) + 1 if eq > START_CAPITAL else 1
        eq += pnl * scale
        minimum = min(minimum, eq)
    return {"min": minimum, "final": eq, "below_ruin": minimum < RUIN_THRESHOLD}


def main() -> None:
    print("=" * 92)
    print(f"RISK ANALYSIS — ${START_CAPITAL:,.0f} starting capital, MGC1! gold scalper")
    print("=" * 92)
    print(f"  scale_step = ${SCALE_STEP:,.0f}  (+1x position per this much equity gained)")
    print(f"  base = {BASE_CONTRACTS} contracts, margin = ${MARGIN_PER_CTR:.0f}/ctr")
    print(f"  ruin threshold = ${RUIN_THRESHOLD:,.0f}  (can't margin base 3-lot day trade)")
    print(f"  Monte Carlo: {N_SIMS:,} bootstrap simulated years per variant")

    loaded = {n: load_exits(p) for n, p in FILES.items()}

    # --- 1. Quarterly walk-forward ---
    print("\n" + "=" * 92)
    print("1. QUARTERLY WALK-FORWARD  (regime consistency across 4 time windows)")
    print("=" * 92)
    q_results = {n: quarterly(ex) for n, ex in loaded.items()}

    print("\nTime windows:")
    for q in next(iter(q_results.values())):
        print(f"  {q['period']}  {q['start']} -> {q['end']}")

    # Net per window + total
    print("\nNet P&L per window:")
    head = f"{'Variant':<8}" + "".join(f"{'W'+str(i+1):>12}" for i in range(4)) + f"{'Total':>14}"
    print(head)
    print("-" * len(head))
    for name, qs in q_results.items():
        tot = sum(q["net"] for q in qs)
        row = f"{name:<8}" + "".join(f"  ${q['net']:>9,.0f}" for q in qs) + f"  ${tot:>11,.0f}"
        print(row)

    # Net/DD per window + quarterly win tally
    print("\nNet/DD ratio per window (higher = better risk-adjusted):")
    wins = {n: 0 for n in loaded}
    for i in range(4):
        best = max(loaded, key=lambda n: q_results[n][i]["n_dd"])
        wins[best] += 1
    head2 = f"{'Variant':<8}" + "".join(f"{'W'+str(i+1):>12}" for i in range(4)) + f"{'Wins':>10}"
    print(head2)
    print("-" * len(head2))
    for name, qs in q_results.items():
        row = f"{name:<8}" + "".join(f"{q['n_dd']:>12.2f}" for q in qs) + f"{wins[name]:>8}/4"
        print(row)

    # --- 2. Monte Carlo ---
    print("\n" + "=" * 92)
    print(f"2. MONTE CARLO RISK-OF-RUIN  ({N_SIMS:,} bootstrap years per variant)")
    print("=" * 92)
    print(f"  Start each sim at ${START_CAPITAL:,.0f}. Ruin = equity ever dropped below "
          f"${RUIN_THRESHOLD:,.0f} during the path.")
    print(f"  P(lose) = final < ${START_CAPITAL:,.0f}. P(10x) = final > ${10*START_CAPITAL:,.0f}.")

    mc = {n: monte_carlo(ex["per_base_pnl"].values, len(ex)) for n, ex in loaded.items()}
    cols = ["Trades", "Median End", "5th Pctl", "95th Pctl", "P(ruin)", "P(lose)", "P(10x+)"]
    head = f"{'Variant':<8} | " + " | ".join(f"{c:>10}" for c in cols)
    print("\n" + head)
    print("-" * len(head))
    for name, r in mc.items():
        print(
            f"{name:<8} | "
            f"{len(loaded[name]):>10,} | "
            f"${r['median_final']:>9,.0f} | "
            f"${r['p5_final']:>9,.0f} | "
            f"${r['p95_final']:>9,.0f} | "
            f"{r['p_ruin']*100:>9.1f}% | "
            f"{r['p_lose']*100:>9.1f}% | "
            f"{r['p_10x']*100:>9.1f}%"
        )

    # --- 3. First 50 trades ---
    print("\n" + "=" * 92)
    print("3. FRAGILE EARLY STAGE  (actual first 50 trades from $3k)")
    print("=" * 92)
    head = f"{'Variant':<8} | {'Min Eq':>10} | {'End Eq':>10} | {'Ruin hit?':>10}"
    print("\n" + head)
    print("-" * len(head))
    early = {}
    for name, ex in loaded.items():
        r = first_n_sim(ex)
        early[name] = r
        flag = "YES" if r["below_ruin"] else "no"
        print(f"{name:<8} | ${r['min']:>9,.0f} | ${r['final']:>9,.0f} | {flag:>10}")

    # --- Verdict ---
    print("\n" + "=" * 92)
    print(f"VERDICT for ${START_CAPITAL:,.0f} starting capital")
    print("=" * 92)

    low_risk  = {n: r for n, r in mc.items() if r["p_ruin"] < 0.05}
    high_risk = {n: r for n, r in mc.items() if r["p_ruin"] >= 0.05}

    print("\nSafe tier  (P(ruin) < 5%):")
    if low_risk:
        for n in sorted(low_risk, key=lambda x: -low_risk[x]["median_final"]):
            r = low_risk[n]
            early_flag = "  !! ruined in first 50 trades" if early[n]["below_ruin"] else ""
            print(f"  {n:<8}  median ${r['median_final']:>8,.0f}  "
                  f"P(ruin) {r['p_ruin']*100:>4.1f}%  "
                  f"quarters won {wins[n]}/4{early_flag}")
    else:
        print("  (none — all variants have P(ruin) >= 5%)")

    print("\nHigh-risk tier  (P(ruin) >= 5%):")
    if high_risk:
        for n in sorted(high_risk, key=lambda x: -high_risk[x]["p_ruin"]):
            r = high_risk[n]
            print(f"  {n:<8}  P(ruin) {r['p_ruin']*100:>5.1f}%  "
                  f"median ${r['median_final']:>8,.0f}")
    else:
        print("  (none)")

    if low_risk:
        winner = max(low_risk, key=lambda n: low_risk[n]["median_final"])
    else:
        winner = min(mc, key=lambda n: mc[n]["p_ruin"])
    print(f"\n>>> RECOMMENDED for ${START_CAPITAL:,.0f}: {winner}")


if __name__ == "__main__":
    main()
