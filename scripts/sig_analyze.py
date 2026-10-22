"""Re-analyze TV strategy report trade list by signature.

Each trade in the CSV has 2 rows: Entry then Exit. Entry's Signal column
is the signature (e.g., FFFFF50-). Exit's Signal column is the exit
reason (TP1, TP2, Stop Hit, Session Close, etc.).

Output:
- Top 20 best signatures (most profitable)
- Top 20 worst signatures (toxic candidates)
- Summary stats per signature
"""
from __future__ import annotations
import sys
import pandas as pd


def main(path: str):
    df = pd.read_csv(path)
    # Each trade has 2 rows — Entry and Exit. Pair them by Trade #.
    entries = df[df["Type"].str.startswith("Entry")].copy()
    exits = df[df["Type"].str.startswith("Exit")].copy()

    # Trades has signature on entry, P&L on exit (Net P&L is per-row but
    # both row's Net P&L should match — it's the trade's P&L allocated).
    # We'll join by Trade #.
    entries = entries[["Trade #", "Date and time", "Signal", "Size (qty)"]].rename(
        columns={"Signal": "signature", "Date and time": "entry_time", "Size (qty)": "qty"}
    )
    exits = exits[["Trade #", "Date and time", "Signal", "Net P&L USD"]].rename(
        columns={"Signal": "exit_reason", "Date and time": "exit_time", "Net P&L USD": "pnl"}
    )
    trades = entries.merge(exits, on="Trade #", how="inner")
    trades["pnl"] = pd.to_numeric(trades["pnl"], errors="coerce")
    trades = trades.dropna(subset=["pnl"])

    print(f"Total trade rows: {len(trades)}")
    print(f"Date range: {trades['entry_time'].min()} -> {trades['exit_time'].max()}")
    print(f"Total P&L: ${trades['pnl'].sum():,.0f}")
    print(f"Avg P&L: ${trades['pnl'].mean():,.2f}")
    print(f"Overall WR: {(trades['pnl'] > 0).mean() * 100:.2f}%")
    print()

    # Group by signature
    sig_stats = trades.groupby("signature").agg(
        n=("pnl", "count"),
        total_pnl=("pnl", "sum"),
        avg_pnl=("pnl", "mean"),
        wr=("pnl", lambda s: (s > 0).mean() * 100),
        win_avg=("pnl", lambda s: s[s > 0].mean() if (s > 0).any() else 0),
        loss_avg=("pnl", lambda s: s[s < 0].mean() if (s < 0).any() else 0),
    ).round(2)

    # Min sample size filter — signatures with too few trades aren't statistical
    sig_stats_filtered = sig_stats[sig_stats["n"] >= 20].copy()

    print(f"Signatures with >=20 trades: {len(sig_stats_filtered)}")
    print()

    print("=== 20 WORST SIGNATURES (sorted by total P&L ascending) ===")
    worst = sig_stats_filtered.sort_values("total_pnl").head(20)
    print(worst.to_string())
    print()

    print("=== 20 BEST SIGNATURES (sorted by total P&L descending) ===")
    best = sig_stats_filtered.sort_values("total_pnl", ascending=False).head(20)
    print(best.to_string())
    print()

    # Check the 7 sigs from memory
    memory_toxic = ["FFFFF45-", "TTTTT65+", "TTFFF50-", "TTTTT80+",
                    "TTFFF25-", "TTFTT65+", "TFFTT45+"]
    print("=== STATUS OF 7 SIGS FROM MEMORY ===")
    for sig in memory_toxic:
        if sig in sig_stats.index:
            row = sig_stats.loc[sig]
            print(f"  {sig:10s}  n={int(row['n']):4d}  total=${row['total_pnl']:+7.0f}  "
                  f"avg=${row['avg_pnl']:+6.2f}  WR={row['wr']:5.1f}%")
        else:
            print(f"  {sig:10s}  NOT FOUND IN DATA")
    print()

    # Identify net-negative sigs with meaningful sample size
    net_neg = sig_stats_filtered[sig_stats_filtered["total_pnl"] < 0]
    print(f"=== ALL net-negative signatures (n>=20) ===")
    print(f"Count: {len(net_neg)}, total drag: ${net_neg['total_pnl'].sum():,.0f}")
    print(net_neg.sort_values("total_pnl").to_string())


if __name__ == "__main__":
    p = sys.argv[1] if len(sys.argv) > 1 else "trades.csv"
    main(p)
