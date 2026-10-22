"""Signature aggregation analysis using only Python stdlib (no pandas)."""
import csv
import sys
from collections import defaultdict


def main(path: str):
    # Pair entry rows with exit rows by Trade #
    entries: dict[str, dict] = {}
    exits: dict[str, dict] = {}

    # utf-8-sig strips BOM if present (TV exports sometimes have one)
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # Detect column-name variations (BOM, trailing whitespace, etc.)
        if reader.fieldnames is None:
            print("ERROR: no header row")
            return
        # Diagnostic on first run
        print(f"DEBUG fieldnames: {reader.fieldnames[:5]}")
        for row in reader:
            trade_id = row["Trade #"]
            type_ = row["Type"]
            if type_.startswith("Entry"):
                entries[trade_id] = row
            elif type_.startswith("Exit"):
                exits[trade_id] = row

    # Aggregate by signature
    sig_pnl: dict[str, list[float]] = defaultdict(list)
    total_pnl = 0.0
    total_trades = 0
    earliest = None
    latest = None

    for tid, ent in entries.items():
        if tid not in exits:
            continue
        sig = ent["Signal"]
        try:
            pnl = float(exits[tid]["Net P&L USD"])
        except (ValueError, KeyError):
            continue
        sig_pnl[sig].append(pnl)
        total_pnl += pnl
        total_trades += 1
        ent_t = ent["Date and time"]
        if earliest is None or ent_t < earliest:
            earliest = ent_t
        if latest is None or ent_t > latest:
            latest = ent_t

    print(f"Total trades: {total_trades}")
    print(f"Date range: {earliest} -> {latest}")
    print(f"Total P&L: ${total_pnl:,.0f}")
    wins = sum(1 for p in (p for pnls in sig_pnl.values() for p in pnls) if p > 0)
    losses = sum(1 for p in (p for pnls in sig_pnl.values() for p in pnls) if p < 0)
    print(f"Overall WR: {wins/(wins+losses)*100:.2f}% ({wins}W/{losses}L)")
    print()

    # Compute per-signature stats
    stats = []
    for sig, pnls in sig_pnl.items():
        n = len(pnls)
        total = sum(pnls)
        avg = total / n if n else 0
        n_wins = sum(1 for p in pnls if p > 0)
        wr = n_wins / n * 100 if n else 0
        win_pnls = [p for p in pnls if p > 0]
        loss_pnls = [p for p in pnls if p < 0]
        win_avg = sum(win_pnls) / len(win_pnls) if win_pnls else 0
        loss_avg = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0
        stats.append({
            "signature": sig,
            "n": n,
            "total_pnl": total,
            "avg_pnl": avg,
            "wr": wr,
            "win_avg": win_avg,
            "loss_avg": loss_avg,
        })

    # Filter to >=20 trades for statistical significance
    stats_filt = [s for s in stats if s["n"] >= 20]
    print(f"Signatures with n>=20: {len(stats_filt)} (of {len(stats)} total)")
    print()

    # Sort worst first
    stats_filt.sort(key=lambda s: s["total_pnl"])
    print("=== 20 WORST signatures (sorted by total P&L) ===")
    print(f"  {'sig':12s}  {'n':>5s}  {'total':>10s}  {'avg':>8s}  {'WR':>7s}  {'win_avg':>8s}  {'loss_avg':>9s}")
    for s in stats_filt[:20]:
        print(f"  {s['signature']:12s}  {s['n']:5d}  ${s['total_pnl']:>+8.0f}  "
              f"${s['avg_pnl']:>+6.1f}  {s['wr']:>5.1f}%  ${s['win_avg']:>+6.1f}  ${s['loss_avg']:>+7.1f}")
    print()

    print("=== 20 BEST signatures ===")
    print(f"  {'sig':12s}  {'n':>5s}  {'total':>10s}  {'avg':>8s}  {'WR':>7s}  {'win_avg':>8s}  {'loss_avg':>9s}")
    for s in sorted(stats_filt, key=lambda s: -s["total_pnl"])[:20]:
        print(f"  {s['signature']:12s}  {s['n']:5d}  ${s['total_pnl']:>+8.0f}  "
              f"${s['avg_pnl']:>+6.1f}  {s['wr']:>5.1f}%  ${s['win_avg']:>+6.1f}  ${s['loss_avg']:>+7.1f}")
    print()

    # Check the 7 from memory
    memory_toxic = ["FFFFF45-", "TTTTT65+", "TTFFF50-", "TTTTT80+",
                    "TTFFF25-", "TTFTT65+", "TFFTT45+"]
    stats_by_sig = {s["signature"]: s for s in stats}
    print("=== Memory's 7 toxic signatures — current status ===")
    print(f"  {'sig':12s}  {'n':>5s}  {'total':>10s}  {'avg':>8s}  {'WR':>7s}")
    drag = 0
    for sig in memory_toxic:
        if sig in stats_by_sig:
            s = stats_by_sig[sig]
            print(f"  {sig:12s}  {s['n']:5d}  ${s['total_pnl']:>+8.0f}  "
                  f"${s['avg_pnl']:>+6.1f}  {s['wr']:>5.1f}%")
            drag += s["total_pnl"]
        else:
            print(f"  {sig:12s}  NOT FOUND")
    print(f"\n  Total drag from these 7: ${drag:+,.0f}")
    print()

    # All net-negative with n>=20
    neg = [s for s in stats_filt if s["total_pnl"] < 0]
    print(f"=== All net-negative sigs (n>=20): {len(neg)} ===")
    print(f"Total drag from these: ${sum(s['total_pnl'] for s in neg):+,.0f}")
    print(f"  {'sig':12s}  {'n':>5s}  {'total':>10s}  {'avg':>8s}  {'WR':>7s}")
    for s in sorted(neg, key=lambda s: s["total_pnl"]):
        print(f"  {s['signature']:12s}  {s['n']:5d}  ${s['total_pnl']:>+8.0f}  "
              f"${s['avg_pnl']:>+6.1f}  {s['wr']:>5.1f}%")


if __name__ == "__main__":
    p = sys.argv[1] if len(sys.argv) > 1 else "trades.csv"
    main(p)
