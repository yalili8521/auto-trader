"""Compare TradingView backtest CSVs across strategy variants."""
import pandas as pd
import os
from pathlib import Path

DESKTOP = Path(os.environ.get("BACKTEST_CSV_DIR", "."))  # folder holding the TradingView export CSVs

FILES = {
    "V3.1-all":  DESKTOP / "Gold_Multi-TF_NYSE_Scalper_v3.1_COMEX_MINI_MGC1!_2026-04-20.csv",
    "V3.1-NYSE": DESKTOP / "Gold_Multi-TF_NYSE_Scalper_v3.1_COMEX_MINI_MGC1!_2026-04-21.csv",
    "V3.1-real": DESKTOP / "Gold_Multi-TF_NYSE_Scalper_v3.1_COMEX_MINI_MGC1!_2026-04-21 (1).csv",
}


def analyze(path: Path) -> dict:
    df = pd.read_csv(path, encoding="utf-8-sig")
    exits = df[df["Type"].str.startswith("Exit", na=False)].copy()
    exits["pnl"] = exits["Net P&L USD"]

    n_legs = len(exits)
    net = exits["pnl"].sum()
    wins = exits[exits["pnl"] > 0]
    losses = exits[exits["pnl"] < 0]
    gp = wins["pnl"].sum()
    gl = abs(losses["pnl"].sum())
    pf = gp / gl if gl > 0 else float("inf")
    win_pct = 100 * len(wins) / n_legs if n_legs else 0
    avg_win = wins["pnl"].mean() if len(wins) else 0
    avg_loss = losses["pnl"].mean() if len(losses) else 0

    # Max DD via cumulative running peak (trade-series, not vs initial equity)
    cum = exits["pnl"].cumsum()
    peak = cum.cummax()
    dd = (cum - peak)
    max_dd = abs(dd.min()) if len(dd) else 0

    # Longest losing streak
    streak, longest = 0, 0
    for p in exits["pnl"].values:
        if p < 0:
            streak += 1
            longest = max(longest, streak)
        else:
            streak = 0

    # Signal breakdown (older CSV exports may lack the Signal column)
    sig = exits["Signal"].value_counts().to_dict() if "Signal" in exits.columns else {}

    # Longs vs shorts
    longs = exits[exits["Type"] == "Exit long"]
    shorts = exits[exits["Type"] == "Exit short"]

    return {
        "legs": n_legs,
        "net": net,
        "pf": pf,
        "win%": win_pct,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "max_dd": max_dd,
        "longest_losing_streak": longest,
        "signals": sig,
        "long_legs": len(longs),
        "short_legs": len(shorts),
        "long_net": longs["pnl"].sum(),
        "short_net": shorts["pnl"].sum(),
    }


def main() -> None:
    results = [analyze(p) for p in FILES.values()]

    col_width = 16
    headers = list(FILES.keys())
    row_hdr = f"{'Metric':<22} | " + " | ".join(f"{h:>{col_width}}" for h in headers)
    sep_width = len(row_hdr)
    print("\n" + "=" * sep_width)
    print(row_hdr)
    print("-" * sep_width)
    for key, fmt in [
        ("legs",                  f"{{:>{col_width}d}}"),
        ("net",                   f"${{:>{col_width - 1},.0f}}"),
        ("pf",                    f"{{:>{col_width}.2f}}"),
        ("win%",                  f"{{:>{col_width - 1}.1f}}%"),
        ("avg_win",               f"${{:>{col_width - 1},.0f}}"),
        ("avg_loss",              f"${{:>{col_width - 1},.0f}}"),
        ("max_dd",                f"${{:>{col_width - 1},.0f}}"),
        ("longest_losing_streak", f"{{:>{col_width}d}}"),
        ("long_legs",             f"{{:>{col_width}d}}"),
        ("short_legs",            f"{{:>{col_width}d}}"),
        ("long_net",              f"${{:>{col_width - 1},.0f}}"),
        ("short_net",             f"${{:>{col_width - 1},.0f}}"),
    ]:
        row = f"{key:<22} | "
        cells = [fmt.format(r[key]) for r in results]
        print(row + " | ".join(cells))

    # Signal section uses a wider label column, so it needs its own separator width
    sig_hdr = f"{'Signal':<28} | " + " | ".join(f"{h:>{col_width}}" for h in headers)
    sig_sep_width = len(sig_hdr)
    print("\n" + "=" * sig_sep_width)
    print("SIGNAL BREAKDOWN (exit legs)")
    print("=" * sig_sep_width)
    all_signals = set()
    for r in results:
        all_signals.update(r["signals"].keys())
    print(sig_hdr)
    print("-" * sig_sep_width)
    for sig in sorted(all_signals):
        row = f"{sig:<28} | "
        cells = [f"{r['signals'].get(sig, 0):>{col_width}d}" for r in results]
        print(row + " | ".join(cells))


if __name__ == "__main__":
    main()
