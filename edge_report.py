"""Print calibration and resolution-held signal results from the read-only ledger."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

import config
from edge_finder import EdgeLedger


def main():
    ledger = EdgeLedger(config.EDGE_DB_PATH)
    summary = ledger.summary()
    print(f"Ledger: {config.EDGE_DB_PATH}")
    if not summary["n"]:
        print("No resolved forecasts yet. Run main.py and wait for markets to settle.")
        return
    print(f"Forecasts: {summary['n']}")
    print(f"Overall Brier: model {summary['brier_model']:.4f} | "
          f"market midpoint {summary['brier_market']:.4f}" if summary["brier_market"] is not None
          else f"Overall Brier: model {summary['brier_model']:.4f} | market midpoint unavailable")
    print("\nBy time remaining (lower Brier is better):")
    print("  seconds left     n     model     market")
    for row in ledger.horizon_scores():
        lo, hi = row["range_seconds"]
        market = "—" if row["brier_market"] is None else f"{row['brier_market']:.4f}"
        print(f"  {lo:4d}–{hi:<4d}       {row['n']:4d}   {row['brier_model']:.4f}   {market}")
    pnl = summary["candidate_pnl"]
    print("\nFirst qualifying signal per resolved market:")
    print(f"  markets: {summary['candidate_n']}")
    print(f"  {config.EDGE_PROBE_SHARES:g}-share-per-market total: {('—' if pnl is None else f'${pnl:+.3f}')}")
    print("  Uses saved ask-depth VWAP, configured fee and slippage buffer; assumes the entry fills and holds to resolution.")


if __name__ == "__main__":
    main()
