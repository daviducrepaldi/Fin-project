#!/usr/bin/env python3
"""
Financial Statement Analyzer
─────────────────────────────
Usage:
  python main.py AAPL
  python main.py AAPL MSFT GOOG
  python main.py AAPL --export
  python main.py AAPL MSFT GOOG --export
  python main.py --offline AAPL       (use cached SQLite data only)
"""

import os
import sys
from src import db, fetcher, analyzer, display, exporter


def main():
    args = sys.argv[1:]
    if not args or args == ['--help'] or args == ['-h']:
        print(__doc__)
        sys.exit(0)

    offline = '--offline' in args
    do_export = '--export' in args
    tickers = [a.upper() for a in args if not a.startswith('--')]

    if not tickers:
        print("Error: no ticker symbols provided.")
        sys.exit(1)

    db.init_db()

    all_data    = {}   # ticker -> raw data dict
    all_results = {}   # ticker -> analyzer result dict

    for ticker in tickers:
        print(f"Fetching {ticker}...", end=' ', flush=True)

        if offline:
            data = db.fetch_all(ticker)
            if not data['income'] and not data['balance']:
                print("no cached data.")
                continue
            print("(cached)")
        else:
            try:
                data = fetcher.fetch_and_store(ticker)
                print("done.")
            except Exception as e:
                print(f"error: {e}")
                continue

        result = analyzer.compute_ratios(data)
        all_data[ticker]    = data
        all_results[ticker] = result

        # Single-ticker detailed view
        display.print_analysis(ticker, result)

        if do_export:
            fin_path, ratio_path = exporter.export_ticker(ticker, data, result)
            print(f"  Exported: {os.path.relpath(fin_path)}")
            print(f"            {os.path.relpath(ratio_path)}")

    # Multi-ticker comparison
    if len(all_results) > 1:
        display.print_comparison(list(all_results.keys()), all_results)

        if do_export:
            comp_path = exporter.export_comparison(list(all_results.keys()), all_results)
            print(f"  Comparison exported: {os.path.relpath(comp_path)}")


if __name__ == '__main__':
    main()
