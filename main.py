#!/usr/bin/env python3
import argparse
import os
import re
from src import db, fetcher, analyzer, display, exporter

# Same shape enforced by the web app: tickers end up in API URL paths and in
# export filenames, so anything else (slashes, "..") must never get through.
TICKER_RE = re.compile(r'^[A-Z]{1,10}([.-][A-Z]{1,4})?$')


def main():
    parser = argparse.ArgumentParser(description="Financial Statement Analyzer")
    parser.add_argument('tickers', nargs='+', metavar='TICKER', type=str.upper)
    parser.add_argument('--offline', action='store_true', help='Use cached SQLite data only')
    parser.add_argument('--export',  action='store_true', help='Export results to CSV')
    args = parser.parse_args()
    offline, do_export = args.offline, args.export

    tickers = [t for t in args.tickers if TICKER_RE.match(t)]
    for bad in [t for t in args.tickers if not TICKER_RE.match(t)]:
        print(f"Skipping invalid ticker: {bad!r}")

    db.init_db()

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
