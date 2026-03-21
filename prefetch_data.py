#!/usr/bin/env python3
"""
Pre-fetch financial data for a list of tickers and save as static JSON files.

Usage:
    python prefetch_data.py --tickers AAPL MSFT GOOGL
    python prefetch_data.py  # uses DEFAULT_TICKERS
"""
import argparse
import json
import sys
import time
from pathlib import Path

DEFAULT_TICKERS = ['AAPL', 'AMZN', 'GOOGL', 'JPM', 'META', 'MSFT', 'NVDA', 'TSLA']
DATA_DIR = Path(__file__).parent / 'data'


def main():
    parser = argparse.ArgumentParser(description="Pre-fetch yfinance data to static JSON files.")
    parser.add_argument(
        '--tickers', nargs='+', metavar='TICKER', type=str.upper,
        default=DEFAULT_TICKERS,
        help='Space-separated list of tickers to fetch (default: all pre-loaded tickers)',
    )
    args = parser.parse_args()

    from src import fetcher
    from src.utils import clean_for_json

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Saving JSON files to: {DATA_DIR.resolve()}\n")

    ok, failed = [], []

    for i, ticker in enumerate(args.tickers):
        if i > 0:
            time.sleep(4)  # avoid rate-limiting between requests

        print(f"[{i+1}/{len(args.tickers)}] Fetching {ticker}...", end=' ', flush=True)
        try:
            data = fetcher.fetch_and_store(ticker)
            path = DATA_DIR / f'{ticker}.json'
            with open(path, 'w') as f:
                json.dump(clean_for_json(data), f, indent=2)
            print(f"saved → {path.resolve()}")
            ok.append(ticker)
        except Exception as e:
            print(f"FAILED: {e}")
            failed.append(ticker)

    print(f"\nDone. {len(ok)} succeeded, {len(failed)} failed.")
    if failed:
        print(f"Failed tickers: {', '.join(failed)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
