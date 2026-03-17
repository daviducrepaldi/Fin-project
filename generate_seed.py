#!/usr/bin/env python3
"""
Run locally to populate data/ with pre-fetched JSON files:
    python3 generate_seed.py
    python3 generate_seed.py AAPL MSFT   # specific tickers only

Commit data/ to the repo so Streamlit Cloud has static data on cold starts.
"""
import json
import math
import os
import sys
from src import db, fetcher

DEFAULT_TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'JPM', 'META', 'NVDA']
DATA_DIR = 'data'


def _clean(obj):
    """Recursively replace NaN/Inf with None for JSON serialisation."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    return obj


def _filter_nulls(data):
    """
    Drop rows where the primary field is null — these are incomplete quarters
    from yfinance that would break TTM and YoY calculations.
    """
    data['income']   = [r for r in data['income']   if r.get('revenue')      is not None]
    data['balance']  = [r for r in data['balance']  if r.get('total_assets') is not None]
    data['cashflow'] = [r for r in data['cashflow'] if r.get('operating_cf') is not None]
    return data


os.makedirs(DATA_DIR, exist_ok=True)
db.init_db()

tickers = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_TICKERS

for ticker in tickers:
    ticker = ticker.upper()
    print(f"Fetching {ticker}…", flush=True)
    try:
        data = fetcher.fetch_and_store(ticker)
        data = _filter_nulls(_clean(data))
        path = os.path.join(DATA_DIR, f'{ticker}.json')
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"  ✓ {len(data['income'])}Q income  "
              f"{len(data['balance'])}Q balance  "
              f"{len(data['cashflow'])}Q cashflow  → {path}")
    except Exception as e:
        print(f"  ✗ {ticker}: {e}")

print(f"\nDone. Regenerate anytime with: python3 generate_seed.py")
